use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};

use crossbeam_epoch::{Atomic, Guard, Owned};
use crossbeam_utils::Backoff;
use inetnum::addr::Prefix;
use log::{debug, log_enabled, trace};
use roaring::RoaringBitmap;

use crate::RouteStatus;
use crate::{
    cht::{bits_for_len, Cht, OnceBoxSlice, Value},
    rib::starcast_af::UpsertReport,
    types::{
        errors::PrefixStoreError, prefix_record::PublicRecord, AddressFamily,
        PrefixId,
    },
    Meta,
};

// ----------- MultiMap ------------------------------------------------------
// This is the record that holds the aggregates at the top-level for a given
// prefix.

#[derive(Debug)]
pub struct MultiMap<M: Meta>(
    Arc<Mutex<std::collections::HashMap<u32, MultiMapValue<M>>>>,
);

impl<M: Send + Sync + Debug + Display + Meta> MultiMap<M> {
    pub(crate) fn new(record_map: HashMap<u32, MultiMapValue<M>>) -> Self {
        Self(Arc::new(Mutex::new(record_map)))
    }

    fn guard_with_retry(
        &self,
        mut retry_count: usize,
    ) -> (MutexGuard<HashMap<u32, MultiMapValue<M>>>, usize) {
        let backoff = Backoff::new();

        loop {
            if let Ok(guard) = self.0.try_lock() {
                return (guard, retry_count);
            }

            backoff.spin();
            retry_count += 1;
        }
    }

    pub fn _len(&self) -> usize {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();
        record_map.len()
    }

    pub fn get_record_for_mui(
        &self,
        mui: u32,
        include_withdrawn: bool,
    ) -> Option<PublicRecord<M>> {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();

        record_map
            .get(&mui)
            .and_then(|r| -> Option<PublicRecord<M>> {
                if include_withdrawn
                    || r.route_status() == RouteStatus::Active
                {
                    Some(PublicRecord::from((mui, r)))
                } else {
                    None
                }
            })
    }

    pub fn best_backup(&self, tbi: M::TBI) -> (Option<u32>, Option<u32>) {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();
        let ord_routes = record_map
            .iter()
            .map(|r| (r.1.meta().as_orderable(tbi), *r.0));
        let (best, bckup) =
            routecore::bgp::path_selection::best_backup_generic(ord_routes);
        (best.map(|b| b.1), bckup.map(|b| b.1))
    }

    pub(crate) fn get_record_for_mui_with_rewritten_status(
        &self,
        mui: u32,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Option<PublicRecord<M>> {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();
        record_map.get(&mui).map(|r| {
            // We'll return a cloned record: the record in the store remains
            // untouched.
            let mut r = r.clone();
            if bmin.contains(mui) {
                r.set_route_status(rewrite_status);
            }
            PublicRecord::from((mui, &r))
        })
    }

    pub fn get_filtered_record_for_mui(
        &self,
        mui: u32,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<PublicRecord<M>> {
        match include_withdrawn {
            false => self.get_record_for_mui(mui, include_withdrawn),
            true => self.get_record_for_mui_with_rewritten_status(
                mui,
                bmin,
                RouteStatus::Withdrawn,
            ),
        }
    }

    // Helper to filter out records that are not-active (Inactive or
    // Withdrawn), or whose mui appears in the global withdrawn index.
    pub fn get_filtered_records(
        &self,
        mui: Option<u32>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Vec<PublicRecord<M>>> {
        if let Some(mui) = mui {
            self.get_filtered_record_for_mui(mui, include_withdrawn, bmin)
                .map(|r| vec![r])
        } else {
            match include_withdrawn {
                false => {
                    let recs = self.as_active_records_not_in_bmin(bmin);
                    if recs.is_empty() {
                        None
                    } else {
                        Some(recs)
                    }
                }
                true => {
                    let recs = self.as_records_with_rewritten_status(
                        bmin,
                        RouteStatus::Withdrawn,
                    );
                    if recs.is_empty() {
                        None
                    } else {
                        Some(recs)
                    }
                }
            }
        }
    }

    // return all records regardless of their local status, or any globally
    // set status for the mui of the record. However, the local status for a
    // record whose mui appears in the specified bitmap index, will be
    // rewritten with the specified RouteStatus.
    pub fn as_records_with_rewritten_status(
        &self,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Vec<PublicRecord<M>> {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();
        record_map
            .iter()
            .map(move |r| {
                let mut rec = r.1.clone();
                if bmin.contains(*r.0) {
                    rec.set_route_status(rewrite_status);
                }
                PublicRecord::from((*r.0, &rec))
            })
            .collect::<Vec<_>>()
    }

    pub fn _as_records(&self) -> Vec<PublicRecord<M>> {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();
        record_map
            .iter()
            .map(|r| PublicRecord::from((*r.0, r.1)))
            .collect::<Vec<_>>()
    }

    // Returns a vec of records whose keys are not in the supplied bitmap
    // index, and whose local Status is set to Active. Used to filter out
    // withdrawn routes.
    pub fn as_active_records_not_in_bmin(
        &self,
        bmin: &RoaringBitmap,
    ) -> Vec<PublicRecord<M>> {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();
        record_map
            .iter()
            .filter_map(|r| {
                if r.1.route_status() == RouteStatus::Active
                    && !bmin.contains(*r.0)
                {
                    Some(PublicRecord::from((*r.0, r.1)))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    // Change the local status of the record for this mui to Withdrawn.
    pub fn mark_as_withdrawn_for_mui(&self, mui: u32, ltime: u64) {
        let c_map = Arc::clone(&self.0);
        let mut record_map = c_map.lock().unwrap();
        if let Some(rec) = record_map.get_mut(&mui) {
            rec.set_route_status(RouteStatus::Withdrawn);
            rec.set_logical_time(ltime);
        }
    }

    // Change the local status of the record for this mui to Active.
    pub fn mark_as_active_for_mui(&self, mui: u32, ltime: u64) {
        let record_map = Arc::clone(&self.0);
        let mut r_map = record_map.lock().unwrap();
        if let Some(rec) = r_map.get_mut(&mui) {
            rec.set_route_status(RouteStatus::Active);
            rec.set_logical_time(ltime);
        }
    }

    // Insert or replace the PublicRecord in the HashMap for the key of
    // record.multi_uniq_id. Returns the number of entries in the HashMap
    // after updating it, if it's more than 1. Returns None if this is the
    // first entry.
    #[allow(clippy::type_complexity)]
    pub(crate) fn upsert_record(
        &self,
        new_rec: PublicRecord<M>,
    ) -> Result<(Option<(MultiMapValue<M>, usize)>, usize), PrefixStoreError>
    {
        let (mut record_map, retry_count) = self.guard_with_retry(0);
        let key = new_rec.multi_uniq_id;

        match record_map.contains_key(&key) {
            true => {
                let old_rec = record_map
                    .insert(key, MultiMapValue::from(new_rec))
                    .map(|r| (r, record_map.len()));
                Ok((old_rec, retry_count))
            }
            false => {
                let new_rec = MultiMapValue::from(new_rec);
                let old_rec = record_map.insert(key, new_rec);
                assert!(old_rec.is_none());
                Ok((None, retry_count))
            }
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct MultiMapValue<M> {
    meta: M,
    ltime: u64,
    route_status: RouteStatus,
}

impl<M: Meta> MultiMapValue<M> {
    pub(crate) fn logical_time(&self) -> u64 {
        self.ltime
    }

    pub(crate) fn set_logical_time(&mut self, ltime: u64) {
        self.ltime = ltime;
    }

    pub(crate) fn meta(&self) -> &M {
        &self.meta
    }

    pub(crate) fn route_status(&self) -> RouteStatus {
        self.route_status
    }

    pub(crate) fn set_route_status(&mut self, status: RouteStatus) {
        self.route_status = status;
    }
}

impl<M: Meta> std::fmt::Display for MultiMapValue<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            // self.meta(),
            self.logical_time(),
            self.route_status()
        )
    }
}

impl<M: Meta> From<PublicRecord<M>> for MultiMapValue<M> {
    fn from(value: PublicRecord<M>) -> Self {
        Self {
            ltime: value.ltime,
            route_status: value.status,
            meta: value.meta,
        }
    }
}

impl<M: Meta> From<(u32, &MultiMapValue<M>)> for PublicRecord<M> {
    fn from(value: (u32, &MultiMapValue<M>)) -> Self {
        Self {
            multi_uniq_id: value.0,
            meta: value.1.meta().clone(),
            ltime: value.1.ltime,
            status: value.1.route_status,
        }
    }
}

impl<M: Meta> Clone for MultiMap<M> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

// ----------- Prefix related structs ---------------------------------------

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PathSelections {
    pub(crate) path_selection_muis: (Option<u32>, Option<u32>),
}

impl PathSelections {
    pub fn best(&self) -> Option<u32> {
        self.path_selection_muis.0
    }

    pub fn _backup(&self) -> Option<u32> {
        self.path_selection_muis.1
    }
}
// ----------- StoredPrefix -------------------------------------------------
// This is the top-level struct that's linked from the slots in the buckets.
// It contains a super_agg_record that is supposed to hold counters for the
// records that are stored inside it, so that iterators over its linked lists
// don't have to go into them if there's nothing there and could stop early.
#[derive(Debug)]
pub struct StoredPrefix<AF: AddressFamily, M: Meta> {
    // the serial number
    // pub serial: usize,
    // the prefix itself,
    pub prefix: PrefixId<AF>,
    // the aggregated data for this prefix
    pub record_map: MultiMap<M>,
    // (mui of best path entry, mui of backup path entry) from the record_map
    path_selections: Atomic<PathSelections>,
    // the reference to the next set of records for this prefix, if any.
    pub next_bucket: PrefixSet<AF, M>,
}

impl<AF: AddressFamily, M: Meta> StoredPrefix<AF, M> {
    pub(crate) fn new(pfx_id: PrefixId<AF>, level: u8) -> Self {
        // start calculation size of next set, it's dependent on the level
        // we're in.
        // let pfx_id = PrefixId::new(record.net, record.len);
        let this_level = bits_for_len(pfx_id.get_len(), level);
        let next_level = bits_for_len(pfx_id.get_len(), level + 1);

        trace!("this level {} next level {}", this_level, next_level);
        let next_bucket: PrefixSet<AF, M> = if next_level > 0 {
            debug!(
                "{} store: INSERT with new bucket of size {} at prefix len {}",
                std::thread::current().name().unwrap_or("unnamed-thread"),
                1 << (next_level - this_level),
                pfx_id.get_len()
            );
            PrefixSet::init_with_p2_children(
                next_level.saturating_sub(this_level) as usize,
            )
        } else {
            debug!(
                "{} store: INSERT at LAST LEVEL with empty bucket at prefix len {}",
                std::thread::current().name().unwrap_or("unnamed-thread"),
                pfx_id.get_len()
            );
            PrefixSet::init_with_p2_children(
                next_level.saturating_sub(this_level) as usize,
            )
        };
        // End of calculation

        let rec_map = HashMap::new();

        StoredPrefix {
            // serial: 1,
            prefix: pfx_id,
            path_selections: Atomic::init(PathSelections {
                path_selection_muis: (None, None),
            }),
            record_map: MultiMap::new(rec_map),
            next_bucket,
        }
    }

    pub(crate) fn get_prefix_id(&self) -> PrefixId<AF> {
        self.prefix
    }

    pub fn get_path_selections(&self, guard: &Guard) -> PathSelections {
        let path_selections =
            self.path_selections.load(Ordering::Acquire, guard);

        unsafe { path_selections.as_ref() }.map_or(
            PathSelections {
                path_selection_muis: (None, None),
            },
            |ps| *ps,
        )
    }

    pub(crate) fn set_path_selections(
        &self,
        path_selections: PathSelections,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        let current = self.path_selections.load(Ordering::SeqCst, guard);

        if unsafe { current.as_ref() } == Some(&path_selections) {
            debug!("unchanged path_selections");
            return Ok(());
        }

        self.path_selections
            .compare_exchange(
                current,
                // Set the tag to indicate we're updated
                Owned::new(path_selections).with_tag(0),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            )
            .map_err(|_| PrefixStoreError::PathSelectionOutdated)?;
        Ok(())
    }

    pub fn set_ps_outdated(
        &self,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        self.path_selections
            .fetch_update(Ordering::Acquire, Ordering::Acquire, guard, |p| {
                Some(p.with_tag(1))
            })
            .map(|_| ())
            .map_err(|_| PrefixStoreError::StoreNotReadyError)
    }

    pub fn is_ps_outdated(&self, guard: &Guard) -> bool {
        self.path_selections.load(Ordering::Acquire, guard).tag() == 1
    }

    pub fn calculate_and_store_best_backup<'a>(
        &'a self,
        tbi: &M::TBI,
        guard: &'a Guard,
    ) -> Result<(Option<u32>, Option<u32>), PrefixStoreError> {
        let path_selection_muis = self.record_map.best_backup(*tbi);

        self.set_path_selections(
            PathSelections {
                path_selection_muis,
            },
            guard,
        )?;

        Ok(path_selection_muis)
    }
}
//------------ PrefixSet ----------------------------------------------------

// The PrefixSet is the ARRAY that holds all the child prefixes in a node.
// Since we are storing these prefixes in the global store in a HashMap that
// is keyed on the tuple (addr_bits, len, serial number) we can get away with
// storing ONLY THE SERIAL NUMBER in the pfx_vec: The addr_bits and len are
// implied in the position in the array a serial number has. A PrefixSet
// doesn't know anything about the node it is contained in, so it needs a base
// address to be able to calculate the complete prefix of a child prefix.

#[derive(Debug)]
#[repr(align(8))]
pub struct PrefixSet<AF: AddressFamily, M: Meta>(
    pub OnceBoxSlice<StoredPrefix<AF, M>>,
);

impl<AF: AddressFamily, M: Meta> Value for PrefixSet<AF, M> {
    fn init_with_p2_children(p2_size: usize) -> Self {
        let size = if p2_size == 0 { 0 } else { 1 << p2_size };
        PrefixSet(OnceBoxSlice::new(size))
    }
    fn init_leaf() -> Self {
        PrefixSet(OnceBoxSlice::new(0))
    }
}

#[derive(Debug)]
pub(crate) struct PrefixCht<
    AF: AddressFamily,
    M: Meta,
    const ROOT_SIZE: usize,
>(Cht<PrefixSet<AF, M>, ROOT_SIZE, 1>);

impl<AF: AddressFamily, M: Meta, const ROOT_SIZE: usize>
    PrefixCht<AF, M, ROOT_SIZE>
{
    pub(crate) fn init() -> Self {
        Self(<Cht<PrefixSet<AF, M>, ROOT_SIZE, 1>>::init())
    }

    pub(crate) fn get_records_for_prefix(
        &self,
        prefix: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Vec<PublicRecord<M>>> {
        let mut prefix_set = self.0.root_for_len(prefix.get_len());
        let mut level: u8 = 0;
        let backoff = Backoff::new();

        loop {
            // The index of the prefix in this array (at this len and
            // level) is calculated by performing the hash function
            // over the prefix.

            // HASHING FUNCTION
            let index = Self::hash_prefix_id(prefix, level);

            if let Some(stored_prefix) = prefix_set.0.get(index) {
                if prefix == stored_prefix.get_prefix_id() {
                    if log_enabled!(log::Level::Trace) {
                        trace!(
                            "found requested prefix {} ({:?})",
                            Prefix::from(prefix),
                            prefix
                        );
                    }

                    return stored_prefix.record_map.get_filtered_records(
                        mui,
                        include_withdrawn,
                        bmin,
                    );
                };

                // Advance to the next level.
                prefix_set = &stored_prefix.next_bucket;
                level += 1;
                backoff.spin();
                continue;
            }

            trace!("no prefix found for {:?}", prefix);
            return None;
        }
    }

    pub(crate) fn upsert_prefix(
        &self,
        prefix: PrefixId<AF>,
        record: PublicRecord<M>,
        update_path_selections: Option<M::TBI>,
        guard: &Guard,
    ) -> Result<(UpsertReport, Option<MultiMapValue<M>>), PrefixStoreError>
    {
        let mut prefix_is_new = true;
        let mut mui_is_new = true;

        let (mui_count, cas_count) =
            match self.non_recursive_retrieve_prefix_mut(prefix) {
                // There's no StoredPrefix at this location yet. Create a new
                // PrefixRecord and try to store it in the empty slot.
                (stored_prefix, false) => {
                    if log_enabled!(log::Level::Debug) {
                        debug!(
                            "{} store: Create new prefix record",
                            std::thread::current()
                                .name()
                                .unwrap_or("unnamed-thread")
                        );
                    }

                    let (mui_count, retry_count) =
                        stored_prefix.record_map.upsert_record(record)?;

                    // See if someone beat us to creating the record.
                    if mui_count.is_some() {
                        mui_is_new = false;
                        prefix_is_new = false;
                    }

                    (mui_count, retry_count)
                }
                // There already is a StoredPrefix with a record at this
                // location.
                (stored_prefix, true) => {
                    if log_enabled!(log::Level::Debug) {
                        debug!(
                        "{} store: Found existing prefix record for {}/{}",
                        std::thread::current()
                            .name()
                            .unwrap_or("unnamed-thread"),
                        prefix.get_net(),
                        prefix.get_len()
                    );
                    }
                    prefix_is_new = false;

                    // Update the already existing record_map with our
                    // caller's record.
                    stored_prefix.set_ps_outdated(guard)?;

                    let (mui_count, retry_count) =
                        stored_prefix.record_map.upsert_record(record)?;
                    mui_is_new = mui_count.is_none();

                    if let Some(tbi) = update_path_selections {
                        stored_prefix
                            .calculate_and_store_best_backup(&tbi, guard)?;
                    }

                    (mui_count, retry_count)
                }
            };

        let count = mui_count.as_ref().map(|m| m.1).unwrap_or(1);
        Ok((
            UpsertReport {
                prefix_new: prefix_is_new,
                cas_count,
                mui_new: mui_is_new,
                mui_count: count,
            },
            mui_count.map(|m| m.0),
        ))
    }
    // This function is used by the upsert_prefix function above.
    //
    // We're using a Chained Hash Table and this function returns one of:
    // - a StoredPrefix that already exists for this search_prefix_id
    // - the Last StoredPrefix in the chain.
    // - an error, if no StoredPrefix whatsoever can be found in the store.
    //
    // The error condition really shouldn't happen, because that basically
    // means the root node for that particular prefix length doesn't exist.
    pub(crate) fn non_recursive_retrieve_prefix_mut(
        &self,
        search_prefix_id: PrefixId<AF>,
    ) -> (&StoredPrefix<AF, M>, bool) {
        trace!("non_recursive_retrieve_prefix_mut_with_guard");
        let mut prefix_set = self.0.root_for_len(search_prefix_id.get_len());
        let mut level: u8 = 0;

        trace!("root prefix_set {:?}", prefix_set);
        loop {
            // HASHING FUNCTION
            let index = Self::hash_prefix_id(search_prefix_id, level);

            // probe the slot with the index that's the result of the hashing.
            let stored_prefix = match prefix_set.0.get(index) {
                Some(p) => {
                    trace!("prefix set found.");
                    (p, true)
                }
                None => {
                    // We're at the end of the chain and haven't found our
                    // search_prefix_id anywhere. Return the end-of-the-chain
                    // StoredPrefix, so the caller can attach a new one.
                    trace!(
                        "no record. returning last found record in level
                        {}, with index {}.",
                        level,
                        index
                    );
                    let index = Self::hash_prefix_id(search_prefix_id, level);
                    trace!("calculate next index {}", index);
                    let var_name = (
                        prefix_set
                            .0
                            .get_or_init(index, || {
                                StoredPrefix::new(
                                    PrefixId::new(
                                        search_prefix_id.get_net(),
                                        search_prefix_id.get_len(),
                                    ),
                                    level,
                                )
                            })
                            .0,
                        false,
                    );
                    var_name
                }
            };

            if search_prefix_id == stored_prefix.0.prefix {
                // GOTCHA!
                // Our search-prefix is stored here, so we're returning
                // it, so its PrefixRecord can be updated by the caller.
                if log_enabled!(log::Level::Trace) {
                    trace!(
                        "found requested prefix {} ({:?})",
                        Prefix::from(search_prefix_id),
                        search_prefix_id
                    );
                }
                return stored_prefix;
            } else {
                // A Collision. Follow the chain.
                level += 1;
                prefix_set = &stored_prefix.0.next_bucket;
                continue;
            }
        }
    }

    // This function is used by the match_prefix, and [more|less]_specifics
    // public methods on the TreeBitMap (indirectly).
    #[allow(clippy::type_complexity)]
    pub fn non_recursive_retrieve_prefix(
        &self,
        id: PrefixId<AF>,
    ) -> (
        Option<&StoredPrefix<AF, M>>,
        Option<(
            PrefixId<AF>,
            u8,
            &PrefixSet<AF, M>,
            [Option<(&PrefixSet<AF, M>, usize)>; 32],
            usize,
        )>,
    ) {
        let mut prefix_set = self.0.root_for_len(id.get_len());
        let mut parents = [None; 32];
        let mut level: u8 = 0;
        let backoff = Backoff::new();

        loop {
            // The index of the prefix in this array (at this len and
            // level) is calculated by performing the hash function
            // over the prefix.

            // HASHING FUNCTION
            let index = Self::hash_prefix_id(id, level);

            if let Some(stored_prefix) = prefix_set.0.get(index) {
                if id == stored_prefix.get_prefix_id() {
                    if log_enabled!(log::Level::Trace) {
                        trace!(
                            "found requested prefix {} ({:?})",
                            Prefix::from(id),
                            id
                        );
                    }
                    parents[level as usize] = Some((prefix_set, index));
                    return (
                        Some(stored_prefix),
                        Some((id, level, prefix_set, parents, index)),
                    );
                };

                // Advance to the next level.
                prefix_set = &stored_prefix.next_bucket;
                level += 1;
                backoff.spin();
                continue;
            }

            trace!("no prefix found for {:?}", id);
            parents[level as usize] = Some((prefix_set, index));
            return (None, Some((id, level, prefix_set, parents, index)));
        }
    }

    pub(crate) fn hash_prefix_id(id: PrefixId<AF>, level: u8) -> usize {
        // And, this is all of our hashing function.
        let last_level = if level > 0 {
            bits_for_len(id.get_len(), level - 1)
        } else {
            0
        };
        let this_level = bits_for_len(id.get_len(), level);
        // trace!(
        //     "bits division {}; no of bits {}",
        //     this_level,
        //     this_level - last_level
        // );
        // trace!(
        //     "calculated index ({} << {}) >> {}",
        //     id.get_net(),
        //     last_level,
        //     ((<AF>::BITS - (this_level - last_level)) % <AF>::BITS) as usize
        // );
        // HASHING FUNCTION
        ((id.get_net() << AF::from_u32(last_level as u32))
            >> AF::from_u8(
                (<AF>::BITS - (this_level - last_level)) % <AF>::BITS,
            ))
        .dangerously_truncate_to_u32() as usize
    }
}
