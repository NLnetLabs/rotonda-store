use std::fmt::Debug;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};

use crossbeam_epoch::{Atomic, Guard, Owned};
use crossbeam_utils::Backoff;
use inetnum::addr::Prefix;
use log::{debug, log_enabled, trace};
use roaring::RoaringBitmap;

use crate::cht::{nodeset_size, prev_node_size};
use crate::errors::{FatalError, FatalResult};
use crate::prefix_record::Meta;
use crate::stats::{Counters, UpsertReport};
#[cfg(test)]
use crate::test_types::NoMeta;
use crate::types::RouteStatus;
#[cfg(test)]
use crate::IPv6;
use crate::{
    cht::{Cht, OnceBoxSlice, Value},
    types::{
        errors::PrefixStoreError, prefix_record::Record, AddressFamily,
        PrefixId,
    },
};

use crate::prefix_cht::map_type::{MapType, RecordKey};

// ----------- Prefix related structs ---------------------------------------

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct PathSelections<RK: RecordKey> {
    pub(crate) path_selection_muis: (Option<RK>, Option<RK>),
}

impl<RK: RecordKey> PathSelections<RK> {
    pub fn best(&self) -> Option<RK> {
        self.path_selection_muis.0
    }

    pub fn _backup(&self) -> Option<RK> {
        self.path_selection_muis.1
    }
}

// ----------- StoredPrefix -------------------------------------------------
// This is the top-level struct that's linked from the slots in the buckets.
// It contains a super_agg_record that is supposed to hold counters for the
// records that are stored inside it, so that iterators over its linked lists
// don't have to go into them if there's nothing there and could stop early.
#[derive(Debug)]
pub struct StoredPrefix<AF: AddressFamily, M: Meta, MT: MapType<M>> {
    // the prefix itself,
    pub prefix: PrefixId<AF>,
    // the aggregated data for this prefix
    pub record_map: Arc<Mutex<MT>>,
    // (mui of best path entry, mui of backup path entry) from the record_map
    path_selections: Atomic<PathSelections<MT::Key>>,
    // the reference to the next set of records for this prefix, if any.
    pub next_bucket: PrefixSet<AF, M, MT>,
}

impl<AF: AddressFamily, M: Meta, MT: MapType<M>> StoredPrefix<AF, M, MT> {
    pub(crate) fn new(pfx_id: PrefixId<AF>, level: u8) -> Self {
        // start calculation size of next set, it's dependent on the level
        // we're in.
        // let pfx_id = PrefixId::new(record.net, record.len);
        // let this_level = bits_for_len(pfx_id.get_len(), level);
        let next_level = nodeset_size(pfx_id.len(), level + 1);

        trace!("next level {}", next_level);
        let next_bucket: PrefixSet<AF, M, MT> = if next_level > 0 {
            debug!(
                "{} store: INSERT with new bucket of size {} at prefix len {}",
                std::thread::current().name().unwrap_or("unnamed-thread"),
                1 << next_level,
                pfx_id.len()
            );
            PrefixSet::init_with_p2_children(next_level as usize)
        } else {
            debug!(
                "{} store: INSERT at LAST LEVEL with empty bucket at prefix len {}",
                std::thread::current().name().unwrap_or("unnamed-thread"),
                pfx_id.len()
            );
            PrefixSet::init_with_p2_children(next_level as usize)
        };
        // End of calculation

        // let rec_map = MT::new();

        StoredPrefix {
            prefix: pfx_id,
            path_selections: Atomic::init(PathSelections {
                path_selection_muis: (None, None),
            }),
            record_map: Arc::new(Mutex::new(MT::new())),
            next_bucket,
        }
    }

    pub(crate) fn get_prefix_id(&self) -> PrefixId<AF> {
        self.prefix
    }

    pub fn get_path_selections(
        &self,
        guard: &Guard,
    ) -> PathSelections<MT::Key> {
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
        path_selections: PathSelections<MT::Key>,
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
    ) -> Result<(Option<MT::Key>, Option<MT::Key>), PrefixStoreError> {
        let rec = self.acquire_read_guard();
        let path_selection_muis = rec.best_backup(*tbi);

        self.set_path_selections(
            PathSelections {
                path_selection_muis,
            },
            guard,
        )?;

        Ok(path_selection_muis)
    }

    pub(crate) fn acquire_read_guard(&self) -> MutexGuard<MT> {
        let backoff = Backoff::new();

        loop {
            if let Ok(guard) = self.record_map.try_lock() {
                return guard;
            }

            backoff.spin();
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn acquire_write_lock(
        &self,
    ) -> FatalResult<(MutexGuard<MT>, usize)> {
        let mut retry_count: usize = 0;
        let backoff = Backoff::new();

        loop {
            // We're using lock(), which returns an Error only if another
            // thread has panicked while holding the lock. In that situtation
            // we are certainly not going to write anything.
            if let Ok(guard) = self.record_map.lock().map_err(|_| FatalError)
            {
                return Ok((guard, retry_count));
            }

            backoff.spin();
            retry_count += 1;
        }
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
pub struct PrefixSet<AF: AddressFamily, M: Meta, MT: MapType<M>>(
    pub(crate) OnceBoxSlice<StoredPrefix<AF, M, MT>>,
);

impl<AF: AddressFamily, M: Meta, MT: MapType<M>> Value
    for PrefixSet<AF, M, MT>
{
    fn init_with_p2_children(p2_size: usize) -> Self {
        let size = if p2_size == 0 { 0 } else { 1 << p2_size };
        PrefixSet(OnceBoxSlice::new(size))
    }
}

//------------ PrefixCht -----------------------------------------------------

// PrefixCht is a simple wrapper around Cht. It stores the meta-data for
// in-memeory strategies.

#[derive(Debug)]
pub(crate) struct PrefixCht<
    AF: AddressFamily,
    M: Meta,
    MT: MapType<M>,
    const ROOT_SIZE: usize,
> {
    bush: Cht<PrefixSet<AF, M, MT>, ROOT_SIZE, 1>,
    counters: Counters,
}

impl<AF: AddressFamily, M: Meta, MT: MapType<M>, const ROOT_SIZE: usize>
    PrefixCht<AF, M, MT, ROOT_SIZE>
{
    pub(crate) fn init() -> Self {
        Self {
            bush: <Cht<PrefixSet<AF, M, MT>, ROOT_SIZE, 1>>::init(),
            counters: Counters::default(),
        }
    }

    pub(crate) fn get_records_for_prefix(
        &self,
        prefix: PrefixId<AF>,
        mui: Option<MT::Key>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Vec<Record<MT::Key, M>>> {
        let mut prefix_set = self.bush.root_for_len(prefix.len());
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

                    let record_map = stored_prefix.acquire_read_guard();

                    return record_map.get_filtered_records(
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
        record: Record<MT::Key, M>,
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

                    let (mut record_map, _) = stored_prefix
                        .acquire_write_lock()
                        .map_err(|_| PrefixStoreError::FatalError)?;
                    let (mui_count, retry_count) = record_map
                        .upsert_record(record)
                        .map_err(|_| PrefixStoreError::FatalError)?;

                    // See if someone beat us to creating the record.
                    if mui_count.is_some() {
                        mui_is_new = false;
                        prefix_is_new = false;
                    } else {
                        self.counters.inc_routes_count();
                    }

                    if prefix_is_new {
                        self.counters
                            .inc_prefixes_count(stored_prefix.prefix.len());
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
                        prefix.bits(),
                        prefix.len()
                    );
                    }
                    prefix_is_new = false;

                    // Update the already existing record_map with our
                    // caller's record.
                    stored_prefix.set_ps_outdated(guard)?;

                    let (mut record_map, _) = stored_prefix
                        .acquire_write_lock()
                        .map_err(|_| PrefixStoreError::FatalError)?;
                    let (mui_count, retry_count) = record_map
                        .upsert_record(record)
                        .map_err(|_| PrefixStoreError::FatalError)?;

                    // if the mui is new, we didn't overwrite an existing
                    // route, so that's a new one!
                    if mui_count.is_none() {
                        mui_is_new = true;
                        self.counters.inc_routes_count();
                    };

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
    ) -> (&StoredPrefix<AF, M, MT>, bool) {
        trace!("non_recursive_retrieve_prefix_mut_with_guard");
        let mut prefix_set = self.bush.root_for_len(search_prefix_id.len());
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
                                        search_prefix_id.bits(),
                                        search_prefix_id.len(),
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
    // This method can never run out of levels, since it only continues if it
    // finds occupied child slots. The indexing can therefore not crash.
    #[allow(clippy::indexing_slicing)]
    pub(crate) fn non_recursive_retrieve_prefix(
        &self,
        id: PrefixId<AF>,
    ) -> (
        Option<&StoredPrefix<AF, M, MT>>,
        Option<(
            PrefixId<AF>,
            u8,
            &PrefixSet<AF, M, MT>,
            [Option<(&PrefixSet<AF, M, MT>, usize)>; 32],
            usize,
        )>,
    ) {
        let mut prefix_set = self.bush.root_for_len(id.len());
        let mut parents = [None; 32];
        let mut level: u8 = 0;
        let backoff = Backoff::new();

        loop {
            // The index of the prefix in this array (at this len and
            // level) is calculated by performing the hash function
            // over the prefix.
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

    pub(crate) fn prefixes_count(&self) -> usize {
        self.counters.prefixes_count().iter().sum()
    }

    pub(crate) fn routes_count(&self) -> usize {
        self.counters.nodes_count()
    }

    fn hash_prefix_id(id: PrefixId<AF>, level: u8) -> usize {
        let last_level = prev_node_size(id.len(), level);

        // HASHING FUNCTION
        let size = nodeset_size(id.len(), level);

        // shifting left and right here should never overflow for inputs
        // (NodeId, level) that are valid for IPv4 and IPv6. In release
        // compiles this may NOT be noticable, because the undefined behaviour
        // is most probably the desired behaviour (saturating). But it's UB
        // for a reason, so we should not rely on it, and verify that we are
        // not hitting that behaviour.
        debug_assert!(id.bits().checked_shl(last_level as u32).is_some());
        debug_assert!((id.bits() << AF::from_u32(last_level as u32))
            .checked_shr(u32::from((<AF>::BITS - size) % <AF>::BITS))
            .is_some());

        ((id.bits() << AF::from_u32(last_level as u32))
            >> AF::from_u8((<AF>::BITS - size) % <AF>::BITS))
        .dangerously_truncate_to_u32() as usize
    }

    #[allow(clippy::unwrap_used)]
    #[cfg(test)]
    fn test_valid_range() {
        let ip_addr = std::net::IpAddr::V6(
            "0::".parse::<std::net::Ipv6Addr>().unwrap(),
        );
        for len in 0..128 {
            for lvl in 0..(len / 4) {
                let p_id =
                    PrefixId::<AF>::from(Prefix::new(ip_addr, len).unwrap());
                Self::hash_prefix_id(p_id, lvl);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct MultiMapValue<M> {
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

impl<K, M: Meta> From<Record<K, M>> for MultiMapValue<M> {
    fn from(value: Record<K, M>) -> Self {
        Self {
            ltime: value.ltime,
            route_status: value.status,
            meta: value.meta,
        }
    }
}

#[test]
fn test_hashing_prefix_id_valid_range() {
    use super::mui_multi_map::MuiMultiMap;
    PrefixCht::<IPv6, NoMeta, MuiMultiMap<NoMeta>, 129>::test_valid_range()
}
