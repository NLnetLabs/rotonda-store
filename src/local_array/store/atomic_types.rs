use flurry::HashMap;
use std::{
    fmt::{Debug, Display},
    mem::MaybeUninit,
    sync::atomic::Ordering,
};

use crossbeam_epoch::{self as epoch, Atomic};

use log::{debug, log_enabled, trace};

use epoch::{Guard, Owned};
use roaring::RoaringBitmap;

use crate::local_array::tree::*;
use crate::prefix_record::PublicRecord;
use crate::prelude::Meta;
use crate::AddressFamily;

use super::errors::PrefixStoreError;

// ----------- Node related structs -----------------------------------------

#[derive(Debug)]
pub struct StoredNode<AF, S>
where
    Self: Sized,
    S: Stride,
    AF: AddressFamily,
{
    pub(crate) node_id: StrideNodeId<AF>,
    // The ptrbitarr and pfxbitarr for this node
    pub(crate) node: TreeBitMapNode<AF, S>,
    // Child nodes linked from this node
    pub(crate) node_set: NodeSet<AF, S>,
}

#[allow(clippy::type_complexity)]
#[derive(Debug, Clone)]
pub struct NodeSet<AF: AddressFamily, S: Stride>(
    pub Atomic<[MaybeUninit<Atomic<StoredNode<AF, S>>>]>,
    // A Bitmap index that keeps track of the `multi_uniq_id`s (mui) that are
    // present in value collections in the meta-data tree in the child nodes
    pub Atomic<RoaringBitmap>,
);

impl<AF: AddressFamily, S: Stride> NodeSet<AF, S> {
    pub fn init(size: usize) -> Self {
        if log_enabled!(log::Level::Debug) {
            debug!(
                "{} store: creating space for {} nodes",
                std::thread::current().name().unwrap(),
                &size
            );
        }

        let mut l =
            Owned::<[MaybeUninit<Atomic<StoredNode<AF, S>>>]>::init(size);
        for i in 0..size {
            l[i] = MaybeUninit::new(Atomic::null());
        }
        NodeSet(l.into(), RoaringBitmap::new().into())
    }

    pub fn update_rbm_index(
        &self,
        multi_uniq_id: u32,
        guard: &crate::epoch::Guard,
    ) -> Result<u32, crate::prelude::multi::PrefixStoreError>
    where
        S: crate::local_array::atomic_stride::Stride,
        AF: crate::AddressFamily,
    {
        let mut try_count = 0;

        self.1
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                guard,
                |mut a_rbm_index| {
                    // SAFETY: The rbm_index gets created as an empty
                    // RoaringBitmap at init time of the NodeSet, so it cannot
                    // be a NULL pointer at this point. We're cloning the
                    // loaded value, NOT mutating it, so we don't run into
                    // concurrent write scenarios (which we would if we'd use
                    // `deref_mut()`).
                    let mut rbm_index =
                        unsafe { a_rbm_index.deref() }.clone();
                    rbm_index.insert(multi_uniq_id);

                    a_rbm_index = Atomic::new(rbm_index).load_consume(guard);

                    try_count += 1;
                    Some(a_rbm_index)
                },
            )
            .map_err(|_| {
                crate::prelude::multi::PrefixStoreError::StoreNotReadyError
            })?;

        trace!("Added {} to {:?}", multi_uniq_id, unsafe {
            self.1
                .load(std::sync::atomic::Ordering::SeqCst, guard)
                .as_ref()
        });
        Ok(try_count)
    }

    pub fn remove_from_rbm_index(
        &self,
        multi_uniq_id: u32,
        guard: &crate::epoch::Guard,
    ) -> Result<u32, crate::prelude::multi::PrefixStoreError>
    where
        S: crate::local_array::atomic_stride::Stride,
        AF: crate::AddressFamily,
    {
        let mut try_count = 0;

        self.1
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                guard,
                |mut a_rbm_index| {
                    // SAFETY: The rbm_index gets created as an empty
                    // RoaringBitmap at init time of the NodeSet, so it cannot
                    // be a NULL pointer at this point. We're cloning the
                    // loaded value, NOT mutating it, so we don't run into
                    // concurrent write scenarios (which we would if we'd use
                    // `deref_mut()`).
                    let mut rbm_index =
                        unsafe { a_rbm_index.deref() }.clone();
                    rbm_index.remove(multi_uniq_id);

                    a_rbm_index = Atomic::new(rbm_index).load_consume(guard);

                    try_count += 1;
                    Some(a_rbm_index)
                },
            )
            .map_err(|_| {
                crate::prelude::multi::PrefixStoreError::StoreNotReadyError
            })?;

        trace!("Removed {} to {:?}", multi_uniq_id, unsafe {
            self.1
                .load(std::sync::atomic::Ordering::SeqCst, guard)
                .as_ref()
        });
        Ok(try_count)
    }
}

// ----------- Prefix related structs ---------------------------------------

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct PathSelections {
    // serial: usize,
    pub(crate) path_selection_muis: (Option<u32>, Option<u32>),
}

impl PathSelections {
    pub fn best(&self) -> Option<u32> {
        self.path_selection_muis.0
    }

    pub fn backup(&self) -> Option<u32> {
        self.path_selection_muis.1
    }
}

// ----------- StoredPrefix -------------------------------------------------
// This is the top-level struct that's linked from the slots in the buckets.
// It contains a super_agg_record that is supposed to hold counters for the
// records that are stored inside it, so that iterators over its linked lists
// don't have to go into them if there's nothing there and could stop early.
#[derive(Debug)]
pub struct StoredPrefix<AF: AddressFamily, M: crate::prefix_record::Meta> {
    // the serial number
    // pub serial: usize,
    // the prefix itself,
    pub prefix: PrefixId<AF>,
    // the aggregated data for this prefix
    pub(crate) record_map: MultiMap<M>,
    // (mui of best path entry, mui of backup path entry) from the record_map
    path_selections: Atomic<PathSelections>,
    // the reference to the next set of records for this prefix, if any.
    pub next_bucket: PrefixSet<AF, M>,
}

impl<AF: AddressFamily, M: crate::prefix_record::Meta> StoredPrefix<AF, M> {
    pub(crate) fn new<PB: PrefixBuckets<AF, M>>(
        pfx_id: PrefixId<AF>,
        level: u8,
    ) -> Self {
        // start calculation size of next set, it's dependent on the level
        // we're in.
        // let pfx_id = PrefixId::new(record.net, record.len);
        let this_level = PB::get_bits_for_len(pfx_id.get_len(), level);
        let next_level = PB::get_bits_for_len(pfx_id.get_len(), level + 1);

        trace!("this level {} next level {}", this_level, next_level);
        let next_bucket: PrefixSet<AF, M> = if next_level > 0 {
            debug!(
                "{} store: INSERT with new bucket of size {} at prefix len {}",
                std::thread::current().name().unwrap(),
                1 << (next_level - this_level),
                pfx_id.get_len()
            );
            PrefixSet::init((1 << (next_level - this_level)) as usize)
        } else {
            debug!(
                "{} store: INSERT at LAST LEVEL with empty bucket at prefix len {}",
                std::thread::current().name().unwrap(),
                pfx_id.get_len()
            );
            PrefixSet::empty()
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

    pub(crate) fn _new_with_record<PB: PrefixBuckets<AF, M>>(
        pfx_id: PrefixId<AF>,
        record: PublicRecord<M>,
        level: u8,
    ) -> Self {
        // start calculation size of next set, it's dependent on the level
        // we're in.
        // let pfx_id = PrefixId::new(record.net, record.len);
        let this_level = PB::get_bits_for_len(pfx_id.get_len(), level);
        let next_level = PB::get_bits_for_len(pfx_id.get_len(), level + 1);

        trace!("this level {} next level {}", this_level, next_level);
        let next_bucket: PrefixSet<AF, M> = if next_level > 0 {
            debug!(
                "{} store: INSERT with new bucket of size {} at prefix len {}",
                std::thread::current().name().unwrap(),
                1 << (next_level - this_level),
                pfx_id.get_len()
            );
            PrefixSet::init((1 << (next_level - this_level)) as usize)
        } else {
            debug!(
                "{} store: INSERT at LAST LEVEL with empty bucket at prefix len {}",
                std::thread::current().name().unwrap(),
                pfx_id.get_len()
            );
            PrefixSet::empty()
        };
        // End of calculation

        let rec_map = HashMap::new();
        let mui = record.multi_uniq_id;
        rec_map
            .pin()
            .insert(record.multi_uniq_id, MultiMapValue::from(record));

        StoredPrefix {
            // serial: 1,
            prefix: pfx_id,
            // In a new prefix, the first inserted record will always be the
            // best path
            path_selections: Atomic::new(PathSelections {
                path_selection_muis: (Some(mui), None),
            }),
            record_map: MultiMap::new(rec_map),
            next_bucket,
        }
    }

    pub(crate) fn get_prefix_id(&self) -> PrefixId<AF> {
        self.prefix
    }

    pub(crate) fn get_path_selections(
        &self,
        guard: &Guard,
    ) -> PathSelections {
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

    pub(crate) fn set_ps_outdated(
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

    pub(crate) fn is_ps_outdated(&self, guard: &Guard) -> bool {
        self.path_selections.load(Ordering::Acquire, guard).tag() == 1
    }

    pub(crate) fn calculate_and_store_best_backup<'a>(
        &'a self,
        tbi: &M::TBI,
        guard: &'a Guard,
    ) -> Result<(Option<u32>, Option<u32>), super::errors::PrefixStoreError>
    {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteStatus {
    Active,
    InActive,
    Withdrawn,
}

impl std::fmt::Display for RouteStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteStatus::Active => write!(f, "active"),
            RouteStatus::InActive => write!(f, "inactive"),
            RouteStatus::Withdrawn => write!(f, "withdrawn"),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MultiMapValue<M> {
    pub meta: M,
    pub ltime: u64,
    pub status: RouteStatus,
}

impl<M: Clone> MultiMapValue<M> {
    pub(crate) fn _new(meta: M, ltime: u64, status: RouteStatus) -> Self {
        Self {
            meta,
            ltime,
            status,
        }
    }
}

impl<M: crate::prefix_record::Meta> std::fmt::Display for MultiMapValue<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.meta, self.ltime, self.status)
    }
}

impl<M: Meta> From<PublicRecord<M>> for MultiMapValue<M> {
    fn from(value: PublicRecord<M>) -> Self {
        Self {
            meta: value.meta,
            ltime: value.ltime,
            status: value.status,
        }
    }
}

// ----------- MultiMap ------------------------------------------------------
// This is the record that holds the aggregates at the top-level for a given
// prefix.

#[derive(Debug)]
pub(crate) struct MultiMap<M: Meta>(
    pub(crate) flurry::HashMap<u32, MultiMapValue<M>>,
);

pub struct IdOrderable<T>(pub u32, T);

impl<T: PartialOrd + Eq + PartialEq> PartialOrd for IdOrderable<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.1.partial_cmp(&other.1)
    }
}

impl<T: PartialOrd + Eq + PartialOrd> PartialEq for IdOrderable<T> {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl<M: Send + Sync + Debug + Display + Meta> MultiMap<M> {
    pub fn new(record_map: HashMap<u32, MultiMapValue<M>>) -> Self {
        Self(record_map)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn guard(&self) -> flurry::Guard<'_> {
        self.0.guard()
    }

    pub(crate) fn get_record_for_active_mui(
        &self,
        mui: u32,
    ) -> Option<PublicRecord<M>> {
        self.0.get(&mui, &self.0.guard()).and_then(|r| {
            if r.status == RouteStatus::Active {
                Some(PublicRecord::from((mui, r.clone())))
            } else {
                None
            }
        })
    }

    pub fn best_backup(&self, tbi: M::TBI) -> (Option<u32>, Option<u32>) {
        let flurry_guard = self.guard();
        let ord_routes = self
            .0
            .iter(&flurry_guard)
            .map(|r| (r.0, r.1.meta.as_orderable(tbi)));
        let (best, bckup) =
            routecore::bgp::path_selection::best_backup(ord_routes);
        (best.map(|b| *b.0), bckup.map(|b| *b.0))
    }

    pub(crate) fn get_record_for_mui_with_rewritten_status(
        &self,
        mui: u32,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Option<PublicRecord<M>> {
        self.0.get(&mui, &self.0.guard()).map(|r| {
            let mut r = r.clone();
            if bmin.contains(mui) {
                r.status = rewrite_status;
            }
            PublicRecord::from((mui, r))
        })
    }

    // Helper to filter out records that are not-active (Inactive or
    // Withdrawn), or whose mui appears in the global withdrawn index.
    pub fn get_filtered_records(
        &self,
        mui: Option<u32>,
        bmin: &RoaringBitmap,
    ) -> Vec<PublicRecord<M>> {
        if let Some(mui) = mui {
            self.get_record_for_active_mui(mui).into_iter().collect()
        } else {
            self.as_active_records_not_in_bmin(bmin)
        }
    }

    pub fn _iter_all_records<'a>(
        &'a self,
        guard: &'a flurry::Guard<'a>,
    ) -> impl Iterator<Item = PublicRecord<M>> + 'a {
        self.0
            .iter(guard)
            .map(|r| PublicRecord::from((*r.0, r.1.clone())))
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
        self.0
            .pin()
            .into_iter()
            .map(move |r| {
                let mut rec = r.1.clone();
                if bmin.contains(*r.0) {
                    rec.status = rewrite_status;
                }
                PublicRecord::from((*r.0, rec))
            })
            .collect::<Vec<_>>()
    }

    pub fn as_records(&self) -> Vec<PublicRecord<M>> {
        self.0
            .pin()
            .iter()
            .map(|r| PublicRecord::from((*r.0, r.1.clone())))
            .collect::<Vec<_>>()
    }

    // Returns a vec of records whose keys are not in the supplied bitmap
    // index, and whose local Status is set to Active. Used to filter out
    // withdrawn routes.
    pub fn as_active_records_not_in_bmin(
        &self,
        bmin: &RoaringBitmap,
    ) -> Vec<PublicRecord<M>> {
        self.0
            .pin()
            .iter()
            .filter_map(|r| {
                if r.1.status == RouteStatus::Active && !bmin.contains(*r.0) {
                    Some(PublicRecord::from((*r.0, r.1.clone())))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    // Change the local status of the record for this mui to Withdrawn.
    pub fn mark_as_withdrawn_for_mui(&self, mui: u32) {
        let record_map = self.0.pin();
        if let Some(mut rec) = record_map.get(&mui).cloned() {
            rec.status = RouteStatus::Withdrawn;
            record_map.insert(mui, rec);
        }
    }

    // Change the local status of the record for this mui to Active.
    pub fn mark_as_active_for_mui(&self, mui: u32) {
        let record_map = self.0.pin();
        if let Some(mut rec) = record_map.get(&mui).cloned() {
            rec.status = RouteStatus::Active;
            record_map.insert(mui, rec);
        }
    }

    // Insert or replace the PublicRecord in the HashMap for the key of
    // record.multi_uniq_id. Returns the number of entries in the HashMap
    // after updating it.
    pub fn upsert_record(&self, record: PublicRecord<M>) -> Option<usize> {
        let record_map = self.0.pin();
        let mui_new = record_map
            .insert(record.multi_uniq_id, MultiMapValue::from(record));
        mui_new.map(|_| self.len())
    }
}

// ----------- AtomicStoredPrefix -------------------------------------------
// Unlike StoredNode, we don't need an Empty variant, since we're using
// serial == 0 as the empty value. We're not using an Option here, to
// avoid going outside our atomic procedure.
#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub struct AtomicStoredPrefix<
    AF: AddressFamily,
    M: crate::prefix_record::Meta,
>(pub Atomic<StoredPrefix<AF, M>>);

impl<AF: AddressFamily, Meta: crate::prefix_record::Meta>
    AtomicStoredPrefix<AF, Meta>
{
    pub(crate) fn empty() -> Self {
        AtomicStoredPrefix(Atomic::null())
    }

    // pub(crate) fn is_empty(&self, guard: &Guard) -> bool {
    //     let pfx = self.0.load(Ordering::SeqCst, guard);
    //     pfx.is_null()
    // }

    pub(crate) fn get_stored_prefix<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Option<&'a StoredPrefix<AF, Meta>> {
        let pfx = self.0.load(Ordering::Acquire, guard);
        match pfx.is_null() {
            true => None,
            false => Some(unsafe { pfx.deref() }),
        }
    }

    pub(crate) fn _get_stored_prefix_with_tag<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Option<(&'a StoredPrefix<AF, Meta>, usize)> {
        let pfx = self.0.load(Ordering::Acquire, guard);
        match pfx.is_null() {
            true => None,
            false => Some((unsafe { pfx.deref() }, pfx.tag())),
        }
    }

    pub(crate) fn get_stored_prefix_mut<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Option<&'a StoredPrefix<AF, Meta>> {
        let mut pfx = self.0.load(Ordering::SeqCst, guard);

        match pfx.is_null() {
            true => None,
            false => Some(unsafe { pfx.deref_mut() }),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get_serial(&self) -> usize {
        let guard = &epoch::pin();
        unsafe { self.0.load(Ordering::Acquire, guard).into_owned() }.tag()
    }

    pub(crate) fn get_prefix_id(&self) -> PrefixId<AF> {
        let guard = &epoch::pin();
        match self.get_stored_prefix(guard) {
            None => {
                panic!("AtomicStoredPrefix::get_prefix_id: empty prefix");
            }
            Some(pfx) => pfx.prefix,
        }
    }

    // PrefixSet is an Atomic that might be a null pointer, which is
    // UB! Therefore we keep the prefix record in an Option: If
    // that Option is None, then the PrefixSet is a null pointer and
    // we'll return None
    pub(crate) fn get_next_bucket<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Option<&PrefixSet<AF, Meta>> {
        // let guard = &epoch::pin();
        if let Some(stored_prefix) = self.get_stored_prefix(guard) {
            // if stored_prefix.super_agg_record.is_some() {
            if !&stored_prefix
                .next_bucket
                .0
                .load(Ordering::SeqCst, guard)
                .is_null()
            {
                Some(&stored_prefix.next_bucket)
            } else {
                None
            }
        } else {
            None
        }
    }

}

// ----------- FamilyBuckets Trait ------------------------------------------
//
// Implementations of this trait are done by a proc-macro called
// `stride_sizes`from the `rotonda-macros` crate.

pub trait NodeBuckets<AF: AddressFamily> {
    fn init() -> Self;
    fn len_to_store_bits(len: u8, level: u8) -> u8;
    fn get_stride_sizes(&self) -> &[u8];
    fn get_stride_for_id(&self, id: StrideNodeId<AF>) -> u8;
    fn get_store3(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride3>;
    fn get_store4(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride4>;
    fn get_store5(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride5>;
    fn get_strides_len() -> u8;
    fn get_first_stride_size() -> u8;
}

pub trait PrefixBuckets<AF: AddressFamily, M: Meta>
where
    Self: Sized,
{
    fn init() -> Self;
    fn remove(&mut self, id: PrefixId<AF>) -> Option<M>;
    fn get_root_prefix_set(&self, len: u8) -> &'_ PrefixSet<AF, M>;
    fn get_bits_for_len(len: u8, level: u8) -> u8;
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
    pub Atomic<[MaybeUninit<AtomicStoredPrefix<AF, M>>]>,
);

impl<AF: AddressFamily, M: Meta> PrefixSet<AF, M> {
    pub fn init(size: usize) -> Self {
        let mut l =
            Owned::<[MaybeUninit<AtomicStoredPrefix<AF, M>>]>::init(size);
        trace!("creating space for {} prefixes in prefix_set", &size);
        for i in 0..size {
            l[i] = MaybeUninit::new(AtomicStoredPrefix::empty());
        }
        PrefixSet(l.into())
    }

    // pub fn get_len_recursive(&self) -> usize {
    //     fn recurse_len<AF: AddressFamily, M: crate::prefix_record::Meta>(
    //         start_set: &PrefixSet<AF, M>,
    //     ) -> usize {
    //         let mut size: usize = 0;
    //         let guard = &epoch::pin();
    //         let start_set = start_set.0.load(Ordering::SeqCst, guard);
    //         for p in unsafe { start_set.deref() } {
    //             let pfx = unsafe { p.assume_init_ref() };
    //             if !pfx.is_empty(guard) {
    //                 size += 1;
    //                 trace!(
    //                     "recurse found pfx {:?} cur size {}",
    //                     pfx.get_prefix_id(),
    //                     size
    //                 );
    //                 if let Some(next_bucket) = pfx.get_next_bucket(guard) {
    //                     trace!("found next bucket");
    //                     size += recurse_len(next_bucket);
    //                 }
    //             }
    //         }

    //         size
    //     }

    //     recurse_len(self)
    // }

    pub(crate) fn get_by_index<'a>(
        &'a self,
        index: usize,
        guard: &'a Guard,
    ) -> &'a AtomicStoredPrefix<AF, M> {
        assert!(!self.0.load(Ordering::SeqCst, guard).is_null());
        unsafe {
            self.0.load(Ordering::SeqCst, guard).deref()[index]
                .assume_init_ref()
        }
    }

    pub(crate) fn empty() -> Self {
        PrefixSet(Atomic::null())
    }
}
