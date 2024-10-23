use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::{
    fmt::{Debug, Display},
    sync::atomic::Ordering,
};

use crossbeam_epoch::{self as epoch, Atomic};

use crossbeam_utils::Backoff;
use log::{debug, log_enabled, trace};

use epoch::{Guard, Owned};
use roaring::RoaringBitmap;

use crate::local_array::store::oncebox::OnceBox;
use crate::local_array::tree::*;
use crate::prefix_record::PublicRecord;
use crate::prelude::Meta;
use crate::AddressFamily;

use super::errors::PrefixStoreError;
use super::oncebox::OnceBoxSlice;

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
#[derive(Debug)]
pub struct NodeSet<AF: AddressFamily, S: Stride>(
    pub OnceBoxSlice<StoredNode<AF, S>>,
    // A Bitmap index that keeps track of the `multi_uniq_id`s (mui) that are
    // present in value collections in the meta-data tree in the child nodes
    pub RwLock<RoaringBitmap>,
);

impl<AF: AddressFamily, S: Stride> NodeSet<AF, S> {
    pub fn init(p2_size: u8) -> Self {
        if log_enabled!(log::Level::Debug) {
            debug!(
                "{} store: creating space for {} nodes",
                std::thread::current().name().unwrap(),
                1 << p2_size
            );
        }

        NodeSet(OnceBoxSlice::new(p2_size), RoaringBitmap::new().into())
    }

    pub fn update_rbm_index(
        &self,
        multi_uniq_id: u32,
    ) -> Result<u32, crate::prelude::multi::PrefixStoreError>
    where
        S: crate::local_array::atomic_stride::Stride,
        AF: crate::AddressFamily,
    {
        let try_count = 0;
        let mut rbm = self.1.write().unwrap();
        rbm.insert(multi_uniq_id);

        Ok(try_count)
    }

    pub fn remove_from_rbm_index(
        &self,
        multi_uniq_id: u32,
        _guard: &crate::epoch::Guard,
    ) -> Result<u32, crate::prelude::multi::PrefixStoreError>
    where
        S: crate::local_array::atomic_stride::Stride,
        AF: crate::AddressFamily,
    {
        let try_count = 0;

        let mut rbm = self.1.write().unwrap();
        rbm.remove(multi_uniq_id);

        Ok(try_count)
    }
}

// ----------- Prefix related structs ---------------------------------------

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PathSelections {
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
    pub record_map: MultiMap<M>,
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

    pub(crate) fn get_next_bucket(&self) -> Option<&PrefixSet<AF, M>> {
        if self.next_bucket.is_empty() {
            None
        } else {
            Some(&self.next_bucket)
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
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
pub struct MultiMap<M: Meta>(
    pub(crate) Arc<Mutex<std::collections::HashMap<u32, MultiMapValue<M>>>>,
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

    pub fn len(&self) -> usize {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();
        record_map.len()
    }

    pub fn get_record_for_active_mui(
        &self,
        mui: u32,
    ) -> Option<PublicRecord<M>> {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();

        record_map.get(&mui).and_then(|r| {
            if r.status == RouteStatus::Active {
                Some(PublicRecord::from((mui, r.clone())))
            } else {
                None
            }
        })
    }

    pub fn best_backup(&self, tbi: M::TBI) -> (Option<u32>, Option<u32>) {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();
        let ord_routes =
            record_map.iter().map(|r| (r.1.meta.as_orderable(tbi), r.0));
        let (best, bckup) =
            routecore::bgp::path_selection::best_backup_generic(ord_routes);
        (best.map(|b| *b.1), bckup.map(|b| *b.1))
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
                    rec.status = rewrite_status;
                }
                PublicRecord::from((*r.0, rec))
            })
            .collect::<Vec<_>>()
    }

    pub fn as_records(&self) -> Vec<PublicRecord<M>> {
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();
        record_map
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
        let c_map = Arc::clone(&self.0);
        let record_map = c_map.lock().unwrap();
        record_map
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
        let c_map = Arc::clone(&self.0);
        let mut record_map = c_map.lock().unwrap();
        if let Some(rec) = record_map.get_mut(&mui) {
            rec.status = RouteStatus::Withdrawn;
            // record_map.insert(mui, rec);
        }
    }

    // Change the local status of the record for this mui to Active.
    pub fn mark_as_active_for_mui(&self, mui: u32) {
        let record_map = Arc::clone(&self.0);
        let mut r_map = record_map.lock().unwrap();
        if let Some(rec) = r_map.get_mut(&mui) {
            rec.status = RouteStatus::Active;
            // r_map.insert(mui, rec);
        }
    }

    // Insert or replace the PublicRecord in the HashMap for the key of
    // record.multi_uniq_id. Returns the number of entries in the HashMap
    // after updating it, if it's more than 1. Returns None if this is the
    // first entry.
    pub fn upsert_record(
        &self,
        record: PublicRecord<M>,
    ) -> (Option<usize>, usize) {
        let c_map = self.clone();
        let (mut record_map, retry_count) = c_map.guard_with_retry(0);

        if record_map
            .insert(record.multi_uniq_id, MultiMapValue::from(record))
            .is_some()
        {
            (Some(record_map.len()), retry_count)
        } else {
            (None, retry_count)
        }
    }
}

impl<M: Meta> Clone for MultiMap<M> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

// ----------- AtomicStoredPrefix -------------------------------------------
// Unlike StoredNode, we don't need an Empty variant, since we're using
// serial == 0 as the empty value. We're not using an Option here, to
// avoid going outside our atomic procedure.
// #[allow(clippy::type_complexity)]
// #[derive(Debug)]
// pub struct AtomicStoredPrefix<
//     AF: AddressFamily,
//     M: crate::prefix_record::Meta,
// >(pub Atomic<StoredPrefix<AF, M>>);

// impl<AF: AddressFamily, Meta: crate::prefix_record::Meta>
//     AtomicStoredPrefix<AF, Meta>
// {
//     pub(crate) fn empty() -> Self {
//         AtomicStoredPrefix(Atomic::null())
//     }

//     // pub(crate) fn is_empty(&self, guard: &Guard) -> bool {
//     //     let pfx = self.0.load(Ordering::SeqCst, guard);
//     //     pfx.is_null()
//     // }

//     pub(crate) fn get_stored_prefix<'a>(
//         &'a self,
//         guard: &'a Guard,
//     ) -> Option<&'a StoredPrefix<AF, Meta>> {
//         let pfx = self.0.load(Ordering::Acquire, guard);
//         match pfx.is_null() {
//             true => None,
//             false => Some(unsafe { pfx.deref() }),
//         }
//     }

//     pub(crate) fn _get_stored_prefix_with_tag<'a>(
//         &'a self,
//         guard: &'a Guard,
//     ) -> Option<(&'a StoredPrefix<AF, Meta>, usize)> {
//         let pfx = self.0.load(Ordering::Acquire, guard);
//         match pfx.is_null() {
//             true => None,
//             false => Some((unsafe { pfx.deref() }, pfx.tag())),
//         }
//     }

//     pub(crate) fn get_stored_prefix_mut<'a>(
//         &'a self,
//         guard: &'a Guard,
//     ) -> Option<&'a StoredPrefix<AF, Meta>> {
//         let pfx = self.0.load(Ordering::SeqCst, guard);

//         match pfx.is_null() {
//             true => None,
//             false => Some(unsafe { pfx.deref() }),
//         }
//     }

//     #[allow(dead_code)]
//     pub(crate) fn get_serial(&self) -> usize {
//         let guard = &epoch::pin();
//         unsafe { self.0.load(Ordering::Acquire, guard).into_owned() }.tag()
//     }

//     pub(crate) fn get_prefix_id(&self) -> PrefixId<AF> {
//         let guard = &epoch::pin();
//         match self.get_stored_prefix(guard) {
//             None => {
//                 panic!("AtomicStoredPrefix::get_prefix_id: empty prefix");
//             }
//             Some(pfx) => pfx.prefix,
//         }
//     }

//     // PrefixSet is an Atomic that might be a null pointer, which is
//     // UB! Therefore we keep the prefix record in an Option: If
//     // that Option is None, then the PrefixSet is a null pointer and
//     // we'll return None
//     pub(crate) fn get_next_bucket<'a>(
//         &'a self,
//         guard: &'a Guard,
//     ) -> Option<&PrefixSet<AF, Meta>> {
//         // let guard = &epoch::pin();
//         if let Some(stored_prefix) = self.get_stored_prefix(guard) {
//             // if stored_prefix.super_agg_record.is_some() {
//             if !&stored_prefix
//                 .next_bucket
//                 .0
//                 .load(Ordering::SeqCst, guard)
//                 .is_null()
//             {
//                 Some(&stored_prefix.next_bucket)
//             } else {
//                 None
//             }
//         } else {
//             None
//         }
//     }
// }

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
    pub Box<[OnceBox<StoredPrefix<AF, M>>]>,
);

impl<AF: AddressFamily, M: Meta> PrefixSet<AF, M> {
    pub fn init(size: usize) -> Self {
        let mut l = Vec::with_capacity(size);

        trace!("creating space for {} prefixes in prefix_set", &size);
        for _i in 0..size {
            l.push(OnceBox::new());
        }
        PrefixSet(l.into_boxed_slice())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.len() == 1
    }

    pub(crate) fn get_by_index(
        &self,
        index: usize,
    ) -> Option<&StoredPrefix<AF, M>> {
        self.0[index].get()
    }

    pub(crate) fn empty() -> Self {
        PrefixSet(Box::new([OnceBox::new()]))
    }
}
