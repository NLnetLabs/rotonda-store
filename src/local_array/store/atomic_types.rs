use std::{fmt::Debug, mem::MaybeUninit, sync::atomic::Ordering};

use crossbeam_epoch::{self as epoch, Atomic};

use log::{debug, trace};

use epoch::{Guard, Owned};

use crate::local_array::tree::*;
use crate::prefix_record::InternalPrefixRecord;
use crate::AddressFamily;

// ----------- Node related structs -----------------------------------------

#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub struct NodeSet<AF: AddressFamily, S: Stride>(
    pub Atomic<[MaybeUninit<Atomic<StoredNode<AF, S>>>]>,
);

#[derive(Debug)]
pub struct StoredNode<AF, S>
where
    Self: Sized,
    S: Stride,
    AF: AddressFamily,
{
    pub(crate) node_id: StrideNodeId<AF>,
    pub(crate) node: TreeBitMapNode<AF, S>,
    pub(crate) node_set: NodeSet<AF, S>,
}

// impl<AF: AddressFamily, S: Stride> Default for StoredNode<AF, S> {
//     fn default() -> Self {
//         StoredNode::Empty
//     }
// }

impl<AF: AddressFamily, S: Stride> NodeSet<AF, S> {
    pub fn init(size: usize) -> Self {
        debug!("creating space for {} nodes!", &size);
        let mut l =
            Owned::<[MaybeUninit<Atomic<StoredNode<AF, S>>>]>::init(size);
        for i in 0..size {
            l[i] = MaybeUninit::new(Atomic::null());
        }
        NodeSet(l.into())
    }
}

// ----------- Prefix related structs ---------------------------------------

// ----------- StoredPrefix -------------------------------------------------
// This is the top-level struct that's linked from the slots in the buckets.
// It contains a super_agg_record that is supposed to hold counters for the
// records that are stored inside it, so that iterators over its linked lists
// don't have to go into them if there's nothing there and could stop early.
#[derive(Debug)]
pub struct StoredPrefix<AF: AddressFamily, M: routecore::record::Meta> {
    // the serial number
    pub serial: usize,
    // the prefix itself,
    pub prefix: PrefixId<AF>,
    // the aggregated data for this prefix
    pub(crate) super_agg_record: AtomicSuperAggRecord<AF, M>,
    // the next aggregated record for this prefix and hash_id
    // pub(crate) next_agg_record: Atomic<StoredAggRecord<AF, M>>,
    // the reference to the next set of records for this prefix, if any.
    pub next_bucket: PrefixSet<AF, M>,
}

impl<AF: AddressFamily, M: routecore::record::Meta> StoredPrefix<AF, M> {
    pub fn new<PB: PrefixBuckets<AF, M>>(
        record: InternalPrefixRecord<AF, M>,
        level: u8,
    ) -> Self {
        // start calculation size of next set, it's dependent on the level
        // we're in.
        let pfx_id = PrefixId::new(record.net, record.len);
        let this_level = PB::get_bits_for_len(pfx_id.get_len(), level);
        let next_level = PB::get_bits_for_len(pfx_id.get_len(), level + 1);

        trace!("this level {} next level {}", this_level, next_level);
        let next_bucket: PrefixSet<AF, M> = if next_level > 0 {
            debug!(
                "INSERT with new bucket of size {} at prefix len {}",
                1 << (next_level - this_level),
                pfx_id.get_len()
            );
            PrefixSet::init((1 << (next_level - this_level)) as usize)
        } else {
            debug!(
                "INSERT at LAST LEVEL with empty bucket at prefix len {}",
                pfx_id.get_len()
            );
            PrefixSet::empty()
        };
        // End of calculation

        // let mut new_super_agg_record =
        //     InternalPrefixRecord::<AF, M>::new_with_meta(
        //         record.net,
        //         record.len,
        //         record.meta.clone(),
        //     );

        // even though we're about to create the first record in this
        // aggregation record, we still need to `clone_merge_update` to
        // create the start data for the aggregation.
        // new_super_agg_record.meta = new_super_agg_record
        //     .meta
        //     .clone_merge_update(&record.meta)
        //     .unwrap();

        StoredPrefix {
            serial: 1,
            prefix: record.get_prefix_id(),
            super_agg_record: AtomicSuperAggRecord::<AF, M>::new(
                record.get_prefix_id(),
                record.meta,
            ),
            next_bucket,
        }
    }

    pub(crate) fn get_record<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Option<&'a InternalPrefixRecord<AF, M>> {
        self.super_agg_record.get_record(guard)
    }

    // pub(crate) fn atomic_update_aggregate(
    //     &self,
    //     record: InternalPrefixRecord<AF, M>,
    //     // guard: &Guard,
    // ) {
    //     // let back_off = Backoff::new();
    //     let g = epoch::pin();

    //     let mut inner_super_agg_record =
    //         self.super_agg_record.0.load(Ordering::Acquire, &g);
    //     let mut new_record;

    //     loop {
    //         let super_agg_record = unsafe { inner_super_agg_record.deref() };

    //         let new_meta = super_agg_record
    //             .meta
    //             .clone_merge_update(&record.meta)
    //             .unwrap();

    //         new_record = Owned::new(InternalPrefixRecord::<AF, M> {
    //             net: record.net,
    //             len: record.len,
    //             meta: new_meta,
    //         });

    //         // drop(new_record);

    //         // let super_agg_record = self.super_agg_record.0.compare_exchange(
    //         //     inner_super_agg_record,
    //         //     new_record,
    //         //     Ordering::SeqCst,
    //         //     Ordering::SeqCst,
    //         //     guard,
    //         // );
    //         match &self.super_agg_record.0.compare_exchange(
    //             inner_super_agg_record,
    //             new_record,
    //             Ordering::AcqRel,
    //             Ordering::Acquire,
    //             &g,
    //         ) {
    //             Ok(_) => {
    //                 return;
    //             }
    //             Err(next_agg) => {
    //                 // Do it again
    //                 // back_off.spin();
    //                 inner_super_agg_record = next_agg.current;
    //             }
    //         };
    //     }
    // }
}

// ----------- SuperAggRecord -----------------------------------------------
// This is the record that holds the aggregates at the top-level for a given
// prefix.

#[derive(Debug)]
pub(crate) struct AtomicSuperAggRecord<
    AF: AddressFamily,
    M: routecore::record::Meta,
>(pub Atomic<InternalPrefixRecord<AF, M>>);

impl<AF: AddressFamily, M: routecore::record::Meta>
    AtomicSuperAggRecord<AF, M>
{
    pub fn new(prefix: PrefixId<AF>, record: M) -> Self {
        debug!("create new stored prefix record");
        AtomicSuperAggRecord(Atomic::new(InternalPrefixRecord {
            net: prefix.get_net(),
            len: prefix.get_len(),
            meta: record,
        }))
    }

    pub fn get_record<'a>(
        &self,
        guard: &'a Guard,
    ) -> Option<&'a InternalPrefixRecord<AF, M>> {
        trace!("get_record {:?}", self.0);
        let rec = self.0.load(Ordering::SeqCst, guard);

        match rec.is_null() {
            true => None,
            false => Some(unsafe { rec.as_ref() }.unwrap()),
        }
    }
}

// ----------- AtomicStoredPrefix -------------------------------------------
// Unlike StoredNode, we don't need an Empty variant, since we're using
// serial == 0 as the empty value. We're not using an Option here, to
// avoid going outside our atomic procedure.
#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub struct AtomicStoredPrefix<AF: AddressFamily, M: routecore::record::Meta>(
    pub Atomic<StoredPrefix<AF, M>>,
);

impl<AF: AddressFamily, Meta: routecore::record::Meta>
    AtomicStoredPrefix<AF, Meta>
{
    pub(crate) fn empty() -> Self {
        AtomicStoredPrefix(Atomic::null())
    }

    pub(crate) fn is_empty(&self, guard: &Guard) -> bool {
        let pfx = self.0.load(Ordering::SeqCst, guard);
        pfx.is_null()
            || unsafe { pfx.deref() }
                .super_agg_record
                .0
                .load(Ordering::SeqCst, guard)
                .is_null()
    }

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
        unsafe { self.0.load(Ordering::SeqCst, guard).into_owned() }.serial
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

    pub fn get_agg_record<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Option<&InternalPrefixRecord<AF, Meta>> {
        self.get_stored_prefix(guard).map(|stored_prefix| unsafe {
            stored_prefix
                .super_agg_record
                .0
                .load(Ordering::SeqCst, guard)
                .deref()
        })
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

// impl<AF: AddressFamily, Meta: routecore::record::Meta>
//     std::convert::From<crossbeam_epoch::Shared<'_, StoredPrefix<AF, Meta>>>
//     for &AtomicStoredPrefix<AF, Meta>
// {
//     fn from(p: crossbeam_epoch::Shared<'_, StoredPrefix<AF, Meta>>) -> Self {
//         unsafe { std::mem::transmute(p) }
//     }
// }

// impl<AF: AddressFamily, Meta: routecore::record::Meta>
//     std::convert::From<
//         crossbeam_epoch::Owned<(
//             usize,
//             Option<InternalPrefixRecord<AF, Meta>>,
//             PrefixSet<AF, Meta>,
//             Option<Box<InternalPrefixRecord<AF, Meta>>>,
//         )>,
//     > for &AtomicStoredPrefix<AF, Meta>
// {
//     fn from(
//         p: crossbeam_epoch::Owned<(
//             usize,
//             Option<InternalPrefixRecord<AF, Meta>>,
//             PrefixSet<AF, Meta>,
//             Option<Box<InternalPrefixRecord<AF, Meta>>>,
//         )>,
//     ) -> Self {
//         unsafe { std::mem::transmute(p) }
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

pub trait PrefixBuckets<AF: AddressFamily, M: routecore::record::Meta>
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
// implied in the position in the array a serial numher has. A PrefixSet
// doesn't know anything about the node it is contained in, so it needs a
// base address to be able to calculate the complete prefix of a child prefix.

#[derive(Debug)]
#[repr(align(8))]
pub struct PrefixSet<AF: AddressFamily, M: routecore::record::Meta>(
    pub Atomic<[MaybeUninit<AtomicStoredPrefix<AF, M>>]>,
);

impl<AF: AddressFamily, M: routecore::record::Meta> PrefixSet<AF, M> {
    pub fn init(size: usize) -> Self {
        let mut l =
            Owned::<[MaybeUninit<AtomicStoredPrefix<AF, M>>]>::init(size);
        debug!("creating space for {} prefixes in prefix_set", &size);
        for i in 0..size {
            l[i] = MaybeUninit::new(AtomicStoredPrefix::empty());
        }
        PrefixSet(l.into())
    }

    pub fn get_len_recursive(&self) -> usize {
        fn recurse_len<AF: AddressFamily, M: routecore::record::Meta>(
            start_set: &PrefixSet<AF, M>,
        ) -> usize {
            let mut size: usize = 0;
            let guard = &epoch::pin();
            let start_set = start_set.0.load(Ordering::SeqCst, guard);
            for p in unsafe { start_set.deref() } {
                let pfx = unsafe { p.assume_init_ref() };
                if !pfx.is_empty(guard) {
                    size += 1;
                    debug!(
                        "recurse found pfx {:?} cur size {}",
                        pfx.get_prefix_id(),
                        size
                    );
                    if let Some(next_bucket) = pfx.get_next_bucket(guard) {
                        trace!("found next bucket");
                        size += recurse_len(next_bucket);
                    }
                }
            }

            size
        }

        recurse_len(self)
    }

    pub(crate) fn get_by_index<'a>(
        &'a self,
        index: usize,
        guard: &'a Guard,
    ) -> &'a AtomicStoredPrefix<AF, M> {
        assert!(!self.0.load(Ordering::SeqCst, guard).is_null());
        unsafe {
            self.0.load(Ordering::SeqCst, guard).deref()[index as usize]
                .assume_init_ref()
        }
    }

    pub(crate) fn empty() -> Self {
        PrefixSet(Atomic::null())
    }
}
