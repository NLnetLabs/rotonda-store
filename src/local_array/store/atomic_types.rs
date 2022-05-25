use std::{fmt::Debug, mem::MaybeUninit, sync::atomic::Ordering};

use crossbeam_epoch::{self as epoch, Atomic};

use log::{info, trace};

use epoch::{Guard, Owned};
use std::marker::PhantomData;

use crate::local_array::tree::*;

use crate::prefix_record::InternalPrefixRecord;

use crate::AddressFamily;
use routecore::record::Meta;

// ----------- Node related structs -----------------------------------------

#[derive(Debug)]
pub struct NodeSet<AF: AddressFamily, S: Stride>(
    pub Atomic<[MaybeUninit<StoredNode<AF, S>>]>,
);

#[derive(Debug)]
pub enum StoredNode<AF, S>
where
    Self: Sized,
    S: Stride,
    AF: AddressFamily,
{
    NodeWithRef((StrideNodeId<AF>, TreeBitMapNode<AF, S>, NodeSet<AF, S>)),
    Empty,
}

impl<AF: AddressFamily, S: Stride> Default for StoredNode<AF, S> {
    fn default() -> Self {
        StoredNode::Empty
    }
}

impl<AF: AddressFamily, S: Stride> NodeSet<AF, S> {
    pub fn init(size: usize) -> Self {
        info!("creating space for {} nodes", &size);
        let mut l = Owned::<[MaybeUninit<StoredNode<AF, S>>]>::init(size);
        for i in 0..size {
            l[i] = MaybeUninit::new(StoredNode::Empty);
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
pub struct StoredPrefix<AF: AddressFamily, M: routecore::record::Meta> {
    // the serial number
    pub serial: usize,
    // the prefix itself,
    pub prefix: PrefixId<AF>,
    // the aggregated data for this prefix (all hash_ids)
    pub(crate) super_agg_record: AtomicSuperAggRecord<AF, M>,
    // the next aggregated record for this prefix and hash_id
    pub(crate) next_agg_record: Atomic<StoredAggRecord<AF, M>>,
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
            info!(
                "INSERT with new bucket of size {} at prefix len {}",
                1 << (next_level - this_level),
                pfx_id.get_len()
            );
            PrefixSet::init((1 << (next_level - this_level)) as usize)
        } else {
            info!(
                "INSERT at LAST LEVEL with empty bucket at prefix len {}",
                pfx_id.get_len()
            );
            PrefixSet::empty()
        };
        // End of calculation

        let mut new_super_agg_record =
            InternalPrefixRecord::<AF, M>::new_with_meta(
                record.net,
                record.len,
                record.meta.clone(),
            );

        // even though we're about to create the first record in this
        // aggregation record, we still need to `clone_merge_update` to
        // create the start data for the aggregation.
        new_super_agg_record.meta = new_super_agg_record
            .meta
            .clone_merge_update(&record.meta)
            .unwrap();

        StoredPrefix {
            serial: 1,
            prefix: record.get_prefix_id(),
            super_agg_record: AtomicSuperAggRecord::<AF, M>::new(
                record.get_prefix_id(),
                new_super_agg_record.meta,
            ),
            next_agg_record: Atomic::new(StoredAggRecord::<AF, M>::new(
                record,
            )),
            next_bucket,
        }
    }

    pub(crate) fn atomic_update_aggregate(
        &mut self,
        record: &InternalPrefixRecord<AF, M>,
    ) {
        let guard = &epoch::pin();
        let mut inner_super_agg_record =
            self.super_agg_record.0.load(Ordering::SeqCst, guard);
        loop {
            let tag = inner_super_agg_record.tag();
            let mut super_agg_record =
                unsafe { inner_super_agg_record.into_owned() };

            super_agg_record.meta = super_agg_record
                .meta
                .clone_merge_update(&record.meta)
                .unwrap();

            let super_agg_record = self.super_agg_record.0.compare_exchange(
                self.super_agg_record.0.load(Ordering::SeqCst, guard),
                super_agg_record.with_tag(tag + 1),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            );
            match super_agg_record {
                Ok(_) => return,
                Err(next_agg) => {
                    // Do it again
                    // TODO BACKOFF
                    inner_super_agg_record = next_agg.current;
                }
            };
        }
    }

    pub(crate) fn iter_agg_records<'a>(
        &'a self,
        guard: &'a epoch::Guard,
    ) -> impl Iterator<Item = &'a StoredAggRecord<AF, M>> {
        let start_r = self.next_agg_record.load(Ordering::SeqCst, guard);

        match start_r.is_null() {
            true => None,
            false => Some(AggRecordIterator {
                current: unsafe { start_r.deref() },
                guard,
            }),
        }
        .into_iter()
        .flatten()
    }

    pub(crate) fn get_latest_unique_records<'a>(
        &'a self,
        guard: &'a epoch::Guard,
    ) -> Vec<&'a InternalPrefixRecord<AF, M>> {
        let mut records = Vec::new();
        for agg_r in self.iter_agg_records(guard) {
            // We're only trying once to get the most recent record:
            // receiving a None from get_most_recent_record indicates that
            // the record is busy being written to.
            if let Some(agg_r) = agg_r.get_last_record(guard) {
                records.push(agg_r);
            }
        }
        records
    }
}

// ----------- SuperAggRecord -----------------------------------------------
// This is the record that holds the aggregates at the top-level for a given
// prefix.

#[derive(Debug)]
pub(crate) struct SuperAggRecordExample<
    AF: AddressFamily,
    M: routecore::record::Meta,
> where
    Self: routecore::record::Meta,
{
    last: M,
    total_count: usize,
    _af: PhantomData<AF>,
}

#[derive(Debug)]
pub(crate) struct AtomicSuperAggRecord<
    AF: AddressFamily,
    M: routecore::record::Meta,
>(Atomic<InternalPrefixRecord<AF, M>>);

impl<AF: AddressFamily, M: routecore::record::Meta>
    AtomicSuperAggRecord<AF, M>
{
    pub fn new(prefix: PrefixId<AF>, record: M) -> Self {
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
            false => Some(unsafe { rec.deref() }),
        }
    }
}

// ----------- StoredAggRecord ----------------------------------------------
// This is the second-level struct that's linked from the `StoredPrefix` top-
// level struct. It has an aggregated record field that holds counters and
// other aggregated data for the records that are stored inside it.
pub(crate) struct StoredAggRecord<
    AF: AddressFamily,
    M: routecore::record::Meta,
> {
    // the aggregated meta-data for this prefix and hash_id.
    pub agg_record: Atomic<InternalPrefixRecord<AF, M>>,
    // the reference to the next record for this prefix and the same hash_id.
    pub(crate) next_record: Atomic<LinkedListRecord<AF, M>>,
    // the reference to the next record for this prefix and another hash_id.
    pub next_agg: Atomic<StoredAggRecord<AF, M>>,
}

impl<AF: AddressFamily, M: routecore::record::Meta> StoredAggRecord<AF, M> {
    // This creates a new aggregation record with the record supplied in the
    // argument atomically linked to it. It doesn't make sense to have a
    // aggregation record without a record.
    pub(crate) fn new(record: InternalPrefixRecord<AF, M>) -> Self {
        let mut new_agg_record = InternalPrefixRecord::<AF, M>::new_with_meta(
            record.net,
            record.len,
            record.meta.clone(),
        );

        // even though we're about to create the first record in this
        // aggregation record, we still need to `clone_merge_update` to
        // create the start data for the aggregation.
        new_agg_record.meta = new_agg_record
            .meta
            .clone_merge_update(&record.meta)
            .unwrap();

        StoredAggRecord {
            agg_record: Atomic::new(new_agg_record),
            next_record: Atomic::new(LinkedListRecord::new(record)),
            next_agg: Atomic::null(),
        }
    }

    pub(crate) fn get_last_record<'a>(
        &self,
        guard: &'a Guard,
    ) -> Option<&'a InternalPrefixRecord<AF, M>> {
        let next_record = self.next_record.load(Ordering::SeqCst, guard);
        // Note that an Atomic::null() indicates that a thread is busy creating it.
        // It's the responsability of the caller to retry if it wants that data.
        match next_record.is_null() {
            true => None,
            false => Some(unsafe { &next_record.deref().record }),
        }
    }

    // aggregated records don't need to be prepended (you, know, HEAD), but
    // can just be appended to the tail.
    pub(crate) fn atomic_tail_agg(
        &mut self,
        agg_record: InternalPrefixRecord<AF, M>,
    ) {
        let guard = &epoch::pin();
        let mut inner_next_agg = self.next_agg.load(Ordering::SeqCst, guard);
        let tag = inner_next_agg.tag();
        let agg_record = Owned::new(Self::new(agg_record)).into_shared(guard);

        loop {
            let next_agg = self.next_agg.compare_exchange(
                inner_next_agg,
                agg_record.with_tag(tag + 1),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            );
            match next_agg {
                Ok(_) => return,
                Err(next_agg) => {
                    // Do it again
                    // TODO BACKOFF
                    inner_next_agg = next_agg.current;
                }
            };
        }
    }

    // only 'normal', non-aggegation records need to be prependend, so that
    // the most recent record is the first in the list.
    pub(crate) fn atomic_prepend_record(
        &mut self,
        record: InternalPrefixRecord<AF, M>,
    ) {
        trace!("New record: {}", record);
        let guard = &epoch::pin();
        let mut inner_next_record =
            self.next_record.load(Ordering::SeqCst, guard);
        trace!("Existing record {:?}", inner_next_record);
        let tag = inner_next_record.tag();
        let new_inner_next_record = Owned::new(LinkedListRecord {
            record,
            prev: unsafe { inner_next_record.into_owned() }.into(),
        })
        .into_shared(guard);

        loop {
            let next_record = self.next_record.compare_exchange(
                inner_next_record,
                new_inner_next_record.with_tag(tag + 1),
                Ordering::SeqCst,
                Ordering::SeqCst,
                guard,
            );

            match next_record {
                Ok(rec) => {
                    trace!("wrote record {:?}", rec);
                    return;
                }
                Err(next_record) => {
                    // Do it again
                    // TODO BACKOFF
                    inner_next_record = next_record.current;
                }
            };
        }
    }
}

pub(crate) struct AggRecordIterator<
    'a,
    AF: AddressFamily,
    M: routecore::record::Meta,
> {
    current: Option<&'a StoredAggRecord<AF, M>>,
    guard: &'a Guard,
}

impl<'a, AF: AddressFamily, M: routecore::record::Meta> std::iter::Iterator
    for AggRecordIterator<'a, AF, M>
{
    type Item = &'a StoredAggRecord<AF, M>;

    fn next(&mut self) -> Option<Self::Item> {
        trace!("next agg_rec {:?}", self.current);
        match self.current {
            Some(agg_rec) => {
                let inner_next =
                    agg_rec.next_agg.load(Ordering::SeqCst, self.guard);
                if !inner_next.is_null() {
                    self.current = Some(unsafe { inner_next.deref() });
                } else {
                    self.current = None;
                }

                Some(agg_rec)
            }
            None => None,
        }
    }
}

// ----------- LinkedListRecord ---------------------------------------------
// This is the third-and-lowest-level struct that holds the actual record and
// a link to (a list) of another one, if any.
#[derive(Debug)]
pub(crate) struct LinkedListRecord<
    AF: AddressFamily,
    M: routecore::record::Meta,
> {
    record: InternalPrefixRecord<AF, M>,
    prev: Atomic<LinkedListRecord<AF, M>>,
}

impl<'a, AF: AddressFamily, M: routecore::record::Meta>
    LinkedListRecord<AF, M>
{
    fn new(record: InternalPrefixRecord<AF, M>) -> Self {
        LinkedListRecord {
            record,
            prev: Atomic::null(),
        }
    }

    pub(crate) fn iter(
        &'a self,
        guard: &'a Guard,
    ) -> LinkedListIterator<'a, AF, M> {
        LinkedListIterator {
            current: Some(self),
            guard,
        }
    }
}

pub(crate) struct LinkedListIterator<
    'a,
    AF: AddressFamily + 'a,
    M: routecore::record::Meta + 'a,
> {
    current: Option<&'a LinkedListRecord<AF, M>>,
    guard: &'a Guard,
}

impl<'a, AF: AddressFamily, M: routecore::record::Meta> std::iter::Iterator
    for LinkedListIterator<'a, AF, M>
{
    type Item = &'a InternalPrefixRecord<AF, M>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(rec) => {
                let inner_next = rec.prev.load(Ordering::SeqCst, self.guard);
                if !inner_next.is_null() {
                    self.current = Some(unsafe { inner_next.deref() });
                } else {
                    self.current = None;
                }

                Some(&rec.record)
            }
            None => None,
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
        // AtomicStoredPrefix(Atomic::new(StoredPrefix {
        //     serial: 0,
        //     super_agg_record: AtomicInternalPrefixRecord::empty(),
        //     next_bucket: PrefixSet::empty(),
        //     next_agg_record: Atomic::null(),
        // }))
        AtomicStoredPrefix(Atomic::null())
    }

    pub(crate) fn is_empty(&self) -> bool {
        let guard = &epoch::pin();
        let pfx = self.0.load(Ordering::Relaxed, guard);
        pfx.is_null()
            || unsafe { pfx.deref() }
                .super_agg_record
                .0
                .load(Ordering::Relaxed, guard)
                .is_null()
    }

    pub(crate) fn get_stored_prefix<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Option<&'a StoredPrefix<AF, Meta>> {
        let pfx = self.0.load(Ordering::SeqCst, guard);
        match pfx.is_null() {
            true => None,
            false => Some(unsafe { pfx.deref() }),
        }
    }

    pub(crate) fn get_stored_prefix_mut<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Option<&'a mut StoredPrefix<AF, Meta>> {
        let mut pfx = self.0.load(Ordering::SeqCst, guard);
        match pfx.is_null() {
            true => None,
            false => Some(unsafe { pfx.deref_mut() }),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get_serial(&self) -> usize {
        let guard = &epoch::pin();
        unsafe { self.0.load(Ordering::Relaxed, guard).into_owned() }.serial
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

    pub(crate) fn get_agg_record<'a>(
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

    pub(crate) fn get_last_record<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Option<&InternalPrefixRecord<AF, Meta>> {
        self.get_stored_prefix(guard)
            .and_then(|stored_prefix| unsafe {
                stored_prefix
                    .next_agg_record
                    .load(Ordering::SeqCst, guard)
                    .deref()
                    .get_last_record(guard)
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
                .load(Ordering::Relaxed, guard)
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

impl<AF: AddressFamily, Meta: routecore::record::Meta>
    std::convert::From<crossbeam_epoch::Shared<'_, StoredPrefix<AF, Meta>>>
    for &AtomicStoredPrefix<AF, Meta>
{
    fn from(p: crossbeam_epoch::Shared<'_, StoredPrefix<AF, Meta>>) -> Self {
        unsafe { std::mem::transmute(p) }
    }
}

impl<AF: AddressFamily, Meta: routecore::record::Meta>
    std::convert::From<
        crossbeam_epoch::Owned<(
            usize,
            Option<InternalPrefixRecord<AF, Meta>>,
            PrefixSet<AF, Meta>,
            Option<Box<InternalPrefixRecord<AF, Meta>>>,
        )>,
    > for &AtomicStoredPrefix<AF, Meta>
{
    fn from(
        p: crossbeam_epoch::Owned<(
            usize,
            Option<InternalPrefixRecord<AF, Meta>>,
            PrefixSet<AF, Meta>,
            Option<Box<InternalPrefixRecord<AF, Meta>>>,
        )>,
    ) -> Self {
        unsafe { std::mem::transmute(p) }
    }
}

// ----------  AtomicInternalPrefixRecord -----------------------------------

pub(crate) struct AtomicInternalPrefixRecord<
    AF: AddressFamily,
    M: routecore::record::Meta,
>(Atomic<InternalPrefixRecord<AF, M>>);

impl<AF: AddressFamily, M: routecore::record::Meta>
    AtomicInternalPrefixRecord<AF, M>
{
    pub(crate) fn empty() -> Self {
        Self(Atomic::null())
    }

    pub(crate) fn is_empty(&self) -> bool {
        let guard = &epoch::pin();
        let pfx = self.0.load(Ordering::Relaxed, guard);
        pfx.is_null()
    }

    pub(crate) fn get_record<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Option<&InternalPrefixRecord<AF, M>> {
        let pfx = self.0.load(Ordering::Relaxed, guard);
        match pfx.is_null() {
            true => None,
            false => Some(unsafe { pfx.deref() }),
        }
    }
}

impl<AF: AddressFamily, M: routecore::record::Meta>
    std::convert::From<crossbeam_epoch::Atomic<InternalPrefixRecord<AF, M>>>
    for AtomicInternalPrefixRecord<AF, M>
{
    fn from(p: crossbeam_epoch::Atomic<InternalPrefixRecord<AF, M>>) -> Self {
        unsafe { std::mem::transmute(p) }
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
pub struct PrefixSet<AF: AddressFamily, M: routecore::record::Meta>(
    pub Atomic<[MaybeUninit<AtomicStoredPrefix<AF, M>>]>,
);

impl<AF: AddressFamily, M: Meta> std::fmt::Display for PrefixSet<AF, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<AF: AddressFamily, M: routecore::record::Meta> PrefixSet<AF, M> {
    pub fn init(size: usize) -> Self {
        let mut l =
            Owned::<[MaybeUninit<AtomicStoredPrefix<AF, M>>]>::init(size);
        info!("creating space for {} prefixes in prefix_set", &size);
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
            let start_set = start_set.0.load(Ordering::Relaxed, guard);
            for p in unsafe { start_set.deref() } {
                let pfx = unsafe { p.assume_init_ref() };
                if !pfx.is_empty() {
                    size += 1;
                    info!(
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
        assert!(!self.0.load(Ordering::Relaxed, guard).is_null());
        unsafe {
            self.0.load(Ordering::Relaxed, guard).deref()[index as usize]
                .assume_init_ref()
        }
    }

    pub(crate) fn empty() -> Self {
        PrefixSet(Atomic::null())
    }
}
