use std::{
    fmt::Debug,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::{self as epoch, Atomic};

use log::{info, trace};

use epoch::{Guard, Owned};
use std::marker::PhantomData;

use crate::local_array::{
    bit_span::BitSpan, node::NodeChildIter,
};
use crate::local_array::{node::NodeMoreSpecificsPrefixIter, tree::*};

use crate::prefix_record::InternalPrefixRecord;
use crate::{impl_search_level, impl_search_level_mut, impl_write_level};

use crate::AddressFamily;
use routecore::record::MergeUpdate;
use routecore::record::Meta;

// ----------- Node related structs -----------------------------------------

pub(crate) type SizedNodeRefOption<'a, AF> = Option<SizedStrideRef<'a, AF>>;

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

// Unlike StoredNode, we don't need an Empty variant, since we're using
// serial == 0 as the empty value. We're not using an Option here, to
// avoid going outside our atomic procedure.
#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub struct StoredPrefix<AF: AddressFamily, Meta: routecore::record::Meta>(
    Atomic<(
        usize,                                       // 0 the serial
        Option<InternalPrefixRecord<AF, Meta>>,      // 1 the record
        PrefixSet<AF, Meta>, // 2 the next set of nodes
        Option<Box<InternalPrefixRecord<AF, Meta>>>, // 3 the previous record that lived here (one serial down)
    )>,
);

impl<AF: AddressFamily, Meta: routecore::record::Meta>
    StoredPrefix<AF, Meta>
{
    pub(crate) fn empty() -> Self {
        StoredPrefix(Atomic::new((0, None, PrefixSet(Atomic::null()), None)))
    }

    pub(crate) fn new(record: InternalPrefixRecord<AF, Meta>) -> Self {
        StoredPrefix(Atomic::new((
            1,
            Some(record),
            PrefixSet(Atomic::null()),
            None,
        )))
    }

    pub(crate) fn is_empty(&self) -> bool {
        let guard = &epoch::pin();
        let pfx = self.0.load(Ordering::Relaxed, guard);
        pfx.is_null() || unsafe { pfx.deref() }.1.is_none()
    }

    pub(crate) fn get_serial(&self) -> usize {
        let guard = &epoch::pin();
        unsafe { self.0.load(Ordering::Relaxed, guard).into_owned() }.0
    }

    pub(crate) fn get_prefix_id(&self) -> PrefixId<AF> {
        let guard = &epoch::pin();
        if let Some(pfx_rec) =
            &unsafe { self.0.load(Ordering::Relaxed, guard).deref() }.1
        {
            PrefixId::new(pfx_rec.net, pfx_rec.len)
        } else {
            panic!("Empty prefix encountered and that's fatal.");
        }
    }

    pub(crate) fn get_prefix_record<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> &Option<InternalPrefixRecord<AF, Meta>> {
        &unsafe { self.0.load(Ordering::Relaxed, guard).deref() }.1
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
        let stored_prefix =
            unsafe { self.0.load(Ordering::Relaxed, guard).deref() };

        if stored_prefix.1.is_some() {
            if !&stored_prefix.2 .0.load(Ordering::Relaxed, guard).is_null() {
                Some(&stored_prefix.2)
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<AF: AddressFamily, Meta: routecore::record::Meta>
    std::convert::From<
        crossbeam_epoch::Shared<
            '_,
            (
                usize,
                Option<InternalPrefixRecord<AF, Meta>>,
                PrefixSet<AF, Meta>,
                Option<Box<InternalPrefixRecord<AF, Meta>>>,
            ),
        >,
    > for &StoredPrefix<AF, Meta>
{
    fn from(
        p: crossbeam_epoch::Shared<
            '_,
            (
                usize,
                Option<InternalPrefixRecord<AF, Meta>>,
                PrefixSet<AF, Meta>,
                Option<Box<InternalPrefixRecord<AF, Meta>>>,
            ),
        >,
    ) -> Self {
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
    > for &StoredPrefix<AF, Meta>
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

// ----------- FamilyBuckets Trait ------------------------------------------
//
// Implementations of this trait are done by a proc-macro called
// `stride_sizes`from the `rotonda-macros` crate.

#[derive(Debug)]
pub(crate) struct LenToBits([[u8; 10]; 33]);

pub trait NodeBuckets<AF: AddressFamily> {
    fn init() -> Self;
    fn len_to_store_bits(len: u8, level: u8) -> Option<&'static u8>;
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
    fn remove(
        &mut self,
        id: PrefixId<AF>,
    ) -> Option<InternalPrefixRecord<AF, M>>;
    fn get_root_prefix_set(&self, len: u8) -> &'_ PrefixSet<AF, M>;
    fn get_bits_for_len(len: u8, level: u8) -> Option<&'static u8>;
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
    pub Atomic<[MaybeUninit<StoredPrefix<AF, M>>]>,
);

impl<AF: AddressFamily, M: Meta> std::fmt::Display for PrefixSet<AF, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<AF: AddressFamily, M: routecore::record::Meta> PrefixSet<AF, M> {
    pub fn init(size: usize) -> Self {
        let mut l = Owned::<[MaybeUninit<StoredPrefix<AF, M>>]>::init(size);
        info!("creating space for {} prefixes in prefix_set", &size);
        for i in 0..size {
            l[i] = MaybeUninit::new(StoredPrefix::empty());
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
    ) -> &'a StoredPrefix<AF, M> {
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

// ----------- CustomAllocStorage Implementation ----------------------------
//
// CustomAllocStorage is a storage backend that uses a custom allocator, that
// consitss of arrays that point to other arrays on collision.
#[derive(Debug)]
pub struct CustomAllocStorage<
    AF: AddressFamily,
    Meta: routecore::record::Meta + routecore::record::MergeUpdate,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, Meta>,
> {
    pub(crate) buckets: NB,
    pub prefixes: PB,
    pub default_route_prefix_serial: AtomicUsize,
    _m: PhantomData<Meta>,
    _af: PhantomData<AF>,
}

impl<
        'a,
        AF: AddressFamily,
        Meta: routecore::record::Meta + MergeUpdate,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, Meta>,
    > CustomAllocStorage<AF, Meta, NB, PB>
{
    pub(crate) fn init(root_node: SizedStrideNode<AF>) -> Self {
        trace!("initialize storage backend");

        let store = CustomAllocStorage {
            buckets: NodeBuckets::<AF>::init(),
            prefixes: PrefixBuckets::<AF, Meta>::init(),
            // len_to_stride_size,
            default_route_prefix_serial: AtomicUsize::new(0),
            _af: PhantomData,
            _m: PhantomData,
        };

        store.store_node(
            StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0),
            root_node,
        );
        store
    }

    pub(crate) fn acquire_new_node_id(
        &self,
        (prefix_net, sub_prefix_len): (AF, u8),
    ) -> StrideNodeId<AF> {
        StrideNodeId::new_with_cleaned_id(prefix_net, sub_prefix_len)
    }

    // Create a new node in the store with paylaod `next_node`.
    //
    // Next node will be ignored if a node with the same `id` already exists.
    #[allow(clippy::type_complexity)]
    pub(crate) fn store_node(
        &self,
        id: StrideNodeId<AF>,
        next_node: SizedStrideNode<AF>,
    ) -> Option<StrideNodeId<AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                TreeBitMapNode<AF, S>,
                u8,
            ) -> Option<StrideNodeId<AF>>,
        }

        let search_level_3 = impl_write_level![Stride3; id;];
        let search_level_4 = impl_write_level![Stride4; id;];
        let search_level_5 = impl_write_level![Stride5; id;];

        trace!("insert node {}: {:?}", id, next_node);
        match next_node {
            SizedStrideNode::Stride3(new_node) => (search_level_3.f)(
                &search_level_3,
                self.buckets.get_store3(id),
                new_node,
                0,
            ),
            SizedStrideNode::Stride4(new_node) => (search_level_4.f)(
                &search_level_4,
                self.buckets.get_store4(id),
                new_node,
                0,
            ),
            SizedStrideNode::Stride5(new_node) => (search_level_5.f)(
                &search_level_5,
                self.buckets.get_store5(id),
                new_node,
                0,
            ),
        }
    }

    #[allow(clippy::type_complexity)]
    fn update_node(
        &self,
        id: StrideNodeId<AF>,
        updated_node: SizedStrideRefMut<AF>,
    ) {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                TreeBitMapNode<AF, S>,
                u8,
            ) -> Option<StrideNodeId<AF>>,
        }

        let search_level_3 = impl_write_level![Stride3; id;];
        let search_level_4 = impl_write_level![Stride4; id;];
        let search_level_5 = impl_write_level![Stride5; id;];

        match updated_node {
            SizedStrideRefMut::Stride3(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3(id),
                    new_node,
                    // Self::len_to_store_bits(id.get_id().1),
                    0,
                )
            }
            SizedStrideRefMut::Stride4(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4(id),
                    new_node,
                    0,
                )
            }
            SizedStrideRefMut::Stride5(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5(id),
                    new_node,
                    0,
                )
            }
        };
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_node_with_guard(
        &'a self,
        id: StrideNodeId<AF>,
        guard: &'a Guard,
    ) -> Option<SizedStrideRef<'a, AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                u8,
                &'a Guard,
            )
                -> Option<SizedStrideRef<'a, AF>>,
        }

        let search_level_3 = impl_search_level![Stride3; id;];
        let search_level_4 = impl_search_level![Stride4; id;];
        let search_level_5 = impl_search_level![Stride5; id;];

        match self.get_stride_for_id(id) {
            3 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3(id),
                    0,
                    guard,
                )
            }

            4 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4(id),
                    0,
                    guard,
                )
            }
            _ => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5(id),
                    0,
                    guard,
                )
            }
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_node_mut_with_guard(
        &'a self,
        id: StrideNodeId<AF>,
        guard: &'a Guard,
    ) -> Option<SizedStrideRefMut<'a, AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                // [u8; 10],
                u8,
                &'a Guard,
            )
                -> Option<SizedStrideRefMut<'a, AF>>,
        }

        let search_level_3 = impl_search_level_mut![Stride3; id;];
        let search_level_4 = impl_search_level_mut![Stride4; id;];
        let search_level_5 = impl_search_level_mut![Stride5; id;];

        match self.buckets.get_stride_for_id(id) {
            3 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3(id),
                    0,
                    guard,
                )
            }

            4 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4(id),
                    0,
                    guard,
                )
            }
            _ => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5(id),
                    0,
                    guard,
                )
            }
        }
    }

    fn store_node_with_guard(
        &self,
        _current_node: SizedNodeRefOption<AF>,
        _next_node: SizedStrideNode<AF>,
        _guard: &epoch::Guard,
    ) -> Option<StrideNodeId<AF>> {
        unimplemented!()
    }

    pub(crate) fn get_root_node_id(&self) -> StrideNodeId<AF> {
        StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0)
    }

    pub fn get_nodes_len(&self) -> usize {
        0
    }

    // Prefixes related methods

    pub(crate) fn load_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial.load(Ordering::Acquire)
    }

    fn increment_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial
            .fetch_add(1, Ordering::Acquire)
    }

    fn acquire_new_prefix_id(
        &self,
        prefix: &InternalPrefixRecord<AF, Meta>,
    ) -> PrefixId<AF> {
        PrefixId::<AF>::new(prefix.net, prefix.len)
    }

    fn store_prefix(
        &self,
        _id: PrefixId<AF>,
        _pfx_rec: InternalPrefixRecord<AF, Meta>,
        _serial: usize,
    ) -> Option<PrefixId<AF>> {
        unimplemented!()
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn upsert_prefix(
        &self,
        pfx_rec: InternalPrefixRecord<AF, Meta>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let pfx_id = PrefixId::new(pfx_rec.net, pfx_rec.len);
        struct UpdateMeta<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &UpdateMeta<AF, M>,
                &StoredPrefix<AF, M>,
                InternalPrefixRecord<AF, M>,
                u8,
            )
                -> Result<(), Box<dyn std::error::Error>>,
        }

        let update_meta = UpdateMeta {
            f: &|update_meta: &UpdateMeta<AF, Meta>,
                 stored_prefix,
                 mut pfx_rec,
                 level: u8| {
                let guard = &epoch::pin();
                let atomic_curr_prefix =
                    stored_prefix.0.load(Ordering::SeqCst, guard);
                let curr_prefix =
                    unsafe { atomic_curr_prefix.into_owned().into_box() };
                let tag = atomic_curr_prefix.tag();
                let prev_rec;
                let next_set;
                match curr_prefix.1.as_ref() {
                    // insert or...
                    None => {
                        prev_rec = None;

                        // start calculation size of next set
                        let this_level =
                            *PB::get_bits_for_len(pfx_id.get_len(), level)
                                .unwrap();

                        let next_level = *PB::get_bits_for_len(
                            pfx_id.get_len(),
                            level + 1,
                        )
                        .unwrap();

                        trace!(
                            "this level {} next level {}",
                            this_level,
                            next_level
                        );
                        next_set = if next_level > 0 {
                            info!(
                                "INSERT with new bucket of size {} at prefix len {}",
                                1 << (next_level - this_level), pfx_id.get_len()
                            );
                            PrefixSet::init(
                                (1 << (next_level - this_level)) as usize,
                            )
                        } else {
                            info!("INSERT at LAST LEVEL with empty bucket at prefix len {}", pfx_id.get_len());
                            PrefixSet(Atomic::null())
                        };
                        // End of calculation
                    }
                    // ...update
                    Some(curr_pfx_rec) => {
                        trace!("UPDATE");
                        pfx_rec.meta = Some(
                            curr_pfx_rec
                                .meta
                                .as_ref()
                                .unwrap()
                                .clone_merge_update(&pfx_rec.meta.unwrap())?,
                        );
                        // Tuck the current record away on the heap.
                        // This doesn't have to be an atomic pointer, since
                        // we're doing this in one (atomic) transaction.
                        prev_rec = Some(Box::new(curr_prefix.1.unwrap()));
                        next_set = curr_prefix.2;
                    }
                };

                match stored_prefix.0.compare_exchange(
                    atomic_curr_prefix,
                    Owned::new((tag + 1, Some(pfx_rec), next_set, prev_rec))
                        .with_tag(tag + 1),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                ) {
                    Ok(_) => {
                        trace!("prefix successfully updated {:?}", pfx_id);
                        Ok(())
                    }
                    Err(store_error) => {
                        trace!(
                            "Contention. Prefix update failed {:?}",
                            pfx_id
                        );
                        // Try again. TODO: backoff neeeds to be implemented
                        // hers.
                        (update_meta.f)(
                            update_meta,
                            store_error.current.into(),
                            store_error.new.1.clone().unwrap(),
                            level,
                        )
                    }
                }
            },
        };

        let guard = &epoch::pin();
        trace!("UPSERT PREFIX {:?}", pfx_rec);

        let (stored_prefix, level) =
            self.retrieve_prefix_mut_with_guard(pfx_id, guard);

        (update_meta.f)(&update_meta, stored_prefix, pfx_rec, level)
    }

    #[allow(clippy::type_complexity)]
    fn retrieve_prefix_mut_with_guard(
        &'a self,
        id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> (&'a mut StoredPrefix<AF, Meta>, u8) {
        struct SearchLevel<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                u8,
                &'a Guard,
            )
                -> (&'a mut StoredPrefix<AF, M>, u8),
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, Meta>,
                 prefix_set: &PrefixSet<AF, Meta>,
                 //  new_prefix: InternalPrefixRecord<AF, Meta>,
                 mut level: u8,
                 guard: &Guard| {
                let last_level = if level > 0 {
                    *PB::get_bits_for_len(id.get_len(), level - 1).unwrap()
                } else {
                    0
                };

                let this_level =
                    *PB::get_bits_for_len(id.get_len(), level).unwrap();

                let index = ((id.get_net().dangerously_truncate_to_u32()
                    << last_level)
                    >> (AF::BITS - (this_level - last_level)))
                    as usize;
                trace!("retrieve prefix with guard");
                trace!(
                    "{:032b} (pfx)",
                    id.get_net().dangerously_truncate_to_u32()
                );
                trace!("this_level {}", this_level);
                trace!("last_level {}", last_level);
                trace!("id {:?}", id);
                trace!("calculated index {}", index);
                trace!("level {}", level);
                trace!(
                    "bits_division {}",
                    <NB as NodeBuckets<AF>>::len_to_store_bits(
                        id.get_len(),
                        level
                    )
                    .unwrap()
                );

                let mut prefixes =
                    prefix_set.0.load(Ordering::Relaxed, guard);
                // trace!("nodes {:?}", unsafe { unwrapped_nodes.deref_mut().len() });
                trace!(
                    "prefixes at level {}? {:?}",
                    level,
                    !prefixes.is_null()
                );
                let prefix_ref = unsafe { &mut prefixes.deref_mut()[index] };
                let stored_prefix = unsafe { prefix_ref.assume_init_mut() };

                // if stored_prefix.is_empty() {
                //     return stored_prefix;
                // }

                match unsafe {
                    stored_prefix.0.load(Ordering::Relaxed, guard).deref_mut()
                } {
                    (_serial, Some(pfx_rec), next_set, _prev_record) => {
                        if id == PrefixId::new(pfx_rec.net, pfx_rec.len) {
                            trace!("found requested prefix {:?}", id);
                            (stored_prefix, level)
                        } else {
                            level += 1;
                            (search_level.f)(
                                search_level,
                                next_set,
                                level,
                                guard,
                            )
                        }
                    }
                    (_serial, None, _next_set, _prev_record) => {
                        // No record at the deepest level, still we're returning a reference to it,
                        // so the caller can insert a new record here.
                        (stored_prefix, level)
                    }
                }
            },
        };

        (search_level.f)(
            &search_level,
            self.prefixes.get_root_prefix_set(id.get_len()),
            0,
            guard,
        )
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_prefix(
        &self,
        id: PrefixId<AF>,
    ) -> Option<InternalPrefixRecord<AF, Meta>> {
        let guard = epoch::pin();
        struct SearchLevel<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                u8,
                &'a Guard,
            )
                -> Option<InternalPrefixRecord<AF, M>>,
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, Meta>,
                 prefix_set: &PrefixSet<AF, Meta>,
                 //  new_prefix: InternalPrefixRecord<AF, Meta>,
                 mut level: u8,
                 guard: &Guard| {
                let last_level = if level > 0 {
                    *PB::get_bits_for_len(id.get_len(), level - 1).unwrap()
                } else {
                    0
                };
                let this_level =
                    *PB::get_bits_for_len(id.get_len(), level).unwrap();

                let index = ((id.get_net().dangerously_truncate_to_u32()
                    << last_level)
                    >> (AF::BITS - (this_level - last_level)))
                    as usize;
                trace!("retrieve prefix");
                trace!("{:032b}", id.get_net().dangerously_truncate_to_u32());
                trace!("this_level {}", this_level);
                trace!("last_level {}", last_level);
                trace!("id {:?}", id);
                trace!("calculated index {}", index);
                trace!("level {}", level);
                trace!(
                    "bits_division {}",
                    <NB as NodeBuckets<AF>>::len_to_store_bits(
                        id.get_len(),
                        level
                    )
                    .unwrap()
                );
                let mut prefixes =
                    prefix_set.0.load(Ordering::Relaxed, guard);
                // trace!("nodes {:?}", unsafe { unwrapped_nodes.deref_mut().len() });
                let prefix_ref = unsafe { &mut prefixes.deref_mut()[index] };
                match unsafe {
                    prefix_ref
                        .assume_init_ref()
                        .0
                        .load(Ordering::Relaxed, guard)
                        .deref()
                } {
                    (_serial, Some(pfx_rec), next_set, _prev_record) => {
                        if id == PrefixId::from(pfx_rec) {
                            trace!("found requested prefix {:?}", id);
                            return Some(pfx_rec.clone());
                        };
                        level += 1;
                        (search_level.f)(search_level, next_set, level, guard)
                    }
                    (_serial, None, _next_set, _prev_record) => None,
                }
            },
        };

        (search_level.f)(
            &search_level,
            self.prefixes.get_root_prefix_set(id.get_len()),
            0,
            &guard,
        )
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn non_recursive_retrieve_prefix_with_guard(
        &'a self,
        id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> (
        Option<(&InternalPrefixRecord<AF, Meta>, &'a usize)>,
        Option<(
            PrefixId<AF>,
            u8,
            &'a PrefixSet<AF, Meta>,
            [Option<(&'a PrefixSet<AF, Meta>, usize)>; 26],
            usize,
        )>,
    ) {
        let mut prefix_set = self.prefixes.get_root_prefix_set(id.get_len());
        let mut parents = [None; 26];
        let mut index: usize;
        let mut level: u8 = 0;

        loop {
            let last_level = if level > 0 {
                *PB::get_bits_for_len(id.get_len(), level - 1).unwrap()
            } else {
                0
            };
            let this_level =
                *PB::get_bits_for_len(id.get_len(), level).unwrap();
            // The index of the prefix in this array (at this len and
            // level) is calculated by performing the hash function
            // over the prefix.

            index = ((id.get_net().dangerously_truncate_to_u32()
                << last_level)
                >> (AF::BITS - (this_level - last_level)))
                as usize;

            let mut prefixes = prefix_set.0.load(Ordering::Relaxed, guard);
            // trace!("nodes {:?}", unsafe { unwrapped_nodes.deref_mut().len() });
            let prefix_ref = unsafe { &mut prefixes.deref_mut()[index] };
            match unsafe {
                prefix_ref
                    .assume_init_ref()
                    .0
                    .load(Ordering::SeqCst, guard)
                    .deref()
            } {
                (serial, Some(pfx_rec), next_set, _prev_record) => {
                    if id == PrefixId::new(pfx_rec.net, pfx_rec.len) {
                        trace!("found requested prefix {:?}", id);
                        parents[level as usize] = Some((prefix_set, index));
                        return (
                            Some((pfx_rec, serial)),
                            Some((id, level, prefix_set, parents, index)),
                        );
                    };
                    // Advance to the next level.
                    prefix_set = next_set;
                    level += 1;
                }
                (_serial, None, _next_set, _prev_record) => {
                    trace!("no prefix found for {:?}", id);
                    parents[level as usize] = Some((prefix_set, index));
                    return (
                        None,
                        Some((id, level, prefix_set, parents, index)),
                    );
                }
            };
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_prefix_with_guard(
        &'a self,
        id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> Option<(&InternalPrefixRecord<AF, Meta>, &'a usize)> {
        struct SearchLevel<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                u8,
                &'a Guard,
            ) -> Option<(
                &'a InternalPrefixRecord<AF, M>,
                &'a usize,
            )>,
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, Meta>,
                 prefix_set: &PrefixSet<AF, Meta>,
                 mut level: u8,
                 guard: &Guard| {
                let last_level = if level > 0 {
                    *PB::get_bits_for_len(id.get_len(), level - 1).unwrap()
                } else {
                    0
                };
                let this_level =
                    *PB::get_bits_for_len(id.get_len(), level).unwrap();
                let index = ((id.get_net().dangerously_truncate_to_u32()
                    << last_level)
                    >> (AF::BITS - (this_level - last_level)))
                    as usize;
                trace!("retrieve prefix");
                trace!(
                    "{:032b} (pfx)",
                    id.get_net().dangerously_truncate_to_u32()
                );
                trace!("this_level {}", this_level);
                trace!("last_level {}", last_level);
                trace!("id {:?}", id);
                trace!("calculated index {}", index);
                trace!("level {}", level);
                trace!(
                    "bits_division {}",
                    <NB as NodeBuckets<AF>>::len_to_store_bits(
                        id.get_len(),
                        level
                    )
                    .unwrap()
                );
                let mut prefixes =
                    prefix_set.0.load(Ordering::Relaxed, guard);
                // trace!("nodes {:?}", unsafe { unwrapped_nodes.deref_mut().len() });
                let prefix_ref = unsafe { &mut prefixes.deref_mut()[index] };
                match unsafe {
                    prefix_ref
                        .assume_init_ref()
                        .0
                        .load(Ordering::SeqCst, guard)
                        .deref()
                } {
                    (serial, Some(pfx_rec), next_set, _prev_record) => {
                        if id == PrefixId::new(pfx_rec.net, pfx_rec.len) {
                            trace!("found requested prefix {:?}", id);
                            return Some((pfx_rec, serial));
                        };
                        level += 1;
                        (search_level.f)(search_level, next_set, level, guard)
                    }
                    (_serial, None, _next_set, _prev_record) => None,
                }
            },
        };

        (search_level.f)(
            &search_level,
            self.prefixes.get_root_prefix_set(id.get_len()),
            0,
            guard,
        )
    }

    fn remove_prefix(
        &mut self,
        index: PrefixId<AF>,
    ) -> Option<InternalPrefixRecord<AF, Meta>> {
        match index.is_empty() {
            false => self.prefixes.remove(index),
            true => None,
        }
    }

    pub fn get_prefixes_len(&self) -> usize {
        (0..=AF::BITS)
            .map(|pfx_len| -> usize {
                self.prefixes
                    .get_root_prefix_set(pfx_len)
                    .get_len_recursive()
            })
            .sum()
    }

    // Stride related methods

    pub(crate) fn get_stride_for_id(&self, id: StrideNodeId<AF>) -> u8 {
        self.buckets.get_stride_for_id(id)
    }

    pub fn get_stride_sizes(&self) -> &[u8] {
        self.buckets.get_stride_sizes()
    }

    pub(crate) fn get_strides_len() -> u8 {
        NB::get_strides_len()
    }

    pub(crate) fn get_first_stride_size() -> u8 {
        NB::get_first_stride_size()
    }

    // Calculates the id of the node that COULD host a prefix in its
    // ptrbitarr.
    fn get_node_id_for_prefix(
        &self,
        prefix: &PrefixId<AF>,
    ) -> (StrideNodeId<AF>, BitSpan) {
        let mut acc = 0;
        for i in self.get_stride_sizes() {
            acc += *i;
            if acc >= prefix.get_len() {
                let node_len = acc - i;
                return (
                    StrideNodeId::new_with_cleaned_id(
                        prefix.get_net(),
                        node_len,
                    ),
                    BitSpan::new(
                        (prefix.get_net().dangerously_truncate_to_u32()
                            << node_len)
                            >> (32 - (prefix.get_len() - node_len)),
                        prefix.get_len() - node_len,
                    ),
                );
            }
        }
        panic!("prefix length for {:?} is too long", prefix);
    }

    // Iterator over all more-specific prefixes, starting from the given
    // prefix at the given level and cursor.
    pub(crate) fn more_specific_prefix_iter_from(
        &'a self,
        start_prefix_id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> Result<MoreSpecificsPrefixIter<AF, Meta, NB, PB>, std::io::Error>
    {
        trace!("more specifics for {:?}", start_prefix_id);

        // A v4 /32 or a v4 /128 doesn't have more specific prefixes 🤓.
        if start_prefix_id.get_len() >= AF::BITS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "prefix length is too long. No more-specifics can live here.",
            ));
        }

        // calculate the node start_prefix_id lives in.
        let (cur_node_id, cur_bit_span) =
            self.get_node_id_for_prefix(&start_prefix_id.inc_len());
        trace!("start node {}", cur_node_id);

        trace!(
            "start prefix id {:032b} (len {})",
            start_prefix_id.get_net(),
            start_prefix_id.get_len()
        );
        trace!(
            "start node id   {:032b} (bits {} len {})",
            cur_node_id.get_id().0,
            cur_node_id.get_id().0,
            cur_node_id.get_len()
        );
        trace!(
            "start bit span  {:032b} {}",
            cur_bit_span,
            cur_bit_span.bits
        );
        let cur_pfx_iter: SizedPrefixIter<AF>;
        let cur_ptr_iter: SizedNodeIter<AF>;

        match self.retrieve_node_with_guard(cur_node_id, guard).unwrap() {
            SizedStrideRef::Stride3(n) => {
                cur_pfx_iter = SizedPrefixIter::Stride3(
                    n.more_specific_pfx_iter(cur_node_id, cur_bit_span),
                );
                cur_ptr_iter = SizedNodeIter::Stride3(
                    n.more_specific_ptr_iter(cur_node_id, cur_bit_span),
                );
            }
            SizedStrideRef::Stride4(n) => {
                cur_pfx_iter = SizedPrefixIter::Stride4(
                    n.more_specific_pfx_iter(cur_node_id, cur_bit_span),
                );
                cur_ptr_iter = SizedNodeIter::Stride4(
                    n.more_specific_ptr_iter(cur_node_id, cur_bit_span),
                );
            }
            SizedStrideRef::Stride5(n) => {
                cur_pfx_iter = SizedPrefixIter::Stride5(
                    n.more_specific_pfx_iter(cur_node_id, cur_bit_span),
                );
                cur_ptr_iter = SizedNodeIter::Stride5(
                    n.more_specific_ptr_iter(cur_node_id, cur_bit_span),
                );
            }
        };

        Ok(MoreSpecificsPrefixIter {
            store: self,
            guard,
            cur_pfx_iter,
            cur_ptr_iter,
            parent_and_position: vec![],
        })
    }
}

impl<
        'a,
        AF: AddressFamily + 'a,
        M: Meta + MergeUpdate + 'a,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > CustomAllocStorage<AF, M, NB, PB>
{
    // Iterator over all the prefixes in the storage.
    pub fn prefixes_iter(
        &'a self,
        guard: &'a Guard,
    ) -> PrefixIter<AF, M, PB> {
        PrefixIter {
            prefixes: &self.prefixes,
            cur_bucket: self.prefixes.get_root_prefix_set(0),
            cur_len: 0,
            cur_level: 0,
            cursor: 0,
            parents: [None; 26],
            guard,
            _af: PhantomData,
            _meta: PhantomData,
        }
    }

    // Iterator over all less-specific prefixes, starting from the given
    // prefix at the given level and cursor.
    pub fn less_specific_prefix_iter(
        &'a self,
        start_prefix_id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> impl Iterator<Item = &'a InternalPrefixRecord<AF, M>> {
        trace!("less specifics for {:?}", start_prefix_id);
        trace!("level {}, len {}", 0, start_prefix_id.get_len());

        // We could just let the /0 prefix search the tree and have it return
        // an empty iterator, but to avoid having to read out the root node
        // for this prefix, we'll just return an empty iterator. The trade-off
        // is that the whole iterator has to be wrapped in a Box<dyn ...>
        if start_prefix_id.get_len() < 1 {
            None
        } else {
            let cur_len = start_prefix_id.get_len() - 1;
            let cur_bucket = self.prefixes.get_root_prefix_set(cur_len);

            Some(LessSpecificPrefixIter::new(
                &self.prefixes,
                cur_len,
                cur_bucket,
                0,
                start_prefix_id,
                guard,
            ))
        }
        .into_iter()
        .flatten()
    }
}

// Iterator over all the prefixes in the storage.
pub struct PrefixIter<
    'a,
    AF: AddressFamily + 'a,
    M: Meta + 'a,
    PB: PrefixBuckets<AF, M>,
> {
    prefixes: &'a PB,
    cur_len: u8,
    cur_bucket: &'a PrefixSet<AF, M>,
    cur_level: u8,
    // level depth of IPv4 as defined in rotonda-macros/maps.rs
    // Option(parent, cursor position at the parent)
    // 26 is the max number of levels in IPv6, which is the max number of
    // of both IPv4 and IPv6.
    parents: [Option<(&'a PrefixSet<AF, M>, usize)>; 26],
    cursor: usize,
    guard: &'a Guard,
    _af: PhantomData<AF>,
    _meta: PhantomData<M>,
}

impl<'a, AF: AddressFamily + 'a, M: Meta + 'a, PB: PrefixBuckets<AF, M>>
    Iterator for PrefixIter<'a, AF, M, PB>
{
    type Item = &'a InternalPrefixRecord<AF, M>;

    fn next(&mut self) -> Option<Self::Item> {
        info!(
            "starting next loop for level {} cursor {} (len {})",
            self.cur_level, self.cursor, self.cur_len
        );

        loop {
            if self.cur_len > AF::BITS as u8 {
                // This is the end, my friend
                trace!("reached max length {}, returning None", self.cur_len);
                return None;
            }

            if *PB::get_bits_for_len(self.cur_len, self.cur_level).unwrap()
                == 0
            {
                // END OF THE LENGTH
                // This length is done too, go to the next length
                trace!("next length {}", self.cur_len + 1);
                self.cur_len += 1;

                // a new length, a new life
                // reset the level depth and cursor,
                // but also empty all the parents
                self.cur_level = 0;
                self.cursor = 0;
                self.parents = [None; 26];

                // let's continue, get the prefixes for the next length
                self.cur_bucket =
                    self.prefixes.get_root_prefix_set(self.cur_len);
                continue;
            }
            let bucket_size = 1_usize
                << (if self.cur_level > 0 {
                    *PB::get_bits_for_len(self.cur_len, self.cur_level)
                        .unwrap()
                        - *PB::get_bits_for_len(
                            self.cur_len,
                            self.cur_level - 1,
                        )
                        .unwrap()
                } else {
                    *PB::get_bits_for_len(self.cur_len, self.cur_level)
                        .unwrap()
                });

            if self.cursor >= bucket_size {
                if self.cur_level == 0 {
                    // END OF THE LENGTH
                    // This length is done too, go to the next length
                    trace!("next length {}", self.cur_len);
                    self.cur_len += 1;

                    // a new length, a new life
                    // reset the level depth and cursor,
                    // but also empty all the parents
                    self.cur_level = 0;
                    self.cursor = 0;
                    self.parents = [None; 26];

                    if self.cur_len > AF::BITS as u8 {
                        // This is the end, my friend
                        return None;
                    }

                    // let's continue, get the prefixes for the next length
                    self.cur_bucket =
                        self.prefixes.get_root_prefix_set(self.cur_len);
                } else {
                    // END OF THIS BUCKET
                    // GO BACK UP ONE LEVEL
                    // The level is done, but the length isn't
                    // Go back up one level and continue
                    match self.parents[self.cur_level as usize] {
                        Some(parent) => {
                            // trace!("back up one level");

                            // There is a parent, go back up. Since we're doing depth-first
                            // we have to check if there's a prefix directly at the parent
                            // and return that.
                            self.cur_level -= 1;

                            // move the current bucket to the parent and move
                            // the cursor position where we left off. The
                            // next run of the loop will read it.
                            self.cur_bucket = parent.0;
                            self.cursor = parent.1 + 1;

                            continue;
                        }
                        None => {
                            trace!(
                                "c {} lvl {} len {}",
                                self.cursor,
                                self.cur_level,
                                self.cur_len
                            );
                            // trace!("parent {:?}", self.parent);
                            panic!(
                                "Where do we belong? Where do we come from?"
                            );
                        }
                    };
                }
            };

            // we're somewhere in the PrefixSet iteration, read the next StoredPrefix.
            // We are doing depth-first iteration, so we check for a child first and
            // descend into that if it exists.
            // trace!(
            //     "c{} l{} len {}",
            //     self.cursor,
            //     self.cur_level,
            //     self.cur_len
            // );

            let s_pfx = self
                .cur_bucket
                .get_by_index(self.cursor as usize, self.guard);
            // trace!("s_pfx {:?}", s_pfx);
            // DEPTH FIRST ITERATION
            match s_pfx.get_next_bucket(self.guard) {
                Some(bucket) => {
                    // DESCEND ONe LEVEL
                    // There's a child here, descend into it, but...
                    // trace!("C. got next bucket {:?}", bucket);

                    // save our parent and cursor position first, and then..
                    self.parents[(self.cur_level + 1) as usize] =
                        Some((self.cur_bucket, self.cursor));

                    // move to the next bucket,
                    self.cur_bucket = bucket;

                    // increment the level and reset the cursor.
                    self.cur_level += 1;
                    self.cursor = 0;

                    // If there's a child here there MUST be a prefix here,
                    // as well.
                    if let Some(prefix) = s_pfx.get_prefix_record(self.guard)
                    {
                        // There's a prefix here, that's the next one
                        info!("D. found prefix {:?}", prefix);
                        return Some(prefix);
                    } else {
                        panic!("No prefix here, but there's a child here?");
                    }
                }
                None => {
                    // No reference to another PrefixSet, all that's
                    // left, is checking for a prefix at the current
                    // cursor position.
                    if let Some(prefix) = s_pfx.get_prefix_record(self.guard)
                    {
                        // There's a prefix here, that's the next one
                        info!("E. found prefix {:?}", prefix);
                        self.cursor += 1;
                        return Some(prefix);
                    }
                }
            };
            self.cursor += 1;
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum SizedNodeIter<AF: AddressFamily> {
    Stride3(NodeChildIter<AF, Stride3>),
    Stride4(NodeChildIter<AF, Stride4>),
    Stride5(NodeChildIter<AF, Stride5>),
}

impl<AF: AddressFamily> SizedNodeIter<AF> {
    fn next(&mut self) -> Option<StrideNodeId<AF>> {
        match self {
            SizedNodeIter::Stride3(iter) => iter.next(),
            SizedNodeIter::Stride4(iter) => iter.next(),
            SizedNodeIter::Stride5(iter) => iter.next(),
        }
    }
}

pub(crate) enum SizedPrefixIter<AF: AddressFamily> {
    Stride3(NodeMoreSpecificsPrefixIter<AF, Stride3>),
    Stride4(NodeMoreSpecificsPrefixIter<AF, Stride4>),
    Stride5(NodeMoreSpecificsPrefixIter<AF, Stride5>),
}

impl<AF: AddressFamily> SizedPrefixIter<AF> {
    fn next(&mut self) -> Option<PrefixId<AF>> {
        match self {
            SizedPrefixIter::Stride3(iter) => iter.next(),
            SizedPrefixIter::Stride4(iter) => iter.next(),
            SizedPrefixIter::Stride5(iter) => iter.next(),
        }
    }
}

// This iterator is somewhat different from the other *PrefixIterator types,
// since it uses the Nodes to select the more specifics. Am Iterator that
// would only use the Prefixes in the store could exist, but iterating over
// those in search of more specifics would be way more expensive.
pub struct MoreSpecificsPrefixIter<
    'a,
    AF: AddressFamily,
    M: Meta + MergeUpdate,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, M>,
    // Store: StorageBackend,
> {
    store: &'a CustomAllocStorage<AF, M, NB, PB>,
    cur_ptr_iter: SizedNodeIter<AF>,
    cur_pfx_iter: SizedPrefixIter<AF>,
    parent_and_position: Vec<SizedNodeIter<AF>>,
    guard: &'a Guard,
}

impl<
        'a,
        AF: AddressFamily + 'a,
        M: Meta + MergeUpdate,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > Iterator for MoreSpecificsPrefixIter<'a, AF, M, NB, PB>
{
    type Item = PrefixId<AF>;

    fn next(&mut self) -> Option<Self::Item> {
        trace!("MoreSpecificsPrefixIter");

        loop {
            let next_pfx = self.cur_pfx_iter.next();

            if next_pfx.is_some() {
                return next_pfx;
            }

            // Our current prefix iterator for this node is done, look for
            // the next pfx iterator of the next child node in the current
            // ptr iterator.
            let mut next_ptr = self.cur_ptr_iter.next();

            // Our current ptr iterator is also done, maybe we have a parent
            if next_ptr.is_none() {
                if let Some(cur_ptr_iter) = self.parent_and_position.pop() {
                    self.cur_ptr_iter = cur_ptr_iter;
                    next_ptr = self.cur_ptr_iter.next();
                } else {
                    return None;
                }
            }

            if let Some(next_ptr) = next_ptr {
                match self
                    .store
                    .retrieve_node_with_guard(next_ptr, self.guard)
                {
                    Some(SizedStrideRef::Stride3(next_node)) => {
                        // copy the current iterator into the parent vec and create
                        // a new ptr iterator for this node
                        self.parent_and_position.push(self.cur_ptr_iter);
                        let ptr_iter = next_node.ptr_iter(next_ptr);
                        self.cur_ptr_iter = ptr_iter.wrap();

                        self.cur_pfx_iter = next_node
                            .more_specific_pfx_iter(
                                next_ptr,
                                BitSpan::new(0, 1),
                            )
                            .wrap();
                    }
                    Some(SizedStrideRef::Stride4(next_node)) => {
                        // create new ptr iterator for this node.
                        self.parent_and_position.push(self.cur_ptr_iter);
                        let ptr_iter = next_node.ptr_iter(next_ptr);
                        self.cur_ptr_iter = ptr_iter.wrap();

                        self.cur_pfx_iter = next_node
                            .more_specific_pfx_iter(
                                next_ptr,
                                BitSpan::new(0, 1),
                            )
                            .wrap();
                    }
                    Some(SizedStrideRef::Stride5(next_node)) => {
                        // create new ptr iterator for this node.
                        self.parent_and_position.push(self.cur_ptr_iter);
                        let ptr_iter = next_node.ptr_iter(next_ptr);
                        self.cur_ptr_iter = ptr_iter.wrap();

                        self.cur_pfx_iter = next_node
                            .more_specific_pfx_iter(
                                next_ptr,
                                BitSpan::new(0, 1),
                            )
                            .wrap();
                    }
                    None => return None, // if let Some(next_id) = next {
                };
            }
        }
    }
}

pub struct LessSpecificPrefixIter<
    'a,
    AF: AddressFamily + 'a,
    M: Meta + 'a,
    PB: PrefixBuckets<AF, M>,
> {
    prefixes: &'a PB,
    cur_len: u8,
    cur_bucket: &'a PrefixSet<AF, M>,
    cur_level: u8,
    cur_prefix_id: PrefixId<AF>,
    guard: &'a Guard,
    _af: PhantomData<AF>,
    _meta: PhantomData<M>,
    _pb: PhantomData<PB>,
}

impl<'a, AF: AddressFamily + 'a, M: Meta + 'a, PB: PrefixBuckets<AF, M>>
    LessSpecificPrefixIter<'a, AF, M, PB>
{
    pub(crate) fn new(
        prefixes: &'a PB,
        cur_len: u8,
        cur_bucket: &'a PrefixSet<AF, M>,
        cur_level: u8,
        cur_prefix_id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> impl Iterator<Item = &'a InternalPrefixRecord<AF, M>> {
        LessSpecificPrefixIter {
            prefixes,
            cur_len,
            cur_bucket,
            cur_level,
            cur_prefix_id,
            guard,
            _af: PhantomData,
            _meta: PhantomData,
            _pb: PhantomData,
        }
    }
}

impl<'a, AF: AddressFamily + 'a, M: Meta + 'a, PB: PrefixBuckets<AF, M>>
    Iterator for LessSpecificPrefixIter<'a, AF, M, PB>
{
    type Item = &'a InternalPrefixRecord<AF, M>;

    // This iterator moves down all prefix lengths, starting with the length
    // of the (search prefix - 1), looking for shorter prefixes, where the
    // its bits are the same as the bits of the search prefix.
    fn next(&mut self) -> Option<Self::Item> {
        trace!("search next less-specific for {:?}", self.cur_prefix_id);

        loop {
            if self.cur_len == 0 {
                // This is the end, my friend
                trace!("reached min length {}, returning None", self.cur_len);
                return None;
            }

            // shave a bit of the current prefix.
            trace!(
                "truncate to len {} (level {})",
                self.cur_len,
                self.cur_level
            );
            self.cur_prefix_id = PrefixId::new(
                self.cur_prefix_id.get_net().truncate_to_len(self.cur_len),
                self.cur_len,
            );

            let last_level = if self.cur_level > 0 {
                *PB::get_bits_for_len(self.cur_len, self.cur_level - 1)
                    .unwrap()
            } else {
                0
            };

            let this_level =
                *PB::get_bits_for_len(self.cur_len, self.cur_level).unwrap();

            let index =
                ((self.cur_prefix_id.get_net().dangerously_truncate_to_u32()
                    << last_level)
                    >> (AF::BITS - (this_level - last_level)))
                    as usize;

            if this_level == 0 {
                // END OF THE LENGTH
                // This length is done too, go to the next length
                trace!("next length {}", self.cur_len + 1);
                self.cur_len -= 1;

                // a new length, a new life
                // reset the level depth and cursor,
                // but also empty all the parents
                self.cur_level = 0;
                // self.parents = [None; 26];

                // let's continue, get the prefixes for the next length
                self.cur_bucket =
                    self.prefixes.get_root_prefix_set(self.cur_len);
                continue;
            }

            // LEVEL DEPTH ITERATION
            let s_pfx =
                self.cur_bucket.get_by_index(index as usize, self.guard);
            // trace!("s_pfx {:?}", s_pfx);
            match unsafe {
                s_pfx.0.load(Ordering::SeqCst, self.guard).deref()
            } {
                (_serial, Some(pfx_rec), next_set, _prev_record) => {
                    // There is a prefix  here, but we need to checkt if it's
                    // the right one.
                    if self.cur_prefix_id
                        == PrefixId::new(pfx_rec.net, pfx_rec.len)
                    {
                        trace!(
                            "found requested prefix {:?}",
                            self.cur_prefix_id
                        );
                        self.cur_len -= 1;
                        self.cur_level = 0;
                        self.cur_bucket =
                            self.prefixes.get_root_prefix_set(self.cur_len);
                        return Some(pfx_rec);
                    };
                    // Advance to the next level or the next len.
                    match next_set
                        .0
                        .load(Ordering::SeqCst, self.guard)
                        .is_null()
                    {
                        // No child here, move one length down.
                        true => {
                            self.cur_len -= 1;
                            self.cur_level = 0;
                            self.cur_bucket = self
                                .prefixes
                                .get_root_prefix_set(self.cur_len);
                        }
                        // There's a child, move a level up and set the child
                        // as current. Length remains the same.
                        false => {
                            self.cur_bucket = next_set;
                            self.cur_level += 1;
                        }
                    }
                }
                (_serial, None, next_set, _prev_record) => {
                    // No prefix here, let's see if there's a child here
                    trace!(
                        "no prefix found for {:?} in len {}",
                        self.cur_prefix_id,
                        self.cur_len
                    );
                    // Advance to the next level or the next len.
                    match next_set
                        .0
                        .load(Ordering::SeqCst, self.guard)
                        .is_null()
                    {
                        // No child here, move one length down.
                        true => {
                            self.cur_len -= 1;
                            self.cur_level = 0;
                            self.cur_bucket = self
                                .prefixes
                                .get_root_prefix_set(self.cur_len);
                        }
                        // There's a child, move a level up and set the child
                        // as current. Length remains the same.
                        false => {
                            self.cur_bucket = next_set;
                            self.cur_level += 1;
                        }
                    }
                }
            };
        }
    }
}
