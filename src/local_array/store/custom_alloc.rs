use std::{
    fmt::Debug,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::{self as epoch, Atomic};

use log::{info, trace};

use epoch::{Guard, Owned};
use std::marker::PhantomData;

use crate::local_array::storage_backend::StorageBackend;
use crate::local_array::tree::*;

use crate::prefix_record::InternalPrefixRecord;
use crate::{impl_search_level, impl_search_level_mut, impl_write_level};

use crate::AddressFamily;
use routecore::record::MergeUpdate;
use routecore::record::Meta;

use super::storage_backend::SizedNodeRefOption;

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
    // fn len_to_store_bits(len: u8, level: u8) -> Option<&'static u8> {
    //     todo!()
    // }
    // fn get_store_mut(
    //     &mut self,
    //     id: StrideNodeId<AF>,
    // ) -> &mut NodeSet<AF, Stride3> {
    //     todo!()
    // }
    // fn get_store(&self, id: PrefixId<AF>) -> &NodeSet<AF, Stride3> {
    //     todo!()
    // }
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

    // pub(crate) fn get_serial_mut(&mut self) -> &mut AtomicUsize {
    //     &mut self.0.load(Ordering::Relaxed, guard).0
    // }

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

    pub(crate) fn get_prefix_record_mut<'a>(
        &'a mut self,
        guard: &'a Guard,
    ) -> &'a mut Option<InternalPrefixRecord<AF, Meta>> {
        // let guard = &epoch::pin();
        &mut unsafe { self.0.load(Ordering::Relaxed, guard).deref_mut() }.1
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
    fn get_store3_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride3>;
    fn get_store4_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride4>;
    fn get_store5_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride5>;
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
    // fn len_to_store_bits(len: u8, level: u8) -> Option<&'static u8>;
    // fn get<'a>(
    //     &'a self,
    //     id: PrefixId<AF>,
    // ) -> Option<&'a InternalPrefixRecord<AF, M>>;
    // fn len(&self) -> usize;
    // fn iter<'a>(&'a self, guard: &'a Guard) -> PrefixIter<'a, AF, M>;
    fn remove(
        &mut self,
        id: PrefixId<AF>,
    ) -> Option<InternalPrefixRecord<AF, M>>;
    fn get_root_prefix_set(&self, len: u8) -> &'_ PrefixSet<AF, M>;
    fn get_root_prefix_set_mut(&mut self, len: u8) -> &mut PrefixSet<AF, M>;
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
        unsafe {
            self.0.load(Ordering::Relaxed, guard).deref()[index as usize]
                .assume_init_ref()
        }
    }

    // fn iter<'a>(&'a self, guard: &'a Guard) -> PrefixIter<'a, AF, M> {
    //     PrefixIter {
    //         prefixes_for_len:
    //         cur_bucket: self,
    //         cur_len: 0,
    //         cur_level: 0,
    //         cur_max_index: 0,
    //         cursor: 0,
    //         guard,
    //         _af: PhantomData,
    //     }
    // }
}

// impl<AF: AddressFamily, M: Meta> std::ops::Index<usize> for PrefixSet<AF, M> {
//     type Output = StoredPrefix<AF, M>;

//     fn index(&self, idx: usize) -> &StoredPrefix<AF, M> {
//         let guard = &epoch::pin();
//         unsafe {
//             self.0.load(Ordering::Relaxed, guard).as_ref().unwrap()
//                 [idx as usize]
//                 .assume_init_ref()
//         }
//     }
// }

// impl<AF: AddressFamily, M: Meta> std::ops::IndexMut<usize>
//     for PrefixSet<AF, M>
// {
//     fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
//         let guard = &epoch::pin();
//         unsafe {
//             self.0.load(Ordering::Relaxed, guard).deref_mut()[idx as usize]
//                 .assume_init_mut()
//         }
//     }
// }

// ----------- CustomAllocStorage Implementation ----------------------------
//
// CustomAllocStorage is a storage backend that uses a custom allocator, that
// consitss of arrays that point to other arrays on collision.
#[derive(Debug)]
pub struct CustomAllocStorage<
    AF: AddressFamily,
    Meta: routecore::record::Meta,
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
        AF: AddressFamily,
        Meta: routecore::record::Meta + MergeUpdate,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, Meta>,
    > StorageBackend for CustomAllocStorage<AF, Meta, NB, PB>
{
    type AF = AF;
    type Meta = Meta;

    fn init(root_node: SizedStrideNode<Self::AF>) -> Self {
        trace!("initialize storage backend");

        let mut store = CustomAllocStorage {
            buckets: NB::init(),
            prefixes: PB::init(),
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

    fn acquire_new_node_id(
        &self,
        (prefix_net, sub_prefix_len): (Self::AF, u8),
    ) -> StrideNodeId<Self::AF> {
        StrideNodeId::new_with_cleaned_id(prefix_net, sub_prefix_len)
    }

    // Create a new node in the store with paylaod `next_node`.
    //
    // Next node will be ignored if a node with the same `id` already exists.
    #[allow(clippy::type_complexity)]
    fn store_node(
        &mut self,
        id: StrideNodeId<Self::AF>,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId<Self::AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn Fn(
                &SearchLevel<AF, S>,
                &mut NodeSet<AF, S>,
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
                self.buckets.get_store3_mut(id),
                new_node,
                0,
            ),
            SizedStrideNode::Stride4(new_node) => (search_level_4.f)(
                &search_level_4,
                self.buckets.get_store4_mut(id),
                new_node,
                0,
            ),
            SizedStrideNode::Stride5(new_node) => (search_level_5.f)(
                &search_level_5,
                self.buckets.get_store5_mut(id),
                new_node,
                0,
            ),
        }
    }

    #[allow(clippy::type_complexity)]
    fn update_node(
        &mut self,
        id: StrideNodeId<AF>,
        updated_node: SizedStrideRefMut<AF>,
    ) {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn Fn(
                &SearchLevel<AF, S>,
                &mut NodeSet<AF, S>,
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
                    self.buckets.get_store3_mut(id),
                    new_node,
                    // Self::len_to_store_bits(id.get_id().1),
                    0,
                )
            }
            SizedStrideRefMut::Stride4(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4_mut(id),
                    new_node,
                    0,
                )
            }
            SizedStrideRefMut::Stride5(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5_mut(id),
                    new_node,
                    0,
                )
            }
        };
    }

    #[allow(clippy::type_complexity)]
    fn retrieve_node_with_guard<'a>(
        &'a self,
        id: StrideNodeId<Self::AF>,
        guard: &'a Guard,
    ) -> Option<SizedStrideRef<'a, Self::AF>> {
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
    fn retrieve_node_mut_with_guard<'a>(
        &'a self,
        id: StrideNodeId<Self::AF>,
        guard: &'a Guard,
    ) -> Option<SizedStrideRefMut<'a, Self::AF>> {
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
        _current_node: SizedNodeRefOption<Self::AF>,
        _next_node: SizedStrideNode<AF>,
        _guard: &epoch::Guard,
    ) -> Option<StrideNodeId<Self::AF>> {
        unimplemented!()
    }

    fn get_root_node_id(&self, _stride_size: u8) -> StrideNodeId<Self::AF> {
        StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0)
    }

    fn get_nodes_len(&self) -> usize {
        0
    }

    // Prefixes related methods

    fn load_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial.load(Ordering::Acquire)
    }

    fn increment_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial
            .fetch_add(1, Ordering::Acquire)
    }

    fn acquire_new_prefix_id(
        &self,
        prefix: &InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> PrefixId<Self::AF> {
        PrefixId::<AF>::new(prefix.net, prefix.len)
    }

    fn store_prefix(
        &self,
        _id: PrefixId<Self::AF>,
        _pfx_rec: InternalPrefixRecord<Self::AF, Self::Meta>,
        _serial: usize,
    ) -> Option<PrefixId<Self::AF>> {
        unimplemented!()
    }

    #[allow(clippy::type_complexity)]
    fn upsert_prefix(
        &mut self,
        pfx_rec: InternalPrefixRecord<Self::AF, Self::Meta>,
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
                        // This doesn't have to be an aotmid pointer, since
                        // we're doing this in one transaction.
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
    fn retrieve_prefix_mut_with_guard<'a>(
        &'a mut self,
        id: PrefixId<Self::AF>,
        guard: &'a Guard,
    ) -> (&'a mut StoredPrefix<Self::AF, Self::Meta>, u8) {
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
    fn retrieve_prefix(
        &self,
        id: PrefixId<Self::AF>,
    ) -> Option<InternalPrefixRecord<Self::AF, Self::Meta>> {
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
    fn retrieve_prefix_with_guard<'a>(
        &'a self,
        id: PrefixId<Self::AF>,
        guard: &'a Guard,
    ) -> Option<(&InternalPrefixRecord<Self::AF, Self::Meta>, &'a usize)>
    {
        struct SearchLevel<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                u8,
                &'a Guard,
                // InternalPrefixRecord<AF, M>,
            ) -> Option<(
                &'a InternalPrefixRecord<AF, M>,
                &'a usize,
            )>,
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
        index: PrefixId<Self::AF>,
    ) -> Option<InternalPrefixRecord<Self::AF, Self::Meta>> {
        match index.is_empty() {
            false => self.prefixes.remove(index),
            true => None,
        }
    }

    fn get_prefixes_len(&self) -> usize {
        (0..=AF::BITS)
            .map(|pfx_len| -> usize {
                self.prefixes
                    .get_root_prefix_set(pfx_len)
                    .get_len_recursive()
            })
            .sum()
    }


    // Stride related methods

    fn get_stride_for_id(&self, id: StrideNodeId<Self::AF>) -> u8 {
        self.buckets.get_stride_for_id(id)
    }

    fn get_stride_sizes(&self) -> &[u8] {
        self.buckets.get_stride_sizes()
    }

    fn get_strides_len() -> u8 {
        NB::get_strides_len()
    }

    fn get_first_stride_size() -> u8 {
        NB::get_first_stride_size()
    }
}

impl<
        AF: AddressFamily,
        Meta: routecore::record::Meta + MergeUpdate,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, Meta>,
    > CustomAllocStorage<AF, Meta, NB, PB>
{
    pub fn prefixes_iter<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> PrefixIter<AF, Meta, PB> {
        PrefixIter {
            prefixes: &self.prefixes,
            cur_bucket: self.prefixes.get_root_prefix_set(0),
            cur_len: 0,
            cur_level: 0,
            cursor: 0,
            parent: [None; 26],
            guard,
            _af: PhantomData,
            _meta: PhantomData,
        }
    }
}

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
    parent: [Option<(&'a PrefixSet<AF, M>, usize)>; 26],
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
            // trace!("node {:?}", self.cur_bucket);

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
                self.parent = [None; 26];

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
            // EXIT CONDITIONS FOR THIS BUCKET
            trace!(
                "c{} lvl{} len{} bsize {}",
                self.cursor,
                self.cur_level,
                self.cur_len,
                bucket_size
            );

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
                    self.parent = [None; 26];

                    if self.cur_len > AF::BITS as u8 {
                        // This is the end, my friend
                        trace!(
                            "reached max length {}, returning None",
                            self.cur_len
                        );
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
                    match self.parent[self.cur_level as usize] {
                        Some(parent) => {
                            trace!("back up one level");

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
                            trace!("parent {:?}", self.parent);
                            panic!(
                                "Where do we belong? Where do we come from?"
                            );
                        }
                    };
                }
                // continue;
            };

            // we're somewhere in the PrefixSet iteration, read the next StoredPrefix.
            // We are doing depth-first iteration, so we check for a child first and
            // descend into that if it exists.
            trace!(
                "c{} l{} len {}",
                self.cursor,
                self.cur_level,
                self.cur_len
            );

            let s_pfx = self
                .cur_bucket
                .get_by_index(self.cursor as usize, self.guard);
            trace!("s_pfx {:?}", s_pfx);
            // DEPTH FIRST ITERATION
            match s_pfx.get_next_bucket(self.guard) {
                Some(bucket) => {
                    // DESCEND ONe LEVEL
                    // There's a child here, descend into it, but...
                    trace!("C. got next bucket {:?}", bucket);

                    // save our parent and cursor position first, and then..
                    self.parent[(self.cur_level + 1) as usize] =
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
