use std::{
    fmt::Debug,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::{self as epoch, Atomic};
use dashmap::DashMap;
use epoch::Guard;

use crate::local_array::storage_backend::StorageBackend;
use crate::local_array::tree::*;

use crate::{impl_search_level, impl_write_level};
use crate::prefix_record::InternalPrefixRecord;

use crate::af::AddressFamily;
use routecore::record::{MergeUpdate, Meta};

use super::storage_backend::{
    PrefixHashMap, SizedNodeRefOption, StrideReadStore, StrideWriteStore,
};

// #[derive(Debug)]
// pub(crate) struct NodeSet<AF: AddressFamily, S: Stride>(
//     pub Box<[StoredNode<AF, S>]>,
// );

#[derive(Debug)]
pub(crate) struct NodeSet<AF: AddressFamily, S: Stride>(
    pub Atomic<[MaybeUninit<StoredNode<AF, S>>]>,
)
where
    MaybeNode<AF, S>: Sized;

pub(crate) struct MaybeNode<AF: AddressFamily, S: Stride>(
    MaybeUninit<StoredNode<AF, S>>,
);

#[derive(Debug)]
pub(crate) enum StoredNode<AF, S>
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

#[derive(Debug)]
pub(crate) struct LenToBits([[u8; 10]; 32]);

#[derive(Debug)]
pub(crate) struct CustomAllocStorage<
    AF: AddressFamily,
    Meta: routecore::record::Meta,
> {
    pub(crate) l5: NodeSet<AF, Stride5>,
    pub(crate) l10: NodeSet<AF, Stride5>,
    pub(crate) l14: NodeSet<AF, Stride4>,
    pub(crate) l17: NodeSet<AF, Stride3>,
    pub(crate) l20: NodeSet<AF, Stride3>,
    pub(crate) l24: NodeSet<AF, Stride3>,
    pub(crate) l28: NodeSet<AF, Stride3>,
    pub(crate) l32: NodeSet<AF, Stride3>,
    pub(crate) prefixes:
        DashMap<PrefixId<AF>, InternalPrefixRecord<AF, Meta>>,
    pub(crate) len_to_stride_size: [StrideType; 128],
    pub(crate) len_to_store_bits: LenToBits,
    pub default_route_prefix_serial: AtomicUsize,
}

impl<AF: AddressFamily, Meta: routecore::record::Meta + MergeUpdate>
    CustomAllocStorage<AF, Meta>
{
    pub(crate) fn len_to_store_bits() -> LenToBits {
        // (hor x vert) = level x len -> number of bits
        LenToBits([
            [1, 0, 0, 0, 0, 0, 0, 0, 0, 0], // len 1
            [2, 0, 0, 0, 0, 0, 0, 0, 0, 0], // len 2
            [3, 0, 0, 0, 0, 0, 0, 0, 0, 0], // len 3
            [4, 0, 0, 0, 0, 0, 0, 0, 0, 0], // etc.
            [5, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [6, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [7, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [8, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [9, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [10, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [11, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 1, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 2, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 3, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 4, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 5, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 6, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 7, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 8, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 9, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 10, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 11, 0, 0, 0, 0, 0, 0, 0, 0],
            [12, 12, 0, 0, 0, 0, 0, 0, 0, 0],
            [4, 4, 4, 4, 4, 4, 1, 0, 0, 0],
            [4, 4, 4, 4, 4, 4, 2, 0, 0, 0],
            [4, 4, 4, 4, 4, 4, 3, 0, 0, 0],
            [4, 4, 4, 4, 4, 4, 4, 0, 0, 0],
            [4, 4, 4, 4, 4, 4, 5, 0, 0, 0],
            [4, 4, 4, 4, 4, 4, 6, 0, 0, 0],
            [4, 4, 4, 4, 4, 4, 7, 0, 0, 0],
            [4, 4, 4, 4, 4, 4, 4, 4, 0, 0],
        ])
    }
}

impl<AF: AddressFamily, Meta: routecore::record::Meta + MergeUpdate>
    StorageBackend for CustomAllocStorage<AF, Meta>
{
    type AF = AF;
    type Meta = Meta;

    fn init(
        len_to_stride_size: [StrideType; 128],
        root_node: SizedStrideNode<Self::AF>,
    ) -> Self {
        let len_to_store_bits = Self::len_to_store_bits();
        let mut store = CustomAllocStorage {
            l5: NodeSet(Atomic::init(1 << 5)),
            l10: NodeSet(Atomic::init(1 << 10)),
            l14: NodeSet(Atomic::init(1 << 12)),
            l17: NodeSet(Atomic::init(1 << 12)),
            l20: NodeSet(Atomic::init(1 << 12)),
            l24: NodeSet(Atomic::init(1 << 12)),
            l28: NodeSet(Atomic::init(1 << 4)),
            l32: NodeSet(Atomic::init(1 << 4)),
            prefixes: DashMap::new(),
            len_to_stride_size,
            len_to_store_bits,
            default_route_prefix_serial: AtomicUsize::new(0),
        };
        store.store_node(
            StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0),
            root_node,
        );
        store
    }

    fn acquire_new_node_id(
        &self,
        // sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        //
        (prefix_net, sub_prefix_len): (Self::AF, u8),
    ) -> StrideNodeId<Self::AF> {
        StrideNodeId::new_with_cleaned_id(prefix_net, sub_prefix_len)
    }

    // Create a new node in the store with paylaod `next_node`.
    //
    // Next node will be ignored if a node with the same `id` already exists.
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
                [u8; 10],
                u8,
            ) -> Option<StrideNodeId<AF>>,
        }

        let search_level_3 = impl_write_level![Stride3; id;];
        let search_level_4 = impl_write_level![Stride4; id;];
        let search_level_5 = impl_write_level![Stride5; id;];

        match next_node {
            SizedStrideNode::Stride3(new_node) => (search_level_3.f)(
                &search_level_3,
                match id.get_id().1 {
                    17 => &mut self.l17,
                    20 => &mut self.l20,
                    24 => &mut self.l24,
                    28 => &mut self.l28,
                    32 => &mut self.l32,
                    _ => panic!("unexpected sub prefix length"),
                },
                new_node,
                self.len_to_store_bits.0[id.get_id().1 as usize],
                0,
            ),
            SizedStrideNode::Stride4(new_node) => (search_level_4.f)(
                &search_level_4,
                match id.get_id().1 {
                    14 => &mut self.l14,
                    _ => panic!("unexpected sub prefix length"),
                },
                new_node,
                self.len_to_store_bits.0[id.get_id().1 as usize],
                0,
            ),
            SizedStrideNode::Stride5(new_node) => (search_level_5.f)(
                &search_level_5,
                match id.get_id().1 {
                    5 => &mut self.l5,
                    10 => &mut self.l10,
                    _ => panic!("unexpected sub prefix length"),
                },
                new_node,
                self.len_to_store_bits.0[id.get_id().1 as usize],
                0,
            ),
        }
    }

    fn store_node_in_store(
        store: &mut StrideWriteStore<Self::AF>,
        id: StrideNodeId<Self::AF>,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId<Self::AF>> {
        todo!()
    }

    fn update_node(
        &self,
        current_node_id: StrideNodeId<Self::AF>,
        updated_node: SizedStrideNode<Self::AF>,
    ) {
        todo!()
    }

    fn update_node_in_store(
        &self,
        store: &mut StrideWriteStore<Self::AF>,
        current_node_id: StrideNodeId<Self::AF>,
        updated_node: SizedStrideNode<Self::AF>,
    ) {
        todo!()
    }

    fn retrieve_node(
        &self,
        _id: StrideNodeId<AF>,
    ) -> SizedNodeRefOption<'_, Self::AF> {
        unimplemented!()
    }

    fn retrieve_node_with_guard<'a>(
        &'a self,
        id: StrideNodeId<Self::AF>,
        // result_ref: SizedNodeRefOption<'_, Self::AF>,
        guard: &'a Guard,
    ) -> Option<SizedStrideRefMut<'a, Self::AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                [u8; 10],
                u8,
                &'a Guard,
            ) -> Option<SizedStrideRefMut<'a, AF>>,
        }

        let search_level_3 = impl_search_level![Stride3; id;];
        let search_level_4 = impl_search_level![Stride4; id;];
        let search_level_5 = impl_search_level![Stride5; id;];

        // let search_level_3 = SearchLevel {
        //     f: &|search_level: &SearchLevel<AF, Stride3>,
        //          nodes,
        //          bits_division: [u8; 10],
        //          mut level: u8,
        //          guard| {
        //         // Aaaaand, this is all of our hashing function.
        //         // I'll explain later.
        //         let index = id.get_id().0.dangerously_truncate_to_usize()
        //             >> (AF::BITS - bits_division[level as usize]);

        //         // Read the node from the block pointed to by the
        //         // Atomic pointer.
        //         // let guard = &epoch::pin();
        //         let this_node = unsafe {
        //             &nodes.0.load(Ordering::SeqCst, guard).deref()[index]
        //         };
        //         match unsafe { this_node.assume_init_ref() } {
        //             // No node exists, here
        //             StoredNode::Empty => None,
        //             // A node exists, but since we're not using perfect
        //             // hashing everywhere, this may be very well a node
        //             // we're not searching for, so check that.
        //             StoredNode::NodeWithRef((node_id, node, node_set)) => {
        //                 if &id == node_id {
        //                     // YES, It's the one we're looking for!
        //                     return Some(SizedStrideRef::Stride3(node))
        //                 };
        //                 // Meh, it's not, but we can a go to the next level
        //                 // and see if it lives there.
        //                 level += 1;
        //                 match bits_division.get((level) as usize) {
        //                     // on to the next level!
        //                     Some(next_bit_shift) if next_bit_shift > &0 => {
        //                         (search_level.f)(
        //                             search_level,
        //                             node_set,
        //                             // new_node,
        //                             bits_division,
        //                             level,
        //                             // result_node,
        //                             guard,
        //                         )
        //                     }
        //                     // There's no next level, we found nothing.
        //                     _ => None,
        //                 }
        //             }
        //         }
        //     },
        // };
        // let search_level_4 = SearchLevel {
        //     f: &|search_level: &SearchLevel<AF, Stride4>,
        //          nodes,
        //          bits_division: [u8; 10],
        //          mut level: u8,
        //          guard| {
        //         // Aaaaand, this is all of our hashing function.
        //         // I'll explain later.
        //         let index = id.get_id().0.dangerously_truncate_to_usize()
        //             >> (AF::BITS - bits_division[level as usize]);

        //         // Read the node from the block pointed to by the
        //         // Atomic pointer.
        //         // let guard = &epoch::pin();
        //         let this_node = unsafe {
        //             &nodes.0.load(Ordering::SeqCst, guard).deref()[index]
        //         };
        //         match unsafe { this_node.assume_init_ref() } {
        //             // No node exists, here
        //             StoredNode::Empty => None,
        //             // A node exists, but since we're not using perfect
        //             // hashing everywhere, this may be very well a node
        //             // we're not searching for, so check that.
        //             StoredNode::NodeWithRef((node_id, node, node_set)) => {
        //                 if &id == node_id {
        //                     // YES, It's the one we're looking for!
        //                     return Some(SizedStrideRef::Stride4(node));
        //                 };
        //                 // Meh, it's not, but we can a go to the next level
        //                 // and see if it lives there.
        //                 level += 1;
        //                 match bits_division.get((level) as usize) {
        //                     // on to the next level!
        //                     Some(next_bit_shift) if next_bit_shift > &0 => {
        //                         (search_level.f)(
        //                             search_level,
        //                             node_set,
        //                             // new_node,
        //                             bits_division,
        //                             level,
        //                             // result_node,
        //                             guard,
        //                         )
        //                     }
        //                     // There's no next level, we found nothing.
        //                     _ => None,
        //                 }
        //             }
        //         }
        //     },
        // };
        // let search_level_5 = SearchLevel {
        //     f: &|search_level: &SearchLevel<AF, Stride5>,
        //          nodes,
        //          bits_division: [u8; 10],
        //          mut level: u8,
        //          guard| {
        //         // Aaaaand, this is all of our hashing function.
        //         // I'll explain later.
        //         let index = id.get_id().0.dangerously_truncate_to_usize()
        //             >> (AF::BITS - bits_division[level as usize]);

        //         // Read the node from the block pointed to by the
        //         // Atomic pointer.
        //         // let guard = &epoch::pin();
        //         let this_node = unsafe {
        //             &nodes.0.load(Ordering::SeqCst, guard).deref()[index]
        //         };
        //         match unsafe { this_node.assume_init_ref() } {
        //             // No node exists, here
        //             StoredNode::Empty => None,
        //             // A node exists, but since we're not using perfect
        //             // hashing everywhere, this may be very well a node
        //             // we're not searching for, so check that.
        //             StoredNode::NodeWithRef((node_id, node, node_set)) => {
        //                 if &id == node_id {
        //                     // YES, It's the one we're looking for!
        //                     return Some(SizedStrideRef::Stride5(node));
        //                 };
        //                 // Meh, it's not, but we can a go to the next level
        //                 // and see if it lives there.
        //                 level += 1;
        //                 match bits_division.get((level) as usize) {
        //                     // on to the next level!
        //                     Some(next_bit_shift) if next_bit_shift > &0 => {
        //                         (search_level.f)(
        //                             search_level,
        //                             node_set,
        //                             // new_node,
        //                             bits_division,
        //                             level,
        //                             // result_node,
        //                             guard,
        //                         )
        //                     }
        //                     // There's no next level, we found nothing.
        //                     _ => None,
        //                 }
        //             }
        //         }
        //     },
        // };
        match self.get_stride_for_id(id) {
            StrideType::Stride3 => (search_level_3.f)(
                &search_level_3,
                match id.get_id().1 {
                    17 => &self.l17,
                    20 => &self.l20,
                    24 => &self.l24,
                    28 => &self.l28,
                    32 => &self.l32,
                    _ => panic!("unexpected sub prefix length"),
                },
                self.len_to_store_bits.0[id.get_id().1 as usize],
                0,
                // result_node,
                guard,
            ),

            StrideType::Stride4 => (search_level_4.f)(
                &search_level_4,
                match id.get_id().1 {
                    14 => &self.l14,
                    _ => panic!("unexpected sub prefix length"),
                },
                self.len_to_store_bits.0[id.get_id().1 as usize],
                0,
                // result_node,
                guard,
            ),
            StrideType::Stride5 => (search_level_5.f)(
                &search_level_5,
                match id.get_id().1 {
                    5 => &self.l5,
                    10 => &self.l10,
                    _ => panic!("unexpected sub prefix length"),
                },
                self.len_to_store_bits.0[id.get_id().1 as usize],
                0,
                // result_node,
                guard,
            ),
        }
    }

    fn store_node_with_guard(
        &self,
        current_node: SizedNodeRefOption<Self::AF>,
        next_node: SizedStrideNode<AF>,
        guard: &epoch::Guard,
    ) -> Option<StrideNodeId<Self::AF>> {
        todo!()
    }

    fn get_root_node_id(&self, stride_size: u8) -> StrideNodeId<Self::AF> {
        StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0)
    }

    fn load_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial.load(Ordering::Acquire)
    }

    fn increment_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial
            .fetch_add(1, Ordering::Acquire)
    }

    fn get_nodes_len(&self) -> usize {
        0
    }

    fn acquire_new_prefix_id(
        &self,
        prefix: &InternalPrefixRecord<Self::AF, Self::Meta>,
        // sort: &<<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
    ) -> PrefixId<Self::AF> {
        PrefixId::<AF>::new(prefix.net, prefix.len).set_serial(1)
    }

    fn store_prefix(
        &self,
        id: PrefixId<Self::AF>,
        next_node: InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> Result<PrefixId<Self::AF>, Box<dyn std::error::Error>> {
        self.prefixes.insert(id, next_node);
        Ok(id)
    }

    fn retrieve_prefix(
        &self,
        part_id: PrefixId<Self::AF>,
    ) -> Option<&'_ InternalPrefixRecord<Self::AF, Self::Meta>> {
        self.prefixes.get(&part_id).map(|p| p.value())
    }

    fn remove_prefix(
        &self,
        index: PrefixId<Self::AF>,
    ) -> Option<InternalPrefixRecord<Self::AF, Self::Meta>> {
        match index.is_empty() {
            false => self.prefixes.remove(&index).map(|p| p.1),
            true => None,
        }
    }

    fn get_prefixes(&'_ self) -> &'_ PrefixHashMap<Self::AF, Self::Meta> {
        &self.prefixes
    }

    fn get_prefixes_clear(&self) -> &PrefixHashMap<Self::AF, Self::Meta> {
        &self.prefixes
    }

    fn get_prefixes_len(&self) -> usize {
        self.prefixes.len()
    }

    fn get_stride_for_id(
        &self,
        id: StrideNodeId<Self::AF>,
    ) -> crate::local_array::tree::StrideType {
        todo!()
    }

    fn get_stride_for_id_with_read_store(
        &self,
        id: StrideNodeId<Self::AF>,
    ) -> (StrideNodeId<Self::AF>, StrideReadStore<Self::AF>) {
        todo!()
    }

    fn get_stride_for_id_with_write_store(
        &self,
        id: StrideNodeId<Self::AF>,
    ) -> (StrideNodeId<Self::AF>, StrideWriteStore<Self::AF>) {
        todo!()
    }
}
