use std::{
    fmt::Debug,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::{self as epoch, Atomic};
use dashmap::DashMap;
use epoch::{Guard, Owned};

use rotonda_macros::stride_sizes;

use crate::local_array::storage_backend::StorageBackend;
use crate::local_array::tree::*;

use crate::prefix_record::InternalPrefixRecord;
use crate::{impl_search_level, impl_search_level_mut, impl_write_level};

use crate::af::AddressFamily;
use routecore::record::MergeUpdate;

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

impl<AF: AddressFamily, S: Stride> NodeSet<AF, S> {
    fn init(size: usize) -> Self {
        let mut l = Owned::<[MaybeUninit<StoredNode<AF, S>>]>::init(size);
        for i in 0..size {
            l[i] = MaybeUninit::new(StoredNode::Empty);
        }
        NodeSet(l.into())
    }
}

#[derive(Debug)]
pub(crate) struct LenToBits([[u8; 10]; 33]);

#[derive(Debug)]
pub(crate) struct CustomAllocStorage<
    AF: AddressFamily,
    Meta: routecore::record::Meta,
    Buckets: FamilyBuckets<AF>,
> {
    pub(crate) buckets: Buckets,
    pub(crate) prefixes:
        DashMap<PrefixId<AF>, InternalPrefixRecord<AF, Meta>>,
    // pub(crate) len_to_stride_size: [StrideType; 128],
    pub default_route_prefix_serial: AtomicUsize,
}

pub(crate) trait FamilyBuckets<AF: AddressFamily> {
    fn init() -> Self;
    fn len_to_store_bits(len: u8, level: u8) -> Option<&'static u8>;
    fn get_stride_sizes(&self) -> [u8; 42];
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
}

#[stride_sizes((IPv4, [5, 5, 4, 3, 3, 3, 3, 3, 3]))]
pub(crate) struct NodeBuckets4<AF: AddressFamily>;

// #[derive(Debug)]
// pub(crate) struct NodeBuckets4<AF: AddressFamily> {
//     l0: NodeSet<AF, Stride5>,
//     l5: NodeSet<AF, Stride5>,
//     l10: NodeSet<AF, Stride4>,
//     l14: NodeSet<AF, Stride3>,
//     l17: NodeSet<AF, Stride3>,
//     l20: NodeSet<AF, Stride3>,
//     l23: NodeSet<AF, Stride3>,
//     l26: NodeSet<AF, Stride3>,
//     l29: NodeSet<AF, Stride3>,
// }

// impl<AF: AddressFamily> FamilyBuckets<AF> for NodeBuckets4<AF> {
//     fn init() -> Self {
//         NodeBuckets4 {
//             l0: NodeSet::init(1 << 5),
//             l5: NodeSet::init(1 << 10),
//             l10: NodeSet::init(1 << 12),
//             l14: NodeSet::init(1 << 12),
//             l17: NodeSet::init(1 << 12),
//             l20: NodeSet::init(1 << 12),
//             l23: NodeSet::init(1 << 12),
//             l26: NodeSet::init(1 << 4),
//             l29: NodeSet::init(1 << 4),
//         }
//     }

//     fn len_to_store_bits(len: u8, level: u8) -> Option<&'static u8> {
//         // (vert x hor) = len x level -> number of bits
//         [
//             [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 0
//             [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 1
//             [2, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 2 - never exists
//             [3, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 3
//             [4, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 4
//             [5, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 5
//             [6, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 6
//             [7, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 7
//             [8, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 8
//             [9, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 9
//             [10, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 10
//             [11, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 11
//             [12, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 12
//             [12, 13, 0, 0, 0, 0, 0, 0, 0, 0],     // 13
//             [12, 14, 0, 0, 0, 0, 0, 0, 0, 0],     // 14
//             [12, 15, 0, 0, 0, 0, 0, 0, 0, 0],     // 15
//             [12, 16, 0, 0, 0, 0, 0, 0, 0, 0],     // 16
//             [12, 17, 0, 0, 0, 0, 0, 0, 0, 0],     // 17
//             [12, 18, 0, 0, 0, 0, 0, 0, 0, 0],     // 18
//             [12, 19, 0, 0, 0, 0, 0, 0, 0, 0],     // 19
//             [12, 20, 0, 0, 0, 0, 0, 0, 0, 0],     // 20
//             [12, 21, 0, 0, 0, 0, 0, 0, 0, 0],     // 21
//             [12, 22, 0, 0, 0, 0, 0, 0, 0, 0],     // 22
//             [12, 23, 0, 0, 0, 0, 0, 0, 0, 0],     // 23
//             [12, 24, 0, 0, 0, 0, 0, 0, 0, 0],     // 24
//             [12, 24, 25, 0, 0, 0, 0, 0, 0, 0],    // 25
//             [4, 8, 12, 16, 20, 24, 26, 0, 0, 0],  // 26
//             [4, 8, 12, 16, 20, 24, 27, 0, 0, 0],  // 27
//             [4, 8, 12, 16, 20, 24, 28, 0, 0, 0],  // 28
//             [4, 8, 12, 16, 20, 24, 28, 29, 0, 0], // 29
//             [4, 8, 12, 16, 20, 24, 28, 30, 0, 0], // 30
//         ][len as usize]
//             .get(level as usize)
//     }

//     fn get_store3_mut(
//         &mut self,
//         id: StrideNodeId<AF>,
//     ) -> &mut NodeSet<AF, Stride3> {
//         match id.get_id().1 as usize {
//             14 => &mut self.l14,
//             17 => &mut self.l17,
//             20 => &mut self.l20,
//             23 => &mut self.l23,
//             26 => &mut self.l26,
//             29 => &mut self.l29,
//             _ => panic!(
//                 "unexpected sub prefix length {} in stride size 3 ({})",
//                 id.get_id().1,
//                 id
//             ),
//         }
//     }

//     fn get_store3(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride3> {
//         match id.get_id().1 as usize {
//             14 => &self.l14,
//             17 => &self.l17,
//             20 => &self.l20,
//             23 => &self.l23,
//             26 => &self.l26,
//             29 => &self.l29,
//             _ => panic!(
//                 "unexpected sub prefix length {} in stride size 3 ({})",
//                 id.get_id().1,
//                 id
//             ),
//         }
//     }

//     fn get_store4_mut(
//         &mut self,
//         id: StrideNodeId<AF>,
//     ) -> &mut NodeSet<AF, Stride4> {
//         match id.get_id().1 as usize {
//             10 => &mut self.l10,
//             _ => panic!(
//                 "unexpected sub prefix length {} in stride size 4 ({})",
//                 id.get_id().1,
//                 id
//             ),
//         }
//     }

//     fn get_store4(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride4> {
//         match id.get_id().1 as usize {
//             10 => &self.l10,
//             _ => panic!(
//                 "unexpected sub prefix length {} in stride size 4 ({})",
//                 id.get_id().1,
//                 id
//             ),
//         }
//     }

//     fn get_store5_mut(
//         &mut self,
//         id: StrideNodeId<AF>,
//     ) -> &mut NodeSet<AF, Stride5> {
//         match id.get_id().1 as usize {
//             0 => &mut self.l0,
//             5 => &mut self.l5,
//             _ => panic!(
//                 "unexpected sub prefix length {} in stride size 3 ({})",
//                 id.get_id().1,
//                 id
//             ),
//         }
//     }

//     fn get_store5(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride5> {
//         match id.get_id().1 as usize {
//             0 => &self.l0,
//             5 => &self.l5,
//             _ => panic!(
//                 "unexpected sub prefix length {} in stride size 3 ({})",
//                 id.get_id().1,
//                 id
//             ),
//         }
//     }
// }

pub(crate) struct NodeBuckets6<AF: AddressFamily> {
    l0: NodeSet<AF, Stride4>,
    l4: NodeSet<AF, Stride4>,
    l8: NodeSet<AF, Stride4>,
    l12: NodeSet<AF, Stride4>,
    l16: NodeSet<AF, Stride4>,
    l20: NodeSet<AF, Stride4>,
    l24: NodeSet<AF, Stride4>,
    l28: NodeSet<AF, Stride4>,
    l32: NodeSet<AF, Stride4>,
    l36: NodeSet<AF, Stride4>,
    l40: NodeSet<AF, Stride4>,
    l44: NodeSet<AF, Stride4>,
    l48: NodeSet<AF, Stride4>,
    l52: NodeSet<AF, Stride4>,
    l56: NodeSet<AF, Stride4>,
    l60: NodeSet<AF, Stride4>,
    l64: NodeSet<AF, Stride4>,
    l68: NodeSet<AF, Stride4>,
    l72: NodeSet<AF, Stride4>,
    l76: NodeSet<AF, Stride4>,
    l80: NodeSet<AF, Stride4>,
    l84: NodeSet<AF, Stride4>,
    l88: NodeSet<AF, Stride4>,
    l92: NodeSet<AF, Stride4>,
    l96: NodeSet<AF, Stride4>,
    l100: NodeSet<AF, Stride4>,
    l104: NodeSet<AF, Stride4>,
    l108: NodeSet<AF, Stride4>,
    l112: NodeSet<AF, Stride4>,
    l116: NodeSet<AF, Stride4>,
    l120: NodeSet<AF, Stride4>,
    l124: NodeSet<AF, Stride4>,
}

impl<AF: AddressFamily> FamilyBuckets<AF> for NodeBuckets6<AF> {
    fn init() -> Self {
        NodeBuckets6 {
            l0: NodeSet::init(1 << 4),
            l4: NodeSet::init(1 << 8),
            l8: NodeSet::init(1 << 12),
            l12: NodeSet::init(1 << 12),
            l16: NodeSet::init(1 << 12),
            l20: NodeSet::init(1 << 12),
            l24: NodeSet::init(1 << 12),
            l28: NodeSet::init(1 << 12),
            l32: NodeSet::init(1 << 12),
            l36: NodeSet::init(1 << 12),
            l40: NodeSet::init(1 << 12),
            l44: NodeSet::init(1 << 12),
            l48: NodeSet::init(1 << 4),
            l52: NodeSet::init(1 << 4),
            l56: NodeSet::init(1 << 4),
            l60: NodeSet::init(1 << 4),
            l64: NodeSet::init(1 << 4),
            l68: NodeSet::init(1 << 4),
            l72: NodeSet::init(1 << 4),
            l76: NodeSet::init(1 << 4),
            l80: NodeSet::init(1 << 4),
            l84: NodeSet::init(1 << 4),
            l88: NodeSet::init(1 << 4),
            l92: NodeSet::init(1 << 4),
            l96: NodeSet::init(1 << 4),
            l100: NodeSet::init(1 << 4),
            l104: NodeSet::init(1 << 4),
            l108: NodeSet::init(1 << 4),
            l112: NodeSet::init(1 << 4),
            l116: NodeSet::init(1 << 4),
            l120: NodeSet::init(1 << 4),
            l124: NodeSet::init(1 << 4),
        }
    }

    #[rustfmt::skip]
    fn len_to_store_bits(len: u8, level: u8) -> Option<&'static u8> {
        // (vert x hor) = len x level -> number of bits
        [
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 0
            [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 1 - never exists
            [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 2
            [3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 3
            [4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 4
            [5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 5
            [6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 6
            [7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 7
            [8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 8
            [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 9
            [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // len 10
            [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // len 11
            [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // len 12
            [12, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 13
            [12, 14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 14
            [12, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 15
            [12, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 16
            [12, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 17
            [12, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 18
            [12, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 19
            [12, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 20
            [12, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 21
            [12, 22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 22
            [12, 23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 23
            [12, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 24
            [12, 24, 25, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 25
            [12, 24, 26, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 26
            [12, 24, 27, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 27
            [12, 24, 28, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 28
            [12, 24, 28, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 29
            [12, 24, 28, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 30
            [12, 24, 28, 31, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 31
            [12, 24, 28, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 32
            [12, 24, 28, 33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 33
            [12, 24, 28, 34, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 34
            [12, 24, 28, 35, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 35
            [12, 24, 28, 36, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 36
            [12, 24, 28, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 37
            [12, 24, 28, 38, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 38
            [12, 24, 28, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 39
            [12, 24, 28, 40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 40
            [12, 24, 28, 41, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 41
            [12, 24, 28, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 42
            [12, 24, 28, 43, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 43
            [12, 24, 28, 44, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 44
            [12, 24, 28, 45, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 45
            [12, 24, 28, 46, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 46
            [12, 24, 28, 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 47
            [4, 8, 12, 24, 28, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 48
            [4, 8, 12, 24, 28, 48, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 49
            [4, 8, 12, 24, 28, 48, 50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 50
            [4, 8, 12, 24, 28, 48, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 51
            [4, 8, 12, 24, 28, 48, 52, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 52
            [4, 8, 12, 24, 28, 48, 52, 53, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 53
            [4, 8, 12, 24, 28, 48, 52, 54, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 54
            [4, 8, 12, 24, 28, 48, 52, 55, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 55
            [4, 8, 12, 24, 28, 48, 52, 56, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 56
            [4, 8, 12, 24, 28, 48, 52, 56, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 57
            [4, 8, 12, 24, 28, 48, 52, 56, 58, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 58
            [4, 8, 12, 24, 28, 48, 52, 56, 59, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 59
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 60
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 61, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 61
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 62, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 62
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 63
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 64
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 65
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 66, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 66
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 67
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 68
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 69, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 69
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 70
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 71, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 71
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 72, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 72
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 73, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 73
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 74
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 75, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 75
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 76, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 76
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 77, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 77
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 78
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 79, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 79
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 80
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 81
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 82
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 83, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],        // 83
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],        // 84
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 85, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 85
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 86, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 86
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 87, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 87
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 88
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 89, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 89
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 90, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 90
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 91, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 91
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 92
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 93, 0, 0, 0, 0, 0, 0, 0, 0],     // 93
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 94, 0, 0, 0, 0, 0, 0, 0, 0],     // 94
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 95, 0, 0, 0, 0, 0, 0, 0, 0],     // 95
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 0, 0, 0, 0, 0, 0, 0, 0],     // 96
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 97, 0, 0, 0, 0, 0, 0, 0],    // 97
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 98, 0, 0, 0, 0, 0, 0, 0],    // 98
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 99, 0, 0, 0, 0, 0, 0, 0],        // 99
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 0, 0, 0, 0, 0, 0, 0],       // 100
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 101, 0, 0, 0, 0, 0, 0],     // 101
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 102, 0, 0, 0, 0, 0, 0],     // 102
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 103, 0, 0, 0, 0, 0, 0],     // 103
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 0, 0, 0, 0, 0, 0],     // 104
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 105, 0, 0, 0, 0, 0],   // 105
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 106, 0, 0, 0, 0, 0],       // 106
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 107, 0, 0, 0, 0, 0],       // 107
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 0, 0, 0, 0, 0],       // 108
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 109, 0, 0, 0, 0],     // 109
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 110, 0, 0, 0, 0],     // 110
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 111, 0, 0, 0, 0],           // 111
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 0, 0, 0, 0],           // 112
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 113, 0, 0, 0],         // 113
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 114, 0, 0, 0],         // 114
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 115, 0, 0, 0],         // 115
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 0, 0, 0],         // 116
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 117, 0, 0],       // 117
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 118, 0, 0],       // 118
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 119, 0, 0],       // 119
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 0, 0],       // 120
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 121, 0],     // 121
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 122, 0],     // 122
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 123, 0],     // 123
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 0],     // 124
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 125],   // 125
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 126],   // 126
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 127],   // 127
            [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 128],   // 128
        ][len as usize]
            .get(level as usize)
    }

    fn get_store3_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride3> {
        panic!(
            "unexpected sub prefix length {} in stride size 3 ({})",
            id.get_id().1,
            id
        )
    }

    fn get_store3(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride3> {
        panic!(
            "unexpected sub prefix length {} in stride size 3 ({})",
            id.get_id().1,
            id
        )
    }

    fn get_store4_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride4> {
        match id.get_id().1 as usize {
            0 => &mut self.l0,
            4 => &mut self.l4,
            8 => &mut self.l8,
            12 => &mut self.l12,
            16 => &mut self.l16,
            20 => &mut self.l20,
            24 => &mut self.l24,
            28 => &mut self.l28,
            32 => &mut self.l32,
            36 => &mut self.l36,
            40 => &mut self.l40,
            44 => &mut self.l44,
            48 => &mut self.l48,
            52 => &mut self.l52,
            56 => &mut self.l56,
            60 => &mut self.l60,
            64 => &mut self.l64,
            68 => &mut self.l68,
            72 => &mut self.l72,
            76 => &mut self.l76,
            80 => &mut self.l80,
            84 => &mut self.l84,
            88 => &mut self.l88,
            92 => &mut self.l92,
            96 => &mut self.l96,
            100 => &mut self.l100,
            104 => &mut self.l104,
            108 => &mut self.l108,
            112 => &mut self.l112,
            116 => &mut self.l116,
            120 => &mut self.l120,
            124 => &mut self.l124,
            _ => panic!(
                "unexpected sub prefix length {} in stride size 4 ({})",
                id.get_id().1,
                id
            ),
        }
    }

    fn get_store4(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride4> {
        match id.get_id().1 as usize {
            0 => &self.l0,
            4 => &self.l4,
            8 => &self.l8,
            12 => &self.l12,
            16 => &self.l16,
            20 => &self.l20,
            24 => &self.l24,
            28 => &self.l28,
            32 => &self.l32,
            36 => &self.l36,
            40 => &self.l40,
            44 => &self.l44,
            48 => &self.l48,
            52 => &self.l52,
            56 => &self.l56,
            60 => &self.l60,
            64 => &self.l64,
            68 => &self.l68,
            72 => &self.l72,
            76 => &self.l76,
            80 => &self.l80,
            84 => &self.l84,
            88 => &self.l88,
            92 => &self.l92,
            96 => &self.l96,
            100 => &self.l100,
            104 => &self.l104,
            108 => &self.l108,
            112 => &self.l112,
            116 => &self.l116,
            120 => &self.l120,
            124 => &self.l124,
            _ => panic!(
                "unexpected sub prefix length {} in stride size 4 ({})",
                id.get_id().1,
                id
            ),
        }
    }

    fn get_store5_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride5> {
        panic!(
            "unexpected sub prefix length {} in stride size 3 ({})",
            id.get_id().1,
            id
        )
    }

    fn get_store5(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride5> {
        panic!(
            "unexpected sub prefix length {} in stride size 3 ({})",
            id.get_id().1,
            id
        )
    }

    fn get_stride_sizes(&self) -> [u8; 42] {
        [
            4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
            4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    }

    fn get_stride_for_id(&self, _id: StrideNodeId<AF>) -> u8 {
        4
    }
}

impl<
        AF: AddressFamily,
        Meta: routecore::record::Meta + MergeUpdate,
        Buckets: FamilyBuckets<AF>,
    > StorageBackend for CustomAllocStorage<AF, Meta, Buckets>
{
    type AF = AF;
    type Meta = Meta;

    fn init(
        // len_to_stride_size: [StrideType; 128],
        root_node: SizedStrideNode<Self::AF>,
    ) -> Self {
        println!("init");

        let mut store = CustomAllocStorage {
            buckets: Buckets::init(),
            prefixes: DashMap::new(),
            // len_to_stride_size,
            default_route_prefix_serial: AtomicUsize::new(0),
        };

        store.store_node(
            StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0),
            root_node,
        );
        store
    }

    // fn len_to_store_bits(&self, len: u8, level: u8) -> Option<u8> {
    //     <Buckets as FamilyBuckets<AF>>::len_to_store_bits(len, level)
    // }

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

        println!("insert node {}: {:?}", id, next_node);
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

    fn store_node_in_store(
        _store: &mut StrideWriteStore<Self::AF>,
        _id: StrideNodeId<Self::AF>,
        _next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId<Self::AF>> {
        todo!()
    }

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

    fn update_node_in_store(
        &self,
        _store: &mut StrideWriteStore<Self::AF>,
        _current_node_id: StrideNodeId<Self::AF>,
        _updated_node: SizedStrideNode<Self::AF>,
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
                println!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3(id),
                    0,
                    guard,
                )
            }

            4 => {
                println!("retrieve node {} from l{}", id, id.get_id().1);
                // println!("{:?}", self.l0);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4(id),
                    0,
                    guard,
                )
            }
            _ => {
                println!("retrieve node {} from l{}", id, id.get_id().1);
                // println!("{:?}", self.l0);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5(id),
                    0,
                    guard,
                )
            }
        }
    }

    fn retrieve_node_mut_with_guard<'a>(
        &'a self,
        id: StrideNodeId<Self::AF>,
        // result_ref: SizedNodeRefOption<'_, Self::AF>,
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
                println!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3(id),
                    0,
                    guard,
                )
            }

            4 => {
                println!("retrieve node {} from l{}", id, id.get_id().1);
                // println!("{:?}", self.l0);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4(id),
                    0,
                    guard,
                )
            }
            _ => {
                println!("retrieve node {} from l{}", id, id.get_id().1);
                // println!("{:?}", self.l0);
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
        todo!()
    }

    fn get_root_node_id(&self, _stride_size: u8) -> StrideNodeId<Self::AF> {
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
    ) -> u8 {
        self.buckets.get_stride_for_id(id)
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

    fn get_stride_sizes(&self) -> [u8; 42] {
        self.buckets.get_stride_sizes()
    }
}
