use std::hash::Hash;

use crate::af::AddressFamily;

use crate::local_array::storage_backend::StorageBackend;
use crate::match_node_for_strides;
use crate::prefix_record::InternalPrefixRecord;

#[cfg(feature = "dynamodb")]
use crate::local_array::CacheGuard;

pub(crate) use super::atomic_stride::*;
use super::storage_backend::{SizedNodeRefOption, SizedNodeRefResult};
use crate::stats::{SizedStride, StrideStats};

pub(crate) use crate::local_array::node::TreeBitMapNode;

use std::sync::atomic::{
    AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::{
    fmt::{Binary, Debug},
    marker::PhantomData,
};

#[cfg(feature = "cli")]
use ansi_term::Colour;

use routecore::record::MergeUpdate;

//------------------- Unsized Node Enums ------------------------------------

pub(crate) trait UnsizedNode<AF: AddressFamily> {}

// No, no, NO, NO, no, no! We're not going to Box this, because that's slow!
// This enum is never used to store nodes/prefixes, it's only to be used in
// generic code.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum SizedStrideNode<AF: AddressFamily> {
    Stride3(TreeBitMapNode<AF, Stride3, 14, 8>),
    Stride4(TreeBitMapNode<AF, Stride4, 30, 16>),
    Stride5(TreeBitMapNode<AF, Stride5, 62, 32>),
    // Stride6(TreeBitMapNode<AF, Stride6, NodeId, 126, 64>),
    // Stride7(TreeBitMapNode<AF, Stride7, NodeId, 254, 128>),
    // Stride8(TreeBitMapNode<AF, Stride8, NodeId, 510, 256>),
}

impl<AF, S, const PFXARRAYSIZE: usize, const PTRARRAYSIZE: usize> Default
    for TreeBitMapNode<AF, S, PFXARRAYSIZE, PTRARRAYSIZE>
where
    AF: AddressFamily,
    S: Stride,
    <S as Stride>::PtrSize: Debug + Binary + Copy,
    <S as Stride>::AtomicPfxSize: AtomicBitmap,
    <S as Stride>::AtomicPtrSize: AtomicBitmap,
{
    fn default() -> Self {
        Self {
            ptrbitarr: <<S as Stride>::AtomicPtrSize as AtomicBitmap>::new(),
            pfxbitarr: <<S as Stride>::AtomicPfxSize as AtomicBitmap>::new(),
            pfx_vec: PrefixSet::empty(),
            ptr_vec: NodeSet::empty(),
            _af: PhantomData,
        }
    }
}

impl<AF: AddressFamily> UnsizedNode<AF> for SizedStrideNode<AF> {}

impl<AF> Default for SizedStrideNode<AF>
where
    AF: AddressFamily,
{
    fn default() -> Self {
        SizedStrideNode::Stride3(TreeBitMapNode {
            ptrbitarr: AtomicStride2(AtomicU8::new(0)),
            pfxbitarr: AtomicStride3(AtomicU16::new(0)),
            pfx_vec: PrefixSet::empty(),
            ptr_vec: NodeSet::empty(),
            _af: PhantomData,
        })
    }
}

// Used to create a public iterator over all nodes.
#[derive(Debug)]
pub enum SizedStrideRef<'a, AF: AddressFamily> {
    Stride3(&'a TreeBitMapNode<AF, Stride3, 14, 8>),
    Stride4(&'a TreeBitMapNode<AF, Stride4, 30, 16>),
    Stride5(&'a TreeBitMapNode<AF, Stride5, 62, 32>),
    // Stride6(&'a TreeBitMapNode<AF, Stride6, NodeId, 126, 64>),
    // Stride7(&'a TreeBitMapNode<AF, Stride7, NodeId, 254, 128>),
    // Stride8(&'a TreeBitMapNode<AF, Stride8, NodeId, 510, 256>),
}

#[derive(Debug)]
pub(crate) enum SizedStrideRefMut<'a, AF: AddressFamily> {
    Stride3(&'a mut TreeBitMapNode<AF, Stride3, 14, 8>),
    Stride4(&'a mut TreeBitMapNode<AF, Stride4, 30, 16>),
    Stride5(&'a mut TreeBitMapNode<AF, Stride5, 62, 32>),
    // Stride6(&'a TreeBitMapNode<AF, Stride6, NodeId, 126, 64>),
    // Stride7(&'a TreeBitMapNode<AF, Stride7, NodeId, 254, 128>),
    // Stride8(&'a TreeBitMapNode<AF, Stride8, NodeId, 510, 256>),
}

// impl<'a, AF, NodeId> Default for SizedStrideRefMut<'a, AF, NodeId>
// where
//     AF: AddressFamily,
//     NodeId: SortableNodeId + Copy,
// {
//     fn default() -> Self {
//         SizedStrideRefMut::Stride3(&mut TreeBitMapNode {
//             ptrbitarr: AtomicStride2(AtomicU8::new(0)),
//             pfxbitarr: AtomicStride3(AtomicU16::new(0)),
//             pfx_vec: NodeSet::empty(),
//             ptr_vec: NodeSet::empty(),
//             _af: PhantomData,
//         })
//     }
// }

impl<'a, AF: AddressFamily> UnsizedNode<AF> for SizedStrideRef<'a, AF> {}

pub(crate) trait NodeWrapper<AF: AddressFamily> {
    type Unsized: UnsizedNode<AF>;
    type UnsizedRef: UnsizedNode<AF>;
}

pub(crate) enum NewNodeOrIndex<AF: AddressFamily> {
    NewNode(SizedStrideNode<AF>, u16), // New Node and bit_id of the new node
    ExistingNode(StrideNodeId),
    NewPrefix(u16),
    ExistingPrefix(PrefixId<AF>),
}

#[derive(Hash, Eq, PartialEq, Debug, Copy, Clone)]
pub struct PrefixId<AF: AddressFamily>(Option<AF>);

impl<AF: AddressFamily> PrefixId<AF> {
    pub fn new(net: AF, len: u8) -> Self { PrefixId(Some(net << (AF::BITS - len).into())) }
    pub fn is_empty(&self) -> bool { self.0.is_none() }
}

impl<AF: AddressFamily> std::default::Default for PrefixId<AF> {
    fn default() -> Self {
        PrefixId(None)
    }
}

//--------------------- Per-Stride-Node-Id Type ------------------------------------

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct StrideNodeId(StrideType, Option<u32>);

impl StrideNodeId {
    pub fn empty() -> Self {
        Self(StrideType::Stride4, None)
    }

    pub fn new(stride_type: StrideType, index: u32) -> Self {
        Self(stride_type, Some(index))
    }

    pub fn is_empty(&self) -> bool {
        self.1.is_none()
    }

    pub fn get_id(&self) -> u32 {
        self.1.unwrap()
    }

    pub fn into_inner(self) -> (StrideType, Option<u32>) {
        (self.0, self.1)
    }

    pub fn get_stride_type(&self) -> StrideType {
        self.0
    }
}

impl Default for StrideNodeId {
    fn default() -> Self {
        Self(StrideType::Stride4, None)
    }
}

impl std::convert::From<u16> for StrideNodeId {
    fn from(id: u16) -> Self {
        Self(StrideType::Stride4, Some(id as u32))
    }
}

impl std::fmt::Display for StrideNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}",
            self.0,
            self.1
                .map(|x| x.to_string())
                .unwrap_or_else(|| "-".to_string())
        )
    }
}

impl std::convert::From<StrideNodeId> for usize {
    fn from(id: StrideNodeId) -> Self {
        id.1.unwrap() as usize
    }
}

impl std::convert::From<AtomicStrideNodeId> for StrideNodeId {
    fn from(id: AtomicStrideNodeId) -> Self {
        let i = match id.index.load(Ordering::Relaxed) {
            0 => None,
            x => Some(x as u32),
        };
        Self(id.stride_type, i)
    }
}

#[derive(Debug)]
pub struct AtomicStrideNodeId {
    stride_type: StrideType,
    index: AtomicU32,
    serial: AtomicUsize,
}

impl AtomicStrideNodeId {
    pub fn new(stride_type: StrideType, index: u32) -> Self {
        Self {
            stride_type,
            index: AtomicU32::new(index),
            serial: AtomicUsize::new(1),
        }
    }

    pub fn empty() -> Self {
        Self {
            stride_type: StrideType::Stride4,
            index: AtomicU32::new(0),
            serial: AtomicUsize::new(0),
        }
    }

    // get_serial() and update_serial() are intimiately linked in the
    // critical section of updating a node.
    //
    // The layout of the critical section is as follows:
    // 1. get_serial() to retrieve the serial number of the node
    // 2. do work in the critical section
    // 3. store work result in the node
    // 4. update_serial() to update the serial number of the node if
    //    and only if the serial is the same as the one retrieved in step 1.
    // 5. check the result of update_serial(). When succesful, we're done,
    //    otherwise, rollback the work result & repeat from step 1.
    pub fn get_serial(&self) -> usize {
        let serial = self.serial.load(Ordering::Relaxed);
        std::sync::atomic::fence(Ordering::Acquire);
        serial
    }

    pub fn update_serial(
        &self,
        current_serial: usize,
    ) -> Result<usize, usize> {
        std::sync::atomic::fence(Ordering::Release);
        self.serial.compare_exchange(
            current_serial,
            current_serial + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        )
    }

    // The idea is that we can only set the index once. An uninitialized
    // index has a value of 0, so if we encounter a non-zero value that
    // means somebody else already set it. We'll return an Err(index) with
    // the index that was set.
    pub fn set_id(&self, index: u32) -> Result<u32, u32> {
        self.index.compare_exchange(
            0,
            index,
            Ordering::Relaxed,
            Ordering::Relaxed,
        )
    }

    pub fn is_empty(&self) -> bool {
        self.index.load(Ordering::Relaxed) == 0
    }

    pub fn into_inner(self) -> (StrideType, Option<u32>) {
        match self.serial.load(Ordering::Relaxed) {
            0 => (self.stride_type, None),
            _ => (
                self.stride_type,
                Some(self.index.load(Ordering::Relaxed) as u32),
            ),
        }
    }

    pub fn from_stridenodeid(id: StrideNodeId) -> Self {
        let index = id.1.map_or(0, |i| i as u32);
        Self {
            stride_type: id.0,
            index: AtomicU32::new(index),
            serial: AtomicUsize::new(if index == 0 { 0 } else { 1 }),
        }
    }
}

impl std::convert::From<AtomicStrideNodeId> for usize {
    fn from(id: AtomicStrideNodeId) -> Self {
        id.index.load(Ordering::Relaxed) as usize
    }
}

// #[derive(Eq, PartialEq, Hash, Debug, Default)]
// pub struct InMemStrideNodeId(Option<(u16, StrideNodeId)>);

// This works for both IPv4 and IPv6 up to a certain point.
// the u16 for Sort is used for ordering the local vecs
// inside the nodes.
// The u32 Part is used as an index to the backing global vecs,
// so you CANNOT store all IPv6 prefixes that could exist!
// If you really want that you should implement your own type with trait
// SortableNodeId, e.g., Sort = u16, Part = u128.
// impl SortableNodeId for InMemStrideNodeId {
//     type Sort = u16;
//     type Part = StrideNodeId;

//     fn new(sort: &Self::Sort, part: &Self::Part) -> InMemStrideNodeId {
//         InMemStrideNodeId(Some((*sort, *part)))
//     }

//     fn get_sort(&self) -> Self::Sort {
//         self.0.unwrap().0
//     }

//     fn get_part(&self) -> Self::Part {
//         self.0.unwrap().1
//     }

//     fn is_empty(&self) -> bool {
//         self.0.is_none()
//     }

//     fn empty() -> Self {
//         Self(None)
//     }
// }

// impl std::cmp::Ord for InMemStrideNodeId {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         if self.0.is_none() {
//             std::cmp::Ordering::Greater
//         } else if let Some(sort_id) = other.0 {
//             self.0.unwrap().0.cmp(&sort_id.0)
//         } else {
//             std::cmp::Ordering::Less
//         }
//     }
// }

// impl std::cmp::PartialOrd for InMemStrideNodeId {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.0.cmp(&other.0))
//     }
// }

// impl std::fmt::Display for InMemStrideNodeId {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "{:?}",
//             if self.0.is_none() {
//                 "-".to_string()
//             } else {
//                 self.0.unwrap().1.to_string()
//             }
//         )
//     }
// }

//------------------------- Node Collections --------------------------------

pub trait NodeCollection {
    fn insert(&mut self, index: u16, insert_node: StrideNodeId);
    fn to_vec(&self) -> Vec<StrideNodeId>;
    fn as_slice(&self) -> &[AtomicStrideNodeId];
    fn empty() -> Self;
}

//------------ PrefixSet ----------------------------------------------------

#[derive(Debug)]
pub struct PrefixSet<AF: AddressFamily, const ARRAYSIZE: usize>([PrefixId<AF>; ARRAYSIZE]);

impl<AF: AddressFamily, const ARRAYSIZE: usize> std::fmt::Display for PrefixSet<AF, ARRAYSIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<AF: AddressFamily, const ARRAYSIZE: usize> PrefixSet<AF, ARRAYSIZE> {
    pub(crate) fn insert(&mut self, index: u16, insert_node: PrefixId<AF>) {
        self[index as usize] = insert_node;
    }

    pub(crate) fn to_vec(&self) -> Vec<PrefixId<AF>> {
        self.0.to_vec()
    }

    pub(crate) fn as_slice(&self) -> &[PrefixId<AF>] {
        &self.0[..]
    }

    pub(crate) fn empty() -> Self {
        PrefixSet([<PrefixId<AF>>::default(); ARRAYSIZE])
    }
}

impl<AF: AddressFamily, const ARRAYSIZE: usize> std::ops::Index<usize> for PrefixSet<AF, ARRAYSIZE> {
    type Output = PrefixId<AF>;

    fn index(&self, idx: usize) -> &PrefixId<AF> {
        &self.0[idx as usize]
    }
}

impl<AF: AddressFamily, const ARRAYSIZE: usize> std::ops::IndexMut<usize>
    for PrefixSet<AF, ARRAYSIZE>
{
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        &mut self.0[idx as usize]
    }
}

//------------------------- NodeSet ---------------------------------------------------

#[derive(Debug)]
pub struct NodeSet<const ARRAYSIZE: usize>([AtomicStrideNodeId; ARRAYSIZE]);

impl<const ARRAYSIZE: usize> std::fmt::Display for NodeSet<ARRAYSIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<const ARRAYSIZE: usize> NodeCollection for NodeSet<ARRAYSIZE> {
    fn insert(&mut self, index: u16, insert_node: StrideNodeId) {
        // let idx = self
        //     .0
        //     .as_ref()
        //     .binary_search_by(|n| n.cmp(&insert_node))
        //     .unwrap_or_else(|x| x);
        // if idx + 1 < ARRAYSIZE {
        //     self.0.copy_within(idx..ARRAYSIZE - 1, idx + 1);
        // }
        // if idx < ARRAYSIZE {
        //     self.0[idx] = insert_node;
        // }
        let new_node = AtomicStrideNodeId::from_stridenodeid(insert_node);
        let serial = new_node.get_serial();
        self[index as usize] = new_node;
        match self[index as usize].update_serial(serial) {
            Ok(_) => (),
            Err(other_serial) if other_serial > serial => {
                self.insert(index, insert_node);
            }
            Err(other_serial) if other_serial <= serial => {
                panic!(
                    "NodeSet::insert: serial conflict: {} vs {}",
                    serial, other_serial
                );
            }
            _ => unreachable!(),
        };
    }

    fn to_vec(&self) -> Vec<StrideNodeId> {
        self.as_slice()
            .iter()
            .map(|p| {
                let index = p.index.load(Ordering::Relaxed);
                let serial = p.serial.load(Ordering::Relaxed);
                StrideNodeId(
                    p.stride_type,
                    if serial == 0 { None } else { Some(index) },
                )
            })
            .collect()
    }

    fn as_slice(&self) -> &[AtomicStrideNodeId] {
        // let idx = self
        //     .0
        //     .as_ref()
        //     .binary_search_by(|n| {
        //         if n.is_empty() {
        //             std::cmp::Ordering::Greater
        //         } else {
        //             std::cmp::Ordering::Less
        //         }
        //     })
        //     .unwrap_or_else(|x| x);
        &self.0[..]
    }

    fn empty() -> Self {
        let iter = std::ops::Range {
            start: 0,
            end: ARRAYSIZE,
        };
        NodeSet(
            array_init::from_iter(iter.map(|_| AtomicStrideNodeId::empty()))
                .unwrap(),
        )
    }
}

impl<const ARRAYSIZE: usize> std::ops::Index<usize> for NodeSet<ARRAYSIZE> {
    type Output = AtomicStrideNodeId;

    fn index(&self, idx: usize) -> &AtomicStrideNodeId {
        &self.0[idx as usize]
    }
}

impl<const ARRAYSIZE: usize> std::ops::IndexMut<usize>
    for NodeSet<ARRAYSIZE>
{
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        &mut self.0[idx as usize]
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Copy, Clone)]
pub enum StrideType {
    Stride3,
    Stride4,
    Stride5,
    // Stride6,
    // Stride7,
    // Stride8,
}

impl From<u8> for StrideType {
    fn from(level: u8) -> Self {
        match level {
            3 => StrideType::Stride3,
            4 => StrideType::Stride4,
            5 => StrideType::Stride5,
            // 6 => StrideType::Stride6,
            // 7 => StrideType::Stride7,
            // 8 => StrideType::Stride8,
            _ => panic!("Invalid stride level"),
        }
    }
}

impl std::fmt::Display for StrideType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrideType::Stride3 => write!(f, "S3"),
            StrideType::Stride4 => write!(f, "S4"),
            StrideType::Stride5 => write!(f, "S5"),
            // StrideType::Stride6 => write!(f, "S6"),
            // StrideType::Stride7 => write!(f, "S7"),
            // StrideType::Stride8 => write!(f, "S8"),
        }
    }
}

//--------------------- TreeBitMap -------------------------------------------

pub(crate) struct TreeBitMap<Store>
where
    Store: StorageBackend,
{
    pub strides: Vec<u8>,
    pub stats: Vec<StrideStats>,
    pub store: Store,
}

impl<'a, Store> TreeBitMap<Store>
where
    Store: StorageBackend,
{
    pub fn new(_strides_vec: Vec<u8>) -> TreeBitMap<Store> {
        // Check if the strides division makes sense
        let mut strides = vec![];
        let mut strides_sum = 0;
        for s in _strides_vec.iter().cycle() {
            strides.push(*s);
            strides_sum += s;
            if strides_sum >= Store::AF::BITS - 1 {
                break;
            }
        }
        assert_eq!(strides.iter().sum::<u8>(), Store::AF::BITS);

        let mut stride_stats: Vec<StrideStats> = vec![
            StrideStats::new(SizedStride::Stride3, strides.len() as u8), // 0
            StrideStats::new(SizedStride::Stride4, strides.len() as u8), // 1
            StrideStats::new(SizedStride::Stride5, strides.len() as u8), // 2
                                                                         // StrideStats::new(SizedStride::Stride6, strides.len() as u8), // 3
                                                                         // StrideStats::new(SizedStride::Stride7, strides.len() as u8), // 4
                                                                         // StrideStats::new(SizedStride::Stride8, strides.len() as u8), // 5
        ];

        let node: SizedStrideNode<<Store as StorageBackend>::AF>;

        match strides[0] {
            3 => {
                node = SizedStrideNode::Stride3(TreeBitMapNode {
                    ptrbitarr: AtomicStride2(AtomicU8::new(0)),
                    pfxbitarr: AtomicStride3(AtomicU16::new(0)),
                    ptr_vec: NodeSet::empty(),
                    pfx_vec: PrefixSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[0].inc(0);
            }
            4 => {
                node = SizedStrideNode::Stride4(TreeBitMapNode {
                    ptrbitarr: AtomicStride3(AtomicU16::new(0)),
                    pfxbitarr: AtomicStride4(AtomicU32::new(0)),
                    ptr_vec: NodeSet::empty(),
                    pfx_vec: PrefixSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[1].inc(0);
            }
            5 => {
                node = SizedStrideNode::Stride5(TreeBitMapNode {
                    ptrbitarr: AtomicStride4(AtomicU32::new(0)),
                    pfxbitarr: AtomicStride5(AtomicU64::new(0)),
                    ptr_vec: NodeSet::empty(),
                    pfx_vec: PrefixSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[2].inc(0);
            }
            // 6 => {
            //     node = SizedStrideNode::Stride6(TreeBitMapNode {
            //         ptrbitarr: 0,
            //         pfxbitarr: 0,
            //         ptr_vec: NodeSet::empty(),
            //         pfx_vec: NodeSet::empty(),
            //         _af: PhantomData,
            //     });
            //     stride_stats[3].inc(0);
            // }
            // 7 => {
            //     node = SizedStrideNode::Stride7(TreeBitMapNode {
            //         ptrbitarr: 0,
            //         pfxbitarr: U256(0, 0),
            //         ptr_vec: NodeSet::empty(),
            //         pfx_vec: NodeSet::empty(),
            //         _af: PhantomData,
            //     });
            //     stride_stats[4].inc(0);
            // }
            // 8 => {
            //     node = SizedStrideNode::Stride8(TreeBitMapNode {
            //         ptrbitarr: U256(0, 0),
            //         pfxbitarr: U512(0, 0, 0, 0),
            //         ptr_vec: NodeSet::empty(),
            //         pfx_vec: NodeSet::empty(),
            //         _af: PhantomData,
            //     });
            //     stride_stats[5].inc(0);
            // }
            _ => {
                panic!("unknown stride size encountered in STRIDES array");
            }
        };

        TreeBitMap {
            strides,
            stats: stride_stats,
            store: Store::init(Some(node)),
        }
    }

    // Partition for stride 4
    //
    // ptr bits never happen in the first half of the bitmap for the stride-size. Consequently the ptrbitarr can be an integer type
    // half the size of the pfxbitarr.
    //
    // ptr bit arr (u16)                                                        0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15    x
    // pfx bit arr (u32)   0 1 2  3  4  5  6   7   8   9  10  11  12  13  14   15   16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
    // nibble              * 0 1 00 01 10 11 000 001 010 011 100 101 110 111 0000 0001 0010 0011 0100 0101 0110 0111 1000 1001 1010 1011 1100 1101 1110 1111    x
    // nibble len offset   0 1    2            3                                4
    //
    // stride 3: 1 + 2 + 4 + 8                              =  15 bits. 2^4 - 1 (1 << 4) - 1. ptrbitarr starts at pos  7 (1 << 3) - 1
    // stride 4: 1 + 2 + 4 + 8 + 16                         =  31 bits. 2^5 - 1 (1 << 5) - 1. ptrbitarr starts at pos 15 (1 << 4) - 1
    // stride 5: 1 + 2 + 4 + 8 + 16 + 32 + 64               =  63 bits. 2^6 - 1
    // stride 6: 1 + 2 + 4 + 8 + 16 + 32 + 64               = 127 bits. 2^7 - 1
    // stride 7: 1 + 2 + 4 + 8 + 16 + 32 + 64 = 128         = 256 bits. 2^8 - 1126
    // stride 8: 1 + 2 + 4 + 8 + 16 + 32 + 64 + 128 + 256   = 511 bits. 2^9 - 1
    //
    // Ex.:
    // pfx            65.0.0.252/30                                             0100_0001_0000_0000_0000_0000_1111_1100
    //
    // nibble 1       (pfx << 0) >> 28                                          0000_0000_0000_0000_0000_0000_0000_0100
    // bit_pos        (1 << nibble length) - 1 + nibble                         0000_0000_0000_0000_0000_1000_0000_0000
    //
    // nibble 2       (pfx << 4) >> 24                                          0000_0000_0000_0000_0000_0000_0000_0001
    // bit_pos        (1 << nibble length) - 1 + nibble                         0000_0000_0000_0000_1000_0000_0000_0000
    // ...
    // nibble 8       (pfx << 28) >> 0                                          0000_0000_0000_0000_0000_0000_0000_1100
    // bit_pos        (1 << nibble length) - 1 + nibble = (1 << 2) - 1 + 2 = 5  0000_0010_0000_0000_0000_0000_0000_0000
    // 5 - 5 - 5 - 4 - 4 - [4] - 5
    // startpos (2 ^ nibble length) - 1 + nibble as usize

    pub(crate) fn insert(
        &mut self,
        pfx: InternalPrefixRecord<Store::AF, Store::Meta>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stride_end: u8 = 0;
        let mut cur_i = self.store.get_root_node_id(self.strides[0]);
        let mut level: u8 = 0;

        loop {
            let stride = self.strides[level as usize];
            stride_end += stride;
            let nibble_len = if pfx.len < stride_end {
                stride + pfx.len - stride_end
            } else {
                stride
            };

            let nibble = Store::AF::get_nibble(
                pfx.net,
                stride_end - stride,
                nibble_len,
            );
            let is_last_stride = pfx.len <= stride_end;

            let next_node_idx = match_node_for_strides![
                // applicable to the whole outer match in the macro
                self;
                nibble_len;
                nibble;
                is_last_stride;
                pfx;
                cur_i;
                level;
                // Strides to create match arm for; stats level
                Stride3; 0,
                Stride4; 1,
                Stride5; 2
                // Stride6; 3,
                // Stride7; 4,
                // Stride8; 5
            ];

            if let Some(i) = next_node_idx {
                cur_i = i;
                level += 1;
            } else {
                return Ok(());
            }
        }
    }

    fn store_node(
        &mut self,
        id: StrideNodeId,
        next_node: SizedStrideNode<Store::AF>,
    ) -> Option<StrideNodeId> {
        self.store.store_node(id, next_node)
    }

    #[inline]
    pub(crate) fn retrieve_node(
        &self,
        id: StrideNodeId,
    ) -> SizedNodeRefOption<Store::AF> {
        self.store.retrieve_node(id)
    }

    #[inline]
    #[cfg(feature = "dynamodb")]
    pub(crate) fn retrieve_node_with_guard(
        &self,
        id: StrideNodeId,
    ) -> CacheGuard<Store::AF, StrideNodeId> {
        self.store.retrieve_node_with_guard(id)
    }

    pub(crate) fn get_root_node_id(&self) -> StrideNodeId {
        self.store.get_root_node_id(self.strides[0])
    }

    #[inline]
    pub(crate) fn retrieve_node_mut(
        &'a mut self,
        index: StrideNodeId,
    ) -> SizedNodeRefResult<'a, Store::AF> {
        self.store.retrieve_node_mut(index)
    }

    pub(crate) fn store_prefix(
        &mut self,
        next_node: InternalPrefixRecord<Store::AF, Store::Meta>,
    ) -> Result<PrefixId<Store::AF>, Box<dyn std::error::Error>> {
        // let id = self.prefixes.len() as u32;
        let id = next_node.net << (Store::AF::BITS - next_node.len) as usize;
        self.store.store_prefix(PrefixId::from(next_node), next_node)
        // id
    }

    fn update_prefix_meta(
        &mut self,
        update_node_idx: PrefixId<Store::AF>,
        meta: Store::Meta,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self.store.retrieve_prefix_mut(update_node_idx) {
            Some(update_pfx) => match update_pfx.meta.as_mut() {
                Some(exist_meta) => {
                    <Store::Meta>::merge_update(exist_meta, meta)
                }
                None => {
                    update_pfx.meta = Some(meta);
                    Ok(())
                }
            },
            // TODO
            // Use/create proper error types
            None => Err("Prefix not found".into()),
        }
    }

    #[inline]
    pub(crate) fn retrieve_prefix(
        &self,
        index: PrefixId<Store::AF>,
    ) -> Option<&InternalPrefixRecord<Store::AF, Store::Meta>> {
        self.store.retrieve_prefix(index)
    }

    #[inline]
    #[cfg(feature = "dynamodb")]
    pub(crate) fn retrieve_prefix_mut(
        &mut self,
        index: StrideNodeId,
    ) -> Option<&mut InternalPrefixRecord<Store::AF, Store::Meta>> {
        self.store.retrieve_prefix_mut(index)
    }

    // This function assembles all entries in the `pfx_vec` of all child nodes of the
    // `start_node` into one vec, starting from iself and then recursively assembling
    // adding all `pfx_vec`s of its children.
    fn get_all_more_specifics_for_node(
        &self,
        start_node: SizedStrideRef<Store::AF>,
        found_pfx_vec: &mut Vec<PrefixId<Store::AF>>,
    ) {
        match start_node {
            SizedStrideRef::Stride3(n) => {
                found_pfx_vec.extend_from_slice(n.pfx_vec.as_slice());
                found_pfx_vec.retain(|&x| !x.is_empty());

                for nn in n.ptr_vec.as_slice().iter() {
                    if !nn.is_empty() {
                        self.get_all_more_specifics_for_node(
                            self.retrieve_node((*nn).into()).unwrap(),
                            found_pfx_vec,
                        );
                    }
                }
            }
            SizedStrideRef::Stride4(n) => {
                found_pfx_vec.extend(n.pfx_vec.to_vec());
                found_pfx_vec.retain(|&x| !x.is_empty());

                for nn in n.ptr_vec.as_slice().iter() {
                    if !nn.is_empty() {
                        self.get_all_more_specifics_for_node(
                            self.retrieve_node((*nn).into()).unwrap(),
                            found_pfx_vec,
                        );
                    }
                }
            }
            SizedStrideRef::Stride5(n) => {
                found_pfx_vec.extend(n.pfx_vec.to_vec());
                found_pfx_vec.retain(|&x| !x.is_empty());

                for nn in n.ptr_vec.as_slice().iter() {
                    if !nn.is_empty() {
                        self.get_all_more_specifics_for_node(
                            self.retrieve_node((*nn).into()).unwrap(),
                            found_pfx_vec,
                        );
                    }
                }
            } // SizedStrideNode::Stride6(n) => {
              //     found_pfx_vec.extend_from_slice(n.pfx_vec.as_slice());

              //     for nn in n.ptr_vec.as_slice().iter() {
              //         self.get_all_more_specifics_for_node(
              //             &self.retrieve_node(*nn).unwrap(),
              //             found_pfx_vec,
              //         );
              //     }
              // }
              // SizedStrideNode::Stride7(n) => {
              //     found_pfx_vec.extend_from_slice(n.pfx_vec.as_slice());

              //     for nn in n.ptr_vec.as_slice().iter() {
              //         self.get_all_more_specifics_for_node(
              //             &self.retrieve_node(*nn).unwrap(),
              //             found_pfx_vec,
              //         );
              //     }
              // }
              // SizedStrideNode::Stride8(n) => {
              //     found_pfx_vec.extend_from_slice(n.pfx_vec.as_slice());

              //     for nn in n.ptr_vec.as_slice().iter() {
              //         self.get_all_more_specifics_for_node(
              //             &self.retrieve_node(*nn).unwrap(),
              //             found_pfx_vec,
              //         );
              //     }
              // }
        }
    }

    // This function assembles the prefixes of a child node starting on a specified bit position in a ptr_vec of
    // `current_node` into a vec, then adds all prefixes of these children recursively into a vec and returns that.
    pub(crate) fn get_all_more_specifics_from_nibble<
        S: Stride,
        const PFXARRAYSIZE: usize,
        const PTRARRAYSIZE: usize,
    >(
        &self,
        current_node: &TreeBitMapNode<
            Store::AF,
            S,
            PFXARRAYSIZE,
            PTRARRAYSIZE,
        >,
        nibble: u32,
        nibble_len: u8,
    ) -> Option<Vec<PrefixId<Store::AF>>>
    where
        S: Stride
            + std::ops::BitAnd<Output = S>
            + std::ops::BitOr<Output = S>
            + num::Zero,
        <S as Stride>::PtrSize: Debug
            + Binary
            + Copy
            + std::ops::BitAnd<Output = S::PtrSize>
            + PartialOrd
            + num::Zero,
        <S as Stride>::AtomicPfxSize: AtomicBitmap,
        <S as Stride>::AtomicPtrSize: AtomicBitmap,
    {
        let (cnvec, mut msvec) =
            current_node.add_more_specifics_at(nibble, nibble_len);

        for child_node in cnvec.iter() {
            self.get_all_more_specifics_for_node(
                self.retrieve_node(*child_node).unwrap(),
                &mut msvec,
            );
        }
        Some(msvec)
    }
}

// This implements the funky stats for a tree
#[cfg(feature = "cli")]
impl<'a, Store: StorageBackend> std::fmt::Display for TreeBitMap<Store> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total_nodes = self.store.get_nodes_len();

        println!("prefix vec size {}", self.store.get_prefixes_len());
        println!("finished building tree...");
        println!("{:?} nodes created", total_nodes);
        println!(
            "size of node: {} bytes",
            std::mem::size_of::<SizedStrideNode<u32>>()
        );
        println!(
            "memory used by nodes: {}kb",
            self.store.get_nodes_len()
                * std::mem::size_of::<SizedStrideNode<u32>>()
                / 1024
        );

        println!("stride division {:?}", self.strides);
        for s in &self.stats {
            println!("{:?}", s);
        }

        println!(
            "level\t[{}|{}] nodes occupied/max nodes percentage_max_nodes_occupied prefixes",
            Colour::Blue.paint("nodes"),
            Colour::Green.paint("prefixes")
        );
        let bars = ["▏", "▎", "▍", "▌", "▋", "▊", "▉"];
        let mut stride_bits = [0, 0];
        const SCALE: u32 = 5500;

        for stride in self.strides.iter().enumerate() {
            // let level = stride.0;
            stride_bits = [stride_bits[1] + 1, stride_bits[1] + stride.1];
            let nodes_num = self
                .stats
                .iter()
                .find(|s| s.stride_len == *stride.1)
                .unwrap()
                .created_nodes[stride.0]
                .count as u32;
            let prefixes_num = self
                .stats
                .iter()
                .find(|s| s.stride_len == *stride.1)
                .unwrap()
                .prefixes_num[stride.0]
                .count as u32;

            let n = (nodes_num / SCALE) as usize;
            let max_pfx = u128::overflowing_pow(2, stride_bits[1] as u32);

            print!("{}-{}\t", stride_bits[0], stride_bits[1]);

            for _ in 0..n {
                print!("{}", Colour::Blue.paint("█"));
            }

            print!(
                "{}",
                Colour::Blue.paint(
                    bars[((nodes_num % SCALE) / (SCALE / 7)) as usize]
                ) //  = scale / 7
            );

            print!(
                " {}/{} {:.2}%",
                nodes_num,
                max_pfx.0,
                (nodes_num as f64 / max_pfx.0 as f64) * 100.0
            );
            print!("\n\t");

            let n = (prefixes_num / SCALE) as usize;
            for _ in 0..n {
                print!("{}", Colour::Green.paint("█"));
            }

            print!(
                "{}",
                Colour::Green.paint(
                    bars[((nodes_num % SCALE) / (SCALE / 7)) as usize]
                ) //  = scale / 7
            );

            println!(" {}", prefixes_num);
        }
        Ok(())
    }
}
