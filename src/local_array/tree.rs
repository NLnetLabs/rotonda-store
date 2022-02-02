use crossbeam_epoch::{self as epoch};
use std::hash::Hash;
use std::sync::atomic::{
    AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::{fmt::Debug, marker::PhantomData};

use crate::af::{AddressFamily, Zero};
use crate::local_array::storage_backend::StorageBackend;
use crate::match_node_for_strides;
use crate::prefix_record::InternalPrefixRecord;

#[cfg(feature = "dynamodb")]
use crate::local_array::CacheGuard;

pub(crate) use super::atomic_stride::*;
use crate::stats::{SizedStride, StrideStats};

pub(crate) use crate::local_array::node::TreeBitMapNode;

#[cfg(feature = "cli")]
use ansi_term::Colour;

use routecore::record::MergeUpdate;

//------------------- Sized Node Enums ------------------------------------

// No, no, NO, NO, no, no! We're not going to Box this, because that's slow!
// This enum is never used to store nodes/prefixes, it's only to be used in
// generic code.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum SizedStrideNode<AF: AddressFamily> {
    Stride3(TreeBitMapNode<AF, Stride3>),
    Stride4(TreeBitMapNode<AF, Stride4>),
    Stride5(TreeBitMapNode<AF, Stride5>),
    // Stride6(TreeBitMapNode<AF, Stride6, NodeId, 126, 64>),
    // Stride7(TreeBitMapNode<AF, Stride7, NodeId, 254, 128>),
    // Stride8(TreeBitMapNode<AF, Stride8, NodeId, 510, 256>),
}

impl<AF, S> Default for TreeBitMapNode<AF, S>
where
    AF: AddressFamily,
    S: Stride,
{
    fn default() -> Self {
        Self {
            ptrbitarr: <<S as Stride>::AtomicPtrSize as AtomicBitmap>::new(),
            pfxbitarr: <<S as Stride>::AtomicPfxSize as AtomicBitmap>::new(),
            pfx_vec: PrefixSet::empty(S::BITS),
            _af: PhantomData,
        }
    }
}

impl<AF> Default for SizedStrideNode<AF>
where
    AF: AddressFamily,
{
    fn default() -> Self {
        SizedStrideNode::Stride3(TreeBitMapNode {
            ptrbitarr: AtomicStride2(AtomicU8::new(0)),
            pfxbitarr: AtomicStride3(AtomicU16::new(0)),
            pfx_vec: PrefixSet::empty(14),
            _af: PhantomData,
        })
    }
}

// Used to create a public iterator over all nodes.
#[derive(Debug)]
pub enum SizedStrideRef<'a, AF: AddressFamily> {
    Stride3(&'a TreeBitMapNode<AF, Stride3>),
    Stride4(&'a TreeBitMapNode<AF, Stride4>),
    Stride5(&'a TreeBitMapNode<AF, Stride5>),
    // Stride6(&'a TreeBitMapNode<AF, Stride6, NodeId, 126, 64>),
    // Stride7(&'a TreeBitMapNode<AF, Stride7, NodeId, 254, 128>),
    // Stride8(&'a TreeBitMapNode<AF, Stride8, NodeId, 510, 256>),
}
//
#[derive(Debug)]
pub(crate) enum SizedStrideRefMut<'a, AF: AddressFamily> {
    Stride3(&'a mut TreeBitMapNode<AF, Stride3>),
    Stride4(&'a mut TreeBitMapNode<AF, Stride4>),
    Stride5(&'a mut TreeBitMapNode<AF, Stride5>),
    // Stride6(&'a TreeBitMapNode<AF, Stride6, NodeId, 126, 64>),
    // Stride7(&'a TreeBitMapNode<AF, Stride7, NodeId, 254, 128>),
    // Stride8(&'a TreeBitMapNode<AF, Stride8, NodeId, 510, 256>),
}

pub(crate) enum NewNodeOrIndex<'a, AF: AddressFamily> {
    NewNode(SizedStrideNode<AF>),
    ExistingNode(StrideNodeId<AF>),
    NewPrefix(u16),
    ExistingPrefix(PrefixId<AF>, &'a mut AtomicUsize),
}

#[derive(Hash, Eq, PartialEq, Debug, Copy, Clone)]
pub struct PrefixId<AF: AddressFamily>(pub Option<(AF, u8, usize)>);

impl<AF: AddressFamily> PrefixId<AF> {
    pub fn new(net: AF, len: u8) -> Self {
        PrefixId(Some((net, len, 1)))
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    pub fn set_serial(mut self, serial: usize) -> Self {
        self.0.as_mut().unwrap().2 = serial;
        self
    }

    pub fn get_net(&self) -> AF {
        self.0.unwrap().0
    }

    pub fn get_len(&self) -> u8 {
        self.0.unwrap().1
    }
}

impl<AF: AddressFamily> std::default::Default for PrefixId<AF> {
    fn default() -> Self {
        PrefixId(None)
    }
}

//--------------------- Per-Stride-Node-Id Type ------------------------------------

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct StrideNodeId<AF: AddressFamily>(Option<(AF, u8)>);

impl<AF: AddressFamily> StrideNodeId<AF> {
    pub fn empty() -> Self {
        Self(None)
    }

    pub fn dangerously_new_with_id_as_is(addr_bits: AF, len: u8) -> Self {
        Self(Some((addr_bits, len)))
    }

    #[inline]
    pub fn new_with_cleaned_id(addr_bits: AF, len: u8) -> Self {
        Self(Some((addr_bits.truncate_to_len(len), len)))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    pub fn get_id(&self) -> (AF, u8) {
        self.0.unwrap()
    }

    pub fn set_len(mut self, len: u8) -> Self {
        self.0.as_mut().unwrap().1 = len;
        self
    }

    pub fn add_to_len(mut self, len: u8) -> Self {
        self.0.as_mut().unwrap().1 += len;
        self
    }

    #[inline]
    pub fn truncate_to_len(self) -> Self {
        let (addr_bits, len) = self.0.unwrap();
        StrideNodeId::new_with_cleaned_id(addr_bits, len)
    }

    // clean out all bits that are set beyond the len. This function should
    // be used before doing any ORing to add a nibble.
    #[inline]
    pub fn unwrap_with_cleaned_id(&self) -> (AF, u8) {
        let (addr_bits, len) = self.0.unwrap();
        (addr_bits.truncate_to_len(len), len)
    }

    pub fn add_nibble(&self, nibble: u32, nibble_len: u8) -> Self {
        let (addr_bits, len) = self.unwrap_with_cleaned_id();
        let res = addr_bits.add_nibble(len, nibble, nibble_len);
        Self(Some(res))
    }

    pub fn into_inner(self) -> Option<(AF, u8)> {
        self.0
    }
}

// impl<AF: AddressFamily> Default for StrideNodeId<AF> {
//     fn default() -> Self {
//         Self(None)
//     }
// }

// impl<AF: AddressFamily + From<u32> + From<u16>> std::convert::From<u16>
//     for StrideNodeId<AF>
// {
//     fn from(id: u16) -> Self {
//         Self(Some((id.into(), 0)))
//     }
// }

impl<AF: AddressFamily> std::fmt::Display for StrideNodeId<AF> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .map(|x| format!("{}-{}", x.0, x.1))
                .unwrap_or_else(|| "-".to_string())
        )
    }
}

impl<AF: AddressFamily> std::convert::From<AtomicStrideNodeId<AF>>
    for StrideNodeId<AF>
{
    fn from(id: AtomicStrideNodeId<AF>) -> Self {
        let i = match id.index.load(Ordering::Relaxed) {
            0 => None,
            // THIS DOESN'T ACTUALLY WORK. TEMPORARY.
            x => Some((x.into(), 0)),
        };
        Self(i)
    }
}

impl<AF: AddressFamily> std::convert::From<&AtomicStrideNodeId<AF>>
    for StrideNodeId<AF>
{
    fn from(id: &AtomicStrideNodeId<AF>) -> Self {
        let i = match id.index.load(Ordering::Relaxed) {
            0 => None,
            x => Some((x.into(), 0)),
        };
        Self(i)
    }
}

impl<AF: AddressFamily> std::convert::From<StrideNodeId<AF>>
    for PrefixId<AF>
{
    fn from(id: StrideNodeId<AF>) -> Self {
        let (addr_bits, len) = id.0.unwrap();
        PrefixId::new(addr_bits, len)
    }
}

#[derive(Debug)]
pub struct AtomicStrideNodeId<AF: AddressFamily> {
    stride_type: StrideType,
    index: AtomicU32,
    serial: AtomicUsize,
    _af: PhantomData<AF>,
}

impl<AF: AddressFamily> AtomicStrideNodeId<AF> {
    pub fn new(stride_type: StrideType, index: u32) -> Self {
        Self {
            stride_type,
            index: AtomicU32::new(index),
            serial: AtomicUsize::new(1),
            _af: PhantomData,
        }
    }

    pub fn empty() -> Self {
        Self {
            stride_type: StrideType::Stride4,
            index: AtomicU32::new(0),
            serial: AtomicUsize::new(0),
            _af: PhantomData,
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
        self.serial.load(Ordering::Relaxed) == 0
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

    pub fn from_stridenodeid(
        stride_type: StrideType,
        id: StrideNodeId<AF>,
    ) -> Self {
        let index: AF = id.0.map_or(AF::zero(), |i| i.0);
        Self {
            stride_type,
            index: AtomicU32::new(index.dangerously_truncate_to_u32()),
            serial: AtomicUsize::new(if index == AF::zero() { 0 } else { 1 }),
            _af: PhantomData,
        }
    }
}

impl<AF: AddressFamily> std::convert::From<AtomicStrideNodeId<AF>> for usize {
    fn from(id: AtomicStrideNodeId<AF>) -> Self {
        id.index.load(Ordering::Relaxed) as usize
    }
}

//------------------------- Node Collections --------------------------------

pub trait NodeCollection<AF: AddressFamily> {
    fn insert(&mut self, index: u16, insert_node: StrideNodeId<AF>);
    fn to_vec(&self) -> Vec<StrideNodeId<AF>>;
    fn as_slice(&self) -> &[AtomicStrideNodeId<AF>];
    fn empty() -> Self;
}

//------------ PrefixSet ----------------------------------------------------

// The PrefixSet is the type that powers pfx_vec, the ARRAY that holds all
// the child prefixes in a node. Since we are storing these prefixes in the
// global store in a HashMap that is keyed on the tuple (addr_bits, len,
// serial number) we can get away with storing ONLY THE SERIAL NUMBER in the
// pfx_vec: The addr_bits and len are implied in the position in the array a
// serial numher has. A PrefixSet doesn't know anything about the node it is
// contained in, so it needs a base address to be able to calculate the
// complete prefix of a child prefix.

#[derive(Debug)]
pub struct PrefixSet(Box<[AtomicUsize]>, u8);

impl std::fmt::Display for PrefixSet {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PrefixSet {
    // Collect all PrefixIds int a vec. Since the net and len of the
    // PrefixIds are implied by the position in the pfx_vec we can
    // calculate them with if we know the base address of the node
    // this PrefixSet lives in.
    pub(crate) fn to_vec<AF: AddressFamily>(
        &self,
        base_prefix: StrideNodeId<AF>,
    ) -> Vec<PrefixId<AF>> {
        let mut vec = vec![];
        let mut i: usize = 0;
        let mut nibble_len = 1;
        while i < self.0.len() {
            for nibble in 0..1 << nibble_len {
                match self.0[i].load(Ordering::Relaxed) {
                    0 => (),
                    serial => vec.push(
                        PrefixId::<AF>::new(
                            base_prefix
                                .get_id()
                                .0
                                .add_nibble(
                                    base_prefix.get_id().1,
                                    nibble,
                                    nibble_len,
                                )
                                .0,
                            base_prefix.get_id().1 + nibble_len,
                        )
                        .set_serial(serial),
                    ),
                }
                i += 1;
            }
            nibble_len += 1;
        }
        vec
    }

    pub(crate) fn empty(len: u8) -> Self {
        // let arr = array_init::array_init(|_| AtomicUsize::new(0));
        let mut v: Vec<AtomicUsize> = Vec::new();
        for _ in 0..len {
            v.push(AtomicUsize::new(0));
        }
        PrefixSet(v.into_boxed_slice(), len)
    }

    pub(crate) fn get_serial_at(&mut self, index: usize) -> &mut AtomicUsize {
        &mut self.0[index as usize]
    }
}

impl std::ops::Index<usize> for PrefixSet {
    type Output = AtomicUsize;

    fn index(&self, idx: usize) -> &AtomicUsize {
        &self.0[idx as usize]
    }
}

impl std::ops::IndexMut<usize> for PrefixSet {
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        &mut self.0[idx as usize]
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Copy, Clone)]
pub enum StrideType {
    Stride3,
    Stride4,
    Stride5,
}

impl From<u8> for StrideType {
    fn from(level: u8) -> Self {
        match level {
            3 => StrideType::Stride3,
            4 => StrideType::Stride4,
            5 => StrideType::Stride5,
            _ => panic!("Invalid stride level {}", level),
        }
    }
}

impl std::fmt::Display for StrideType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrideType::Stride3 => write!(f, "S3"),
            StrideType::Stride4 => write!(f, "S4"),
            StrideType::Stride5 => write!(f, "S5"),
        }
    }
}

//--------------------- TreeBitMap ------------------------------------------

pub(crate) struct TreeBitMap<Store>
where
    Store: StorageBackend,
{
    // pub strides: Vec<u8>,
    pub stats: Vec<StrideStats>,
    pub store: Store,
}

impl<'a, Store> TreeBitMap<Store>
where
    Store: StorageBackend,
{
    pub fn new(strides_vec: Vec<u8>) -> TreeBitMap<Store> {
        // Check if the strides division makes sense
        let mut strides = vec![];
        let mut len_to_stride_size: [StrideType; 128] =
            [StrideType::Stride3; 128];
        let mut strides_sum = 0;
        for s in strides_vec.iter().cycle() {
            strides.push(*s);
            len_to_stride_size[strides_sum as usize] = StrideType::from(*s);
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
        ];

        let root_node: SizedStrideNode<<Store as StorageBackend>::AF>;

        match strides[0] {
            3 => {
                root_node = SizedStrideNode::Stride3(TreeBitMapNode {
                    ptrbitarr: AtomicStride2(AtomicU8::new(0)),
                    pfxbitarr: AtomicStride3(AtomicU16::new(0)),
                    pfx_vec: PrefixSet::empty(14),
                    _af: PhantomData,
                });
                stride_stats[0].inc(0);
            }
            4 => {
                root_node = SizedStrideNode::Stride4(TreeBitMapNode {
                    ptrbitarr: AtomicStride3(AtomicU16::new(0)),
                    pfxbitarr: AtomicStride4(AtomicU32::new(0)),
                    pfx_vec: PrefixSet::empty(30),
                    _af: PhantomData,
                });
                stride_stats[1].inc(0);
            }
            5 => {
                root_node = SizedStrideNode::Stride5(TreeBitMapNode {
                    ptrbitarr: AtomicStride4(AtomicU32::new(0)),
                    pfxbitarr: AtomicStride5(AtomicU64::new(0)),
                    pfx_vec: PrefixSet::empty(62),
                    _af: PhantomData,
                });
                stride_stats[2].inc(0);
            }
            unknown_stride_size => {
                panic!(
                    "unknown stride size {} encountered in STRIDES array",
                    unknown_stride_size
                );
            }
        };

        TreeBitMap {
            // strides,
            stats: stride_stats,
            store: Store::init(root_node),
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
        if pfx.len == 0 {
            let _res =
                self.update_default_route_prefix_meta(pfx.meta.unwrap());
            return Ok(());
        }

        let mut stride_end: u8 = 0;
        let mut cur_i = self
            .store
            .get_root_node_id(self.store.get_stride_sizes()[0]);
        let mut level: u8 = 0;

        loop {
            let stride = self.store.get_stride_sizes()[level as usize];
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
            let stride_start = stride_end - stride;
            let guard = &epoch::pin();

            let next_node_idx = match_node_for_strides![
                // applicable to the whole outer match in the macro
                self;
                guard;
                nibble_len;
                nibble;
                is_last_stride;
                pfx;
                stride_start; // the length at the start of the stride a.k.a. start_bit
                stride;
                cur_i;
                level;
                // Strides to create match arm for; stats level
                Stride3; 0,
                Stride4; 1,
                Stride5; 2
            ];

            if let Some(i) = next_node_idx {
                cur_i = i;
                level += 1;
            } else {
                return Ok(());
            }
        }
    }

    // #[inline]
    // pub(crate) fn retrieve_node(
    //     &self,
    //     id: StrideNodeId<Store::AF>,
    // ) -> SizedNodeRefOption<Store::AF> {
    //     self.store.retrieve_node(id)
    // }

    #[inline]
    #[cfg(feature = "dynamodb")]
    pub(crate) fn retrieve_node_with_guard(
        &self,
        id: StrideNodeId,
    ) -> CacheGuard<Store::AF, StrideNodeId> {
        self.store.retrieve_node_with_guard(id)
    }

    pub(crate) fn get_root_node_id(&self) -> StrideNodeId<Store::AF> {
        self.store
            .get_root_node_id(self.store.get_stride_sizes()[0])
    }

    // #[inline]
    // pub(crate) fn retrieve_node_mut(
    //     &'a self,
    //     index: StrideNodeId<Store::AF>,
    // ) -> SizedNodeRefResult<'a, Store::AF> {
    //     self.store.retrieve_node_mut(index)
    // }

    pub(crate) fn store_prefix(
        &self,
        next_node: InternalPrefixRecord<Store::AF, Store::Meta>,
    ) -> Result<PrefixId<Store::AF>, Box<dyn std::error::Error>> {
        self.store
            .store_prefix(PrefixId::from(next_node.clone()), next_node)
    }

    // Yes, we're hating this. But, the root node has no room for a serial
    // of the prefix 0/0 (the default route), which doesn't even matter,
    // unless, UNLESS, somwbody want to store a default route. So we have
    // to store a serial for this prefix. The normal place for a serial of
    // any prefix is on the pfxvec of its paren. But, hey, guess what, the
    // default-route-prefix lives *on* the root node, and, you know, the
    // root node doesn't have a parent. We can:
    // - Create a type RootTreeBitmapNode with a ptrbitarr with a size one
    //   bigger than a "normal" TreeBitMapNod for the first stride size.
    //   no we have to iterate over the rootnode type in all matches on
    //   stride_size, just because we have exactly one instance of the
    //   RootTreeBitmapNode. So no.
    // - Make the `get_pfx_index` method on the implementations of the
    //   `Stride` trait check for a length of zero and branch if it is and
    //   return the serial of the root node. Now each and every call to this
    //   method will have to check a condition for exactly one instance of
    //   RootTreeBitmapNode. So again, no.
    // - The root node only gets used at the beginning of a seach query or
    //   an insert. So if we provide two speciliased methods that will now
    //   how to search for the default-route prefix and now how to set serial
    //  for that prefix and make sure we start searching/inserting with one
    //   of those specialized methods we're good to go.
    fn update_default_route_prefix_meta(
        &self,
        new_meta: Store::Meta,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // println!("Updating the default route...");
        let mut old_serial =
            self.store.increment_default_route_prefix_serial();
        let new_serial = old_serial + 1;
        let df_pfx_id =
            PrefixId::new(Store::AF::zero(), 0).set_serial(old_serial);

        loop {
            match old_serial {
                0 => {
                    // println!(
                    //     "No default route prefix found, creating one..."
                    // );
                    self.store_prefix(InternalPrefixRecord {
                        net: Store::AF::zero(),
                        len: 0,
                        meta: Some(new_meta),
                    })?;
                    return Ok(());
                }
                // SUCCESS (Step 6) !
                // Nobody messed with our prefix meta-data in between us loading the
                // serial and creating the entry with that serial. Update the ptrbitarr
                // in the current node in the global store and be done with it.
                old_serial if old_serial == new_serial - 1 => {
                    let pfx_idx_clone = df_pfx_id;
                    self.update_prefix_meta(
                        pfx_idx_clone,
                        new_serial,
                        &new_meta,
                    )?;
                    self.store
                        .remove_prefix(pfx_idx_clone.set_serial(old_serial));
                    return Ok(());
                }
                // FAILURE (Step 7)
                // Some other thread messed it up. Try again by upping a newly-read serial once
                // more, reading the newly-current meta-data, updating it with our meta-data and
                // see if it works then. rinse-repeat.
                newer_serial => {
                    println!(
                        "contention for {:?} with serial {} -> {}",
                        df_pfx_id, old_serial, newer_serial
                    );
                    old_serial =
                        self.store.increment_default_route_prefix_serial();
                    self.store
                        .get_prefixes()
                        .get(&df_pfx_id.set_serial(old_serial));

                    self.update_prefix_meta(
                        df_pfx_id,
                        newer_serial,
                        &new_meta,
                    )?;
                }
            };
        }
    }

    // Upserts the meta-data in the global store.
    //
    // When updating an existing prefix the MergeUpdate trait implmented
    // for the meta-data type will be used. Creates a new prefix entry
    // in the global store with the serial number `new_serial`.
    fn update_prefix_meta(
        &self,
        update_prefix_idx: PrefixId<Store::AF>,
        new_serial: usize,
        merge_meta: &Store::Meta,
    ) -> Result<PrefixId<Store::AF>, Box<dyn std::error::Error>> {
        // Create a clone of the meta-data of the current prefix, since we
        // don't want to mutate the current entry in the store. Instead
        // we want to create a new entry with the same prefix, but with
        // the merged meta-data and a new serial number.
        let new_meta = match self.store.get_prefixes().get(&update_prefix_idx)
        {
            Some(update_prefix) => update_prefix
                .meta
                .as_ref()
                .unwrap()
                .clone_merge_update(merge_meta)?,
            None => {
                // panic!(
                //     "panic: {}/{} with serial {} not found.",
                //     update_prefix_idx.get_net().into_ipaddr(),
                //     update_prefix_idx.get_len(),
                //     new_serial
                // );
                return Err(format!(
                    "-Prefix {}/{} (serial {}) not found",
                    update_prefix_idx.get_net().into_ipaddr(),
                    update_prefix_idx.get_len(),
                    new_serial
                )
                .into());
            }
        };
        let new_prefix = InternalPrefixRecord::new_with_meta(
            update_prefix_idx.get_net(),
            update_prefix_idx.get_len(),
            new_meta,
        );
        self.store.store_prefix(
            update_prefix_idx.set_serial(new_serial),
            new_prefix,
        )
    }

    // #[inline]
    // pub(crate) fn retrieve_prefix(
    //     &self,
    //     index: PrefixId<Store::AF>,
    // ) -> Option<&InternalPrefixRecord<Store::AF, Store::Meta>> {
    //     self.store.get_prefixes().get(&index)
    // }

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
        start_node_id: StrideNodeId<Store::AF>,
        found_pfx_vec: &mut Vec<PrefixId<Store::AF>>,
    ) {
        // match self.retrieve_node(start_node_id).unwrap() {
        // let (id , store) = self.store.get_stride_for_id(start_node_id);
        let guard = &epoch::pin();
        match self.store.retrieve_node_with_guard(start_node_id, guard) {
            Some(SizedStrideRef::Stride3(n)) => {
                // let n = store.get(&id).unwrap();
                found_pfx_vec.extend(n.pfx_vec.to_vec(start_node_id));

                for child_node in n.ptr_vec(start_node_id) {
                    self.get_all_more_specifics_for_node(
                        child_node,
                        found_pfx_vec,
                    );
                }
            }
            Some(SizedStrideRef::Stride4(n)) => {
                // let n = store.get(&id).unwrap();
                found_pfx_vec.extend(n.pfx_vec.to_vec(start_node_id));

                for child_node in n.ptr_vec(start_node_id) {
                    self.get_all_more_specifics_for_node(
                        child_node,
                        found_pfx_vec,
                    );
                }
            }
            Some(SizedStrideRef::Stride5(n)) => {
                // let n = store.get(&id).unwrap();
                found_pfx_vec.extend(n.pfx_vec.to_vec(start_node_id));

                for child_node in n.ptr_vec(start_node_id) {
                    self.get_all_more_specifics_for_node(
                        child_node,
                        found_pfx_vec,
                    );
                }
            }
            _ => {
                panic!("can't find node {}", start_node_id);
            }
        }
    }

    // This function assembles the prefixes of a child node starting on a
    // specified bit position in a ptr_vec of `current_node` into a vec,
    // then adds all prefixes of these children recursively into a vec and
    // returns that.
    pub(crate) fn get_all_more_specifics_from_nibble<S: Stride>(
        &self,
        current_node: &TreeBitMapNode<Store::AF, S>,
        nibble: u32,
        nibble_len: u8,
        base_prefix: StrideNodeId<Store::AF>,
    ) -> Option<Vec<PrefixId<Store::AF>>>
    where
        S: Stride,
    {
        let (cnvec, mut msvec) = current_node.add_more_specifics_at(
            nibble,
            nibble_len,
            base_prefix,
        );

        for child_node in cnvec.iter() {
            self.get_all_more_specifics_for_node(*child_node, &mut msvec);
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

        println!(
            "stride division {:?}",
            self.store
                .get_stride_sizes()
                .iter()
                .map_while(|s| if s > &0 { Some(*s) } else { None })
                .collect::<Vec<_>>()
        );
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

        println!(
            "stride_sizes {:?}",
            self.store
                .get_stride_sizes()
                .iter()
                .map_while(|s| if s > &0 { Some(*s) } else { None })
                .enumerate()
                .collect::<Vec<(usize, u8)>>()
        );
        for stride in self
            .store
            .get_stride_sizes()
            .iter()
            .map_while(|s| if s > &0 { Some(*s) } else { None })
            .enumerate()
        {
            // let level = stride.0;
            stride_bits = [stride_bits[1] + 1, stride_bits[1] + stride.1];
            let nodes_num = self
                .stats
                .iter()
                .find(|s| s.stride_len == stride.1)
                .unwrap()
                .created_nodes[stride.0]
                .count as u32;
            let prefixes_num = self
                .stats
                .iter()
                .find(|s| s.stride_len == stride.1)
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
