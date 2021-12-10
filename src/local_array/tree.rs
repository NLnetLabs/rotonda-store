use std::hash::Hash;
use std::sync::atomic::{
    AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::{fmt::Debug, marker::PhantomData};

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
{
    fn default() -> Self {
        Self {
            ptrbitarr: <<S as Stride>::AtomicPtrSize as AtomicBitmap>::new(),
            pfxbitarr: <<S as Stride>::AtomicPfxSize as AtomicBitmap>::new(),
            pfx_vec: PrefixSet::empty(),
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

impl<'a, AF: AddressFamily> UnsizedNode<AF> for SizedStrideRef<'a, AF> {}

pub(crate) trait NodeWrapper<AF: AddressFamily> {
    type Unsized: UnsizedNode<AF>;
    type UnsizedRef: UnsizedNode<AF>;
}

pub(crate) enum NewNodeOrIndex<'a, AF: AddressFamily> {
    NewNode(SizedStrideNode<AF>),
    ExistingNode(StrideNodeId<AF>),
    NewPrefix(u16),
    ExistingPrefix(&'a mut (PrefixId<AF>, AtomicUsize)),
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

#[derive(Debug)]
pub struct PrefixSet<AF: AddressFamily, const ARRAYSIZE: usize>(
    [(PrefixId<AF>, AtomicUsize); ARRAYSIZE],
);

impl<AF: AddressFamily, const ARRAYSIZE: usize> std::fmt::Display
    for PrefixSet<AF, ARRAYSIZE>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<AF: AddressFamily, const ARRAYSIZE: usize> PrefixSet<AF, ARRAYSIZE> {
    pub(crate) fn insert(&mut self, index: u16, insert_node: PrefixId<AF>) {
        let n = self.0.get_mut(index as usize);
        if n.is_some() {
            self[index as usize] = insert_node;
        }
        else {
            println!("Can't find node with index {} in local array for {:?}", index, insert_node);
        }
        // self[index as usize] = insert_node;
    }

    pub(crate) fn to_vec(&self) -> Vec<PrefixId<AF>> {
        self.0.iter().map(|p| p.0).collect()
    }

    pub(crate) fn as_slice(&self) -> &[(PrefixId<AF>, AtomicUsize)] {
        &self.0[..]
    }

    pub(crate) fn empty() -> Self {
        let arr: [(PrefixId<AF>, AtomicUsize); ARRAYSIZE] =
            array_init::array_init(|_| {
                (PrefixId::default(), AtomicUsize::new(0))
            });
        PrefixSet(arr)
    }

    pub(crate) fn get_prefix_with_serial_at(
        &mut self,
        index: usize,
    ) -> &mut (PrefixId<AF>, AtomicUsize) {
        &mut self.0[index as usize]
    }

    pub(crate) fn atomically_load_serial_at(
        &self,
        index: usize,
    ) -> Option<usize> {
        self.0
            .get(index as usize)
            .map(|p| p.1.load(Ordering::Relaxed))
    }

    pub(crate) fn atomically_update_serial_at(self, index: usize) -> usize {
        self.0
            .get(index as usize)
            .map_or(0, |p| p.1.fetch_add(1, Ordering::Relaxed))
    }
}

impl<AF: AddressFamily, const ARRAYSIZE: usize> std::ops::Index<usize>
    for PrefixSet<AF, ARRAYSIZE>
{
    type Output = PrefixId<AF>;

    fn index(&self, idx: usize) -> &PrefixId<AF> {
        &self.0[idx as usize].0
    }
}

impl<AF: AddressFamily, const ARRAYSIZE: usize> std::ops::IndexMut<usize>
    for PrefixSet<AF, ARRAYSIZE>
{
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        &mut self.0[idx as usize].0
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
        }
    }
}

//--------------------- TreeBitMap ------------------------------------------

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
                    // ptr_vec: NodeSet::empty(),
                    pfx_vec: PrefixSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[0].inc(0);
            }
            4 => {
                root_node = SizedStrideNode::Stride4(TreeBitMapNode {
                    ptrbitarr: AtomicStride3(AtomicU16::new(0)),
                    pfxbitarr: AtomicStride4(AtomicU32::new(0)),
                    // ptr_vec: NodeSet::empty(),
                    pfx_vec: PrefixSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[1].inc(0);
                // start_stride_type = StrideType::Stride4;
            }
            5 => {
                root_node = SizedStrideNode::Stride5(TreeBitMapNode {
                    ptrbitarr: AtomicStride4(AtomicU32::new(0)),
                    pfxbitarr: AtomicStride5(AtomicU64::new(0)),
                    // ptr_vec: NodeSet::empty(),
                    pfx_vec: PrefixSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[2].inc(0);
            }
            _ => {
                panic!("unknown stride size encountered in STRIDES array");
            }
        };

        TreeBitMap {
            strides,
            stats: stride_stats,
            store: Store::init(len_to_stride_size, root_node),
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
                stride_end;
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

    fn store_node(
        &mut self,
        id: StrideNodeId<Store::AF>,
        next_node: SizedStrideNode<Store::AF>,
    ) -> Option<StrideNodeId<Store::AF>> {
        self.store.store_node(id, next_node)
    }

    #[inline]
    pub(crate) fn retrieve_node(
        &self,
        id: StrideNodeId<Store::AF>,
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

    pub(crate) fn get_root_node_id(&self) -> StrideNodeId<Store::AF> {
        self.store.get_root_node_id(self.strides[0])
    }

    #[inline]
    pub(crate) fn retrieve_node_mut(
        &'a mut self,
        index: StrideNodeId<Store::AF>,
    ) -> SizedNodeRefResult<'a, Store::AF> {
        self.store.retrieve_node_mut(index)
    }

    pub(crate) fn store_prefix(
        &mut self,
        next_node: InternalPrefixRecord<Store::AF, Store::Meta>,
    ) -> Result<PrefixId<Store::AF>, Box<dyn std::error::Error>> {
        self.store
            .store_prefix(PrefixId::from(next_node.clone()), next_node)
    }

    // Upserts the meta-data in the global store.
    //
    // When updating an existing prefix the MergeUpdate trait implmented
    // for the meta-data type will be used. Creates a new prefix entry
    // in the global store with the serial number `new_serial`.
    fn update_prefix_meta(
        &mut self,
        update_prefix_idx: PrefixId<Store::AF>,
        new_serial: usize,
        merge_meta: &Store::Meta,
    ) -> Result<PrefixId<Store::AF>, Box<dyn std::error::Error>> {
        // Create a clone of the meta-data of the current prefix, since we
        // don't want to mutate the current entry in the store. Instead
        // we want to create a new entry with the same prefix, but with
        // the merged meta-data and a new serial number.
        let new_meta = match self.store.retrieve_prefix(update_prefix_idx) {
            Some(update_prefix) => update_prefix
                .meta
                .as_ref()
                .unwrap()
                .clone_merge_update(merge_meta)?,
            None => return Err(format!("Prefix {}/{} not found", update_prefix_idx.get_net().into_ipaddr(), update_prefix_idx.get_len()).into()),
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
        start_node_id: StrideNodeId<Store::AF>,
        found_pfx_vec: &mut Vec<PrefixId<Store::AF>>,
    ) {
        match self.retrieve_node(start_node_id).unwrap() {
            SizedStrideRef::Stride3(n) => {
                found_pfx_vec.extend(n.pfx_vec.to_vec());
                found_pfx_vec.retain(|&x| !x.is_empty());

                for child_node in n.ptr_vec(start_node_id) {
                    if !child_node.is_empty() {
                        self.get_all_more_specifics_for_node(
                            child_node,
                            found_pfx_vec,
                        );
                    }
                }
            }
            SizedStrideRef::Stride4(n) => {
                found_pfx_vec.extend(n.pfx_vec.to_vec());
                found_pfx_vec.retain(|&x| !x.is_empty());

                for child_node in n.ptr_vec(start_node_id) {
                    if !child_node.is_empty() {
                        self.get_all_more_specifics_for_node(
                            child_node,
                            found_pfx_vec,
                        );
                    }
                }
            }
            SizedStrideRef::Stride5(n) => {
                found_pfx_vec.extend(n.pfx_vec.to_vec());
                found_pfx_vec.retain(|&x| !x.is_empty());

                for child_node in n.ptr_vec(start_node_id) {
                    if !child_node.is_empty() {
                        self.get_all_more_specifics_for_node(
                            child_node,
                            found_pfx_vec,
                        );
                    }
                }
            }
        }
    }

    // This function assembles the prefixes of a child node starting on a
    // specified bit position in a ptr_vec of `current_node` into a vec,
    // then adds all prefixes of these children recursively into a vec and
    // returns that.
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
