use crate::prefix_record::{MergeUpdate, Meta};
use crossbeam_epoch::{self as epoch};
use log::{error, log_enabled, trace};

use std::hash::Hash;
use std::sync::atomic::{
    AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::{fmt::Debug, marker::PhantomData};

use crate::af::AddressFamily;
use crate::custom_alloc::{CustomAllocStorage, Upsert};
use crate::insert_match;
use crate::local_array::store::atomic_types::{NodeBuckets, PrefixBuckets};

pub(crate) use super::atomic_stride::*;
use super::store::errors::PrefixStoreError;

pub(crate) use crate::local_array::node::TreeBitMapNode;

#[cfg(feature = "cli")]
use ansi_term::Colour;

//------------------- Sized Node Enums ------------------------------------

// No, no, NO, NO, no, no! We're not going to Box this, because that's slow!
// This enum is never used to store nodes/prefixes, it's only to be used in
// generic code.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum SizedStrideNode<AF: AddressFamily> {
    Stride3(TreeBitMapNode<AF, Stride3>),
    Stride4(TreeBitMapNode<AF, Stride4>),
    Stride5(TreeBitMapNode<AF, Stride5>),
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
}

#[derive(Debug)]
pub enum SizedStrideRefMut<'a, AF: AddressFamily> {
    Stride3(&'a mut TreeBitMapNode<AF, Stride3>),
    Stride4(&'a mut TreeBitMapNode<AF, Stride4>),
    Stride5(&'a mut TreeBitMapNode<AF, Stride5>),
}

pub(crate) enum NewNodeOrIndex<AF: AddressFamily> {
    NewNode(SizedStrideNode<AF>),
    ExistingNode(StrideNodeId<AF>),
    NewPrefix,
    ExistingPrefix,
}

#[derive(Hash, Eq, PartialEq, Debug, Copy, Clone)]
pub struct PrefixId<AF: AddressFamily>(Option<(AF, u8)>);

impl<AF: AddressFamily> PrefixId<AF> {
    pub fn new(net: AF, len: u8) -> Self {
        PrefixId(Some((net, len)))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    pub fn get_net(&self) -> AF {
        self.0.unwrap().0
    }

    pub fn get_len(&self) -> u8 {
        self.0.unwrap().1
    }

    // This should never fail, since there shouldn't be a invalid prefix in
    // this prefix id in the first place.
    pub fn into_pub(&self) -> inetnum::addr::Prefix {
        inetnum::addr::Prefix::new(
            self.get_net().into_ipaddr(),
            self.get_len(),
        )
        .unwrap_or_else(|p| panic!("can't convert {:?} into prefix.", p))
    }

    // Increment the length of the prefix without changing the bits part.
    // This is used to iterate over more-specific prefixes for this prefix,
    // since the more specifics iterator includes the requested `base_prefix`
    // itself.
    pub fn inc_len(self) -> Self {
        Self(self.0.map(|(net, len)| (net, len + 1)))
    }
}

impl<AF: AddressFamily> std::default::Default for PrefixId<AF> {
    fn default() -> Self {
        PrefixId(None)
    }
}

impl<AF: AddressFamily> From<inetnum::addr::Prefix> for PrefixId<AF> {
    fn from(value: inetnum::addr::Prefix) -> Self {
        Self(Some((AF::from_ipaddr(value.addr()), value.len())))
    }
}

//--------------------- Per-Stride-Node-Id Type -----------------------------

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
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
    pub fn get_len(&self) -> u8 {
        self.0.unwrap().1
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

    // get_serial() and update_serial() are intimately linked in the
    // critical section of updating a node.
    //
    // The layout of the critical section is as follows:
    // 1. get_serial() to retrieve the serial number of the node
    // 2. do work in the critical section
    // 3. store work result in the node
    // 4. update_serial() to update the serial number of the node if
    //    and only if the serial is the same as the one retrieved in step 1.
    // 5. check the result of update_serial(). When successful, we're done,
    //    otherwise, rollback the work result & repeat from step 1.
    pub fn get_serial(&self) -> usize {
        let serial = self.serial.load(Ordering::SeqCst);
        std::sync::atomic::fence(Ordering::SeqCst);
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
            Ordering::SeqCst,
            Ordering::SeqCst,
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
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
    }

    pub fn is_empty(&self) -> bool {
        self.serial.load(Ordering::SeqCst) == 0
    }

    pub fn into_inner(self) -> (StrideType, Option<u32>) {
        match self.serial.load(Ordering::SeqCst) {
            0 => (self.stride_type, None),
            _ => (self.stride_type, Some(self.index.load(Ordering::SeqCst))),
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
            serial: AtomicUsize::new(usize::from(index != AF::zero())),
            _af: PhantomData,
        }
    }
}

impl<AF: AddressFamily> std::convert::From<AtomicStrideNodeId<AF>> for usize {
    fn from(id: AtomicStrideNodeId<AF>) -> Self {
        id.index.load(Ordering::SeqCst) as usize
    }
}

//------------------------- Node Collections --------------------------------

pub trait NodeCollection<AF: AddressFamily> {
    fn insert(&mut self, index: u16, insert_node: StrideNodeId<AF>);
    fn to_vec(&self) -> Vec<StrideNodeId<AF>>;
    fn as_slice(&self) -> &[AtomicStrideNodeId<AF>];
    fn empty() -> Self;
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

pub struct TreeBitMap<
    AF: AddressFamily,
    M: Meta + MergeUpdate,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, M>,
> {
    pub store: CustomAllocStorage<AF, M, NB, PB>,
}

impl<
        'a,
        AF: AddressFamily,
        M: Meta + MergeUpdate,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > TreeBitMap<AF, M, NB, PB>
{
    pub fn new(
    ) -> Result<TreeBitMap<AF, M, NB, PB>, Box<dyn std::error::Error>> {
        let guard = &epoch::pin();

        let root_node =
            match CustomAllocStorage::<AF, M, NB, PB>::get_first_stride_size()
            {
                3 => SizedStrideNode::Stride3(TreeBitMapNode {
                    ptrbitarr: AtomicStride2(AtomicU8::new(0)),
                    pfxbitarr: AtomicStride3(AtomicU16::new(0)),
                    _af: PhantomData,
                }),
                4 => SizedStrideNode::Stride4(TreeBitMapNode {
                    ptrbitarr: AtomicStride3(AtomicU16::new(0)),
                    pfxbitarr: AtomicStride4(AtomicU32::new(0)),
                    _af: PhantomData,
                }),
                5 => SizedStrideNode::Stride5(TreeBitMapNode {
                    ptrbitarr: AtomicStride4(AtomicU32::new(0)),
                    pfxbitarr: AtomicStride5(AtomicU64::new(0)),
                    _af: PhantomData,
                }),
                unknown_stride_size => {
                    panic!(
                        "unknown stride size {} encountered in STRIDES array",
                        unknown_stride_size
                    );
                }
            };

        Ok(TreeBitMap {
            store: CustomAllocStorage::<AF, M, NB, PB>::init(
                root_node, guard,
            )?,
        })
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

    pub fn insert(
        &self,
        pfx: PrefixId<AF>,
        record: M,
        multi_uniq_id: u32,
        user_data: Option<&<M as MergeUpdate>::UserDataIn>,
    ) -> Result<(Upsert<<M as MergeUpdate>::UserDataOut>, u32), PrefixStoreError> {
        let guard = &epoch::pin();

        if pfx.get_len() == 0 {
            let res = self.update_default_route_prefix_meta(record, guard, user_data)?;
            return Ok(res);
        }

        let mut stride_end: u8 = 0;
        let mut cur_i = self.store.get_root_node_id();
        let mut level: u8 = 0;
        let mut acc_retry_count = 0;

        loop {
            let stride = self.store.get_stride_sizes()[level as usize];
            stride_end += stride;
            let nibble_len = if pfx.get_len() < stride_end {
                stride + pfx.get_len() - stride_end
            } else {
                stride
            };

            let nibble = AF::get_nibble(
                pfx.get_net(),
                stride_end - stride,
                nibble_len,
            );
            let is_last_stride = pfx.get_len() <= stride_end;
            let stride_start = stride_end - stride;
            let back_off = crossbeam_utils::Backoff::new();

            // insert_match! returns the node_id of the next node to be
            // traversed. It was created if it did not exist.
            let node_result = insert_match![
                // applicable to the whole outer match in the macro
                self;
                user_data;
                multi_uniq_id;
                guard;
                nibble_len;
                nibble;
                is_last_stride;
                pfx;
                record;
                stride_start; // the length at the start of the stride a.k.a. start_bit
                stride;
                cur_i;
                level;
                back_off;
                acc_retry_count;
                // Strides to create match arm for; stats level
                Stride3; 0,
                Stride4; 1,
                Stride5; 2
            ];

            match node_result {
                Ok((next_id, retry_count)) => {
                    cur_i = next_id;
                    level += 1;
                    acc_retry_count += retry_count;
                }
                Err(err) => {
                    if log_enabled!(log::Level::Error) {
                        error!("{} failing to store (intermediate) node {}. Giving up this node. This shouldn't happen!",
                            std::thread::current().name().unwrap(),
                            cur_i,
                        );
                        error!(
                            "{} {}",
                            std::thread::current().name().unwrap(),
                            err
                        );
                    }
                }
            }
        }
    }

    pub(crate) fn get_root_node_id(&self) -> StrideNodeId<AF> {
        self.store.get_root_node_id()
    }

    // Yes, we're hating this. But, the root node has no room for a serial of
    // the prefix 0/0 (the default route), which doesn't even matter, unless,
    // UNLESS, somebody wants to store a default route. So we have to store a
    // serial for this prefix. The normal place for a serial of any prefix is
    // on the pfxvec of its paren. But, hey, guess what, the
    // default-route-prefix lives *on* the root node, and, you know, the root
    // node doesn't have a parent. We can:
    // - Create a type RootTreeBitmapNode with a ptrbitarr with a size one
    //   bigger than a "normal" TreeBitMapNod for the first stride size. no we
    //   have to iterate over the root-node type in all matches on
    //   stride_size, just because we have exactly one instance of the
    //   RootTreeBitmapNode. So no.
    // - Make the `get_pfx_index` method on the implementations of the
    //   `Stride` trait check for a length of zero and branch if it is and
    //   return the serial of the root node. Now each and every call to this
    //   method will have to check a condition for exactly one instance of
    //   RootTreeBitmapNode. So again, no.
    // - The root node only gets used at the beginning of a search query or an
    //   insert. So if we provide two specialised methods that will now how to
    //   search for the default-route prefix and now how to set serial for
    //  that prefix and make sure we start searching/inserting with one of
    //   those specialized methods we're good to go.
    fn update_default_route_prefix_meta(
        &self,
        new_meta: M,
        guard: &epoch::Guard,
        user_data: Option<&<M as MergeUpdate>::UserDataIn>,
    ) -> Result<(Upsert<<M as MergeUpdate>::UserDataOut>, u32), PrefixStoreError> {
        trace!("Updating the default route...");
        self.store.upsert_prefix(
            PrefixId::new(AF::zero(), 0),
            new_meta,
            guard,
            user_data,
        )
    }

    // This function assembles all entries in the `pfx_vec` of all child nodes
    // of the `start_node` into one vec, starting from itself and then
    // recursively assembling adding all `pfx_vec`s of its children.
    fn get_all_more_specifics_for_node(
        &self,
        start_node_id: StrideNodeId<AF>,
        found_pfx_vec: &mut Vec<PrefixId<AF>>,
    ) {
        let guard = &epoch::pin();

        trace!("start assembling all more specific prefixes here");
        trace!(
            "{:?}",
            self.store.retrieve_node_with_guard(start_node_id, guard)
        );
        match self.store.retrieve_node_with_guard(start_node_id, guard) {
            Some(SizedStrideRef::Stride3(n)) => {
                found_pfx_vec.extend(
                    n.pfx_iter(start_node_id).collect::<Vec<PrefixId<AF>>>(),
                );

                for child_node in n.ptr_iter(start_node_id) {
                    self.get_all_more_specifics_for_node(
                        child_node,
                        found_pfx_vec,
                    );
                }
            }
            Some(SizedStrideRef::Stride4(n)) => {
                found_pfx_vec.extend(
                    n.pfx_iter(start_node_id).collect::<Vec<PrefixId<AF>>>(),
                );

                for child_node in n.ptr_iter(start_node_id) {
                    self.get_all_more_specifics_for_node(
                        child_node,
                        found_pfx_vec,
                    );
                }
            }
            Some(SizedStrideRef::Stride5(n)) => {
                found_pfx_vec.extend(
                    n.pfx_iter(start_node_id).collect::<Vec<PrefixId<AF>>>(),
                );

                for child_node in n.ptr_iter(start_node_id) {
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
        &'a self,
        current_node: &TreeBitMapNode<AF, S>,
        nibble: u32,
        nibble_len: u8,
        base_prefix: StrideNodeId<AF>,
    ) -> Option<Vec<PrefixId<AF>>>
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

impl<
        AF: AddressFamily,
        M: Meta + MergeUpdate,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > Default for TreeBitMap<AF, M, NB, PB>
{
    fn default() -> Self {
        Self::new().unwrap()
    }
}

// This implements the funky stats for a tree
#[cfg(feature = "cli")]
impl<
        AF: AddressFamily,
        M: Meta + MergeUpdate,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > std::fmt::Display for TreeBitMap<AF, M, NB, PB>
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(_f, "{} prefixes created", self.store.get_prefixes_count())?;
        writeln!(_f, "{} nodes created", self.store.get_nodes_count())?;
        writeln!(_f)?;

        writeln!(
            _f,
            "stride division {:?}",
            self.store
                .get_stride_sizes()
                .iter()
                .map_while(|s| if s > &0 { Some(*s) } else { None })
                .collect::<Vec<_>>()
        )?;

        writeln!(
            _f,
            "level\t[{}] prefixes-occupied/max-prefixes percentage_occupied",
            Colour::Green.paint("prefixes")
        )?;

        let bars = ["▏", "▎", "▍", "▌", "▋", "▊", "▉"];
        const SCALE: u32 = 5500;

        trace!(
            "stride_sizes {:?}",
            self.store
                .get_stride_sizes()
                .iter()
                .map_while(|s| if s > &0 { Some(*s) } else { None })
                .enumerate()
                .collect::<Vec<(usize, u8)>>()
        );

        for crate::stats::CreatedNodes {
            depth_level: len,
            count: prefix_count,
        } in self.store.counters.get_prefix_stats()
        {
            let max_pfx = u128::overflowing_pow(2, len as u32);
            let n = (prefix_count as u32 / SCALE) as usize;

            write!(_f, "/{}\t", len)?;

            for _ in 0..n {
                write!(_f, "{}", Colour::Green.paint("█"))?;
            }

            write!(
                _f,
                "{}",
                Colour::Green.paint(
                    bars[((prefix_count as u32 % SCALE) / (SCALE / 7))
                        as usize]
                ) //  = scale / 7
            )?;

            write!(
                _f,
                " {}/{} {:.2}%",
                prefix_count,
                max_pfx.0,
                (prefix_count as f64 / max_pfx.0 as f64) * 100.0
            )?;

            writeln!(_f)?;
        }

        Ok(())
    }
}
