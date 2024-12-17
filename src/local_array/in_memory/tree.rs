use crate::prefix_record::{Meta, PublicRecord};
use crate::prelude::multi::PrefixId;
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use log::{error, log_enabled, trace};
use roaring::RoaringBitmap;

use std::hash::Hash;
use std::sync::atomic::{
    AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering,
};
use std::{fmt::Debug, marker::PhantomData};

use super::atomic_types::{NodeBuckets, PrefixBuckets};
use crate::af::AddressFamily;
use crate::insert_match;
use crate::rib::{Rib, StoreConfig, UpsertReport};

use super::super::errors::PrefixStoreError;
pub(crate) use super::atomic_stride::*;

use super::node::TreeBitMapNode;

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

pub(crate) enum NewNodeOrIndex<AF: AddressFamily> {
    NewNode(SizedStrideNode<AF>),
    ExistingNode(StrideNodeId<AF>),
    NewPrefix,
    ExistingPrefix,
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

//------------------------- Node Collections --------------------------------

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

#[derive(Debug)]
pub struct TreeBitMap<
    AF: AddressFamily,
    M: Meta,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, M>,
> {
    pub(crate) node_buckets: NB,
    pub(crate) prefix_buckets: PB,
    withdrawn_muis_bmin: Atomic<RoaringBitmap>,
    _af: PhantomData<AF>,
    _m: PhantomData<M>,
}

impl<
        AF: AddressFamily,
        M: Meta,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > TreeBitMap<AF, M, NB, PB>
{
    pub(crate) fn init() -> Self {
        Self {
            node_buckets: NodeBuckets::init(),
            prefix_buckets: PB::init(),
            withdrawn_muis_bmin: RoaringBitmap::new().into(),
            _af: PhantomData,
            _m: PhantomData,
        }
    }

    pub fn withdrawn_muis_bmin<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> &'a RoaringBitmap {
        unsafe {
            self.withdrawn_muis_bmin
                .load(Ordering::Acquire, guard)
                .deref()
        }
    }

    // Change the status of the mui globally to Withdrawn. Iterators and match
    // functions will by default not return any records for this mui.
    pub fn mark_mui_as_withdrawn(
        &self,
        mui: u32,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        let current = self.withdrawn_muis_bmin.load(Ordering::Acquire, guard);

        let mut new = unsafe { current.as_ref() }.unwrap().clone();
        new.insert(mui);

        #[allow(clippy::assigning_clones)]
        loop {
            match self.withdrawn_muis_bmin.compare_exchange(
                current,
                Owned::new(new),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(updated) => {
                    new =
                        unsafe { updated.current.as_ref() }.unwrap().clone();
                }
            }
        }
    }

    // Change the status of the mui globally to Active. Iterators and match
    // functions will default to the status on the record itself.
    pub fn mark_mui_as_active(
        &self,
        mui: u32,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        let current = self.withdrawn_muis_bmin.load(Ordering::Acquire, guard);

        let mut new = unsafe { current.as_ref() }.unwrap().clone();
        new.remove(mui);

        #[allow(clippy::assigning_clones)]
        loop {
            match self.withdrawn_muis_bmin.compare_exchange(
                current,
                Owned::new(new),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(updated) => {
                    new =
                        unsafe { updated.current.as_ref() }.unwrap().clone();
                }
            }
        }
    }

    // Whether this mui is globally withdrawn. Note that this overrules
    // (by default) any (prefix, mui) combination in iterators and match
    // functions.
    pub fn mui_is_withdrawn(&self, mui: u32, guard: &Guard) -> bool {
        unsafe {
            self.withdrawn_muis_bmin
                .load(Ordering::Acquire, guard)
                .as_ref()
        }
        .unwrap()
        .contains(mui)
    }

    // Whether this mui is globally active. Note that the local statuses of
    // records (prefix, mui) may be set to withdrawn in iterators and match
    // functions.
    pub fn mui_is_active(&self, mui: u32, guard: &Guard) -> bool {
        !unsafe {
            self.withdrawn_muis_bmin
                .load(Ordering::Acquire, guard)
                .as_ref()
        }
        .unwrap()
        .contains(mui)
    }
}

impl<
        AF: AddressFamily,
        M: Meta,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
        const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > Rib<AF, M, NB, PB, PREFIX_SIZE, KEY_SIZE>
{
    pub fn new(
        config: StoreConfig,
    ) -> Result<
        Rib<AF, M, NB, PB, PREFIX_SIZE, KEY_SIZE>,
        Box<dyn std::error::Error>,
    > {
        let root_node = match Rib::<
            AF,
            M,
            NB,
            PB,
            PREFIX_SIZE,
            KEY_SIZE,
        >::get_first_stride_size()
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

        Rib::<AF, M, NB, PB, PREFIX_SIZE, KEY_SIZE>::init(root_node, config)
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
        record: PublicRecord<M>,
        update_path_selections: Option<M::TBI>,
    ) -> Result<UpsertReport, PrefixStoreError> {
        let guard = &epoch::pin();

        if pfx.get_len() == 0 {
            let res = self.update_default_route_prefix_meta(record, guard)?;
            return Ok(res);
        }

        let mut stride_end: u8 = 0;
        let mut cur_i = self.get_root_node_id();
        let mut level: u8 = 0;
        let mut acc_retry_count = 0;

        loop {
            let stride = self.get_stride_sizes()[level as usize];
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
            // let back_off = crossbeam_utils::Backoff::new();

            // insert_match! returns the node_id of the next node to be
            // traversed. It was created if it did not exist.
            let node_result = insert_match![
                // applicable to the whole outer match in the macro
                self;
                guard;
                nibble_len;
                nibble;
                is_last_stride;
                pfx;
                record;
                update_path_selections; // perform an update for the paths in this record
                stride_start; // the length at the start of the stride a.k.a. start_bit
                stride;
                cur_i;
                level;
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
                            std::thread::current().name().unwrap_or("unnamed-thread"),
                            cur_i,
                        );
                        error!(
                            "{} {}",
                            std::thread::current()
                                .name()
                                .unwrap_or("unnamed-thread"),
                            err
                        );
                    }
                }
            }
        }
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
        record: PublicRecord<M>,
        guard: &epoch::Guard,
        // user_data: Option<&<M as MergeUpdate>::UserDataIn>,
    ) -> Result<UpsertReport, PrefixStoreError> {
        trace!("Updating the default route...");

        if let Some(root_node) = self.retrieve_node_mut(
            self.get_root_node_id(),
            record.multi_uniq_id,
            // guard,
        ) {
            match root_node {
                SizedStrideRef::Stride3(_) => {
                    self.in_memory_tree
                        .node_buckets
                        .get_store3(self.get_root_node_id())
                        .update_rbm_index(record.multi_uniq_id)?;
                }
                SizedStrideRef::Stride4(_) => {
                    self.in_memory_tree
                        .node_buckets
                        .get_store4(self.get_root_node_id())
                        .update_rbm_index(record.multi_uniq_id)?;
                }
                SizedStrideRef::Stride5(_) => {
                    self.in_memory_tree
                        .node_buckets
                        .get_store5(self.get_root_node_id())
                        .update_rbm_index(record.multi_uniq_id)?;
                }
            };
        };

        self.upsert_prefix(
            PrefixId::new(AF::zero(), 0),
            record,
            // Do not update the path selection for the default route.
            None,
            guard,
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
        trace!("{:?}", self.retrieve_node(start_node_id));
        match self.retrieve_node(start_node_id) {
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
        &self,
        current_node: &TreeBitMapNode<AF, S>,
        nibble: u32,
        nibble_len: u8,
        base_prefix: StrideNodeId<AF>,
    ) -> Option<Vec<PrefixId<AF>>> {
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
impl<
        AF: AddressFamily,
        M: Meta,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
        const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > std::fmt::Display for Rib<AF, M, NB, PB, PREFIX_SIZE, KEY_SIZE>
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(_f, "{} prefixes created", self.get_prefixes_count())?;
        writeln!(_f, "{} nodes created", self.get_nodes_count())?;
        writeln!(_f)?;

        writeln!(
            _f,
            "stride division {:?}",
            self.get_stride_sizes()
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
            self.get_stride_sizes()
                .iter()
                .map_while(|s| if s > &0 { Some(*s) } else { None })
                .enumerate()
                .collect::<Vec<(usize, u8)>>()
        );

        for crate::stats::CreatedNodes {
            depth_level: len,
            count: prefix_count,
        } in self.counters.get_prefix_stats()
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
