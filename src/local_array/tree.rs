use routecore::addr::AddressFamily;

use crate::common::InternalPrefixRecord;
use crate::node_id::SortableNodeId;
use crate::match_node_for_strides;
use crate::local_array::storage_backend::{CacheGuard, StorageBackend};
pub use crate::stride::*;
use crate::stats::{StrideStats, SizedStride};

pub use crate::local_array::node::TreeBitMapNode;
use crate::local_array::storage_backend::{SizedNodeOption, SizedNodeResult};
use crate::synth_int::{Zero, U256, U512};

use std::{
    fmt::{Binary, Debug},
    marker::PhantomData,
};

#[cfg(feature = "cli")]
use ansi_term::Colour;

use routecore::record::MergeUpdate;

//------------------- Unsized Node Enums ------------------------------------------------

pub trait UnsizedNode<AF: AddressFamily, NodeId: SortableNodeId> {}

#[derive(Debug, Copy, Clone)]
pub enum SizedStrideNode<AF: AddressFamily, NodeId: SortableNodeId + Copy> {
    Stride3(TreeBitMapNode<AF, Stride3, NodeId, 14, 8>),
    Stride4(TreeBitMapNode<AF, Stride4, NodeId, 30, 16>),
    Stride5(TreeBitMapNode<AF, Stride5, NodeId, 62, 32>),
    Stride6(TreeBitMapNode<AF, Stride6, NodeId, 126, 64>),
    Stride7(TreeBitMapNode<AF, Stride7, NodeId, 254, 128>),
    Stride8(TreeBitMapNode<AF, Stride8, NodeId, 510, 256>),
}

impl<AF: AddressFamily, NodeId: SortableNodeId + Copy> UnsizedNode<AF, NodeId>
    for SizedStrideNode<AF, NodeId>
{
}

impl<AF, NodeId> Default for SizedStrideNode<AF, NodeId>
where
    AF: AddressFamily,
    NodeId: SortableNodeId + Copy,
{
    fn default() -> Self {
        SizedStrideNode::Stride3(TreeBitMapNode {
            ptrbitarr: 0,
            pfxbitarr: 0,
            pfx_vec: NodeSet::empty(),
            ptr_vec: NodeSet::empty(),
            _af: PhantomData,
        })
    }
}

// Used to create vec over all nodes.
#[derive(Debug)]
pub enum SizedStrideRef<'a, AF: AddressFamily, NodeId: SortableNodeId + Copy> {
    Stride3(&'a TreeBitMapNode<AF, Stride3, NodeId, 14, 8>),
    Stride4(&'a TreeBitMapNode<AF, Stride4, NodeId, 30, 16>),
    Stride5(&'a TreeBitMapNode<AF, Stride5, NodeId, 62, 32>),
    Stride6(&'a TreeBitMapNode<AF, Stride6, NodeId, 126, 64>),
    Stride7(&'a TreeBitMapNode<AF, Stride7, NodeId, 254, 128>),
    Stride8(&'a TreeBitMapNode<AF, Stride8, NodeId, 510, 256>),
}

impl<'a, AF: AddressFamily, NodeId: SortableNodeId + Copy> UnsizedNode<AF, NodeId>
    for SizedStrideRef<'a, AF, NodeId>
{
}

pub trait NodeWrapper<AF: AddressFamily, Node: SortableNodeId + Copy> {
    type Unsized: UnsizedNode<AF, Node>;
    type UnsizedRef: UnsizedNode<AF, Node>;
    // type NodeCollection: NodeCollection<AF, Node>;
}

pub enum NewNodeOrIndex<AF: AddressFamily, NodeId: SortableNodeId + Copy> {
    NewNode(SizedStrideNode<AF, NodeId>, NodeId::Sort), // New Node and bit_id of the new node
    ExistingNode(NodeId),
    NewPrefix,
    ExistingPrefix(NodeId::Part),
}

//--------------------- Per-Stride-Node-Id Type ------------------------------------

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Hash, Ord, Debug)]
pub struct StrideNodeId(pub StrideType, pub u32);

impl Default for StrideNodeId {
    fn default() -> Self {
        Self(StrideType::Stride5, 0)
    }
}

impl StrideNodeId {
    pub fn empty(stride_type: StrideType) -> Self {
        Self(stride_type, 0)
    }
}

impl std::convert::From<u16> for StrideNodeId {
    fn from(id: u16) -> Self {
        Self(StrideType::Stride4, id as u32)
    }
}

impl std::fmt::Display for StrideNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

impl std::convert::From<StrideNodeId> for usize {
    fn from(id: StrideNodeId) -> Self {
        id.1 as usize
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Copy, Clone, Default)]
pub struct InMemStrideNodeId(Option<(u16, StrideNodeId)>);

// This works for both IPv4 and IPv6 up to a certain point.
// the u16 for Sort is used for ordering the local vecs
// inside the nodes.
// The u32 Part is used as an index to the backing global vecs,
// so you CANNOT store all IPv6 prefixes that could exist!
// If you really want that you should implement your own type with trait
// SortableNodeId, e.g., Sort = u16, Part = u128.
impl SortableNodeId for InMemStrideNodeId {
    type Sort = u16;
    type Part = StrideNodeId;

    fn new(sort: &Self::Sort, part: &Self::Part) -> InMemStrideNodeId {
        InMemStrideNodeId(Some((*sort, *part)))
    }

    fn get_sort(&self) -> Self::Sort {
        self.0.unwrap().0
    }

    fn get_part(&self) -> Self::Part {
        self.0.unwrap().1
    }

    fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    fn empty() -> Self {
        Self(None)
    }
}

impl std::cmp::Ord for InMemStrideNodeId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.0.is_none() {
            std::cmp::Ordering::Greater
        } else if let Some(sort_id) = other.0 {
            self.0.unwrap().0.cmp(&sort_id.0)
        } else {
            std::cmp::Ordering::Less
        }
    }
}

impl std::cmp::PartialOrd for InMemStrideNodeId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl std::fmt::Display for InMemStrideNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}",
            if self.0.is_none() {
                "-".to_string()
            } else {
                self.0.unwrap().1.to_string()
            }
        )
    }
}

//------------------------- Node Collections ---------------------------------------------------

pub trait NodeCollection<NodeId: SortableNodeId + Copy> {
    fn insert(&mut self, insert_node: NodeId);
    fn as_slice(&self) -> &[NodeId];
    fn empty() -> Self;
}

#[derive(Debug, Clone, Copy)]
pub struct NodeSet<NodeId: SortableNodeId + Copy, const ARRAYSIZE: usize>([NodeId; ARRAYSIZE]);

impl<NodeId: SortableNodeId + Copy, const ARRAYSIZE: usize> std::fmt::Display
    for NodeSet<NodeId, ARRAYSIZE>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<NodeId: SortableNodeId + Copy, const ARRAYSIZE: usize> NodeCollection<NodeId> for NodeSet<NodeId, ARRAYSIZE> {
    fn insert(&mut self, insert_node: NodeId) {
        let idx = self
            .0
            .as_ref()
            .binary_search_by(|n| n.cmp(&insert_node))
            .unwrap_or_else(|x| x);
        if idx + 1 < ARRAYSIZE {
            self.0.copy_within(idx..ARRAYSIZE - 1, idx + 1);
        }
        if idx < ARRAYSIZE {
            self.0[idx] = insert_node;
        }
    }

    fn as_slice(&self) -> &[NodeId] {
        let idx = self
            .0
            .as_ref()
            .binary_search_by(|n| {
                if n.is_empty() {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .unwrap_or_else(|x| x);
        &self.0[0..idx]
    }

    fn empty() -> Self {
        NodeSet([NodeId::empty(); ARRAYSIZE])
    }
}

impl<NodeId: SortableNodeId + Copy, const ARRAYSIZE: usize> std::ops::Index<usize>
    for NodeSet<NodeId, ARRAYSIZE>
{
    type Output = NodeId;
    fn index(&self, idx: usize) -> &NodeId {
        &self.0[idx]
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Copy, Clone)]
pub enum StrideType {
    Stride3,
    Stride4,
    Stride5,
    Stride6,
    Stride7,
    Stride8,
}

impl From<u8> for StrideType {
    fn from(level: u8) -> Self {
        match level {
            3 => StrideType::Stride3,
            4 => StrideType::Stride4,
            5 => StrideType::Stride5,
            6 => StrideType::Stride6,
            7 => StrideType::Stride7,
            8 => StrideType::Stride8,
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
            StrideType::Stride6 => write!(f, "S6"),
            StrideType::Stride7 => write!(f, "S7"),
            StrideType::Stride8 => write!(f, "S8"),
        }
    }
}

//--------------------- TreeBitMap -------------------------------------------

pub struct TreeBitMap<Store>
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
            StrideStats::new(SizedStride::Stride6, strides.len() as u8), // 3
            StrideStats::new(SizedStride::Stride7, strides.len() as u8), // 4
            StrideStats::new(SizedStride::Stride8, strides.len() as u8), // 5
        ];

        let node: SizedStrideNode<
            <Store as StorageBackend>::AF,
            <Store as StorageBackend>::NodeType,
        >;

        match strides[0] {
            3 => {
                node = SizedStrideNode::Stride3(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: NodeSet::empty(),
                    pfx_vec: NodeSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[0].inc(0);
            }
            4 => {
                node = SizedStrideNode::Stride4(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: NodeSet::empty(),
                    pfx_vec: NodeSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[1].inc(0);
            }
            5 => {
                node = SizedStrideNode::Stride5(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: NodeSet::empty(),
                    pfx_vec: NodeSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[2].inc(0);
            }
            6 => {
                node = SizedStrideNode::Stride6(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: NodeSet::empty(),
                    pfx_vec: NodeSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[3].inc(0);
            }
            7 => {
                node = SizedStrideNode::Stride7(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: U256(0, 0),
                    ptr_vec: NodeSet::empty(),
                    pfx_vec: NodeSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[4].inc(0);
            }
            8 => {
                node = SizedStrideNode::Stride8(TreeBitMapNode {
                    ptrbitarr: U256(0, 0),
                    pfxbitarr: U512(0, 0, 0, 0),
                    ptr_vec: NodeSet::empty(),
                    pfx_vec: NodeSet::empty(),
                    _af: PhantomData,
                });
                stride_stats[5].inc(0);
            }
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

    pub fn insert(
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

            let nibble = Store::AF::get_nibble(pfx.net, stride_end - stride, nibble_len);
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
                Stride5; 2,
                Stride6; 3,
                Stride7; 4,
                Stride8; 5
            ];

            if let Some(i) = next_node_idx {
                cur_i = i;
                level += 1;
            } else {
                return Ok(());
            }
        }
    }

    pub fn store_node(
        &mut self,
        id: Option<Store::NodeType>,
        next_node: SizedStrideNode<Store::AF, Store::NodeType>,
    ) -> Option<Store::NodeType> {
        self.store.store_node(id, next_node)
    }

    #[inline]
    pub fn retrieve_node(
        &self,
        id: Store::NodeType,
    ) -> SizedNodeOption<Store::AF, Store::NodeType> {
        self.store.retrieve_node(id)
    }

    #[inline]
    pub fn retrieve_node_with_guard(
        &self,
        id: Store::NodeType,
    ) -> CacheGuard<Store::AF, Store::NodeType> {
        self.store.retrieve_node_with_guard(id)
    }

    pub fn get_root_node_id(&self) -> Store::NodeType {
        self.store.get_root_node_id(self.strides[0])
    }

    #[inline]
    pub fn retrieve_node_mut(
        &mut self,
        index: Store::NodeType,
    ) -> SizedNodeResult<Store::AF, Store::NodeType> {
        self.store.retrieve_node_mut(index)
    }

    pub fn store_prefix(
        &mut self,
        next_node: InternalPrefixRecord<Store::AF, Store::Meta>,
    ) -> Result<
        <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
        Box<dyn std::error::Error>,
    > {
        // let id = self.prefixes.len() as u32;
        self.store.store_prefix(next_node)
        // id
    }

    fn update_prefix_meta(
        &mut self,
        update_node_idx: <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
        meta: Store::Meta,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self.store.retrieve_prefix_mut(update_node_idx) {
            Some(update_pfx) => match update_pfx.meta.as_mut() {
                Some(exist_meta) => <Store::Meta>::merge_update(exist_meta, meta),
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
    pub fn retrieve_prefix(
        &self,
        index: <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&InternalPrefixRecord<Store::AF, Store::Meta>> {
        self.store.retrieve_prefix(index)
    }

    #[inline]
    pub fn retrieve_prefix_mut(
        &mut self,
        index: <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&mut InternalPrefixRecord<Store::AF, Store::Meta>> {
        self.store.retrieve_prefix_mut(index)
    }

    // This function assembles all entries in the `pfx_vec` of all child nodes of the
    // `start_node` into one vec, starting from iself and then recursively assembling
    // adding all `pfx_vec`s of its children.
    fn get_all_more_specifics_for_node(
        &self,
        start_node: &SizedStrideNode<Store::AF, Store::NodeType>,
        found_pfx_vec: &mut Vec<Store::NodeType>,
    ) {
        match start_node {
            SizedStrideNode::Stride3(n) => {
                found_pfx_vec.extend_from_slice(n.pfx_vec.as_slice());

                for nn in n.ptr_vec.as_slice().iter() {
                    self.get_all_more_specifics_for_node(
                        &self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride4(n) => {
                found_pfx_vec.extend_from_slice(n.pfx_vec.as_slice());

                for nn in n.ptr_vec.as_slice().iter() {
                    self.get_all_more_specifics_for_node(
                        &self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride5(n) => {
                found_pfx_vec.extend_from_slice(n.pfx_vec.as_slice());

                for nn in n.ptr_vec.as_slice().iter() {
                    self.get_all_more_specifics_for_node(
                        &self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride6(n) => {
                found_pfx_vec.extend_from_slice(n.pfx_vec.as_slice());

                for nn in n.ptr_vec.as_slice().iter() {
                    self.get_all_more_specifics_for_node(
                        &self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride7(n) => {
                found_pfx_vec.extend_from_slice(n.pfx_vec.as_slice());

                for nn in n.ptr_vec.as_slice().iter() {
                    self.get_all_more_specifics_for_node(
                        &self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride8(n) => {
                found_pfx_vec.extend_from_slice(n.pfx_vec.as_slice());

                for nn in n.ptr_vec.as_slice().iter() {
                    self.get_all_more_specifics_for_node(
                        &self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
        }
    }

    // This function assembles the prefixes of a child node starting on a specified bit position in a ptr_vec of
    // `current_node` into a vec, then adds all prefixes of these children recursively into a vec and returns that.
    pub fn get_all_more_specifics_from_nibble<
        S: Stride,
        const PFXARRAYSIZE: usize,
        const PTRARRAYSIZE: usize,
    >(
        &self,
        current_node: &TreeBitMapNode<Store::AF, S, Store::NodeType, PFXARRAYSIZE, PTRARRAYSIZE>,
        nibble: u32,
        nibble_len: u8,
    ) -> Option<Vec<Store::NodeType>>
    where
        S: Stride + std::ops::BitAnd<Output = S> + std::ops::BitOr<Output = S> + Zero,
        <S as Stride>::PtrSize:
            Debug + Binary + Copy + std::ops::BitAnd<Output = S::PtrSize> + PartialOrd + Zero,
    {
        let (cnvec, mut msvec) = current_node.add_more_specifics_at(nibble, nibble_len);

        for child_node in cnvec.iter() {
            self.get_all_more_specifics_for_node(
                &self.retrieve_node(*child_node).unwrap(),
                &mut msvec,
            );
        }
        Some(msvec)
    }
}


impl<'a, Store: StorageBackend> std::fmt::Debug for TreeBitMap<Store> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total_nodes = self.store.get_nodes_len();

        println!("prefix vec size {}", self.store.get_prefixes_len());
        println!("finished building tree...");
        println!("{:?} nodes created", total_nodes);
        println!(
            "size of node: {} bytes",
            std::mem::size_of::<SizedStrideNode<u32, InMemStrideNodeId>>()
        );
        println!(
            "memory used by nodes: {}kb",
            self.store.get_nodes_len() * std::mem::size_of::<SizedStrideNode<u32, InMemStrideNodeId>>()
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
            let max_pfx: u64 = u64::pow(2, stride_bits[1] as u32);

            print!("{}-{}\t", stride_bits[0], stride_bits[1]);

            for _ in 0..n {
                print!("{}", Colour::Blue.paint("█"));
            }

            print!(
                "{}",
                Colour::Blue.paint(bars[((nodes_num % SCALE) / (SCALE / 7)) as usize]) //  = scale / 7
            );

            print!(
                " {}/{} {:.2}%",
                nodes_num,
                max_pfx,
                (nodes_num as f64 / max_pfx as f64) * 100.0
            );
            print!("\n\t");

            let n = (prefixes_num / SCALE) as usize;
            for _ in 0..n {
                print!("{}", Colour::Green.paint("█"));
            }

            print!(
                "{}",
                Colour::Green.paint(bars[((nodes_num % SCALE) / (SCALE / 7)) as usize]) //  = scale / 7
            );

            println!(" {}", prefixes_num);
        }
        Ok(())
    }
}
