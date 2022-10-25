use std::{
    fmt::{Binary, Debug},
    marker::PhantomData,
};

use routecore::record::MergeUpdate;

use crate::af::{AddressFamily, Zero};
use crate::local_vec::node::TreeBitMapNode;
use crate::local_vec::storage_backend::StorageBackend;
use crate::match_node_for_strides_with_local_vec;
use crate::node_id::SortableNodeId;
use crate::prefix_record::InternalPrefixRecord;
use crate::stats::{SizedStride, StrideStats};
use crate::stride::*;
use crate::synth_int::{U256, U512};

#[cfg(feature = "cli")]
use crate::node_id::InMemNodeId;
#[cfg(feature = "cli")]
use ansi_term::Colour;

#[derive(Debug)]
pub enum SizedStrideNode<AF: AddressFamily, NodeId: SortableNodeId + Copy> {
    Stride3(TreeBitMapNode<AF, Stride3, NodeId>),
    Stride4(TreeBitMapNode<AF, Stride4, NodeId>),
    Stride5(TreeBitMapNode<AF, Stride5, NodeId>),
    Stride6(TreeBitMapNode<AF, Stride6, NodeId>),
    Stride7(TreeBitMapNode<AF, Stride7, NodeId>),
    Stride8(TreeBitMapNode<AF, Stride8, NodeId>),
}

pub(crate) type SizedNodeResult<'a, AF, NodeType> =
    Result<&'a mut SizedStrideNode<AF, NodeType>, Box<dyn std::error::Error>>;

impl<AF, NodeId> Default for SizedStrideNode<AF, NodeId>
where
    AF: AddressFamily,
    NodeId: SortableNodeId + Copy,
{
    fn default() -> Self {
        SizedStrideNode::Stride3(TreeBitMapNode {
            ptrbitarr: 0,
            pfxbitarr: 0,
            pfx_vec: vec![],
            ptr_vec: vec![],
            _af: PhantomData,
        })
    }
}

pub struct CacheGuard<
    'a,
    AF: 'static + AddressFamily,
    NodeId: SortableNodeId + Copy,
> {
    pub guard: std::cell::Ref<'a, SizedStrideNode<AF, NodeId>>,
}

impl<'a, AF: 'static + AddressFamily, NodeId: SortableNodeId + Copy>
    std::ops::Deref for CacheGuard<'a, AF, NodeId>
{
    type Target = SizedStrideNode<AF, NodeId>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

pub(crate) struct PrefixCacheGuard<
    'a,
    AF: 'static + AddressFamily,
    Meta: routecore::record::Meta,
> {
    pub guard: std::cell::Ref<'a, InternalPrefixRecord<AF, Meta>>,
}

impl<'a, AF: 'static + AddressFamily, Meta: routecore::record::Meta>
    std::ops::Deref for PrefixCacheGuard<'a, AF, Meta>
{
    type Target = InternalPrefixRecord<AF, Meta>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

pub(crate) enum NewNodeOrIndex<
    AF: AddressFamily,
    NodeId: SortableNodeId + Copy,
> {
    NewNode(SizedStrideNode<AF, NodeId>, NodeId::Sort), // New Node and bit_id of the new node
    ExistingNode(NodeId),
    NewPrefix,
    ExistingPrefix(NodeId::Part),
}

pub(crate) struct TreeBitMap<Store>
where
    Store: StorageBackend,
{
    pub strides: Vec<u8>,
    pub stats: Vec<StrideStats>,
    pub store: Store,
}

impl<Store> TreeBitMap<Store>
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
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[0].inc(0);
            }
            4 => {
                node = SizedStrideNode::Stride4(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[1].inc(0);
            }
            5 => {
                node = SizedStrideNode::Stride5(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[2].inc(0);
            }
            6 => {
                node = SizedStrideNode::Stride6(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[3].inc(0);
            }
            7 => {
                node = SizedStrideNode::Stride7(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: U256(0, 0),
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[4].inc(0);
            }
            8 => {
                node = SizedStrideNode::Stride8(TreeBitMapNode {
                    ptrbitarr: U256(0, 0),
                    pfxbitarr: U512(0, 0, 0, 0),
                    ptr_vec: vec![],
                    pfx_vec: vec![],
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

    pub(crate) fn insert(
        &mut self,
        pfx: InternalPrefixRecord<Store::AF, Store::Meta>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stride_end: u8 = 0;
        let mut cur_i = self.store.get_root_node_id();
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

            let next_node_idx = match_node_for_strides_with_local_vec![
                // applicable to the whole outer match in the marco
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

    pub(crate) fn store_node(
        &mut self,
        id: Option<Store::NodeType>,
        next_node: SizedStrideNode<Store::AF, Store::NodeType>,
    ) -> Option<Store::NodeType> {
        self.store.store_node(id, next_node)
    }

    #[inline]
    pub(crate) fn retrieve_node(
        &self,
        id: Store::NodeType,
    ) -> Option<&SizedStrideNode<Store::AF, Store::NodeType>> {
        self.store.retrieve_node(id)
    }

    pub(crate) fn get_root_node_id(&self) -> Store::NodeType {
        self.store.get_root_node_id()
    }

    #[inline]
    pub(crate) fn retrieve_node_mut(
        &mut self,
        index: Store::NodeType,
    ) -> SizedNodeResult<Store::AF, Store::NodeType> {
        self.store.retrieve_node_mut(index)
    }

    pub(crate) fn store_prefix(
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

    pub(crate) fn update_prefix_meta(
        &mut self,
        update_node_idx: <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
        meta: Store::Meta,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self.store.retrieve_prefix_mut(update_node_idx) {
            Some(update_pfx) => {
                <Store::Meta>::merge_update(&mut update_pfx.meta, meta)
            }
            // TODO
            // Use/create proper error types
            None => Err("Prefix not found".into()),
        }
    }

    #[inline]
    pub(crate) fn retrieve_prefix(
        &self,
        index: <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&InternalPrefixRecord<Store::AF, Store::Meta>> {
        self.store.retrieve_prefix(index)
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
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride4(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride5(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride6(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride7(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride8(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
        }
    }

    // This function assembles the prefixes of a child node starting on a specified bit position in a ptr_vec of
    // `current_node` into a vec, then adds all prefixes of these children recursively into a vec and returns that.
    pub fn get_all_more_specifics_from_nibble<S: Stride>(
        &self,
        current_node: &TreeBitMapNode<Store::AF, S, Store::NodeType>,
        nibble: u32,
        nibble_len: u8,
    ) -> Option<Vec<Store::NodeType>>
    where
        S: Stride
            + std::ops::BitAnd<Output = S>
            + std::ops::BitOr<Output = S>
            + Zero,
        <S as Stride>::PtrSize: Debug
            + Binary
            + Copy
            + std::ops::BitAnd<Output = S::PtrSize>
            + PartialOrd
            + Zero,
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
impl<Store: StorageBackend> std::fmt::Display for TreeBitMap<Store> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total_nodes = self.store.get_nodes_len();

        writeln!(_f, "prefix vec size {}", self.store.get_prefixes_len())?;
        writeln!(_f, "finished building tree...")?;
        writeln!(_f, "{:?} nodes created", total_nodes)?;
        writeln!(_f,
            "size of node: {} bytes",
            std::mem::size_of::<SizedStrideNode<u32, InMemNodeId>>()
        )?;
        writeln!(
            _f,
            "memory used by nodes: {}kb",
            self.store.get_nodes_len()
                * std::mem::size_of::<SizedStrideNode<u32, InMemNodeId>>()
                / 1024
        )?;

        writeln!(_f, "stride division {:?}", self.strides)?;
        for s in &self.stats {
            writeln!(_f, "{:?}", s)?;
        }

        writeln!(
            _f,
            "level\t[{}|{}] nodes occupied/max nodes percentage_max_nodes_occupied prefixes",
            Colour::Blue.paint("nodes"),
            Colour::Green.paint("prefixes")
        )?;
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

            write!(_f, "{}-{}\t", stride_bits[0], stride_bits[1])?;

            for _ in 0..n {
                write!(_f, "{}", Colour::Blue.paint("█"))?;
            }

            writeln!(
                _f,
                "{}",
                Colour::Blue.paint(
                    bars[((nodes_num % SCALE) / (SCALE / 7)) as usize]
                ) //  = scale / 7
            )?;

            writeln!(
                _f,
                " {}/{} {:.2}%",
                nodes_num,
                max_pfx.0,
                (nodes_num as f64 / max_pfx.0 as f64) * 100.0
            )?;
            write!(_f, "\n\t")?;

            let n = (prefixes_num / SCALE) as usize;
            for _ in 0..n {
                write!(_f, "{}", Colour::Green.paint("█"))?;
            }

            write!(
                _f,
                "{}",
                Colour::Green.paint(
                    bars[((nodes_num % SCALE) / (SCALE / 7)) as usize]
                ) //  = scale / 7
            )?;

            writeln!(_f," {}", prefixes_num)?;
        }
        Ok(())
    }
}
