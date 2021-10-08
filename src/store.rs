use crate::tree::*;
use crate::stride::*;
use crate::synth_int::Zero;

use crate::{AddressFamily, MergeUpdate, Meta, NoMeta, Prefix};
use crate::synth_int::{U256, U512};
use std::{
    fmt::{Binary, Debug},
    marker::PhantomData,
};

pub trait StorageBackend
where
    Self::NodeType: SortableNodeId + Copy,
{
    type NodeType;
    type AF: AddressFamily;
    type Meta: Meta + MergeUpdate;

    fn init(start_node: Option<SizedStrideNode<Self::AF, Self::NodeType>>) -> Self;
    fn acquire_new_node_id(
        &self,
        sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        //
        level: u8,
    ) -> <Self as StorageBackend>::NodeType;
    // store_node should return an index with the associated type `Part` of the associated type
    // of this trait.
    // `id` is optional, since a vec uses the indexes as the ids of the nodes,
    // other storage data-structures may use unordered lists, where the id is in the
    // record, e.g., dynamodb
    fn store_node(
        &mut self,
        id: Option<Self::NodeType>,
        next_node: SizedStrideNode<Self::AF, Self::NodeType>,
    ) -> Option<Self::NodeType>;
    fn update_node(
        &mut self,
        current_node_id: Self::NodeType,
        updated_node: SizedStrideNode<Self::AF, Self::NodeType>,
    );
    fn retrieve_node(
        &self,
        index: Self::NodeType,
    ) -> Option<SizedStrideNode<Self::AF, Self::NodeType>>;
    fn retrieve_node_mut(
        &mut self,
        index: Self::NodeType,
    ) -> Result<SizedStrideNode<Self::AF, Self::NodeType>, Box<dyn std::error::Error>>;
    fn retrieve_node_with_guard(
        &self,
        index: Self::NodeType,
    ) -> CacheGuard<Self::AF, Self::NodeType>;
    fn get_nodes(&self) -> Vec<SizedStrideRef<Self::AF, Self::NodeType>>;
    fn get_root_node_id(&self, stride_size: u8) -> Self::NodeType;
    fn get_root_node_mut(
        &mut self,
        stride_size: u8,
    ) -> Option<SizedStrideNode<Self::AF, Self::NodeType>>;
    fn get_nodes_len(&self) -> usize;
    fn acquire_new_prefix_id(
        &self,
        sort: &<<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        part: &Prefix<Self::AF, Self::Meta>,
    ) -> <Self as StorageBackend>::NodeType;
    fn store_prefix(
        &mut self,
        next_node: Prefix<Self::AF, Self::Meta>,
    ) -> Result<
        <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
        Box<dyn std::error::Error>,
    >;
    fn retrieve_prefix(
        &self,
        index: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&Prefix<Self::AF, Self::Meta>>;
    fn retrieve_prefix_mut(
        &mut self,
        index: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&mut Prefix<Self::AF, Self::Meta>>;
    fn retrieve_prefix_with_guard(
        &self,
        index: Self::NodeType,
    ) -> PrefixCacheGuard<Self::AF, Self::Meta>;
    fn get_prefixes_len(&self) -> usize;
    fn prefixes_iter(
        &self,
    ) -> Result<std::slice::Iter<'_, Prefix<Self::AF, Self::Meta>>, Box<dyn std::error::Error>>;
    fn prefixes_iter_mut(
        &mut self,
    ) -> Result<std::slice::IterMut<'_, Prefix<Self::AF, Self::Meta>>, Box<dyn std::error::Error>>;
}

#[derive(Debug)]
pub struct InMemStorage<AF: AddressFamily, Meta: crate::common::Meta> {
    // pub nodes: Vec<SizedStrideNode<AF, InMemNodeId>>,
    // each stride in its own vec avoids having to store SizedStrideNode, an enum, that will have
    // the size of the largest variant as its memory footprint (Stride8).
    pub nodes3: Vec<TreeBitMapNode<AF, Stride3, InMemStrideNodeId, 14, 8>>,
    pub nodes4: Vec<TreeBitMapNode<AF, Stride4, InMemStrideNodeId, 30, 16>>,
    pub nodes5: Vec<TreeBitMapNode<AF, Stride5, InMemStrideNodeId, 62, 32>>,
    pub nodes6: Vec<TreeBitMapNode<AF, Stride6, InMemStrideNodeId, 126, 64>>,
    pub nodes7: Vec<TreeBitMapNode<AF, Stride7, InMemStrideNodeId, 254, 128>>,
    pub nodes8: Vec<TreeBitMapNode<AF, Stride8, InMemStrideNodeId, 510, 256>>,
    pub prefixes: Vec<Prefix<AF, Meta>>
}

impl<AF: AddressFamily, Meta: crate::common::Meta + MergeUpdate> StorageBackend
    for InMemStorage<AF, Meta>
{
    type NodeType = InMemStrideNodeId;

    type AF = AF;
    type Meta = Meta;

    fn init(
        start_node: Option<SizedStrideNode<Self::AF, Self::NodeType>>,
    ) -> InMemStorage<AF, Meta> {
        // let mut nodes = vec![];
        let mut nodes3 = vec![];
        let mut nodes4 = vec![];
        let mut nodes5 = vec![];
        let mut nodes6 = vec![];
        let mut nodes7 = vec![];
        let mut nodes8 = vec![];
        if let Some(n) = start_node {
            // nodes = vec![n];
            match n {
                SizedStrideNode::Stride3(nodes) => {
                    nodes3 = vec![nodes];
                }
                SizedStrideNode::Stride4(nodes) => {
                    nodes4 = vec![nodes];
                }
                SizedStrideNode::Stride5(nodes) => {
                    nodes5 = vec![nodes];
                }
                SizedStrideNode::Stride6(nodes) => {
                    nodes6 = vec![nodes];
                }
                SizedStrideNode::Stride7(nodes) => {
                    nodes7 = vec![nodes];
                }
                SizedStrideNode::Stride8(nodes) => {
                    nodes8 = vec![nodes];
                }
            }
        }

        InMemStorage {
            nodes3,
            nodes4,
            nodes5,
            nodes6,
            nodes7,
            nodes8,
            prefixes: vec![]
        }
    }

    fn acquire_new_node_id(
        &self,
        sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        // part: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
        level: u8,
    ) -> <Self as StorageBackend>::NodeType {
        // We're ignoring the part parameter here, because we want to store
        // the index into the global self.nodes vec in the local vec.
        match level {
            3 => InMemStrideNodeId::new(
                &sort,
                &StrideNodeId(StrideType::Stride3, self.nodes3.len() as u32),
            ),
            4 => InMemStrideNodeId::new(
                &sort,
                &StrideNodeId(StrideType::Stride4, self.nodes4.len() as u32),
            ),
            5 => InMemStrideNodeId::new(
                &sort,
                &StrideNodeId(StrideType::Stride5, self.nodes5.len() as u32),
            ),
            6 => InMemStrideNodeId::new(
                &sort,
                &StrideNodeId(StrideType::Stride6, self.nodes6.len() as u32),
            ),
            7 => InMemStrideNodeId::new(
                &sort,
                &StrideNodeId(StrideType::Stride7, self.nodes7.len() as u32),
            ),
            8 => InMemStrideNodeId::new(
                &sort,
                &StrideNodeId(StrideType::Stride8, self.nodes8.len() as u32),
            ),
            _ => panic!("Invalid level"),
        }
        // InMemStrideNodeId(sort, self.nodes3.len() as u32)
    }

    fn store_node(
        &mut self,
        _id: Option<Self::NodeType>,
        next_node: SizedStrideNode<Self::AF, Self::NodeType>,
    ) -> Option<Self::NodeType> {
        match next_node {
            SizedStrideNode::Stride3(node) => {
                let id = self.nodes3.len() as u32;
                self.nodes3.push(node);
                Some(InMemStrideNodeId::new(
                    &0,
                    &StrideNodeId(StrideType::Stride3, id),
                ))
            }
            SizedStrideNode::Stride4(node) => {
                let id = self.nodes4.len() as u32;
                self.nodes4.push(node);
                Some(InMemStrideNodeId::new(
                    &0,
                    &StrideNodeId(StrideType::Stride4, id),
                ))
            }
            SizedStrideNode::Stride5(node) => {
                let id = self.nodes5.len() as u32;
                self.nodes5.push(node);
                Some(InMemStrideNodeId::new(
                    &0,
                    &StrideNodeId(StrideType::Stride5, id),
                ))
            }
            SizedStrideNode::Stride6(node) => {
                let id = self.nodes6.len() as u32;
                self.nodes6.push(node);
                Some(InMemStrideNodeId::new(
                    &0,
                    &StrideNodeId(StrideType::Stride6, id),
                ))
            }
            SizedStrideNode::Stride7(node) => {
                let id = self.nodes7.len() as u32;
                self.nodes7.push(node);
                Some(InMemStrideNodeId::new(
                    &0,
                    &StrideNodeId(StrideType::Stride7, id),
                ))
            }
            SizedStrideNode::Stride8(node) => {
                let id = self.nodes8.len() as u32;
                self.nodes8.push(node);
                Some(InMemStrideNodeId::new(
                    &0,
                    &StrideNodeId(StrideType::Stride8, id),
                ))
            }
        }
        // let id = self.nodes.len() as u32;
        // self.nodes.push(next_node);
        // Some(InMemNodeId::new(&0, &id))
    }

    fn update_node(
        &mut self,
        current_node_id: Self::NodeType,
        updated_node: SizedStrideNode<Self::AF, Self::NodeType>,
    ) {
        // std::mem::replace(&mut self.retrieve_node_mut(current_node_id).unwrap(), node);

        match updated_node {
            SizedStrideNode::Stride3(node) => {
                let _default_val = std::mem::replace(
                    self.nodes3
                        .get_mut::<usize>(current_node_id.get_part().into())
                        .unwrap(),
                    node,
                );
            }
            SizedStrideNode::Stride4(node) => {
                let _default_val = std::mem::replace(
                    self.nodes4
                        .get_mut::<usize>(current_node_id.get_part().into())
                        .unwrap(),
                    node,
                );
            }
            SizedStrideNode::Stride5(node) => {
                let _default_val = std::mem::replace(
                    self.nodes5
                        .get_mut::<usize>(current_node_id.get_part().into())
                        .unwrap(),
                    node,
                );
            }
            SizedStrideNode::Stride6(node) => {
                let _default_val = std::mem::replace(
                    self.nodes6
                        .get_mut::<usize>(current_node_id.get_part().into())
                        .unwrap(),
                    node,
                );
            }
            SizedStrideNode::Stride7(node) => {
                let _default_val = std::mem::replace(
                    self.nodes7
                        .get_mut::<usize>(current_node_id.get_part().into())
                        .unwrap(),
                    node,
                );
            }
            SizedStrideNode::Stride8(node) => {
                let _default_val = std::mem::replace(
                    self.nodes8
                        .get_mut::<usize>(current_node_id.get_part().into())
                        .unwrap(),
                    node,
                );
            }
        }
    }

    fn retrieve_node(
        &self,
        id: Self::NodeType,
    ) -> Option<SizedStrideNode<Self::AF, Self::NodeType>> {
        match id.get_part() {
            StrideNodeId(StrideType::Stride3, part_id) => self
                .nodes3
                .get(part_id as usize)
                .map(|n| SizedStrideNode::Stride3(*n)),
            StrideNodeId(StrideType::Stride4, part_id) => self
                .nodes4
                .get(part_id as usize)
                .map(|n| SizedStrideNode::Stride4(*n)),
            StrideNodeId(StrideType::Stride5, part_id) => self
                .nodes5
                .get(part_id as usize)
                .map(|n| SizedStrideNode::Stride5(*n)),
            StrideNodeId(StrideType::Stride6, part_id) => self
                .nodes6
                .get(part_id as usize)
                .map(|n| SizedStrideNode::Stride6(*n)),
            StrideNodeId(StrideType::Stride7, part_id) => self
                .nodes7
                .get(part_id as usize)
                .map(|n| SizedStrideNode::Stride7(*n)),
            StrideNodeId(StrideType::Stride8, part_id) => self
                .nodes8
                .get(part_id as usize)
                .map(|n| SizedStrideNode::Stride8(*n)),
        }
    }

    fn retrieve_node_mut(
        &mut self,
        id: Self::NodeType,
    ) -> Result<SizedStrideNode<Self::AF, Self::NodeType>, Box<dyn std::error::Error>> {
        match id.get_part() {
            StrideNodeId(StrideType::Stride3, part_id) => Ok(SizedStrideNode::Stride3(
                *self
                    .nodes3
                    .get_mut(part_id as usize)
                    .unwrap_or_else(|| panic!("no {:?} in stride 3 collection", id)),
                // .ok_or_else(|| {
                //     Box::new(Error::new(ErrorKind::Other, "Retrieve Node Error")).into()
                // })
                // .unwrap(),
            )),
            StrideNodeId(StrideType::Stride4, part_id) => Ok(SizedStrideNode::Stride4(
                *self.nodes4.get_mut(part_id as usize).unwrap(),
            )),
            StrideNodeId(StrideType::Stride5, part_id) => Ok(SizedStrideNode::Stride5(
                *self.nodes5.get_mut(part_id as usize).unwrap(),
            )),
            StrideNodeId(StrideType::Stride6, part_id) => Ok(SizedStrideNode::Stride6(
                *self.nodes6.get_mut(part_id as usize).unwrap(),
            )),
            StrideNodeId(StrideType::Stride7, part_id) => Ok(SizedStrideNode::Stride7(
                *self.nodes7.get_mut(part_id as usize).unwrap(),
            )),
            StrideNodeId(StrideType::Stride8, part_id) => Ok(SizedStrideNode::Stride8(
                *self.nodes8.get_mut(part_id as usize).unwrap(),
            )),
        }
    }

    // Don't use this function, this is just a placeholder and a really
    // inefficient implementation.
    fn retrieve_node_with_guard(
        &self,
        _id: Self::NodeType,
    ) -> CacheGuard<Self::AF, Self::NodeType> {
        panic!("Not Implemented for InMeMStorage");
    }

    fn get_nodes(&self) -> Vec<SizedStrideRef<Self::AF, Self::NodeType>> {
        self.nodes3
            .iter()
            .map(|n| SizedStrideRef::Stride3(n))
            .chain(self.nodes4.iter().map(|n| SizedStrideRef::Stride4(n)))
            .chain(self.nodes5.iter().map(|n| SizedStrideRef::Stride5(n)))
            .chain(self.nodes6.iter().map(|n| SizedStrideRef::Stride6(n)))
            .chain(self.nodes7.iter().map(|n| SizedStrideRef::Stride7(n)))
            .chain(self.nodes8.iter().map(|n| SizedStrideRef::Stride8(n)))
            .collect()
    }

    fn get_root_node_id(&self, first_stride_size: u8) -> Self::NodeType {
        let first_stride_type = match first_stride_size {
            3 => StrideType::Stride3,
            4 => StrideType::Stride4,
            5 => StrideType::Stride5,
            6 => StrideType::Stride6,
            7 => StrideType::Stride7,
            8 => StrideType::Stride8,
            _ => panic!("Invalid stride size"),
        };
        InMemStrideNodeId::new(&0, &StrideNodeId(first_stride_type, 0))
    }

    fn get_root_node_mut(
        &mut self,
        stride_size: u8,
    ) -> Option<SizedStrideNode<Self::AF, Self::NodeType>> {
        match stride_size {
            3 => Some(SizedStrideNode::Stride3(self.nodes3[0])),
            4 => Some(SizedStrideNode::Stride4(self.nodes4[0])),
            5 => Some(SizedStrideNode::Stride5(self.nodes5[0])),
            6 => Some(SizedStrideNode::Stride6(self.nodes6[0])),
            7 => Some(SizedStrideNode::Stride7(self.nodes7[0])),
            8 => Some(SizedStrideNode::Stride8(self.nodes8[0])),
            _ => panic!("invalid stride size"),
        }
    }

    fn get_nodes_len(&self) -> usize {
        self.nodes3.len()
            + self.nodes4.len()
            + self.nodes5.len()
            + self.nodes6.len()
            + self.nodes7.len()
            + self.nodes8.len()
    }

    fn acquire_new_prefix_id(
        &self,
        sort: &<<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        _part: &Prefix<<Self as StorageBackend>::AF, <Self as StorageBackend>::Meta>,
    ) -> <Self as StorageBackend>::NodeType {
        // We're ignoring the part parameter here, because we want to store
        // the index into the global self.prefixes vec in the local vec.
        InMemStrideNodeId::new(
            sort,
            &StrideNodeId(StrideType::Stride5, self.prefixes.len() as u32),
        )
    }

    fn store_prefix(
        &mut self,
        next_node: Prefix<Self::AF, Self::Meta>,
    ) -> Result<StrideNodeId, Box<dyn std::error::Error>> {
        let part_id = self.prefixes.len() as u32;
        self.prefixes.push(next_node);
        Ok(StrideNodeId(StrideType::Stride5, part_id))
    }

    fn retrieve_prefix(&self, part_id: StrideNodeId) -> Option<&Prefix<Self::AF, Self::Meta>> {
        self.prefixes.get(part_id.1 as usize)
    }

    fn retrieve_prefix_mut(
        &mut self,
        part_id: StrideNodeId,
    ) -> Option<&mut Prefix<Self::AF, Self::Meta>> {
        self.prefixes.get_mut(part_id.1 as usize)
    }

    fn retrieve_prefix_with_guard(
        &self,
        _index: Self::NodeType,
    ) -> PrefixCacheGuard<Self::AF, Self::Meta> {
        panic!("nOt ImPlEmEnTed for InMemNode");
    }

    fn get_prefixes_len(&self) -> usize {
        self.prefixes.len()
    }

    fn prefixes_iter(
        &self,
    ) -> Result<std::slice::Iter<'_, Prefix<AF, Meta>>, Box<dyn std::error::Error>> {
        Ok(self.prefixes.iter())
    }

    fn prefixes_iter_mut(
        &mut self,
    ) -> Result<std::slice::IterMut<'_, Prefix<AF, Meta>>, Box<dyn std::error::Error>> {
        Ok(self.prefixes.iter_mut())
    }
}

pub struct CacheGuard<'a, AF: 'static + AddressFamily, NodeId: SortableNodeId + Copy> {
    pub guard: std::cell::Ref<'a, SizedStrideNode<AF, NodeId>>,
}

impl<'a, AF: 'static + AddressFamily, NodeId: SortableNodeId + Copy> std::ops::Deref
    for CacheGuard<'a, AF, NodeId>
{
    type Target = SizedStrideNode<AF, NodeId>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

pub struct PrefixCacheGuard<'a, AF: 'static + AddressFamily, Meta: crate::common::Meta> {
    pub guard: std::cell::Ref<'a, Prefix<AF, Meta>>,
}

impl<'a, AF: 'static + AddressFamily, Meta: crate::common::Meta> std::ops::Deref
    for PrefixCacheGuard<'a, AF, Meta>
{
    type Target = Prefix<AF, Meta>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<AF, S, NodeId, const PFXARRAYSIZE: usize, const PTRARRAYSIZE: usize>
    TreeBitMapNode<AF, S, NodeId, PFXARRAYSIZE, PTRARRAYSIZE>
where
    AF: AddressFamily,
    S: Stride + std::ops::BitAnd<Output = S> + std::ops::BitOr<Output = S> + Zero,
    <S as Stride>::PtrSize:
        Debug + Binary + Copy + std::ops::BitAnd<Output = S::PtrSize> + PartialOrd + Zero,
    NodeId: SortableNodeId + Copy,
{
    // Inspects the stride (nibble, nibble_len) to see it there's
    // already a child node (if not at the last stride) or a prefix
    //  (if it's the last stride).
    //
    // Returns one of:
    // - A newly created child node.
    // - The index of the existing child node in the global `nodes` vec
    // - A newly created Prefix
    // - The index of the existing prefix in the global `prefixes` vec
    pub fn eval_node_or_prefix_at(
        &mut self,
        nibble: u32,
        nibble_len: u8,
        next_stride: Option<&u8>,
        is_last_stride: bool,
    ) -> NewNodeOrIndex<AF, NodeId> {
        let bit_pos = S::get_bit_pos(nibble, nibble_len);
        let new_node: SizedStrideNode<AF, NodeId>;

        // Check that we're not at the last stride (pfx.len <= stride_end),
        // Note that next_stride may have a value, but we still don't want to
        // continue, because we've exceeded the length of the prefix to
        // be inserted.
        // Also note that a nibble_len < S::BITS (a smaller than full nibble)
        // does indeed indicate the last stride has been reached, but the
        // reverse is *not* true, i.e. a full nibble can also be the last
        // stride. Hence the `is_last_stride` argument
        if !is_last_stride {
            // We are not at the last stride
            // Check it the ptr bit is already set in this position
            if (S::into_stride_size(self.ptrbitarr) & bit_pos) == S::zero() {
                // Nope, set it and create a child node
                self.ptrbitarr =
                    S::into_ptrbitarr_size(bit_pos | S::into_stride_size(self.ptrbitarr));

                match next_stride.unwrap() {
                    3_u8 => {
                        new_node = SizedStrideNode::Stride3(TreeBitMapNode {
                            ptrbitarr: <Stride3 as Stride>::PtrSize::zero(),
                            pfxbitarr: Stride3::zero(),
                            pfx_vec: NodeSet::empty(),
                            ptr_vec: NodeSet::empty(),
                            _af: PhantomData,
                        });
                    }
                    4_u8 => {
                        new_node = SizedStrideNode::Stride4(TreeBitMapNode {
                            ptrbitarr: <Stride4 as Stride>::PtrSize::zero(),
                            pfxbitarr: Stride4::zero(),
                            pfx_vec: NodeSet::empty(),
                            ptr_vec: NodeSet::empty(),
                            _af: PhantomData,
                        });
                    }
                    5_u8 => {
                        new_node = SizedStrideNode::Stride5(TreeBitMapNode {
                            ptrbitarr: <Stride5 as Stride>::PtrSize::zero(),
                            pfxbitarr: Stride5::zero(),
                            pfx_vec: NodeSet::empty(),
                            ptr_vec: NodeSet::empty(),
                            _af: PhantomData,
                        });
                    }
                    6_u8 => {
                        new_node = SizedStrideNode::Stride6(TreeBitMapNode {
                            ptrbitarr: <Stride6 as Stride>::PtrSize::zero(),
                            pfxbitarr: Stride6::zero(),
                            pfx_vec: NodeSet::empty(),
                            ptr_vec: NodeSet::empty(),
                            _af: PhantomData,
                        });
                    }
                    7_u8 => {
                        new_node = SizedStrideNode::Stride7(TreeBitMapNode {
                            ptrbitarr: 0_u128,
                            pfxbitarr: U256(0_u128, 0_u128),
                            pfx_vec: NodeSet::empty(),
                            ptr_vec: NodeSet::empty(),
                            _af: PhantomData,
                        });
                    }
                    8_u8 => {
                        new_node = SizedStrideNode::Stride8(TreeBitMapNode {
                            ptrbitarr: U256(0_u128, 0_u128),
                            pfxbitarr: U512(0_u128, 0_u128, 0_u128, 0_u128),
                            pfx_vec: NodeSet::empty(),
                            ptr_vec: NodeSet::empty(),
                            _af: PhantomData,
                        });
                    }
                    _ => {
                        panic!("can't happen");
                    }
                };

                // we can return bit_pos.leading_zeros() since bit_pos is the bitmap that
                // points to the current bit in ptrbitarr (it's **not** the prefix of the node!),
                // so the number of zeros in front of it should always be unique and describes
                // the index of this node in the ptrbitarr.
                // ex.:
                // In a stride3 (ptrbitarr lenght is 8):
                // bit_pos 0001 0000
                // so this is the fourth bit, so points to index = 3
                return NewNodeOrIndex::NewNode(new_node, (bit_pos.leading_zeros() as u16).into());
            }
        } else {
            // only at the last stride do we create the bit in the prefix bitmap,
            // and only if it doesn't exist already
            if self.pfxbitarr & bit_pos == <S as std::ops::BitAnd>::Output::zero() {
                self.pfxbitarr = bit_pos | self.pfxbitarr;
                return NewNodeOrIndex::NewPrefix;
            }
            return NewNodeOrIndex::ExistingPrefix(
                self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, nibble_len)].get_part(),
            );
        }

        NewNodeOrIndex::ExistingNode(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)])
    }

    //-------------------- Search nibble functions -------------------------------

    // This function looks for the longest marching prefix in the provided nibble,
    // by iterating over all the bits in it and comparing that with the appriopriate
    // bytes from the requested prefix.
    // It mutates the `less_specifics_vec` that was passed in to hold all the prefixes
    // found along the way.
    pub fn search_stride_for_longest_match_at(
        &self,
        search_pfx: &Prefix<AF, NoMeta>,
        mut nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        less_specifics_vec: &mut Option<Vec<NodeId>>,
    ) -> (Option<NodeId>, Option<NodeId>) {
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;

        for n_l in 1..(nibble_len + 1) {
            // Move the bit in the right position.
            nibble = AddressFamily::get_nibble(search_pfx.net, start_bit, n_l);
            bit_pos = S::get_bit_pos(nibble, n_l);

            // Check if the prefix has been set, if so select the prefix. This is not
            // necessarily the final prefix that will be returned.

            // Check it there's a prefix matching in this bitmap for this nibble
            if self.pfxbitarr & bit_pos > S::zero() {
                let f_pfx = self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, n_l)];

                // Receiving a less_specifics_vec means that the user wants to have
                // all the last-specific prefixes returned, so add the found prefix.
                if let Some(ls_vec) = less_specifics_vec {
                    if !(search_pfx.len <= start_bit + nibble_len
                        || (S::into_stride_size(self.ptrbitarr)
                            & S::get_bit_pos(nibble, nibble_len))
                            == S::zero())
                    {
                        ls_vec.push(f_pfx);
                    }
                }

                found_pfx = Some(f_pfx);
            }
        }

        // Check if this the last stride, or if they're no more children to go to,
        // if so return what we found up until now.
        if search_pfx.len <= start_bit + nibble_len
            || (S::into_stride_size(self.ptrbitarr) & bit_pos) == S::zero()
        // No children or at the end, return the definitive LMP we found.
        {
            return (
                None,      /* no more children */
                found_pfx, /* The definitive LMP if any */
            );
        }

        // There's another child, return it together with the preliminary LMP we found.
        (
            Some(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)]), /* The node that has children the next stride */
            found_pfx,
        )
    }

    // This function looks for the exactly matching prefix in the provided nibble.
    // It doesn't needd to iterate over anything it just compares the complete nibble, with
    // the appropriate bits in the requested prefix.
    // Although this is rather efficient, there's no way to collect less-specific prefixes from
    // the search prefix.
    pub fn search_stride_for_exact_match_at(
        &self,
        search_pfx: &Prefix<AF, NoMeta>,
        nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        _: &mut Option<Vec<NodeId>>,
    ) -> (Option<NodeId>, Option<NodeId>) {
        // This is an exact match, so we're only considering the position of the full nibble.
        let bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;
        let mut found_child = None;

        // Is this the last nibble?
        // Otherwise we're not looking for a prefix (exact matching only lives at last nibble)
        match search_pfx.len <= start_bit + nibble_len {
            // We're at the last nibble.
            true => {
                // Check for an actual prefix at the right position, i.e. consider the complete nibble
                if self.pfxbitarr & bit_pos > S::zero() {
                    found_pfx =
                        Some(self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, nibble_len)]);
                }
            }
            // We're not at the last nibble.
            false => {
                // Check for a child node at the right position, i.e. consider the complete nibble.
                if (S::into_stride_size(self.ptrbitarr) & bit_pos) > S::zero() {
                    found_child = Some(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)]);
                }
            }
        }

        (
            found_child, /* The node that has children in the next stride, if any */
            found_pfx,   /* The exactly matching prefix, if any */
        )
    }

    // This function looks for the exactly matching prefix in the provided nibble,
    // just like the one above, but this *does* iterate over all the bytes in the nibble to collect
    // the less-specific prefixes of the the search prefix.
    // This is of course slower, so it should only be used when the user explicitly requests less-specifics.
    pub fn search_stride_for_exact_match_with_less_specifics_at(
        &self,
        search_pfx: &Prefix<AF, NoMeta>,
        mut nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        less_specifics_vec: &mut Option<Vec<NodeId>>,
    ) -> (Option<NodeId>, Option<NodeId>) {
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;

        let ls_vec = less_specifics_vec
            .as_mut()
            .expect("You shouldn't call this function without a `less_specifics_vec` buffer. Supply one when calling this function or use `search_stride_for_exact_match_at`");

        for n_l in 1..(nibble_len + 1) {
            // Move the bit in the right position.
            nibble = AddressFamily::get_nibble(search_pfx.net, start_bit, n_l);
            bit_pos = S::get_bit_pos(nibble, n_l);

            // Check if the prefix has been set, if so select the prefix. This is not
            // necessarily the final prefix that will be returned.

            // Check it there's a prefix matching in this bitmap for this nibble,

            if self.pfxbitarr & bit_pos > S::zero() {
                // since we want an exact match only, we will fill the prefix field only
                // if we're exactly at the last bit of the nibble
                if n_l == nibble_len {
                    found_pfx = Some(self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, n_l)]);
                }

                // Receiving a less_specifics_vec means that the user wants to have
                // all the last-specific prefixes returned, so add the found prefix.
                ls_vec.push(self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, n_l)]);
            }
        }

        if found_pfx.is_none() {
            // no prefix here, clear out all of the prefixes we found along the way,
            // since it doesn't make sense to return less-specifics if we don't have a exact match.
            ls_vec.clear();
        }

        // Check if this the last stride, or if they're no more children to go to,
        // if so return what we found up until now.
        match search_pfx.len <= start_bit + nibble_len
            || (S::into_stride_size(self.ptrbitarr) & bit_pos)
                == <S as std::ops::BitAnd>::Output::zero()
        {
            // No children or at the end, return the definitive LMP we found.
            true => (
                None,      /* no more children */
                found_pfx, /* The definitive LMP if any */
            ),
            // There's another child, we won't return the found_pfx, since we're not
            // at the last nibble and we want an exact match only.
            false => (
                Some(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)]), /* The node that has children the next stride */
                None,
            ),
        }
    }

    // Search a stride for more-specific prefixes and child nodes containing
    // more specifics for `search_prefix`.
    pub fn add_more_specifics_at(
        &self,
        nibble: u32,
        nibble_len: u8,
    ) -> (
        // Option<NodeId>, /* the node with children in the next stride  */
        Vec<NodeId>, /* child nodes with more more-specifics in this stride */
        Vec<NodeId>, /* more-specific prefixes in this stride */
    ) {
        let mut found_children_with_more_specifics = vec![];
        let mut found_more_specifics_vec: Vec<NodeId> = vec![];

        // This is an exact match, so we're only considering the position of the full nibble.
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_child = None;

        // Is there also a child node here?
        // Note that even without a child node, there may be more specifics further up in this
        // pfxbitarr or children in this ptrbitarr.
        if (S::into_stride_size(self.ptrbitarr) & bit_pos) > S::zero() {
            found_child = Some(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)]);
        }

        if let Some(child) = found_child {
            found_children_with_more_specifics.push(child);
        }

        // println!("{}..{}", nibble_len + start_bit, S::STRIDE_LEN + start_bit);
        // println!("start nibble: {:032b}", nibble);
        // println!("extra bit: {}", (S::STRIDE_LEN - nibble_len));

        // We're expanding the search for more-specifics bit-by-bit.
        // `ms_nibble_len` is the number of bits including the original nibble we're considering,
        // e.g. if our prefix has a length of 25 and we've all strides sized 4,
        // We would end up with a last nibble_len of 1.
        // `ms_nibble_len` will expand then from 2 up and till 4.
        // ex.:
        // nibble: 1 , (nibble_len: 1)
        // Iteration:
        // ms_nibble_len=1,n_l=0: 10, n_l=1: 11
        // ms_nibble_len=2,n_l=0: 100, n_l=1: 101, n_l=2: 110, n_l=3: 111
        // ms_nibble_len=3,n_l=0: 1000, n_l=1: 1001, n_l=2: 1010, ..., n_l=7: 1111

        for ms_nibble_len in nibble_len + 1..S::STRIDE_LEN + 1 {
            // iterate over all the possible values for this `ms_nibble_len`,
            // e.g. two bits can have 4 different values.
            for n_l in 0..(1 << (ms_nibble_len - nibble_len)) {
                // move the nibble left with the amount of bits we're going to loop over.
                // e.g. a stride of size 4 with a nibble 0000 0000 0000 0011 becomes 0000 0000 0000 1100
                // then it will iterate over ...1100,...1101,...1110,...1111
                let ms_nibble = (nibble << (ms_nibble_len - nibble_len)) + n_l as u32;
                bit_pos = S::get_bit_pos(ms_nibble, ms_nibble_len);

                // println!("nibble:    {:032b}", ms_nibble);
                // println!("ptrbitarr: {:032b}", self.ptrbitarr);
                // println!("bitpos:    {:032b}", bit_pos);

                if (S::into_stride_size(self.ptrbitarr) & bit_pos) > S::zero() {
                    found_children_with_more_specifics
                        .push(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, ms_nibble)]);
                }

                if self.pfxbitarr & bit_pos > S::zero() {
                    found_more_specifics_vec.push(
                        self.pfx_vec[S::get_pfx_index(self.pfxbitarr, ms_nibble, ms_nibble_len)],
                    );
                }
            }
        }

        (
            // We're done here, the caller should now go over all nodes in found_children_with_more_specifics vec and add
            // ALL prefixes found in there.
            found_children_with_more_specifics,
            found_more_specifics_vec,
        )
    }
}