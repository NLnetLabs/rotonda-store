use crate::{local_array::tree::*, local_vec::node::Stride};
use crate::node_id::SortableNodeId;

use crate::prefix_record::InternalPrefixRecord;
use std::fmt::Debug;

use crate::af::AddressFamily;
use routecore::record::{MergeUpdate, Meta};

pub(crate) type PrefixIter<'a, AF, Meta> = Result<
    std::slice::Iter<'a, InternalPrefixRecord<AF, Meta>>,
    Box<dyn std::error::Error>,
>;

#[cfg(feature = "dynamodb")]
pub(crate) type PrefixIterMut<'a, AF, Meta> = Result<
    std::slice::IterMut<'a, InternalPrefixRecord<AF, Meta>>,
    Box<dyn std::error::Error>,
>;

pub(crate) type SizedNodeResult<'a, AF> =
    Result<SizedStrideRefMut<'a, AF>, Box<dyn std::error::Error>>;
pub(crate) type SizedNodeRefResult<'a, AF> =
    Result<SizedStrideRefMut<'a, AF>, Box<dyn std::error::Error>>;
pub(crate) type SizedNodeOption<'a, AF> =
    Option<SizedStrideNode<AF>>;
pub(crate) type SizedNodeRefOption<'a, AF> =
    Option<SizedStrideRef<'a, AF>>;

pub(crate) trait StorageBackend
{
    type AF: AddressFamily;
    type Meta: Meta + MergeUpdate;

    fn init(
        start_node: Option<SizedStrideNode<Self::AF>>,
    ) -> Self;
    fn acquire_new_node_id(
        &self,
        // sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        //
        level: u8,
    ) -> StrideNodeId;
    // store_node should return an index with the associated type `Part` of the associated type
    // of this trait.
    // `id` is optional, since a vec uses the indexes as the ids of the nodes,
    // other storage data-structures may use unordered lists, where the id is in the
    // record, e.g., dynamodb
    fn store_node(
        &mut self,
        id: StrideNodeId,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId>;
    fn update_node(
        &mut self,
        current_node_id: StrideNodeId,
        updated_node: SizedStrideNode<Self::AF>,
    );
    fn retrieve_node<'a>(
        &'a self,
        index: StrideNodeId,
    ) -> SizedNodeRefOption<'a, Self::AF>;
    fn retrieve_node_mut(
        &mut self,
        index: StrideNodeId,
    ) -> SizedNodeResult<Self::AF>;
    fn retrieve_node_with_guard(
        &self,
        index: StrideNodeId,
    ) -> CacheGuard<Self::AF>;
    fn get_nodes(&self) -> Vec<SizedStrideRef<Self::AF>>;
    fn get_root_node_id(&self, stride_size: u8) -> StrideNodeId;
    // fn get_root_node_mut(
    //     &mut self,
    //     stride_size: u8,
    // ) -> Option<SizedStrideNode<Self::AF, Self::NodeType>>;
    fn get_nodes_len(&self) -> usize;
    // The Node and Prefix ID consist of the same type, that
    // have a `sort` field, that descibes the index of the local array
    // (stored inside each node) and the `part` fiels, that describes
    // the index of the prefix in the global store.
    fn acquire_new_prefix_id(
        &self,
        // sort: &<<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
    ) -> StrideNodeId;
    fn store_prefix(
        &mut self,
        next_node: InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> Result<
        StrideNodeId,
        Box<dyn std::error::Error>,
    >;
    fn retrieve_prefix(
        &self,
        index: StrideNodeId,
    ) -> Option<&InternalPrefixRecord<Self::AF, Self::Meta>>;
    fn retrieve_prefix_mut(
        &mut self,
        index: StrideNodeId,
    ) -> Option<&mut InternalPrefixRecord<Self::AF, Self::Meta>>;
    fn retrieve_prefix_with_guard(
        &self,
        index: StrideNodeId,
    ) -> PrefixCacheGuard<Self::AF, Self::Meta>;
    fn get_prefixes(
        &self,
    ) -> &Vec<InternalPrefixRecord<Self::AF, Self::Meta>>;
    fn get_prefixes_len(&self) -> usize;
    fn prefixes_iter(&self) -> PrefixIter<'_, Self::AF, Self::Meta>;
    #[cfg(feature = "dynamodb")]
    fn prefixes_iter_mut(
        &mut self,
    ) -> PrefixIterMut<'_, Self::AF, Self::Meta>;
}

#[derive(Debug)]
pub(crate) struct InMemStorage<
    AF: AddressFamily,
    Meta: routecore::record::Meta,
> {
    // each stride in its own vec avoids having to store SizedStrideNode, an enum, that will have
    // the size of the largest variant as its memory footprint (Stride8).
    pub nodes3: Vec<TreeBitMapNode<AF, Stride3, 14, 8>>,
    pub nodes4: Vec<TreeBitMapNode<AF, Stride4, 30, 16>>,
    pub nodes5: Vec<TreeBitMapNode<AF, Stride5, 62, 32>>,
    // pub nodes6: Vec<TreeBitMapNode<AF, Stride6, 126, 64>>,
    // pub nodes7: Vec<TreeBitMapNode<AF, Stride7, 254, 128>>,
    // pub nodes8: Vec<TreeBitMapNode<AF, Stride8, 510, 256>>,
    pub prefixes: Vec<InternalPrefixRecord<AF, Meta>>,
}

impl<AF: AddressFamily, Meta: routecore::record::Meta + MergeUpdate>
    StorageBackend for InMemStorage<AF, Meta>
{
    type AF = AF;
    type Meta = Meta;

    fn init(
        start_node: Option<SizedStrideNode<Self::AF>>,
    ) -> InMemStorage<AF, Meta> {
        let mut nodes3 = vec![];
        let mut nodes4 = vec![];
        let mut nodes5 = vec![];
        // let mut nodes6 = vec![];
        // let mut nodes7 = vec![];
        // let mut nodes8 = vec![];
        if let Some(n) = start_node {
            match n {
                SizedStrideNode::Stride3(nodes) => {
                    nodes3 = vec![nodes];
                }
                SizedStrideNode::Stride4(nodes) => {
                    nodes4 = vec![nodes];
                }
                SizedStrideNode::Stride5(nodes) => {
                    nodes5 = vec![nodes];
                } // SizedStrideNode::Stride6(nodes) => {
                  //     nodes6 = vec![nodes];
                  // }
                  // SizedStrideNode::Stride7(nodes) => {
                  //     nodes7 = vec![nodes];
                  // }
                  // SizedStrideNode::Stride8(nodes) => {
                  //     nodes8 = vec![nodes];
                  // }
            }
        }

        InMemStorage {
            nodes3,
            nodes4,
            nodes5,
            // nodes6,
            // nodes7,
            // nodes8,
            prefixes: vec![],
        }
    }

    fn acquire_new_node_id(
        &self,
        // sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        level: u8,
    ) -> StrideNodeId {
        // We're ignoring the part parameter here, because we want to store
        // the index into the global self.nodes vec in the local vec.
        match level {
            3 => StrideNodeId::new(
                StrideType::Stride3, self.nodes3.len() as u32,
            ),
            4 => StrideNodeId::new(
                StrideType::Stride4, self.nodes4.len() as u32),
            
            5 => StrideNodeId::new(
                StrideType::Stride5, self.nodes5.len() as u32),
            
            // 6 => InMemStrideNodeId::new(
            //     &sort,
            //     &StrideNodeId(StrideType::Stride6, self.nodes6.len() as u32),
            // ),
            // 7 => InMemStrideNodeId::new(
            //     &sort,
            //     &StrideNodeId(StrideType::Stride7, self.nodes7.len() as u32),
            // ),
            // 8 => InMemStrideNodeId::new(
            //     &sort,
            //     &StrideNodeId(StrideType::Stride8, self.nodes8.len() as u32),
            // ),
            _ => panic!("Invalid level"),
        }
    }

    fn store_node(
        &mut self,
        _id: StrideNodeId,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId> {
        match next_node {
            SizedStrideNode::Stride3(node) => {
                let id = self.nodes3.len() as u32;
                self.nodes3.push(node);
                Some(StrideNodeId::new(
                    StrideType::Stride3, id),
                )
            }
            SizedStrideNode::Stride4(node) => {
                let id = self.nodes4.len() as u32;
                self.nodes4.push(node);
                Some(StrideNodeId::new(StrideType::Stride4, id))
            }
            SizedStrideNode::Stride5(node) => {
                let id = self.nodes5.len() as u32;
                self.nodes5.push(node);
                Some(StrideNodeId::new(StrideType::Stride5, id))
            } 
            // SizedStrideNode::Stride6(node) => {
            //     let id = self.nodes6.len() as u32;
            //     self.nodes6.push(node);
            //     Some(InMemStrideNodeId::new(
            //         &0,
            //         &StrideNodeId(StrideType::Stride6, id),
            //     ))
            // }
            // SizedStrideNode::Stride7(node) => {
            //     let id = self.nodes7.len() as u32;
            //     self.nodes7.push(node);
            //     Some(InMemStrideNodeId::new(
            //         &0,
            //         &StrideNodeId(StrideType::Stride7, id),
            //     ))
            // }
            // SizedStrideNode::Stride8(node) => {
            //     let id = self.nodes8.len() as u32;
            //     self.nodes8.push(node);
            //     Some(InMemStrideNodeId::new(
            //         &0,
            //         &StrideNodeId(StrideType::Stride8, id),
            //     ))
            // }
        }
    }

    fn update_node(
        &mut self,
        current_node_id: StrideNodeId,
        updated_node: SizedStrideNode<Self::AF>,
    ) {
        match updated_node {
            SizedStrideNode::Stride3(node) => {
                let _default_val = std::mem::replace(
                    self.nodes3
                        .get_mut::<usize>(current_node_id.into())
                        .unwrap(),
                    node,
                );
            }
            SizedStrideNode::Stride4(node) => {
                let _default_val = std::mem::replace(
                    self.nodes4
                        .get_mut::<usize>(current_node_id.into())
                        .unwrap(),
                    node,
                );
            }
            SizedStrideNode::Stride5(node) => {
                let _default_val = std::mem::replace(
                    self.nodes5
                        .get_mut::<usize>(current_node_id.into())
                        .unwrap(),
                    node,
                );
            } 
            // SizedStrideNode::Stride6(node) => {
            //     let _default_val = std::mem::replace(
            //         self.nodes6
            //             .get_mut::<usize>(current_node_id.get_part().into())
            //             .unwrap(),
            //         node,
            //     );
            // }
            // SizedStrideNode::Stride7(node) => {
            //     let _default_val = std::mem::replace(
            //         self.nodes7
            //             .get_mut::<usize>(current_node_id.get_part().into())
            //             .unwrap(),
            //         node,
            //     );
            // }
            // SizedStrideNode::Stride8(node) => {
            //     let _default_val = std::mem::replace(
            //         self.nodes8
            //             .get_mut::<usize>(current_node_id.get_part().into())
            //             .unwrap(),
            //         node,
            //     );
            // }
        }
    }

    fn retrieve_node(
        &self,
        id: StrideNodeId,
    ) -> SizedNodeRefOption<'_, Self::AF> {
        match id {
            StrideNodeId(StrideType::Stride3, Some(part_id)) => self
                .nodes3
                .get(part_id as usize)
                .map(|n| SizedStrideRef::Stride3(n)),
            StrideNodeId(StrideType::Stride4, Some(part_id)) => self
                .nodes4
                .get(part_id as usize)
                .map(|n| SizedStrideRef::Stride4(n)),
            StrideNodeId(StrideType::Stride5, Some(part_id)) => self
                .nodes5
                .get(part_id as usize)
                .map(|n| SizedStrideRef::Stride5(n)),
            // StrideNodeId(StrideType::Stride6, part_id) => self
            //     .nodes6
            //     .get(part_id as usize)
            //     .map(|n| SizedStrideNode::Stride6(*n)),
            // StrideNodeId(StrideType::Stride7, part_id) => self
            //     .nodes7
            //     .get(part_id as usize)
            //     .map(|n| SizedStrideNode::Stride7(*n)),
            // StrideNodeId(StrideType::Stride8, part_id) => self
            //     .nodes8
            //     .get(part_id as usize)
            //     .map(|n| SizedStrideNode::Stride8(*n)),
            StrideNodeId(_, None) => None,
        }
    }

    fn retrieve_node_mut<'a>(
        &'a mut self,
        id: StrideNodeId,
    ) -> SizedNodeResult<'a, Self::AF> {
        match id {
            StrideNodeId(StrideType::Stride3, Some(part_id)) => {
                Ok(SizedStrideRefMut::Stride3(
                    self.nodes3.get_mut(part_id as usize).unwrap_or_else(
                        || panic!("no {:?} in stride 3 collection", id),
                    ),
                ))
            }
            StrideNodeId(StrideType::Stride4, Some(part_id)) => {
                Ok(SizedStrideRefMut::Stride4(
                    self.nodes4.get_mut(part_id as usize).unwrap(),
                ))
            }
            StrideNodeId(StrideType::Stride5, Some(part_id)) => {
                Ok(SizedStrideRefMut::Stride5(
                    self.nodes5.get_mut(part_id as usize).unwrap(),
                ))
            }
            // StrideNodeId(StrideType::Stride6, part_id) => Ok(SizedStrideNode::Stride6(
            //     *self.nodes6.get_mut(part_id as usize).unwrap(),
            // )),
            // StrideNodeId(StrideType::Stride7, part_id) => Ok(SizedStrideNode::Stride7(
            //     *self.nodes7.get_mut(part_id as usize).unwrap(),
            // )),
            // StrideNodeId(StrideType::Stride8, part_id) => Ok(SizedStrideNode::Stride8(
            //     *self.nodes8.get_mut(part_id as usize).unwrap(),
            // )),
            StrideNodeId(_, None) => panic!("no {:?} in stride collection", id),
        }
    }

    // Don't use this function, this is just a placeholder and a really
    // inefficient implementation.
    fn retrieve_node_with_guard(
        &self,
        _id: StrideNodeId,
    ) -> CacheGuard<Self::AF> {
        panic!("Not Implemented for InMeMStorage");
    }

    fn get_nodes(&self) -> Vec<SizedStrideRef<Self::AF>> {
        self.nodes3
            .iter()
            .map(|n| SizedStrideRef::Stride3(n))
            .chain(self.nodes4.iter().map(|n| SizedStrideRef::Stride4(n)))
            .chain(self.nodes5.iter().map(|n| SizedStrideRef::Stride5(n)))
            // .chain(self.nodes6.iter().map(|n| SizedStrideRef::Stride6(n)))
            // .chain(self.nodes7.iter().map(|n| SizedStrideRef::Stride7(n)))
            // .chain(self.nodes8.iter().map(|n| SizedStrideRef::Stride8(n)))
            .collect()
    }

    fn get_root_node_id(&self, first_stride_size: u8) -> StrideNodeId {
        let first_stride_type = match first_stride_size {
            3 => StrideType::Stride3,
            4 => StrideType::Stride4,
            5 => StrideType::Stride5,
            // 6 => StrideType::Stride6,
            // 7 => StrideType::Stride7,
            // 8 => StrideType::Stride8,
            _ => panic!("Invalid stride size"),
        };
        StrideNodeId::new(first_stride_type, 0)
    }

    // fn get_root_node_mut(
    //     &mut self,
    //     stride_size: u8,
    // ) -> Option<SizedStrideNode<Self::AF, Self::NodeType>> {
    //     match stride_size {
    //         3 => Some(SizedStrideNode::Stride3(self.nodes3[0])),
    //         4 => Some(SizedStrideNode::Stride4(self.nodes4[0])),
    //         5 => Some(SizedStrideNode::Stride5(self.nodes5[0])),
    //         // 6 => Some(SizedStrideNode::Stride6(self.nodes6[0])),
    //         // 7 => Some(SizedStrideNode::Stride7(self.nodes7[0])),
    //         // 8 => Some(SizedStrideNode::Stride8(self.nodes8[0])),
    //         _ => panic!("invalid stride size"),
    //     }
    // }

    fn get_nodes_len(&self) -> usize {
        self.nodes3.len() + self.nodes4.len() + self.nodes5.len()
        // + self.nodes6.len()
        // + self.nodes7.len()
        // + self.nodes8.len()
    }

    fn acquire_new_prefix_id(
        &self,
        // sort: &<<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
    ) -> StrideNodeId {
        // The return value the StrideType doesn't matter here,
        // because we store all prefixes in one huge vec (unlike the nodes,
        // which are stored in separate vec for each stride size).
        // We'll return the index to the end of the vec.
        StrideNodeId::new(StrideType::Stride5, self.prefixes.len() as u32)
    }

    fn store_prefix(
        &mut self,
        next_node: InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> Result<StrideNodeId, Box<dyn std::error::Error>> {
        let part_id = self.prefixes.len() as u32;
        self.prefixes.push(next_node);
        Ok(StrideNodeId::new(StrideType::Stride5, part_id))
    }

    fn retrieve_prefix(
        &self,
        part_id: StrideNodeId,
    ) -> Option<&InternalPrefixRecord<Self::AF, Self::Meta>> {
        self.prefixes.get(part_id.get_id() as usize)
    }

    fn retrieve_prefix_mut(
        &mut self,
        part_id: StrideNodeId,
    ) -> Option<&mut InternalPrefixRecord<Self::AF, Self::Meta>> {
        self.prefixes.get_mut(part_id.get_id() as usize)
    }

    fn retrieve_prefix_with_guard(
        &self,
        _index: StrideNodeId,
    ) -> PrefixCacheGuard<Self::AF, Self::Meta> {
        panic!("nOt ImPlEmEnTed for InMemNode");
    }

    fn get_prefixes(
        &self,
    ) -> &Vec<InternalPrefixRecord<Self::AF, Self::Meta>> {
        &self.prefixes
    }

    fn get_prefixes_len(&self) -> usize {
        self.prefixes.len()
    }

    fn prefixes_iter(&self) -> PrefixIter<Self::AF, Self::Meta> {
        Ok(self.prefixes.iter())
    }

    #[cfg(feature = "dynamodb")]
    fn prefixes_iter_mut(
        &mut self,
    ) -> Result<
        std::slice::IterMut<'_, InternalPrefixRecord<AF, Meta>>,
        Box<dyn std::error::Error>,
    > {
        Ok(self.prefixes.iter_mut())
    }
}

pub(crate) struct CacheGuard<
    'a,
    AF: 'static + AddressFamily
> {
    pub guard: std::cell::Ref<'a, SizedStrideNode<AF>>,
}

impl<'a, AF: 'static + AddressFamily>
    std::ops::Deref for CacheGuard<'a, AF>
{
    type Target = SizedStrideNode<AF>;

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
