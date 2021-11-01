use crate::common::PrefixInfoUnit;
use crate::node_id::{SortableNodeId, InMemNodeId};
pub use crate::stride::*;

use crate::local_vec::tree::*;

use routecore::record::MergeUpdate;
use routecore::addr::AddressFamily;

use std::io::{Error, ErrorKind};
use std::fmt::Debug;

pub trait StorageBackend
where
    Self::NodeType: SortableNodeId + Copy,
{
    type NodeType;
    type AF: AddressFamily;
    type Meta: routecore::record::Meta + MergeUpdate;

    fn init(start_node: Option<SizedStrideNode<Self::AF, Self::NodeType>>) -> Self;
    fn acquire_new_node_id(
        &self,
        sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        part: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
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
    ) -> Option<&SizedStrideNode<Self::AF, Self::NodeType>>;
    fn retrieve_node_mut(
        &mut self,
        index: Self::NodeType,
    ) -> SizedNodeResult<Self::AF, Self::NodeType>;
    fn retrieve_node_with_guard(
        &self,
        index: Self::NodeType,
    ) -> CacheGuard<Self::AF, Self::NodeType>;
    fn get_root_node_id(&self) -> Self::NodeType;
    fn get_root_node_mut(&mut self) -> Option<&mut SizedStrideNode<Self::AF, Self::NodeType>>;
    fn get_nodes(&self) -> &Vec<SizedStrideNode<Self::AF, Self::NodeType>>;
    fn get_nodes_len(&self) -> usize;
    fn acquire_new_prefix_id(
        &self,
        sort: &<<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        part: &PrefixInfoUnit<Self::AF, Self::Meta>,
    ) -> <Self as StorageBackend>::NodeType;
    fn store_prefix(
        &mut self,
        next_node: PrefixInfoUnit<Self::AF, Self::Meta>,
    ) -> Result<
        <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
        Box<dyn std::error::Error>,
    >;
    fn retrieve_prefix(
        &self,
        index: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&PrefixInfoUnit<Self::AF, Self::Meta>>;
    fn retrieve_prefix_mut(
        &mut self,
        index: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&mut PrefixInfoUnit<Self::AF, Self::Meta>>;
    fn retrieve_prefix_with_guard(
        &self,
        index: Self::NodeType,
    ) -> PrefixCacheGuard<Self::AF, Self::Meta>;
    fn get_prefixes(&self) -> &Vec<PrefixInfoUnit<Self::AF, Self::Meta>>;
    fn get_prefixes_len(&self) -> usize;
    fn prefixes_iter(
        &self,
    ) -> PrefixIter<'_, Self::AF, Self::Meta>;
    fn prefixes_iter_mut(
        &mut self,
    ) -> PrefixIterMut<'_, Self::AF, Self::Meta>;
}

#[derive(Debug)]
pub struct InMemStorage<AF: AddressFamily, Meta: routecore::record::Meta> {
    pub nodes: Vec<SizedStrideNode<AF, InMemNodeId>>,
    pub prefixes: Vec<PrefixInfoUnit<AF, Meta>>,
}

impl<AF: AddressFamily, Meta: routecore::record::Meta + MergeUpdate> StorageBackend for InMemStorage<AF, Meta> {
    type NodeType = InMemNodeId;
    type AF = AF;
    type Meta = Meta;

    fn init(
        start_node: Option<SizedStrideNode<Self::AF, Self::NodeType>>,
    ) -> InMemStorage<AF, Meta> {
        let mut nodes = vec![];
        if let Some(n) = start_node {
            nodes = vec![n];
        }
        InMemStorage {
            nodes,
            prefixes: vec![]
        }
    }

    fn acquire_new_node_id(
        &self,
        sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        _part: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> <Self as StorageBackend>::NodeType {
        // We're ignoring the part parameter here, because we want to store
        // the index into the global self.nodes vec in the local vec.
        InMemNodeId(sort, self.nodes.len() as u32)
    }

    fn store_node(
        &mut self,
        _id: Option<Self::NodeType>,
        next_node: SizedStrideNode<Self::AF, Self::NodeType>,
    ) -> Option<Self::NodeType> {
        let id = self.nodes.len() as u32;
        self.nodes.push(next_node);
        //Store::NodeType::new(&bit_id, &i.into())
        //Store::NodeType::new(&((1 << $nibble_len) + $nibble as u16).into(), &i)
        Some(InMemNodeId::new(&0, &id))
    }

    fn update_node(
        &mut self,
        current_node_id: Self::NodeType,
        updated_node: SizedStrideNode<Self::AF, Self::NodeType>,
    ) {
        let _default_val = std::mem::replace(
            self.retrieve_node_mut(current_node_id).unwrap(),
            updated_node,
        );
    }

    fn retrieve_node(
        &self,
        id: Self::NodeType,
    ) -> Option<&SizedStrideNode<Self::AF, Self::NodeType>> {
        self.nodes.get(id.get_part() as usize)
    }

    fn retrieve_node_mut(
        &mut self,
        index: Self::NodeType,
    ) -> SizedNodeResult<Self::AF, Self::NodeType> {
        self.nodes
            .get_mut(index.get_part() as usize)
            .ok_or_else(|| Box::new(Error::new(ErrorKind::Other, "Retrieve Node Error")).into())
    }

    // Don't use this function, this is just a placeholder and a really
    // inefficient implementation.
    fn retrieve_node_with_guard(
        &self,
        _id: Self::NodeType,
    ) -> CacheGuard<Self::AF, Self::NodeType> {
        panic!("Not Implemented for InMeMStorage");
    }

    fn get_root_node_id(&self) -> Self::NodeType {
        InMemNodeId(0, 0)
    }

    fn get_root_node_mut(&mut self) -> Option<&mut SizedStrideNode<Self::AF, Self::NodeType>> {
        Some(&mut self.nodes[0])
    }

    fn get_nodes(&self) -> &Vec<SizedStrideNode<Self::AF, Self::NodeType>> {
        &self.nodes
    }

    fn get_nodes_len(&self) -> usize {
        self.nodes.len()
    }

    fn acquire_new_prefix_id(
        &self,
        sort: &<<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        _part: &PrefixInfoUnit<<Self as StorageBackend>::AF, <Self as StorageBackend>::Meta>,
    ) -> <Self as StorageBackend>::NodeType {
        // We're ignoring the part parameter here, because we want to store
        // the index into the global self.prefixes vec in the local vec.
        InMemNodeId(*sort, self.prefixes.len() as u32)
    }

    fn store_prefix(
        &mut self,
        next_node: PrefixInfoUnit<Self::AF, Self::Meta>,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let id = self.prefixes.len() as u32;
        self.prefixes.push(next_node);
        Ok(id)
    }

    fn retrieve_prefix(&self, index: u32) -> Option<&PrefixInfoUnit<Self::AF, Self::Meta>> {
        self.prefixes.get(index as usize)
    }

    fn retrieve_prefix_mut(&mut self, index: u32) -> Option<&mut PrefixInfoUnit<Self::AF, Self::Meta>> {
        self.prefixes.get_mut(index as usize)
    }

    fn retrieve_prefix_with_guard(
        &self,
        _index: Self::NodeType,
    ) -> PrefixCacheGuard<Self::AF, Self::Meta> {
        panic!("nOt ImPlEmEnTed for InMemNode");
    }

    fn get_prefixes(&self) -> &Vec<PrefixInfoUnit<Self::AF, Self::Meta>> {
        &self.prefixes
    }

    fn get_prefixes_len(&self) -> usize {
        self.prefixes.len()
    }

    fn prefixes_iter(
        &self,
    ) -> Result<std::slice::Iter<'_, PrefixInfoUnit<AF, Meta>>, Box<dyn std::error::Error>> {
        Ok(self.prefixes.iter())
    }

    fn prefixes_iter_mut(
        &mut self,
    ) -> Result<std::slice::IterMut<'_, PrefixInfoUnit<AF, Meta>>, Box<dyn std::error::Error>> {
        Ok(self.prefixes.iter_mut())
    }
}