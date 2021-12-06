use std::collections::HashMap;

use crate::local_array::tree::*;

use crate::prefix_record::InternalPrefixRecord;
use std::fmt::Debug;

use crate::af::AddressFamily;
use routecore::record::{MergeUpdate, Meta};

pub(crate) type PrefixIterResult<'a, AF, Meta> = Result<
    std::collections::hash_map::Values<
        'a,
        PrefixId<AF>,
        InternalPrefixRecord<AF, Meta>,
    >,
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
pub(crate) type SizedNodeRefOption<'a, AF> = Option<SizedStrideRef<'a, AF>>;

pub(crate) type PrefixHashMap<AF, Meta> =
    HashMap<PrefixId<AF>, InternalPrefixRecord<AF, Meta>>;

pub(crate) trait StorageBackend {
    type AF: AddressFamily;
    type Meta: Meta + MergeUpdate;

    fn init(
        len_to_stride_size: [StrideType; 128],
        root_node: SizedStrideNode<Self::AF>,
    ) -> Self;
    fn acquire_new_node_id(
        &self,
        // sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        //
        sub_prefix: (Self::AF, u8),
    ) -> StrideNodeId<Self::AF>;
    // store_node should return an index with the associated type `Part` of the associated type
    // of this trait.
    // `id` is optional, since a vec uses the indexes as the ids of the nodes,
    // other storage data-structures may use unordered lists, where the id is in the
    // record, e.g., dynamodb
    fn store_node(
        &mut self,
        id: StrideNodeId<Self::AF>,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId<Self::AF>>;
    fn update_node(
        &mut self,
        current_node_id: StrideNodeId<Self::AF>,
        updated_node: SizedStrideNode<Self::AF>,
    );
    fn retrieve_node(
        &'_ self,
        index: StrideNodeId<Self::AF>,
    ) -> SizedNodeRefOption<'_, Self::AF>;
    fn retrieve_node_mut(
        &mut self,
        index: StrideNodeId<Self::AF>,
    ) -> SizedNodeResult<Self::AF>;
    fn retrieve_node_with_guard(
        &self,
        index: StrideNodeId<Self::AF>,
    ) -> CacheGuard<Self::AF>;
    fn get_nodes(&self) -> Vec<SizedStrideRef<Self::AF>>;
    fn get_root_node_id(&self, stride_size: u8) -> StrideNodeId<Self::AF>;
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
        prefix: &InternalPrefixRecord<Self::AF, Self::Meta>,
        // sort: &<<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
    ) -> PrefixId<Self::AF>;
    fn store_prefix(
        &mut self,
        id: PrefixId<Self::AF>,
        next_node: InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> Result<PrefixId<Self::AF>, Box<dyn std::error::Error>>;
    fn retrieve_prefix(
        &self,
        index: PrefixId<Self::AF>,
    ) -> Option<&InternalPrefixRecord<Self::AF, Self::Meta>>;
    fn retrieve_prefix_mut(
        &mut self,
        index: PrefixId<Self::AF>,
    ) -> Option<&mut InternalPrefixRecord<Self::AF, Self::Meta>>;
    fn retrieve_prefix_with_guard(
        &self,
        index: StrideNodeId<Self::AF>,
    ) -> PrefixCacheGuard<Self::AF, Self::Meta>;
    fn get_prefixes(&self) -> &PrefixHashMap<Self::AF, Self::Meta>;
    fn get_prefixes_len(&self) -> usize;
    fn prefixes_iter(&self) -> PrefixIterResult<'_, Self::AF, Self::Meta>;
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
    pub nodes3: HashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride3, 14, 8>>,
    pub nodes4:
        HashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride4, 30, 16>>,
    pub nodes5:
        HashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride5, 62, 32>>,
    // pub nodes6: Vec<TreeBitMapNode<AF, Stride6, 126, 64>>,
    // pub nodes7: Vec<TreeBitMapNode<AF, Stride7, 254, 128>>,
    // pub nodes8: Vec<TreeBitMapNode<AF, Stride8, 510, 256>>,
    pub prefixes: HashMap<PrefixId<AF>, InternalPrefixRecord<AF, Meta>>,
    len_to_stride_size: [StrideType; 128],
}

impl<AF: AddressFamily, Meta: routecore::record::Meta + MergeUpdate>
    StorageBackend for InMemStorage<AF, Meta>
{
    type AF = AF;
    type Meta = Meta;

    fn init(
        len_to_stride_size: [StrideType; 128],
        root_node: SizedStrideNode<Self::AF>,
    ) -> InMemStorage<Self::AF, Self::Meta> {
        let nodes3 = <HashMap<
            StrideNodeId<AF>,
            TreeBitMapNode<AF, Stride3, 14, 8>,
        >>::new();
        let nodes4 = <HashMap<
            StrideNodeId<AF>,
            TreeBitMapNode<AF, Stride4, 30, 16>,
        >>::new();
        let nodes5 = <HashMap<
            StrideNodeId<AF>,
            TreeBitMapNode<AF, Stride5, 62, 32>,
        >>::new();

        let mut store = InMemStorage {
            nodes3,
            nodes4,
            nodes5,
            // nodes6,
            // nodes7,
            // nodes8,
            prefixes: HashMap::new(),
            len_to_stride_size,
        };
        let first_stride_type = match len_to_stride_size[0] {
            StrideType::Stride3 => 3,
            StrideType::Stride4 => 4,
            StrideType::Stride5 => 5,
        };
        store
            .store_node(store.get_root_node_id(first_stride_type), root_node);
        store
    }

    fn acquire_new_node_id(
        &self,
        // sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        (prefix_net, sub_prefix_len): (Self::AF, u8),
    ) -> StrideNodeId<AF> {
        StrideNodeId::new_with_cleaned_id(prefix_net, sub_prefix_len)
    }

    fn store_node(
        &mut self,
        id: StrideNodeId<AF>,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId<AF>> {
        match next_node {
            SizedStrideNode::Stride3(node) => {
                // let id = self.nodes3.len() as u32;
                self.nodes3.insert(id, node);
                Some(id)
            }
            SizedStrideNode::Stride4(node) => {
                // let id = self.nodes4.len() as u32;
                self.nodes4.insert(id, node);
                Some(id)
            }
            SizedStrideNode::Stride5(node) => {
                // let id = self.nodes5.len() as u32;
                self.nodes5.insert(id, node);
                Some(id)
            }
        }
    }

    fn update_node(
        &mut self,
        current_node_id: StrideNodeId<AF>,
        updated_node: SizedStrideNode<AF>,
    ) {
        match updated_node {
            SizedStrideNode::Stride3(node) => {
                let _default_val = self.nodes3.insert(current_node_id, node);
            }
            SizedStrideNode::Stride4(node) => {
                let _default_val = self.nodes4.insert(current_node_id, node);
            }
            SizedStrideNode::Stride5(node) => {
                let _default_val = self.nodes5.insert(current_node_id, node);
            }
        }
    }

    fn retrieve_node(
        &self,
        id: StrideNodeId<AF>,
    ) -> SizedNodeRefOption<'_, Self::AF> {
        match self.len_to_stride_size[id.get_id().1 as usize] {
            StrideType::Stride3 => {
                self.nodes3.get(&id).map(|n| SizedStrideRef::Stride3(n))
            }
            StrideType::Stride4 => {
                self.nodes4.get(&id).map(|n| SizedStrideRef::Stride4(n))
            }
            StrideType::Stride5 => {
                self.nodes5.get(&id).map(|n| SizedStrideRef::Stride5(n))
            }
        }
    }

    fn retrieve_node_mut(
        &'_ mut self,
        id: StrideNodeId<AF>,
    ) -> SizedNodeResult<'_, Self::AF> {
        match self.len_to_stride_size[id.get_id().1 as usize] {
            StrideType::Stride3 => Ok(self
                .nodes3
                .get_mut(&id)
                .map(|n| SizedStrideRefMut::Stride3(n))
                .unwrap_or_else(|| panic!("Node not found"))),
            StrideType::Stride4 => Ok(self
                .nodes4
                .get_mut(&id)
                .map(|n| SizedStrideRefMut::Stride4(n))
                .unwrap_or_else(|| panic!("Node not found"))),
            StrideType::Stride5 => Ok(self
                .nodes5
                .get_mut(&id)
                .map(|n| SizedStrideRefMut::Stride5(n))
                .unwrap_or_else(|| panic!("Node not found"))),
        }
    }

    // Don't use this function, this is just a placeholder and a really
    // inefficient implementation.
    fn retrieve_node_with_guard(
        &self,
        _id: StrideNodeId<AF>,
    ) -> CacheGuard<Self::AF> {
        panic!("Not Implemented for InMeMStorage");
    }

    fn get_nodes(&self) -> Vec<SizedStrideRef<Self::AF>> {
        self.nodes3
            .iter()
            .map(|n| SizedStrideRef::Stride3(n.1))
            .chain(self.nodes4.iter().map(|n| SizedStrideRef::Stride4(n.1)))
            .chain(self.nodes5.iter().map(|n| SizedStrideRef::Stride5(n.1)))
            .collect()
    }

    fn get_root_node_id(&self, first_stride_size: u8) -> StrideNodeId<AF> {
        let (addr_bits, len) = match first_stride_size {
            3 => (AF::zero(), 0),
            4 => (AF::zero(), 0),
            5 => (AF::zero(), 0),
            _ => panic!("Invalid stride size"),
        };
        StrideNodeId::dangerously_new_with_id_as_is(addr_bits, len)
    }

    fn get_nodes_len(&self) -> usize {
        self.nodes3.len() + self.nodes4.len() + self.nodes5.len()
        // + self.nodes6.len()
        // + self.nodes7.len()
        // + self.nodes8.len()
    }

    fn acquire_new_prefix_id(
        &self,
        prefix: &InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> PrefixId<AF> {
        // The return value the StrideType doesn't matter here,
        // because we store all prefixes in one huge vec (unlike the nodes,
        // which are stored in separate vec for each stride size).
        // We'll return the index to the end of the vec.
        PrefixId::<AF>::new(prefix.net, prefix.len)
    }

    fn store_prefix(
        &mut self,
        id: PrefixId<Self::AF>,
        next_node: InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> Result<PrefixId<Self::AF>, Box<dyn std::error::Error>> {
        self.prefixes.insert(id, next_node);
        Ok(id)
    }

    fn retrieve_prefix(
        &self,
        part_id: PrefixId<Self::AF>,
    ) -> Option<&InternalPrefixRecord<Self::AF, Self::Meta>> {
        self.prefixes.get(&part_id)
    }

    fn retrieve_prefix_mut(
        &mut self,
        part_id: PrefixId<Self::AF>,
    ) -> Option<&mut InternalPrefixRecord<Self::AF, Self::Meta>> {
        self.prefixes.get_mut(&part_id)
    }

    fn retrieve_prefix_with_guard(
        &self,
        _index: StrideNodeId<AF>,
    ) -> PrefixCacheGuard<Self::AF, Self::Meta> {
        panic!("nOt ImPlEmEnTed for InMemNode");
    }

    fn get_prefixes(&self) -> &PrefixHashMap<Self::AF, Self::Meta> {
        &self.prefixes
    }

    fn get_prefixes_len(&self) -> usize {
        self.prefixes.len()
    }

    fn prefixes_iter(&self) -> PrefixIterResult<Self::AF, Self::Meta> {
        Ok(self.prefixes.values())
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

pub(crate) struct CacheGuard<'a, AF: 'static + AddressFamily> {
    pub guard: std::cell::Ref<'a, SizedStrideNode<AF>>,
}

impl<'a, AF: 'static + AddressFamily> std::ops::Deref for CacheGuard<'a, AF> {
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
