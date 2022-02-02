use crossbeam_epoch::{Atomic, Guard};
use dashmap::DashMap;

use crate::local_array::tree::*;

use crate::prefix_record::InternalPrefixRecord;
use std::fmt::Debug;

use crate::af::AddressFamily;
use routecore::record::{MergeUpdate, Meta};

use super::custom_alloc::FamilyBuckets;

pub(crate) type PrefixIterResult<'a, AF, Meta> = Result<
    std::collections::hash_map::Values<
        'a,
        PrefixId<AF>,
        InternalPrefixRecord<AF, Meta>,
    >,
    Box<dyn std::error::Error>,
>;

// pub(crate) type PrefixIterResult<'a, AF, Meta> = Result<
//     HashMap::iter::Iter<
//         'a,
//         crate::local_array::tree::PrefixId<AF>,
//         crate::prefix_record::InternalPrefixRecord<AF, Meta>,
//     >,
//     Box<dyn std::error::Error>,
// >;

#[cfg(feature = "dynamodb")]
pub(crate) type PrefixIterMut<'a, AF, Meta> = Result<
    std::slice::IterMut<'a, InternalPrefixRecord<AF, Meta>>,
    Box<dyn std::error::Error>,
>;

// pub(crate) type SizedNodeRefResult<'a, AF> =
//     Result<SizedStrideRefMut<'a, AF>, Box<dyn std::error::Error>>;

pub(crate) type SizedNodeRefOption<'a, AF> = Option<SizedStrideRef<'a, AF>>;

pub(crate) type PrefixHashMap<AF, Meta> =
    DashMap<PrefixId<AF>, InternalPrefixRecord<AF, Meta>>;

pub(crate) enum StrideReadStore<'a, AF: AddressFamily> {
    Stride3(&'a DashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride3>>),
    Stride4(&'a DashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride4>>),
    Stride5(&'a DashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride5>>),
}

pub(crate) enum StrideWriteStore<'a, AF: AddressFamily> {
    Stride3(&'a DashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride3>>),
    Stride4(&'a DashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride4>>),
    Stride5(&'a DashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride5>>),
}

pub(crate) trait StorageBackend {
    type AF: AddressFamily;
    type Meta: Meta + MergeUpdate;

    fn init(
        len_to_stride_size: [StrideType; 128],
        root_node: SizedStrideNode<Self::AF>,
    ) -> Self;

    // fn len_to_store_bits(&self, len: u8, level: u8) -> Option<u8>;
    fn acquire_new_node_id(
        &self,
        // sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        //
        sub_prefix: (Self::AF, u8),
    ) -> StrideNodeId<Self::AF>;
    // store_node should return an index with the associated type `Part` of
    // the associated type of this trait.
    // `id` is optional, since a vec uses the indexes as the ids of the
    // nodes, other storage data-structures may use unordered lists, where
    // the id is in the record, e.g., dynamodb
    fn store_node(
        &mut self,
        id: StrideNodeId<Self::AF>,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId<Self::AF>>;
    // This is for storing child nodes (which may be in a different node
    // than its parent, that's why you have to specify the store).
    fn store_node_in_store(
        store: &mut StrideWriteStore<Self::AF>,
        id: StrideNodeId<Self::AF>,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId<Self::AF>>;
    fn update_node(
        &mut self,
        current_node_id: StrideNodeId<Self::AF>,
        updated_node: SizedStrideRefMut<Self::AF>,
    );
    fn update_node_in_store(
        &self,
        store: &mut StrideWriteStore<Self::AF>,
        current_node_id: StrideNodeId<Self::AF>,
        updated_node: SizedStrideNode<Self::AF>,
    );
    fn retrieve_node(
        &self,
        index: StrideNodeId<Self::AF>,
    ) -> SizedNodeRefOption<'_, Self::AF>;
    // fn retrieve_node_mut(
    //     &self,
    //     index: StrideNodeId<Self::AF>,
    // ) -> SizedNodeRefResult<Self::AF>;
    fn retrieve_node_mut_with_guard<'a>(
        &'a self,
        id: StrideNodeId<Self::AF>,
        // result_ref: SizedNodeRefOption<'a, Self::AF>,
        guard: &'a Guard,
    ) -> Option<SizedStrideRefMut<'a, Self::AF>>;
    fn retrieve_node_with_guard<'a>(
        &'a self,
        id: StrideNodeId<Self::AF>,
        // result_ref: SizedNodeRefOption<'a, Self::AF>,
        guard: &'a Guard,
    ) -> Option<SizedStrideRef<'a, Self::AF>>;
    fn store_node_with_guard<'a>(
        &'a self,
        current_node: SizedNodeRefOption<'a, Self::AF>,
        next_node: SizedStrideNode<Self::AF>,
        guard: &'a Guard,
    ) -> Option<StrideNodeId<Self::AF>>;

    // fn nodes3_read(
    //     &self,
    // ) -> &DashMap<StrideNodeId<Self::AF>, TreeBitMapNode<Self::AF, Stride3>>;
    // fn nodes3_write(
    //     &self,
    // ) -> &DashMap<StrideNodeId<Self::AF>, TreeBitMapNode<Self::AF, Stride3>>;
    // fn nodes4_read(
    //     &self,
    // ) -> &DashMap<StrideNodeId<Self::AF>, TreeBitMapNode<Self::AF, Stride4>>;
    // fn nodes4_write(
    //     &self,
    // ) -> &DashMap<StrideNodeId<Self::AF>, TreeBitMapNode<Self::AF, Stride4>>;
    // fn nodes5_read(
    //     &self,
    // ) -> &DashMap<StrideNodeId<Self::AF>, TreeBitMapNode<Self::AF, Stride5>>;
    // fn nodes5_write(
    //     &self,
    // ) -> &DashMap<StrideNodeId<Self::AF>, TreeBitMapNode<Self::AF, Stride5>>;
    // fn get_nodes(&self) -> Vec<SizedStrideRef<Self::AF>>;
    fn get_root_node_id(&self, stride_size: u8) -> StrideNodeId<Self::AF>;
    fn load_default_route_prefix_serial(&self) -> usize;
    fn increment_default_route_prefix_serial(&self) -> usize;
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
        &self,
        id: PrefixId<Self::AF>,
        next_node: InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> Result<PrefixId<Self::AF>, Box<dyn std::error::Error>>;
    fn retrieve_prefix(
        &self,
        index: PrefixId<Self::AF>,
    ) -> Option<&'_ InternalPrefixRecord<Self::AF, Self::Meta>>;
    // fn retrieve_prefix_mut(
    //     &mut self,
    //     index: PrefixId<Self::AF>,
    // ) -> Option<&mut InternalPrefixRecord<Self::AF, Self::Meta>>;
    fn remove_prefix(
        &self,
        index: PrefixId<Self::AF>,
    ) -> Option<InternalPrefixRecord<Self::AF, Self::Meta>>;
    fn get_prefixes(&'_ self) -> &'_ PrefixHashMap<Self::AF, Self::Meta>;
    fn get_prefixes_clear(&self) -> &PrefixHashMap<Self::AF, Self::Meta>;
    fn get_prefixes_len(&self) -> usize;
    // fn prefixes_iter(&self) -> PrefixIterResult<'_, Self::AF, Self::Meta>;
    #[cfg(feature = "dynamodb")]
    fn prefixes_iter_mut(
        &mut self,
    ) -> PrefixIterMut<'_, Self::AF, Self::Meta>;
    fn get_stride_for_id(
        &self,
        id: StrideNodeId<Self::AF>,
    ) -> crate::local_array::tree::StrideType;
    fn get_stride_for_id_with_read_store(
        &self,
        id: StrideNodeId<Self::AF>,
    ) -> (StrideNodeId<Self::AF>, StrideReadStore<Self::AF>);
    fn get_stride_for_id_with_write_store(
        &self,
        id: StrideNodeId<Self::AF>,
    ) -> (StrideNodeId<Self::AF>, StrideWriteStore<Self::AF>);
    fn get_stride_sizes(&self) -> [u8; 42];
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
