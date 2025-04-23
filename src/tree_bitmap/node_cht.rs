use std::sync::RwLock;

use log::{debug, log_enabled};

use roaring::RoaringBitmap;

use super::tree_bitmap_node::{NodeId, TreeBitMapNode};
use crate::cht::{Cht, OnceBoxSlice, Value};
use crate::errors::{FatalError, FatalResult};
use crate::types::errors::PrefixStoreError;
use crate::types::AddressFamily;

pub(crate) type NodeCht<AF, const ROOT_SIZE: usize> =
    Cht<NodeSet<AF>, ROOT_SIZE, 4>;

#[derive(Debug)]
pub(crate) struct StoredNode<AF>
where
    Self: Sized,
    AF: AddressFamily,
{
    // the id of this node. since we're using linked lists to store nodes in
    // first-come-first-served order, we need to store the actual node id.
    pub(crate) node_id: NodeId<AF>,
    // The ptrbitarr and pfxbitarr for this node
    pub(crate) node: TreeBitMapNode<AF>,
    // Child nodes linked from this node
    pub(crate) node_set: NodeSet<AF>,
}

#[derive(Debug)]
pub(crate) struct NodeSet<AF: AddressFamily>(
    OnceBoxSlice<StoredNode<AF>>,
    // A Bitmap index that keeps track of the `multi_uniq_id`s (mui) that are
    // present in value collections in the meta-data tree in the child nodes
    RwLock<RoaringBitmap>,
);

impl<AF: AddressFamily> NodeSet<AF> {
    pub(crate) fn rbm(&self) -> &RwLock<RoaringBitmap> {
        &self.1
    }

    pub(crate) fn update_rbm_index(
        &self,
        multi_uniq_id: u32,
    ) -> FatalResult<(u32, bool)>
    where
        AF: crate::types::AddressFamily,
    {
        let try_count = 0;
        let mut rbm = self.1.write().map_err(|_| FatalError)?;
        let absent = rbm.insert(multi_uniq_id);

        Ok((try_count, !absent))
    }

    pub(crate) fn _remove_from_rbm_index(
        &self,
        multi_uniq_id: u32,
        _guard: &crate::epoch::Guard,
    ) -> Result<u32, PrefixStoreError>
    where
        AF: crate::types::AddressFamily,
    {
        let try_count = 0;

        let mut rbm = self
            .1
            .write()
            .map_err(|_| PrefixStoreError::StoreNotReadyError)?;
        rbm.remove(multi_uniq_id);

        Ok(try_count)
    }

    pub(crate) fn read(&self) -> &OnceBoxSlice<StoredNode<AF>> {
        &self.0
    }
}

impl<AF: AddressFamily> Value for NodeSet<AF> {
    fn init_with_p2_children(p2_size: usize) -> Self {
        if log_enabled!(log::Level::Debug) {
            debug!(
                "{} store: creating space for {} nodes",
                std::thread::current().name().unwrap_or("unnamed-thread"),
                2_usize.pow(p2_size as u32)
            );
        }

        let size = if p2_size == 0 { 0 } else { 1 << p2_size };

        NodeSet(OnceBoxSlice::new(size), RoaringBitmap::new().into())
    }
}
