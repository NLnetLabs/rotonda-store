use log::trace;
use roaring::RoaringBitmap;

use crate::{
    tree_bitmap::{NodeMoreSpecificChildIter, NodeMoreSpecificsPrefixIter},
    types::{AddressFamily, BitSpan, PrefixId},
    TreeBitMap,
};

// This iterator is unused right now: all iterators go over the in-memory
// treebitmap, and retreive metadata based on the persist_strategy per prefix
// from the relevant tree.
//
// However this tree may ultimately be more efficient for the MemoryOnly
// strategy.

pub(crate) struct _MoreSpecificPrefixIter<
    'a,
    AF: AddressFamily,
    const ROOT_SIZE: usize,
> {
    store: &'a TreeBitMap<AF, ROOT_SIZE>,
    cur_ptr_iter: NodeMoreSpecificChildIter<AF>,
    cur_pfx_iter: NodeMoreSpecificsPrefixIter<AF>,
    parent_and_position: Vec<NodeMoreSpecificChildIter<AF>>,
    // If specified, we're only iterating over records for this mui.
    mui: Option<u32>,
    // This is the tree-wide index of withdrawn muis, used to rewrite the
    // statuses of these records, or filter them out.
    global_withdrawn_bmin: &'a RoaringBitmap,
    // Whether we should filter out the withdrawn records in the search result
    include_withdrawn: bool,
}

impl<'a, AF: AddressFamily + 'a, const ROOT_SIZE: usize> Iterator
    for _MoreSpecificPrefixIter<'a, AF, ROOT_SIZE>
{
    type Item = PrefixId<AF>;

    fn next(&mut self) -> Option<Self::Item> {
        trace!("MoreSpecificsPrefixIter");

        loop {
            // first drain the current prefix iterator until empty.
            let next_pfx = self.cur_pfx_iter.next();

            if next_pfx.is_some() {
                return next_pfx;
            }

            // Our current prefix iterator for this node is done, look for
            // the next pfx iterator of the next child node in the current
            // ptr iterator.
            trace!("resume ptr iterator {:?}", self.cur_ptr_iter);

            let mut next_ptr = self.cur_ptr_iter.next();

            // Our current ptr iterator is also done, maybe we have a parent
            if next_ptr.is_none() {
                trace!("try for parent");
                if let Some(cur_ptr_iter) = self.parent_and_position.pop() {
                    trace!("continue with parent");
                    self.cur_ptr_iter = cur_ptr_iter;
                    next_ptr = self.cur_ptr_iter.next();
                } else {
                    trace!("no more parents");
                    return None;
                }
            }

            if let Some(next_ptr) = next_ptr {
                let node = if self.mui.is_none() {
                    trace!("let's retriev node {}", next_ptr);
                    self.store.retrieve_node(next_ptr)
                } else {
                    self.store
                        .retrieve_node_for_mui(next_ptr, self.mui.unwrap())
                };

                match node {
                    Some(next_node) => {
                        // create new ptr iterator for this node.
                        self.parent_and_position.push(self.cur_ptr_iter);
                        let ptr_iter = next_node.more_specific_ptr_iter(
                            next_ptr,
                            BitSpan { bits: 0, len: 0 },
                        );
                        self.cur_ptr_iter = ptr_iter;

                        trace!(
                            "next stride new iterator stride 4 {:?} start \
                        bit_span 0 0",
                            self.cur_ptr_iter,
                        );
                        self.cur_pfx_iter = next_node.more_specific_pfx_iter(
                            next_ptr,
                            BitSpan::new(0, 0),
                        )
                    }
                    None => {
                        println!("no node here.");
                        return None;
                    }
                };
            }
        }
    }
}
