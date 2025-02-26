use log::trace;
use roaring::RoaringBitmap;

use crate::{
    local_array::{
        bit_span::BitSpan,
        in_memory::{node::SizedStrideRef, tree::TreeBitMap},
    },
    prelude::multi::{NodeBuckets, PrefixId},
    rib::iterators::{SizedNodeMoreSpecificIter, SizedPrefixIter},
    AddressFamily,
};

type Type<AF: AddressFamily> = SizedNodeMoreSpecificIter<AF>;

pub(crate) struct MoreSpecificPrefixIter<
    'a,
    AF: AddressFamily,
    // M: Meta,
    NB: NodeBuckets<AF>,
    // PB: PrefixBuckets<AF, M>,
> {
    store: &'a TreeBitMap<AF, NB>,
    cur_ptr_iter: SizedNodeMoreSpecificIter<AF>,
    cur_pfx_iter: SizedPrefixIter<AF>,
    // start_bit_span: BitSpan,
    // skip_self: bool,
    parent_and_position: Vec<Type<AF>>,
    // If specified, we're only iterating over records for this mui.
    mui: Option<u32>,
    // This is the tree-wide index of withdrawn muis, used to rewrite the
    // statuses of these records, or filter them out.
    global_withdrawn_bmin: &'a RoaringBitmap,
    // Whether we should filter out the withdrawn records in the search result
    include_withdrawn: bool,
}

impl<
        'a,
        AF: AddressFamily + 'a,
        // M: Meta,
        NB: NodeBuckets<AF>,
        // PB: PrefixBuckets<AF, M>,
    > Iterator for MoreSpecificPrefixIter<'a, AF, NB>
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
                    self.store
                        .retrieve_node(next_ptr)
                        .map(|n| SizedStrideRef::Stride4(n))
                } else {
                    self.store
                        .retrieve_node_for_mui(next_ptr, self.mui.unwrap())
                        .map(|n| SizedStrideRef::Stride4(n))
                };

                match node {
                    Some(SizedStrideRef::Stride3(next_node)) => {
                        // copy the current iterator into the parent vec and create
                        // a new ptr iterator for this node
                        self.parent_and_position.push(self.cur_ptr_iter);
                        let ptr_iter = next_node.more_specific_ptr_iter(
                            next_ptr,
                            BitSpan { bits: 0, len: 0 },
                        );
                        self.cur_ptr_iter = ptr_iter.wrap();

                        // trace!(
                        //     "next stride new iterator stride 3 {:?} start \
                        // bit_span {:?}",
                        //     self.cur_ptr_iter,
                        //     self.start_bit_span
                        // );
                        self.cur_pfx_iter = next_node
                            .more_specific_pfx_iter(
                                next_ptr,
                                BitSpan::new(0, 0),
                            )
                            .wrap();
                    }
                    Some(SizedStrideRef::Stride4(next_node)) => {
                        // create new ptr iterator for this node.
                        self.parent_and_position.push(self.cur_ptr_iter);
                        let ptr_iter = next_node.more_specific_ptr_iter(
                            next_ptr,
                            BitSpan { bits: 0, len: 0 },
                        );
                        self.cur_ptr_iter = ptr_iter.wrap();

                        trace!(
                            "next stride new iterator stride 4 {:?} start \
                        bit_span 0 0",
                            self.cur_ptr_iter,
                        );
                        self.cur_pfx_iter = next_node
                            .more_specific_pfx_iter(
                                next_ptr,
                                BitSpan::new(0, 0),
                            )
                            .wrap();
                    }
                    Some(SizedStrideRef::Stride5(next_node)) => {
                        // create new ptr iterator for this node.
                        self.parent_and_position.push(self.cur_ptr_iter);
                        let ptr_iter = next_node.more_specific_ptr_iter(
                            next_ptr,
                            BitSpan { bits: 0, len: 0 },
                        );
                        self.cur_ptr_iter = ptr_iter.wrap();

                        // trace!(
                        //     "next stride new iterator stride 5 {:?} start \
                        // bit_span {:?}",
                        //     self.cur_ptr_iter,
                        //     self.start_bit_span
                        // );
                        self.cur_pfx_iter = next_node
                            .more_specific_pfx_iter(
                                next_ptr,
                                BitSpan::new(0, 0),
                            )
                            .wrap();
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
