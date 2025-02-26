// ----------- Store Iterators ----------------------------------------------
//
// This file hosts the iterators for the Rib and implementations for the
// methods that start'em. There are 3 Iterators:
//
// 1. an iterator `PrefixIter` that iterates over ALL of the prefix buckets of
// the CHT backing the TreeBitMap.
//
// 2. a MoreSpecificsIterator that starts from a prefix in the prefix buckets
// for that particular prefix length, but uses the node in the TreeBitMap to
// find its more specifics.
//
// 3. a LessSpecificIterator, that just reduces the prefix size bit-by-bit and
// looks in the prefix buckets for the diminuishing prefix.
//
// The Iterators that start from the root node of the TreeBitMap (which
// is the only option for the single-threaded TreeBitMap) live in the
// deprecated_node.rs file. They theoretically should be slower and cause more
// contention, since every lookup has to go through the levels near the root
// in the TreeBitMap.

use crate::local_array::in_memory::atomic_types::NodeBuckets;
use crate::local_array::in_memory::node::{SizedStrideRef, StrideNodeId};
use crate::local_array::in_memory::tree::{
    Stride3, Stride4, Stride5, TreeBitMap,
};
use crate::{
    af::AddressFamily,
    local_array::{
        bit_span::BitSpan,
        in_memory::node::{
            NodeMoreSpecificChildIter, NodeMoreSpecificsPrefixIter,
        },
        types::PrefixId,
    },
};

use inetnum::addr::Prefix;
use log::{log_enabled, trace};

// ----------- Sized Wrappers -----------------------------------------------

// These are enums to abstract over the Stride Size of the iterators. Each
// iterator in here need to go over iterators that have different underlying
// stride sizes. To facilitate this these wrapper enums exist.

#[derive(Copy, Clone, Debug)]
pub(crate) enum SizedNodeMoreSpecificIter<AF: AddressFamily> {
    Stride3(NodeMoreSpecificChildIter<AF, Stride3>),
    Stride4(NodeMoreSpecificChildIter<AF, Stride4>),
    Stride5(NodeMoreSpecificChildIter<AF, Stride5>),
}

impl<AF: AddressFamily> SizedNodeMoreSpecificIter<AF> {
    pub(crate) fn next(&mut self) -> Option<StrideNodeId<AF>> {
        match self {
            SizedNodeMoreSpecificIter::Stride3(iter) => iter.next(),
            SizedNodeMoreSpecificIter::Stride4(iter) => iter.next(),
            SizedNodeMoreSpecificIter::Stride5(iter) => iter.next(),
        }
    }
}

pub(crate) enum SizedPrefixIter<AF: AddressFamily> {
    Stride3(NodeMoreSpecificsPrefixIter<AF, Stride3>),
    Stride4(NodeMoreSpecificsPrefixIter<AF, Stride4>),
    Stride5(NodeMoreSpecificsPrefixIter<AF, Stride5>),
}

impl<AF: AddressFamily> SizedPrefixIter<AF> {
    pub(crate) fn next(&mut self) -> Option<PrefixId<AF>> {
        match self {
            SizedPrefixIter::Stride3(iter) => iter.next(),
            SizedPrefixIter::Stride4(iter) => iter.next(),
            SizedPrefixIter::Stride5(iter) => iter.next(),
        }
    }
}

// ----------- MoreSpecificPrefixIter ------------------------------------

// A iterator over all the more-specifics for a given prefix.
//
// This iterator is somewhat different from the other *PrefixIterator types,
// since it uses the Nodes to select the more specifics. An Iterator that
// would only use the Prefixes in the store could exist, but iterating over
// those in search of more specifics would be way more expensive.

// The first iterator it goes over should have a bit_span that is the
// difference between the requested prefix and the node that hosts that
// prefix. See the method initializing this iterator (`get_node_for_id_prefix`
// takes care of it in there). The consecutive iterators will all have a
// bit_span of { bits: 0, len: 0 }. Yes, we could also use the PrefixIter
// there (it iterates over all prefixes of a node), but then we would have to
// deal with two different types of iterators. Note that the iterator is
// neither depth- or breadth-first and the results are essentially unordered.

pub(crate) struct MoreSpecificPrefixIter<
    'a,
    AF: AddressFamily,
    NB: NodeBuckets<AF>,
> {
    tree: &'a TreeBitMap<AF, NB>,
    cur_ptr_iter: SizedNodeMoreSpecificIter<AF>,
    cur_pfx_iter: SizedPrefixIter<AF>,
    parent_and_position: Vec<SizedNodeMoreSpecificIter<AF>>,
}

impl<'a, AF: AddressFamily + 'a, NB: NodeBuckets<AF>> Iterator
    for MoreSpecificPrefixIter<'a, AF, NB>
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
                let node = self.tree.retrieve_node(next_ptr);

                match node {
                    // Some(next_node) => {
                    //     // copy the current iterator into the parent vec and create
                    //     // a new ptr iterator for this node
                    //     self.parent_and_position.push(self.cur_ptr_iter);
                    //     let ptr_iter = next_node.more_specific_ptr_iter(
                    //         next_ptr,
                    //         BitSpan { bits: 0, len: 0 },
                    //     );
                    //     self.cur_ptr_iter = ptr_iter.wrap();

                    //     // trace!(
                    //     //     "next stride new iterator stride 3 {:?} start \
                    //     // bit_span {:?}",
                    //     //     self.cur_ptr_iter,
                    //     //     self.start_bit_span
                    //     // );
                    //     self.cur_pfx_iter = next_node
                    //         .more_specific_pfx_iter(
                    //             next_ptr,
                    //             BitSpan::new(0, 0),
                    //         )
                    //         .wrap();
                    // }
                    Some(next_node) => {
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
                    // Some(SizedStrideRef::Stride5(next_node)) => {
                    //     // create new ptr iterator for this node.
                    //     self.parent_and_position.push(self.cur_ptr_iter);
                    //     let ptr_iter = next_node.more_specific_ptr_iter(
                    //         next_ptr,
                    //         BitSpan { bits: 0, len: 0 },
                    //     );
                    //     self.cur_ptr_iter = ptr_iter.wrap();

                    //     // trace!(
                    //     //     "next stride new iterator stride 5 {:?} start \
                    //     // bit_span {:?}",
                    //     //     self.cur_ptr_iter,
                    //     //     self.start_bit_span
                    //     // );
                    //     self.cur_pfx_iter = next_node
                    //         .more_specific_pfx_iter(
                    //             next_ptr,
                    //             BitSpan::new(0, 0),
                    //         )
                    //         .wrap();
                    // }
                    None => {
                        println!("no node here.");
                        return None;
                    }
                };
            }
        }
    }
}

pub(crate) struct LMPrefixIter<'a, AF: AddressFamily, NB: NodeBuckets<AF>> {
    tree: &'a TreeBitMap<AF, NB>,
    prefix: PrefixId<AF>,
}

impl<AF: AddressFamily, NB: NodeBuckets<AF>> Iterator
    for LMPrefixIter<'_, AF, NB>
{
    type Item = PrefixId<AF>;
    fn next(&mut self) -> Option<Self::Item> {
        trace!("search lm prefix for {:?}", self.prefix);

        loop {
            if self.prefix.get_len() == 0 {
                return None;
            }

            if self.tree.prefix_exists(self.prefix) {
                return Some(self.prefix);
            }

            self.prefix =
                self.prefix.truncate_to_len(self.prefix.get_len() - 1);
        }
    }
}

// ----------- LessSpecificPrefixIter ---------------------------------------

// This iterator iterates over all the less-specifics for a given prefix. It
// does *not* use the tree, it goes directly into the CustomAllocStorage and
// retrieves the less-specifics by going from len to len, searching for the
// prefixes.

pub(crate) struct LessSpecificPrefixIter<
    'a,
    AF: AddressFamily,
    NB: NodeBuckets<AF>,
> {
    tree: &'a TreeBitMap<AF, NB>,
    prefix: PrefixId<AF>,
    cur_level: u8,
}

impl<AF: AddressFamily, NB: NodeBuckets<AF>> Iterator
    for LessSpecificPrefixIter<'_, AF, NB>
{
    type Item = PrefixId<AF>;

    // This iterator moves down all prefix lengths, starting with the length
    // of the (search prefix - 1), looking for shorter prefixes, where the
    // its bits are the same as the bits of the search prefix.
    fn next(&mut self) -> Option<Self::Item> {
        trace!("search next less-specific for {:?}", self.prefix);
        self.cur_level = self.cur_level.saturating_sub(1);

        loop {
            if self.cur_level == 0 {
                return None;
            }

            let lvl_pfx = self.prefix.truncate_to_len(self.cur_level);
            if self.tree.prefix_exists(lvl_pfx) {
                self.cur_level = self.cur_level.saturating_sub(1);
                return Some(lvl_pfx);
            }

            self.cur_level = self.cur_level.saturating_sub(1);
        }
    }
}

// ----------- Iterator initialization methods for Rib -----------------------

// These are only the methods that are starting the iterations. All other
// methods for Rib are in the main rib.rs file.

impl<
        'a,
        AF: AddressFamily,
        // M: crate::prefix_record::Meta,
        NB: NodeBuckets<AF>,
        // PB: PrefixBuckets<AF, M>,
    > TreeBitMap<AF, NB>
{
    // Iterator over all more-specific prefixes, starting from the given
    // prefix at the given level and cursor.
    pub fn more_specific_prefix_iter_from(
        &'a self,
        start_prefix_id: PrefixId<AF>,
    ) -> impl Iterator<Item = PrefixId<AF>> + 'a {
        trace!("more specifics for {:?}", start_prefix_id);

        // A v4 /32 or a v6 /128 doesn't have more specific prefixes 🤓.
        if start_prefix_id.get_len() >= AF::BITS {
            None
        } else {
            // calculate the node start_prefix_id lives in.
            let (start_node_id, start_bs) =
                self.get_node_id_for_prefix(&start_prefix_id);
            trace!("start node {}", start_node_id);
            trace!(
                "start prefix id {:032b} (len {})",
                start_prefix_id.get_net(),
                start_prefix_id.get_len()
            );
            trace!(
                "start node id   {:032b} (bits {} len {})",
                start_node_id.get_id().0,
                start_node_id.get_id().0,
                start_node_id.get_len()
            );
            trace!(
                "start pfx bit span  {:08b} {} len {}",
                start_bs.bits,
                start_bs.bits,
                start_bs.len
            );
            trace!(
                "start ptr bit span  {:08b} {} len {}",
                start_bs.bits,
                start_bs.bits,
                start_bs.len
            );

            let cur_pfx_iter: SizedPrefixIter<AF>;
            let cur_ptr_iter: SizedNodeMoreSpecificIter<AF>;
            let node = self.retrieve_node(start_node_id);

            if let Some(node) = node {
                match node {
                    // SizedStrideRef::Stride3(n) => {
                    //     cur_pfx_iter = SizedPrefixIter::Stride3(
                    //         n.more_specific_pfx_iter(start_node_id, start_bs),
                    //     );
                    //     cur_ptr_iter = SizedNodeMoreSpecificIter::Stride3(
                    //         n.more_specific_ptr_iter(start_node_id, start_bs),
                    //     );
                    // }
                    n => {
                        cur_pfx_iter = SizedPrefixIter::Stride4(
                            n.more_specific_pfx_iter(start_node_id, start_bs),
                        );
                        trace!("---------------------");
                        trace!("start iterating nodes");
                        cur_ptr_iter = SizedNodeMoreSpecificIter::Stride4(
                            n.more_specific_ptr_iter(start_node_id, start_bs),
                        );
                    } // SizedStrideRef::Stride5(n) => {
                      //     cur_pfx_iter = SizedPrefixIter::Stride5(
                      //         n.more_specific_pfx_iter(start_node_id, start_bs),
                      //     );
                      //     cur_ptr_iter = SizedNodeMoreSpecificIter::Stride5(
                      //         n.more_specific_ptr_iter(start_node_id, start_bs),
                      //     );
                      // }
                };

                Some(MoreSpecificPrefixIter {
                    tree: self,
                    cur_pfx_iter,
                    cur_ptr_iter,
                    parent_and_position: vec![],
                })
            } else {
                None
            }
        }
        .into_iter()
        .flatten()
    }

    // Iterator over all less-specific prefixes, starting from the given
    // prefix at the given level and cursor.
    pub fn less_specific_prefix_iter(
        &'a self,
        start_prefix_id: PrefixId<AF>,
    ) -> impl Iterator<Item = PrefixId<AF>> + 'a {
        if log_enabled!(log::Level::Trace) {
            trace!("less specifics for {}", Prefix::from(start_prefix_id));
            trace!("level {}, len {}", 0, start_prefix_id.get_len());
        }

        LessSpecificPrefixIter {
            tree: self,
            prefix: start_prefix_id,
            cur_level: start_prefix_id.get_len(),
        }
    }

    pub fn longest_matching_prefix(
        &'a self,
        prefix: PrefixId<AF>,
    ) -> Option<PrefixId<AF>> {
        if log_enabled!(log::Level::Trace) {
            trace!("lmp for {}", Prefix::from(prefix));
        }

        LMPrefixIter { tree: self, prefix }.next()
    }

    // Iterator over all the prefixes in the in_memory store.
    pub fn prefixes_iter(&'a self) -> impl Iterator<Item = Prefix> + 'a {
        self.more_specific_prefix_iter_from(PrefixId::new(
            AF::new(0_u32.into()),
            0,
        ))
        .map(Prefix::from)
    }
}
