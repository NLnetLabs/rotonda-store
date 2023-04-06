// ----------- Store Iterators ----------------------------------------------
//
// This file hosts the iterators for the CustomAllocStorage type and the
// implementations for the methods that start'em.
// Note that these iterators are only the iterators that go over the
// storage (and some over the TreeBitMap nodes, the parent of the store),
// as such all the iterators here are composed of iterators over the
// individual nodes. The Node Iterators live in the node.rs file.

use std::sync::Arc;
use std::{marker::PhantomData, sync::atomic::Ordering};

use super::atomic_types::{NodeBuckets, PrefixBuckets, PrefixSet};
use super::custom_alloc::CustomAllocStorage;
use crate::{
    af::AddressFamily,
    local_array::{
        bit_span::BitSpan,
        node::{
            NodeMoreSpecificChildIter, NodeMoreSpecificsPrefixIter, PrefixId,
            SizedStrideRef, Stride3, Stride4, Stride5, StrideNodeId,
        },
    }
};

use crossbeam_epoch::Guard;
use log::{debug, log_enabled, trace};
use routecore::{
    addr::Prefix,
    record::Meta,
};

// ----------- PrefixIter ---------------------------------------------------

// Iterator over all the prefixes in the storage.
// This Iterator does *not* use the tree, it iterates over all the length
// arrays in the CustomAllocStorage.

pub(crate) struct PrefixIter<
    'a,
    AF: AddressFamily + 'a,
    M: Meta + 'a,
    PB: PrefixBuckets<AF, M>,
> {
    prefixes: &'a PB,
    cur_len: u8,
    cur_bucket: &'a PrefixSet<AF, M>,
    cur_level: u8,
    // level depth of IPv4 as defined in rotonda-macros/maps.rs
    // Option(parent, cursor position at the parent)
    // 26 is the max number of levels in IPv6, which is the max number of
    // of both IPv4 and IPv6.
    parents: [Option<(&'a PrefixSet<AF, M>, usize)>; 26],
    cursor: usize,
    guard: &'a Guard,
    _af: PhantomData<AF>,
    _meta: PhantomData<M>,
}

impl<'a, AF: AddressFamily + 'a, M: Meta + 'a, PB: PrefixBuckets<AF, M>>
    Iterator for PrefixIter<'a, AF, M, PB>
{
    type Item = (routecore::addr::Prefix, M);

    fn next(&mut self) -> Option<Self::Item> {
        trace!(
            "starting next loop for level {} cursor {} (len {})",
            self.cur_level,
            self.cursor,
            self.cur_len
        );

        loop {
            if self.cur_len > AF::BITS {
                // This is the end, my friend
                trace!("reached max length {}, returning None", self.cur_len);
                return None;
            }

            if PB::get_bits_for_len(self.cur_len, self.cur_level) == 0 {
                // END OF THE LENGTH
                // This length is done too, go to the next length
                trace!("next length {}", self.cur_len + 1);
                self.cur_len += 1;

                // a new length, a new life
                // reset the level depth and cursor,
                // but also empty all the parents
                self.cur_level = 0;
                self.cursor = 0;
                self.parents = [None; 26];

                // let's continue, get the prefixes for the next length
                self.cur_bucket =
                    self.prefixes.get_root_prefix_set(self.cur_len);
                continue;
            }
            let bucket_size = 1_usize
                << (if self.cur_level > 0 {
                    PB::get_bits_for_len(self.cur_len, self.cur_level)
                        - PB::get_bits_for_len(
                            self.cur_len,
                            self.cur_level - 1,
                        )
                } else {
                    PB::get_bits_for_len(self.cur_len, self.cur_level)
                });

            if self.cursor >= bucket_size {
                if self.cur_level == 0 {
                    // END OF THE LENGTH
                    // This length is done too, go to the next length
                    trace!("next length {}", self.cur_len);
                    self.cur_len += 1;

                    // a new length, a new life
                    // reset the level depth and cursor,
                    // but also empty all the parents
                    self.cur_level = 0;
                    self.cursor = 0;
                    self.parents = [None; 26];

                    if self.cur_len > AF::BITS {
                        // This is the end, my friend
                        return None;
                    }

                    // let's continue, get the prefixes for the next length
                    self.cur_bucket =
                        self.prefixes.get_root_prefix_set(self.cur_len);
                } else {
                    // END OF THIS BUCKET
                    // GO BACK UP ONE LEVEL
                    // The level is done, but the length isn't
                    // Go back up one level and continue
                    match self.parents[self.cur_level as usize] {
                        Some(parent) => {
                            // There is a parent, go back up. Since we're doing depth-first
                            // we have to check if there's a prefix directly at the parent
                            // and return that.
                            self.cur_level -= 1;

                            // move the current bucket to the parent and move
                            // the cursor position where we left off. The
                            // next run of the loop will read it.
                            self.cur_bucket = parent.0;
                            self.cursor = parent.1 + 1;

                            continue;
                        }
                        None => {
                            trace!(
                                "c {} lvl {} len {}",
                                self.cursor,
                                self.cur_level,
                                self.cur_len
                            );
                            panic!(
                                "Where do we belong? Where do we come from?"
                            );
                        }
                    };
                }
            };

            // we're somewhere in the PrefixSet iteration, read the next StoredPrefix.
            // We are doing depth-first iteration, so we check for a child first and
            // descend into that if it exists.

            let s_pfx = self
                .cur_bucket
                .get_by_index(self.cursor, self.guard);

            // DEPTH FIRST ITERATION
            match s_pfx.get_next_bucket(self.guard) {
                Some(bucket) => {
                    // DESCEND ONe LEVEL
                    // There's a child here, descend into it, but...
                    // trace!("C. got next bucket {:?}", bucket);

                    // save our parent and cursor position first, and then..
                    self.parents[(self.cur_level + 1) as usize] =
                        Some((self.cur_bucket, self.cursor));

                    // move to the next bucket,
                    self.cur_bucket = bucket;

                    // increment the level and reset the cursor.
                    self.cur_level += 1;
                    self.cursor = 0;

                    // If there's a child here there MUST be a prefix here,
                    // as well.
                    if let Some(meta) =
                        s_pfx.get_stored_prefix(self.guard).map(|p| {
                            if log_enabled!(log::Level::Trace) {
                                // There's a prefix here, that's the next one
                                trace!("D. found prefix {:?}", p.prefix);
                            }
                            p.get_meta_cloned()
                        })
                    {
                        return Some((
                            s_pfx.get_prefix_id().into_pub(),
                            meta,
                        ));
                    } else {
                        panic!("No prefix here, but there's a child here?");
                    }
                }
                None => {
                    // No reference to another PrefixSet, all that's
                    // left, is checking for a prefix at the current
                    // cursor position.
                    if let Some(meta) =
                        s_pfx.get_stored_prefix(self.guard).map(|p| {
                            // There's a prefix here, that's the next one
                            if log_enabled!(log::Level::Debug) {
                                debug!("E. found prefix {:?}", p.prefix);
                            }
                            p.get_meta_cloned()
                        })
                    {
                        self.cursor += 1;
                        return Some((
                            s_pfx.get_prefix_id().into_pub(),
                            meta,
                        ));
                    }
                }
            };
            self.cursor += 1;
        }
    }
}

// ----------- Sized Wrappers -----------------------------------------------

// These are enums to abstract over the Stride Size of the iterators. Each
// iterator in here need to go over iterators that have different underlying
// stride sizes. To facilate this these wrapper enums exist.

#[derive(Copy, Clone, Debug)]
pub(crate) enum SizedNodeMoreSpecificIter<AF: AddressFamily> {
    Stride3(NodeMoreSpecificChildIter<AF, Stride3>),
    Stride4(NodeMoreSpecificChildIter<AF, Stride4>),
    Stride5(NodeMoreSpecificChildIter<AF, Stride5>),
}

impl<AF: AddressFamily> SizedNodeMoreSpecificIter<AF> {
    fn next(&mut self) -> Option<StrideNodeId<AF>> {
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
    fn next(&mut self) -> Option<PrefixId<AF>> {
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
// since it uses the Nodes to select the more specifics. Am Iterator that
// would only use the Prefixes in the store could exist, but iterating over
// those in search of more specifics would be way more expensive.

// The first iterator it goes over should have a bit_span that is the
// difference between the requested prefix and the node that hosts that
// prefix. See the method initializing this iterator
// (`get_node_for_id_prefix` takes care of it in there). The consecutive
// iterators will all have a bit_span of { bits: 0, len: 0 }. Yes, we could
// also use the PrefixIter there (it iterates over all prefixes of a node),
// but then we would have to deal with two different types of iterators.
// Note that the iterator is neither depth- or breadth-first and the
// results are essentially unordered.

pub(crate) struct MoreSpecificPrefixIter<
    'a,
    AF: AddressFamily,
    M: Meta,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, M>,
> {
    store: &'a CustomAllocStorage<AF, M, NB, PB>,
    cur_ptr_iter: SizedNodeMoreSpecificIter<AF>,
    cur_pfx_iter: SizedPrefixIter<AF>,
    start_bit_span: BitSpan,
    // skip_self: bool,
    parent_and_position: Vec<SizedNodeMoreSpecificIter<AF>>,
    guard: &'a Guard,
}

impl<
        'a,
        AF: AddressFamily + 'a,
        M: Meta,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > Iterator for MoreSpecificPrefixIter<'a, AF, M, NB, PB>
{
    type Item = (PrefixId<AF>, Arc<M>);

    fn next(&mut self) -> Option<Self::Item> {
        trace!("MoreSpecificsPrefixIter");

        loop {
            // first drain the current prefix iterator until empty.
            let next_pfx = self.cur_pfx_iter.next();

            if next_pfx.is_some() {
                return self
                    .store
                    .non_recursive_retrieve_prefix_with_guard(
                        next_pfx.unwrap_or_else(
                            || panic!(
                                "BOOM! More-specific prefix {:?} disappeared from the store",
                                next_pfx
                            )
                        ),
                        self.guard,
                    )
                    .0.map(|p| (p.prefix, p.get_record_as_arc()));
            }

            // Our current prefix iterator for this node is done, look for
            // the next pfx iterator of the next child node in the current
            // ptr iterator.
            trace!("start first ptr_iter");
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
                match self
                    .store
                    .retrieve_node_with_guard(next_ptr, self.guard)
                {
                    Some(SizedStrideRef::Stride3(next_node)) => {
                        // copy the current iterator into the parent vec and create
                        // a new ptr iterator for this node
                        self.parent_and_position.push(self.cur_ptr_iter);
                        let ptr_iter = next_node.more_specific_ptr_iter(
                            next_ptr,
                            BitSpan::new(0, 0),
                        );
                        self.cur_ptr_iter = ptr_iter.wrap();

                        trace!("next stride new iterator stride 3 {:?} start bit_span {:?}", self.cur_ptr_iter, self.start_bit_span);
                        self.cur_pfx_iter = next_node
                            .more_specific_pfx_iter(
                                next_ptr,
                                BitSpan::new(0, 0),
                                false,
                            )
                            .wrap();
                    }
                    Some(SizedStrideRef::Stride4(next_node)) => {
                        // create new ptr iterator for this node.
                        self.parent_and_position.push(self.cur_ptr_iter);
                        let ptr_iter = next_node.more_specific_ptr_iter(
                            next_ptr,
                            BitSpan::new(0, 0),
                        );
                        self.cur_ptr_iter = ptr_iter.wrap();

                        trace!("next stride new iterator stride 4 {:?} start bit_span {:?}", self.cur_ptr_iter, self.start_bit_span);
                        self.cur_pfx_iter = next_node
                            .more_specific_pfx_iter(
                                next_ptr,
                                BitSpan::new(0, 0),
                                false,
                            )
                            .wrap();
                    }
                    Some(SizedStrideRef::Stride5(next_node)) => {
                        // create new ptr iterator for this node.
                        self.parent_and_position.push(self.cur_ptr_iter);
                        let ptr_iter = next_node.more_specific_ptr_iter(
                            next_ptr,
                            BitSpan::new(0, 0),
                        );
                        self.cur_ptr_iter = ptr_iter.wrap();

                        trace!("next stride new iterator stride 5 {:?} start bit_span {:?}", self.cur_ptr_iter, self.start_bit_span);
                        self.cur_pfx_iter = next_node
                            .more_specific_pfx_iter(
                                next_ptr,
                                BitSpan::new(0, 0),
                                false,
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

// ----------- LessSpecificPrefixIter ---------------------------------------

// This iterator iterates over all the less-specifics for a given prefix.
// It does *not* use the tree, it goes directly into the CustomAllocStorage
// and retrieves the less-specifics by going from len to len, searching for
// the prefixes.

pub(crate) struct LessSpecificPrefixIter<
    'a,
    AF: AddressFamily + 'a,
    M: Meta + 'a,
    PB: PrefixBuckets<AF, M>,
> {
    prefixes: &'a PB,
    cur_len: u8,
    cur_bucket: &'a PrefixSet<AF, M>,
    cur_level: u8,
    cur_prefix_id: PrefixId<AF>,
    guard: &'a Guard,
    _af: PhantomData<AF>,
    _meta: PhantomData<M>,
    _pb: PhantomData<PB>,
}

impl<'a, AF: AddressFamily + 'a, M: Meta + 'a, PB: PrefixBuckets<AF, M>>
    Iterator for LessSpecificPrefixIter<'a, AF, M, PB>
{
    type Item = (PrefixId<AF>, Arc<M>);

    // This iterator moves down all prefix lengths, starting with the length
    // of the (search prefix - 1), looking for shorter prefixes, where the
    // its bits are the same as the bits of the search prefix.
    fn next(&mut self) -> Option<Self::Item> {
        trace!("search next less-specific for {:?}", self.cur_prefix_id);

        loop {
            if self.cur_len == 0 {
                // This is the end, my friend
                trace!("reached min length {}, returning None", self.cur_len);
                return None;
            }

            // shave a bit of the current prefix.
            trace!(
                "truncate to len {} (level {})",
                self.cur_len,
                self.cur_level
            );
            self.cur_prefix_id = PrefixId::new(
                self.cur_prefix_id.get_net().truncate_to_len(self.cur_len),
                self.cur_len,
            );

            let last_level = if self.cur_level > 0 {
                PB::get_bits_for_len(self.cur_len, self.cur_level - 1)
            } else {
                0
            };

            let this_level =
                PB::get_bits_for_len(self.cur_len, self.cur_level);

            // NOT THE HASHING FUNCTION
            let index = ((self.cur_prefix_id.get_net() << last_level)
                >> ((AF::BITS - (this_level - last_level)) % AF::BITS))
                .dangerously_truncate_to_u32()
                as usize;

            if this_level == 0 {
                // END OF THE LENGTH
                // This length is done too, go to the next length
                trace!("next length {}", self.cur_len + 1);
                self.cur_len -= 1;

                // a new length, a new life
                // reset the level depth and cursor,
                // but also empty all the parents
                self.cur_level = 0;
                // self.parents = [None; 26];

                // let's continue, get the prefixes for the next length
                self.cur_bucket =
                    self.prefixes.get_root_prefix_set(self.cur_len);
                continue;
            }

            // LEVEL DEPTH ITERATION
            let s_pfx =
                self.cur_bucket.get_by_index(index, self.guard);

            if let Some(stored_prefix) = s_pfx.get_stored_prefix(self.guard) {
                trace!("get_record {:?}", stored_prefix.record);
                let pfx_rec = stored_prefix.get_record_as_arc();
                // There is a prefix  here, but we need to checkt if it's
                // the right one.
                if self.cur_prefix_id
                    == stored_prefix.prefix
                {
                    trace!(
                        "found requested prefix {:?}",
                        self.cur_prefix_id
                    );
                    self.cur_len -= 1;
                    self.cur_level = 0;
                    self.cur_bucket = self
                        .prefixes
                        .get_root_prefix_set(self.cur_len);
                    return Some((stored_prefix.prefix, pfx_rec));
                };
                // Advance to the next level or the next len.
                match stored_prefix
                    .next_bucket
                    .0
                    .load(Ordering::SeqCst, self.guard)
                    .is_null()
                {
                    // No child here, move one length down.
                    true => {
                        self.cur_len -= 1;
                        self.cur_level = 0;
                        self.cur_bucket = self
                            .prefixes
                            .get_root_prefix_set(self.cur_len);
                    }
                    // There's a child, move a level up and set the child
                    // as current. Length remains the same.
                    false => {
                        self.cur_bucket = &stored_prefix.next_bucket;
                        self.cur_level += 1;
                    }
                };
            } else {
                trace!("no prefix at this level. Move one down.");
                self.cur_len -= 1;
                self.cur_level = 0;
                self.cur_bucket =
                    self.prefixes.get_root_prefix_set(self.cur_len);
            }
        }
    }
}

// ----------- Iterator initialization methods for CustomAllocStorage -------

// These are only the methods that are starting the iterations. All other
// methods for CustomAllocStorage are in the main custom_alloc.rs file.

impl<
        'a,
        AF: AddressFamily,
        M: routecore::record::Meta,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > CustomAllocStorage<AF, M, NB, PB>
{
    // Iterator over all more-specific prefixes, starting from the given
    // prefix at the given level and cursor.
    pub fn more_specific_prefix_iter_from(
        &'a self,
        start_prefix_id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> impl Iterator<Item = (PrefixId<AF>, Arc<M>)> + '_ {
        trace!("more specifics for {:?}", start_prefix_id);

        // A v4 /32 or a v4 /128 doesn't have more specific prefixes 🤓.
        if start_prefix_id.get_len() >= AF::BITS {
            None
        } else {
            // calculate the node start_prefix_id lives in.
            let (start_node_id, start_bit_span) =
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
                "start bit span  {:032b} {}",
                start_bit_span,
                start_bit_span.bits
            );
            let cur_pfx_iter: SizedPrefixIter<AF>;
            let cur_ptr_iter: SizedNodeMoreSpecificIter<AF>;

            if let Some(node) =
                self.retrieve_node_with_guard(start_node_id, guard)
            {
                match node {
                    SizedStrideRef::Stride3(n) => {
                        cur_pfx_iter = SizedPrefixIter::Stride3(
                            n.more_specific_pfx_iter(
                                start_node_id,
                                start_bit_span,
                                true,
                            ),
                        );
                        cur_ptr_iter = SizedNodeMoreSpecificIter::Stride3(
                            n.more_specific_ptr_iter(
                                start_node_id,
                                start_bit_span,
                            ),
                        );
                    }
                    SizedStrideRef::Stride4(n) => {
                        cur_pfx_iter = SizedPrefixIter::Stride4(
                            n.more_specific_pfx_iter(
                                start_node_id,
                                start_bit_span,
                                true,
                            ),
                        );
                        cur_ptr_iter = SizedNodeMoreSpecificIter::Stride4(
                            n.more_specific_ptr_iter(
                                start_node_id,
                                start_bit_span,
                            ),
                        );
                    }
                    SizedStrideRef::Stride5(n) => {
                        cur_pfx_iter = SizedPrefixIter::Stride5(
                            n.more_specific_pfx_iter(
                                start_node_id,
                                start_bit_span,
                                true,
                            ),
                        );
                        cur_ptr_iter = SizedNodeMoreSpecificIter::Stride5(
                            n.more_specific_ptr_iter(
                                start_node_id,
                                start_bit_span,
                            ),
                        );
                    }
                };

                Some(MoreSpecificPrefixIter {
                    store: self,
                    guard,
                    cur_pfx_iter,
                    cur_ptr_iter,
                    start_bit_span,
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
        guard: &'a Guard,
    ) -> impl Iterator<Item = (PrefixId<AF>, Arc<M>)> + '_ {
        trace!("less specifics for {:?}", start_prefix_id);
        trace!("level {}, len {}", 0, start_prefix_id.get_len());

        // We could just let the /0 prefix search the tree and have it return
        // an empty iterator, but to avoid having to read out the root node
        // for this prefix, we'll just return an empty iterator. None can be
        // turned into an Iterator!
        if start_prefix_id.get_len() < 1 {
            None
        } else {
            let cur_len = start_prefix_id.get_len() - 1;
            let cur_bucket = self.prefixes.get_root_prefix_set(cur_len);

            Some(LessSpecificPrefixIter {
                prefixes: &self.prefixes,
                cur_len,
                cur_bucket,
                cur_level: 0,
                cur_prefix_id: start_prefix_id,
                guard,
                _af: PhantomData,
                _meta: PhantomData,
                _pb: PhantomData,
            })
        }
        .into_iter()
        .flatten()
    }

    // Iterator over all the prefixes in the storage.
    pub fn prefixes_iter(
        &'a self,
        guard: &'a Guard,
    ) -> impl Iterator<Item = (Prefix, M)> + 'a {
        PrefixIter {
            prefixes: &self.prefixes,
            cur_bucket: self.prefixes.get_root_prefix_set(0),
            cur_len: 0,
            cur_level: 0,
            cursor: 0,
            parents: [None; 26],
            guard,
            _af: PhantomData,
            _meta: PhantomData,
        }
    }
}

// ----------- InternalPrefixRecord -> RecordSet (public) -------------------

// impl<'a, AF: AddressFamily, Meta: routecore::record::Meta>
//     std::iter::FromIterator<InternalPrefixRecord<AF, Meta>>
//     for routecore::bgp::RecordSet<'a, Meta>
// {
//     fn from_iter<I: IntoIterator<Item = InternalPrefixRecord<AF, Meta>>>(
//         iter: I,
//     ) -> Self {
//         let mut v4 = vec![];
//         let mut v6 = vec![];
//         for pfx in iter {
//             let addr = pfx.net.into_ipaddr();
//             match addr {
//                 std::net::IpAddr::V4(_) => {
//                     v4.push(
//                         routecore::bgp::PrefixRecord::new_with_local_meta(
//                             Prefix::new(addr, pfx.len).unwrap(),
//                             pfx.meta,
//                         ),
//                     );
//                 }
//                 std::net::IpAddr::V6(_) => {
//                     v6.push(
//                         routecore::bgp::PrefixRecord::new_with_local_meta(
//                             Prefix::new(addr, pfx.len).unwrap(),
//                             pfx.meta,
//                         ),
//                     );
//                 }
//             }
//         }
//         Self { v4, v6 }
//     }
// }
