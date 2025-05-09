mod atomic_bitmap;
mod node_cht;

mod tree_bitmap_iterators;
mod tree_bitmap_node;
mod tree_bitmap_query;

pub(crate) use tree_bitmap_node::{
    NodeId, NodeMoreSpecificChildIter, NodeMoreSpecificsPrefixIter,
    TreeBitMapNode,
};
use zerocopy::FromZeros;

// ----------- Dev Log for the RIB -------------------------------------------
//
// The StarCastAfRib provides in-memory storage for the TreeBitMapNodes
// and for prefixes and their meta-data. The storage for node is on the
// `buckets` field, and the prefixes are stored in, well, the `prefixes`
// field. They are both organised in the same way, as chained hash tables,
// one per (prefix|node)-length. The hashing function (that is detailed
// lower down in this file), basically takes the address part of the
// node|prefix and uses `(node|prefix)-address part % bucket size`
// as its index.
//
// Both the prefixes and the buckets field have one bucket per (prefix|node)
// -length that start out with a fixed-size array. The size of the arrays is
// set in the rotonda_macros/maps.rs file.
//
// For lower (prefix|node)-lengths the number of elements in the array is
// equal to the number of prefixes in that length, so there's exactly one
// element per (prefix|node). For greater lengths there will be collisions,
// in that case the stored (prefix|node) will have a reference to another
// bucket (also of a fixed size), that holds a (prefix|node) that collided
// with the one that was already stored. A (node|prefix) lookup will have to
// go over all (node|prefix) buckets until it matches the requested (node|
// prefix) or it reaches the end of the chain.
//
// The chained (node|prefixes) are occupied at a first-come, first-serve
// basis, and are not re-ordered on new insertions of (node|prefixes). This
// may change in the future, since it prevents iterators from being ordered.
//
// One of the nice things of having one table per (node|prefix)-length is that
// a search can start directly at the prefix-length table it wishes, and go
// go up and down into other tables if it needs to (e.g., because more- or
// less-specifics were asked for). In contrast if you do a lookup by
// traversing the tree of nodes, we would always have to go through the root-
// node first and then go up the tree to the requested node. The lower nodes
// of the tree (close to the root) would be a formidable bottle-neck then.
//
// Previously, the meta-data was an atomically stored value, that was required
// to implement the `Meta` and the `Clone` trait. New meta-data instances were
// stored atomically without further ado, but updates to a piece of meta-data
// were done by merging the previous meta-data with the new meta-data, through
// use of the `MergeUpdate` trait.
//
// The `upsert_prefix` methods were used to retrieve only the most recent
// insert for a prefix (for now).
//
// Prefix example
//
//         (level 0 arrays)         prefixes  bucket
//                                    /len     size
//         ┌──┐
// len /0  │ 0│                        1        1     ■
//         └──┘                                       │
//         ┌──┬──┐                                    │
// len /1  │00│01│                     2        2     │
//         └──┴──┘                                 perfect
//         ┌──┬──┬──┬──┐                             hash
// len /2  │  │  │  │  │               4        4     │
//         └──┴──┴──┴──┘                              │
//         ┌──┬──┬──┬──┬──┬──┬──┬──┐                  │
// len /3  │  │  │  │  │  │  │  │  │   8        8     ■
//         └──┴──┴──┴──┴──┴──┴──┴──┘
//         ┌──┬──┬──┬──┬──┬──┬──┬──┐                        ┌────────────┐
// len /4  │  │  │  │  │  │  │  │  │   8        16 ◀────────│ collision  │
//         └──┴──┴──┴┬─┴──┴──┴──┴──┘                        └────────────┘
//                   └───┐
//                       │              ┌─collision─────────┐
//                   ┌───▼───┐          │                   │
//                   │       │ ◀────────│ 0x0100 and 0x0101 │
//                   │ 0x010 │          └───────────────────┘
//                   │       │
//                   ├───────┴──────────────┬──┬──┐
//                   │ StoredPrefix 0x0101  │  │  │
//                   └──────────────────────┴─┬┴─┬┘
//                                            │  │
//                       ┌────────────────────┘  └──┐
//            ┌──────────▼──────────┬──┐          ┌─▼┬──┐
//         ┌─▶│ metadata (current)  │  │          │ 0│ 1│ (level 1 array)
//         │  └─────────────────────┴──┘          └──┴──┘
//    merge└─┐                        │             │
//    update │           ┌────────────┘             │
//           │┌──────────▼──────────┬──┐        ┌───▼───┐
//         ┌─▶│ metadata (previous) │  │        │       │
//         │  └─────────────────────┴──┘        │  0x0  │
//    merge└─┐                        │         │       │
//    update │           ┌────────────┘         ├───────┴──────────────┬──┐
//           │┌──────────▼──────────┬──┐        │ StoredPrefix 0x0110  │  │
//            │ metadata (oldest)   │  │        └──────────────────────┴──┘
//            └─────────────────────┴──┘                                 │
//                                                         ┌─────────────┘
//                                              ┌──────────▼──────────────┐
//                                              │ metadata (current)      │
//                                              └─────────────────────────┘

// Note about the memory usage of the data-structures of the Buckets
//
// As said, the prefixes and nodes are stored in buckets. A bucket right now
// is of type `[MaybeUnit<Atomic<StoredPrefix>>]`, this has the advantage
// that the length can be variable, based on the stride size for that level.
// It saves us to have to implement a generic something.
// Another advantage is the fixed place in which an atomic StoredPrefix
// lives: this makes compare-and-swapping it relatively straight forward.
// Each accessing thread would try to read/write the exact same entry in the
// array, so shouldn't be any 'rug pulling' on the whole array.
//
// A disadvantage is that this is a fixed size, sparse array the moment it
// is created. Theoretically, an `Atomic<Vec<StoredPrefix>`
// would not have this disadvantage. Manipulating the whole vec atomically
// though is very tricky (we would have to atomically compare-and-swap the
// whole vec each time the prefix meta-data is changed) and inefficient,
// since we would have to either keep the vec sorted on `PrefixId` at all
// times, or, we would have to inspect each value in the vec on *every* read
// or write. the StoredPrefix (this is a challenge in itself, since the
// StoredPrefix needs to be read atomically to retrieve the PrefixId).
// Compare-and-swapping a whole vec most probably would need a hash over the
// vec to determine whether it was changed. I gave up on this approach,
//
// Another approach to try to limit the memory use is to try to use other
// indexes in the same array on collision (the array mentioned above), before
// heading off and following the reference to the next bucket. This would
// limit the amount of (sparse) arrays being created for a typical prefix
// treebitmap, at the cost of longer average search times. Two
// implementations of this approach are Cuckoo hashing[^1], and Skip Lists.
// Skip lists[^2] are a probabilistic data-structure, famously used by Redis,
// (and by TiKv). I haven't tries either of these. Crossbeam has a SkipList
// implementation, that wasn't ready at the time I wrote this. Cuckoo
// hashing has the advantage of being easier to understand/implement. Maybe
// Cuckoo hashing can also be combined with Fibonacci hashing[^3]. Note that
// Robin Hood hashing maybe faster than Cuckoo hashing for reads, but it
// requires shifting around existing entries, which is rather costly to do
// atomically (and complex).

// [^1]: [https://en.wikipedia.org/wiki/Cuckoo_hashing]
// [^3]: [https://docs.rs/crossbeam-skiplist/0.1.1/crossbeam_skiplist/]
// [^3]: [https://probablydance.com/2018/06/16/fibonacci-hashing-
//  the-optimization-that-the-world-forgot-or-a-better-alternative-
//  to-integer-modulo/]

// Notes on memory leaks in Rotonda-store
//
// Both valgrind and miri report memory leaks on the multi-threaded prefix
// store. Valgrind only reports it when it a binary stops using the tree,
// while still keeping it around. An interrupted use of the mt-prefix-store
// does not report any memory leaks. Miri is persistent in reporting memory
// leaks in the mt-prefix-store. They both report the memory leaks in the same
// location: the init method of the node- and prefix-buckets.
//
// I have reasons to believe these reported memory leaks aren't real, or that
// crossbeam-epoch leaks a bit of memory when creating a new `Atomic`
// instance. Since neither prefix nor node buckets can or should be dropped
// this is not a big issue anyway, it just means that an `Atomic` occupies
// more memory than it could in an optimal situation. Since we're not storing
// the actual meta-data in an `Atomic` (it is stored in an `flurry Map`), this
// means memory usage won't grow on updating the meta-data on a prefix,
// (unless the meta-data itself grows of course, but that's up to the user).
//
// To get a better understanding on the nature of the reported memory leaks I
// have created a branch (`vec_set`) that replaces the dynamically sized array
// with a (equally sparse) Vec, that is not filled with `Atomic:::null()`, but
// with `Option<StoredPrefix` instead, in order to see if this would eliminate
// the memory leaks reporting. It did not. Valgrind still reports the memory
// leaks at the same location, although they're now reported as `indirectly
// leaked`, instead of `directly`. Miri is unchanged. The attentive reader may
// now suspect that the `Atomic` inside the `AtomicStoredPrefix` may be the
// culprit, but: a. Both miri and valgrind report the leaks still in the same
// place: the creation of the buckets, not the creation of the stored
//    prefixes. b. Tests that look at the memory usage of the prefix stores
// under heavy modification of existing prefixes do not exhibit memory leaking
//    behavior. c. Tests that look at memory usage under addition of new
//    prefixes exhibit linear incrementation of memory usage (which is
// expected). d. Tests that look at memory usage under contention do not
//    exhibit increased memory usage either.
//
// My strong suspicion is that both Miri and Valgrind report the sparse slots
// in the buckets as leaks, no matter whether they're `Atomic::null()`, or
// `None` values, probably as a result of the way `crossbeam-epoch` indexes
// into these, with pointer arithmetic (unsafe as hell).
//
// I would be super grateful if somebody would prove me wrong and can point to
// an actual memory leak in the mt-prefix-store (and even more if they can
// produce a fix for it).

// >= 0.4

// The above scheme is outdated! After done a few day of bench marking, it was
// found that storing the meta-data in `RwLock<HashMap>` structures actually
// performs better in both time, and space. Also the overall performance
// is  way more predictable and somewhat linearly related to the busyness of
// the whole system. Furthermore it was found that using RwLock around the
// HashMaps, instead of mutexes (from std) was around 2% slower at insert
// time, while we believe (we haven't tested this), that read performance will
// be superior to mutex. In terms of usability `RwLock<HasMap>` do not require
// the user to implement the RCU-style `MergeUpdate` trait (it is removed
// now).

// Adding the possibilty of storing more than one piece of meta-data for a
// prefix (through the use the MUI), made the RCU style storing very awkward:
// all the previous pieces of meta-data (let's call them records), collected
// in a HashMap, needed to copied out of the store, modified, and copied back
// in, while being able to fail, and retried. Locking these HashMaps is way
// more efficient, both in time (copying costs time), and memory (copying,
// costs, well, memory). So what we have now, is a hybrid tree, where the
// "core" consists of RCU-style, lock-free nodes (that can't be deleted!), and
// locked structures at the "edges" (not leaves, because all nodes can carry
// meta-data).

use crate::cht::{nodeset_size, prev_node_size, Cht, Value};
use crate::errors::{FatalError, FatalResult};
use crate::rib::STRIDE_SIZE;
use crate::stats::Counters;
use crate::types::{BitSpan, PrefixId};
#[cfg(test)]
use crate::IPv6;
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use log::{debug, error, log_enabled, trace};
use node_cht::{NodeCht, NodeSet, StoredNode};
use roaring::RoaringBitmap;
use tree_bitmap_node::NewNodeOrIndex;

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, Ordering};
use std::{fmt::Debug, marker::PhantomData};

use crate::types::AddressFamily;
use atomic_bitmap::{AtomicBitmap, AtomicPfxBitArr, AtomicPtrBitArr};

use crate::types::errors::PrefixStoreError;

#[cfg(feature = "cli")]
use ansi_term::Colour;

//--------------------- TreeBitMap ------------------------------------------

// The tree that holds the existence information for all prefixes for all
//strategies. This tree is also used to find all less- and more-specifics and
//iterate over them. It also holds a bitmap that contains RIB-wide withdrawn
//muis (peers in most cases).
#[derive(Debug)]
pub(crate) struct TreeBitMap<AF: AddressFamily, const ROOT_SIZE: usize> {
    // the chained hash table that backs the treebitmap
    node_cht: NodeCht<AF, ROOT_SIZE>,
    // the bitmap that holds RIB-wide withdrawn muis (e.g. peers)
    withdrawn_muis_bmin: Atomic<RoaringBitmap>,
    // number of prefixes in the store, etc.
    counters: Counters,
    // see the rant on update_default_route_prefix_meta
    default_route_exists: AtomicBool,
}

impl<AF: AddressFamily, const ROOT_SIZE: usize> TreeBitMap<AF, ROOT_SIZE> {
    pub(crate) fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let tree_bitmap = Self {
            node_cht: Cht::init(),
            withdrawn_muis_bmin: RoaringBitmap::new().into(),
            counters: Counters::default(),
            default_route_exists: AtomicBool::new(false),
        };

        let _retry_count = tree_bitmap
            .store_node(
                NodeId::dangerously_new_with_id_as_is(
                    <AF as FromZeros>::new_zeroed(),
                    0,
                ),
                0_u32,
                TreeBitMapNode {
                    ptrbitarr: AtomicPtrBitArr(AtomicU16::new(0)),
                    pfxbitarr: AtomicPfxBitArr(AtomicU32::new(0)),
                    _af: PhantomData,
                },
            )
            .map_err(|_| "Cannot create root for in memory tree")?;

        Ok(tree_bitmap)
    }

    // Sets the bit for the requested prefix to 1 in the corresponding
    // pfxbitarr in the tree.
    //
    // returns a Result over a tuple of (retry_count, existed), where
    // `retry_count` is the accumulated number of times all the atomic
    // operations involved had to be retried.
    pub(crate) fn set_prefix_exists(
        &self,
        pfx: PrefixId<AF>,
        mui: u32,
    ) -> Result<(u32, bool), PrefixStoreError> {
        if pfx.len() == 0 {
            let prefix_new =
                !self.default_route_exists.swap(true, Ordering::Acquire);
            return self
                .update_default_route_prefix_meta(mui)
                .map(|(rc, _mui_exists)| (rc, !prefix_new))
                .map_err(|_| PrefixStoreError::StoreNotReadyError);
        }

        let mut stride_end: u8 = 0;
        let mut cur_i = self.get_root_node_id();
        // let mut level: u8 = 0;
        let mut acc_retry_count = 0;

        let retry_and_exists = loop {
            stride_end += STRIDE_SIZE;
            let nibble_len = if pfx.len() < stride_end {
                STRIDE_SIZE + pfx.len() - stride_end
            } else {
                STRIDE_SIZE
            };
            let bit_span = AF::into_bit_span(
                pfx.bits(),
                stride_end - STRIDE_SIZE,
                nibble_len,
            );
            let is_last_stride = pfx.len() <= stride_end;
            let stride_start = stride_end - STRIDE_SIZE;

            let node_result = {
                let local_retry_count = 0;
                // retrieve_node_mut updates the bitmap index if
                // necessary.
                if let Some(current_node) = self.retrieve_node_mut(cur_i, mui)
                {
                    match current_node.eval_node_or_prefix_at(
                        bit_span,
                        // All the bits of the search prefix, but with
                        // a length set to the start of the current
                        // stride.
                        NodeId::dangerously_new_with_id_as_is(
                            pfx.bits(),
                            stride_start,
                        ),
                        is_last_stride,
                    ) {
                        (NewNodeOrIndex::NewNode(n), retry_count) => {
                            // Stride3 logs to stats[0], Stride4 logs
                            // to stats[1], etc.
                            // $self.stats[$stats_level].inc($level);

                            // get a new identifier for the node we're
                            // going to create.
                            let new_id = NodeId::new_with_cleaned_id(
                                pfx.bits(),
                                stride_start + bit_span.len,
                            );

                            // store the new node in the in_memory
                            // part of the RIB. It returns the created
                            // id and the number of retries before
                            // success.
                            match self.store_node(new_id, mui, n) {
                                Ok((node_id, s_retry_count)) => Ok((
                                    node_id,
                                    acc_retry_count
                                        + s_retry_count
                                        + retry_count,
                                )),
                                Err(err) => Err(err),
                            }
                        }
                        (
                            NewNodeOrIndex::ExistingNode(node_id),
                            retry_count,
                        ) => {
                            if log_enabled!(log::Level::Trace)
                                && local_retry_count > 0
                            {
                                trace!(
                                    "{} contention: Node already exists {}",
                                    std::thread::current()
                                        .name()
                                        .unwrap_or("unnamed-thread"),
                                    node_id
                                )
                            }
                            Ok((
                                node_id,
                                acc_retry_count
                                    + local_retry_count
                                    + retry_count,
                            ))
                        }
                        (NewNodeOrIndex::NewPrefix, retry_count) => {
                            break (
                                acc_retry_count
                                    + local_retry_count
                                    + retry_count,
                                false,
                            )
                        }
                        (NewNodeOrIndex::ExistingPrefix, retry_count) => {
                            break (
                                acc_retry_count
                                    + local_retry_count
                                    + retry_count,
                                true,
                            )
                        }
                    }
                } else {
                    return Err(PrefixStoreError::NodeCreationMaxRetryError);
                }
            };

            match node_result {
                Ok((next_id, retry_count)) => {
                    cur_i = next_id;
                    // level += 1;
                    acc_retry_count += retry_count;
                }
                Err(err) => {
                    if log_enabled!(log::Level::Error) {
                        error!(
                            "{} failing to store (intermediate) node {}.
Giving up this node. This shouldn't happen!",
                            std::thread::current()
                                .name()
                                .unwrap_or("unnamed-thread"),
                            cur_i,
                        );
                        error!(
                            "{} {}",
                            std::thread::current()
                                .name()
                                .unwrap_or("unnamed-thread"),
                            err
                        );
                    }
                }
            }
        };

        Ok(retry_and_exists)
    }

    pub fn prefix_exists(&self, prefix_id: PrefixId<AF>) -> bool {
        trace!("pe exists {:?}?", prefix_id);
        let (node_id, bs) = self.node_id_for_prefix(&prefix_id);

        match self.retrieve_node(node_id) {
            Some(n) => {
                let pfxbitarr = n.pfxbitarr.load();
                pfxbitarr & bs.into_bit_pos() > 0
            }
            None => false,
        }
    }

    pub fn prefix_exists_for_mui(
        &self,
        prefix_id: PrefixId<AF>,
        mui: u32,
    ) -> bool {
        trace!("pe exists {:?}?", prefix_id);
        let (node_id, bs) = self.node_id_for_prefix(&prefix_id);

        match self.retrieve_node_for_mui(node_id, mui) {
            Some(n) => {
                let pfxbitarr = n.pfxbitarr.load();
                pfxbitarr & bs.into_bit_pos() > 0
            }
            None => false,
        }
    }

    // Yes, we're hating this. But, the root node has no room for a serial of
    // the prefix 0/0 (the default route), which doesn't even matter, unless,
    // UNLESS, somebody wants to store a default route. So we have to store a
    // serial for this prefix. The normal place for a serial of any prefix is
    // on the pfxvec of its paren. But, hey, guess what, the
    // default-route-prefix lives *on* the root node, and, you know, the root
    // node doesn't have a parent. We can:
    // - Create a type RootTreeBitmapNode with a ptrbitarr with a size one
    //   bigger than a "normal" TreeBitMapNod for the first stride size. no we
    //   have to iterate over the root-node type in all matches on
    //   stride_size, just because we have exactly one instance of the
    //   RootTreeBitmapNode. So no.
    // - Make the `get_pfx_index` method on the implementations of the
    //   `Stride` trait check for a length of zero and branch if it is and
    //   return the serial of the root node. Now each and every call to this
    //   method will have to check a condition for exactly one instance of
    //   RootTreeBitmapNode. So again, no.
    // - The root node only gets used at the beginning of a search query or an
    //   insert. So if we provide two specialised methods that will now how to
    //   search for the default-route prefix and now how to set serial for
    //  that prefix and make sure we start searching/inserting with one of
    //   those specialized methods we're good to go.
    fn update_default_route_prefix_meta(
        &self,
        mui: u32,
    ) -> FatalResult<(u32, bool)> {
        trace!("Updating the default route...");

        if let Some(_root_node) =
            self.retrieve_node_mut(self.get_root_node_id(), mui)
        {
            self.node_cht
                .root_for_len(self.get_root_node_id().len())
                .update_rbm_index(mui)
        } else {
            Err(FatalError)
        }
    }

    pub(crate) fn withdrawn_muis_bmin<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> &'a RoaringBitmap {
        unsafe {
            self.withdrawn_muis_bmin
                .load(Ordering::Acquire, guard)
                .deref()
        }
    }

    pub fn mark_mui_as_active(
        &self,
        mui: u32,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        let current = self.withdrawn_muis_bmin.load(Ordering::Acquire, guard);

        let mut new = unsafe { current.as_ref() }
            .ok_or(PrefixStoreError::StoreNotReadyError)?
            .clone();

        new.remove(mui);
        self.update_withdrawn_muis_bmin(current, new, guard)
    }

    pub fn mark_mui_as_withdrawn(
        &self,
        mui: u32,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        let current = self.withdrawn_muis_bmin.load(Ordering::Acquire, guard);

        let mut new = unsafe { current.as_ref() }
            .ok_or(PrefixStoreError::StoreNotReadyError)?
            .clone();

        new.insert(mui);

        self.update_withdrawn_muis_bmin(current, new, guard)
    }

    pub(crate) fn update_withdrawn_muis_bmin<'a>(
        &self,
        current: Shared<'a, RoaringBitmap>,
        mut new: RoaringBitmap,
        guard: &'a Guard,
    ) -> Result<(), PrefixStoreError> {
        loop {
            match self.withdrawn_muis_bmin.compare_exchange(
                current,
                Owned::new(new),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(updated) => {
                    new = unsafe { updated.current.as_ref() }
                        .ok_or(PrefixStoreError::StoreNotReadyError)?
                        .clone();
                }
            }
        }
    }

    // Store a new node in the tree, or merge the existing node with this
    // node. This might fail disastrously, e.g. in case of failed I/O.
    fn store_node(
        &self,
        id: NodeId<AF>,
        multi_uniq_id: u32,
        new_node: TreeBitMapNode<AF>,
    ) -> FatalResult<(NodeId<AF>, u32)> {
        if log_enabled!(log::Level::Trace) {
            debug!(
                "{} store: Store node {}: {:?} mui {}",
                std::thread::current().name().unwrap_or("unnamed-thread"),
                id,
                new_node,
                multi_uniq_id
            );
        }
        self.counters.inc_nodes_count();

        let mut nodes = self.node_cht.root_for_len(id.len());
        let mut level = 0;
        let mut retry_count = 0;

        loop {
            // let this_level = bits_for_len(id.len(), level);

            trace!("{:032b}", id.len());
            trace!("id {:?}", id);
            trace!("multi_uniq_id {}", multi_uniq_id);

            // HASHING FUNCTION
            let index = Self::hash_node_id(id, level);

            match nodes.read().get(index) {
                None => {
                    // No node exists, so we create one here.
                    let next_level = nodeset_size(id.len(), level + 1);

                    if log_enabled!(log::Level::Trace) {
                        trace!(
                "Empty node found,creating new node {} len{} vl{}",
                            id,
                            id.len(),
                            level + 1
                        );
                        trace!("Next level {}", next_level);
                        trace!("Creating space for {} nodes", next_level);
                    }

                    trace!("multi uniq id {}", multi_uniq_id);
                    trace!("next level {}", next_level);

                    // A weird trick to create either a NodeSet with 16 nodes,
                    // or one without any (for the last stride)
                    let node_set = NodeSet::init_with_p2_children(
                        // next_level.saturating_sub(this_level) as usize,
                        next_level as usize,
                    );

                    let ptrbitarr = new_node.ptrbitarr.load();
                    let pfxbitarr = new_node.pfxbitarr.load();

                    let (stored_node, its_us) =
                        nodes.read().get_or_init(index, || StoredNode {
                            node_id: id,
                            node: new_node,
                            node_set,
                        });

                    if stored_node.node_id == id {
                        stored_node
                            .node_set
                            .update_rbm_index(multi_uniq_id)?;

                        // merge_with herre contains the critical section!
                        if !its_us && ptrbitarr != 0 {
                            retry_count += 1;
                            stored_node.node.ptrbitarr.merge_with(ptrbitarr);
                        }

                        if !its_us && pfxbitarr != 0 {
                            retry_count += 1;
                            stored_node.node.pfxbitarr.merge_with(pfxbitarr);
                        }
                    }

                    return Ok((id, retry_count));
                }
                Some(stored_node) => {
                    // A node exists, might be ours, might be
                    // another one.

                    if log_enabled!(log::Level::Trace) {
                        trace!(
                            "{} store: Node here exists {:?}",
                            std::thread::current()
                                .name()
                                .unwrap_or("unnamed-thread"),
                            stored_node.node_id
                        );
                        trace!("node_id {:?}", stored_node.node_id);
                        trace!("node_id {:032b}", stored_node.node_id.bits());
                        trace!("id {}", id);
                        trace!("     id {:032b}", id.bits());
                    }

                    // See if somebody beat us to creating our
                    // node already, if so, we still need to do
                    // work: we have to update the bitmap index
                    // with the multi_uniq_id we've got from the
                    // caller.
                    if id == stored_node.node_id {
                        stored_node
                            .node_set
                            .update_rbm_index(multi_uniq_id)?;

                        if new_node.ptrbitarr.load() != 0 {
                            stored_node
                                .node
                                .ptrbitarr
                                .merge_with(new_node.ptrbitarr.load());
                        }
                        if new_node.pfxbitarr.load() != 0 {
                            stored_node
                                .node
                                .pfxbitarr
                                .merge_with(new_node.pfxbitarr.load());
                        }

                        return Ok((id, retry_count));
                    } else {
                        // it's not "our" node, make a (recursive)
                        // call to create it.
                        level += 1;
                        trace!(
"Collision with node_id {}, move to next level: {} len{} next_lvl{} index {}",
                            stored_node.node_id,
                            id,
                            id.len(),
                            level,
                            index
                        );

                        match nodeset_size(id.len(), level) {
                            // on to the next level!
                            next_bit_shift if next_bit_shift > 0 => {
                                nodes = &stored_node.node_set;
                            }
                            // There's no next level anymore, we ran out of
                            // the maximum number of levels for this AF. This
                            // should happen under no circumstance, there's a
                            // serious logic error here somewhere.
                            _ => {
                                return Err(FatalError);
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn retrieve_node_mut(
        &self,
        id: NodeId<AF>,
        mui: u32,
    ) -> Option<&TreeBitMapNode<AF>> {
        // HASHING FUNCTION
        let mut level = 0;
        let mut node;
        let mut nodes = self.node_cht.root_for_len(id.len());

        loop {
            let index = Self::hash_node_id(id, level);
            match nodes.read().get(index) {
                // This arm only ever gets called in multi-threaded code
                // where our thread (running this code *now*), andgot
                // ahead of another thread: After the other thread created
                // the TreeBitMapNode first, it was overtaken by our
                // thread running this method, so our thread enounters an
                // empty node in the store.
                None => {
                    // let this_level = bits_for_len(id.len(), level);
                    let next_level = nodeset_size(id.len(), level + 1);
                    let node_set = NodeSet::init_with_p2_children(
                        next_level as usize, // next_level.saturating_sub(this_level) as usize,
                    );

                    // See if we can create the node
                    (node, _) =
                        nodes.read().get_or_init(index, || StoredNode {
                            node_id: id,
                            node: TreeBitMapNode::new(),
                            node_set,
                        });

                    // We may have lost, and a different node than we
                    // intended could live here, if so go a level deeper
                    if id == node.node_id {
                        // Nope, its ours or at least the node we need.
                        let _retry_count =
                            node.node_set.update_rbm_index(mui).ok();

                        return Some(&node.node);
                    };
                }
                Some(this_node) => {
                    node = this_node;
                    if id == this_node.node_id {
                        // YES, It's the one we're looking for!

                        // Update the rbm_index in this node with the
                        // multi_uniq_id that the caller specified. This
                        // is the only atomic operation we need to do
                        // here. The NodeSet that the index is attached
                        // to, does not need to be written to, it's part
                        // of a trie, so it just needs to "exist" (and it
                        // already does).
                        let retry_count =
                            this_node.node_set.update_rbm_index(mui).ok();

                        trace!("Retry_count rbm index {:?}", retry_count);
                        trace!(
                        "add multi uniq id to bitmap index {} for node {}",
                        mui,
                        this_node.node
                    );
                        return Some(&this_node.node);
                    };
                }
            }
            // It isn't ours. Move one level deeper.
            level += 1;
            match nodeset_size(id.len(), level) {
                // on to the next level!
                next_bit_shift if next_bit_shift > 0 => {
                    nodes = &node.node_set;
                }
                // There's no next level, we found nothing.
                _ => return None,
            }
        }
    }

    pub fn retrieve_node(
        &self,
        id: NodeId<AF>,
    ) -> Option<&TreeBitMapNode<AF>> {
        // HASHING FUNCTION
        let mut level = 0;
        let mut node;
        let mut nodes = self.node_cht.root_for_len(id.len());

        loop {
            let index = Self::hash_node_id(id, level);
            match nodes.read().get(index) {
                // This arm only ever gets called in multi-threaded code
                // where our thread (running this code *now*), andgot
                // ahead of another thread: After the other thread created
                // the TreeBitMapNode first, it was overtaken by our
                // thread running this method, so our thread enounters an
                // empty node in the store.
                None => {
                    return None;
                }
                Some(this_node) => {
                    node = this_node;
                    if id == this_node.node_id {
                        // YES, It's the one we're looking for!
                        return Some(&this_node.node);
                    };
                }
            }
            // It isn't ours. Move one level deeper.
            level += 1;
            match nodeset_size(id.len(), level) {
                // on to the next level!
                next_bit_shift if next_bit_shift > 0 => {
                    nodes = &node.node_set;
                }
                // There's no next level, we found nothing.
                _ => return None,
            }
        }
    }

    pub(crate) fn retrieve_node_for_mui(
        &self,
        id: NodeId<AF>,
        mui: u32,
    ) -> Option<&TreeBitMapNode<AF>> {
        // HASHING FUNCTION
        let mut level = 0;
        let mut node;
        let mut nodes = self.node_cht.root_for_len(id.len());

        loop {
            let index = Self::hash_node_id(id, level);
            match nodes.read().get(index) {
                // This arm only ever gets called in multi-threaded code
                // where our thread (running this code *now*), andgot
                // ahead of another thread: After the other thread created
                // the TreeBitMapNode first, it was overtaken by our
                // thread running this method, so our thread enounters an
                // empty node in the store.
                None => {
                    return None;
                }
                Some(this_node) => {
                    // early return if the mui is not in the index
                    // stored in this node, meaning the mui does not
                    // appear anywhere in the sub-tree formed from
                    // this node.
                    node = this_node;

                    let bmin = match this_node.node_set.rbm().read() {
                        Ok(bmin) => bmin,
                        // if this lock is poisened, we are still going to
                        // work with the bmin. The data in the bmin may be
                        // stale, because of the lock poisoning, but this may
                        // also happen because of delays in other parts of the
                        // store in normal circumstances. We are counting on a
                        // future call to a write method to actually propagate
                        // a FatalError to the user of the store.
                        Err(bmin) => bmin.into_inner(),
                    };
                    if !bmin.contains(mui) {
                        return None;
                    }

                    if id == this_node.node_id {
                        // YES, It's the one we're looking for!
                        return Some(&this_node.node);
                    };
                }
            }
            // It isn't ours. Move one level deeper.
            level += 1;
            match nodeset_size(id.len(), level) {
                // on to the next level!
                next_bit_shift if next_bit_shift > 0 => {
                    nodes = &node.node_set;
                }
                // There's no next level, we found nothing.
                _ => return None,
            }
        }
    }

    pub(crate) fn get_root_node_id(&self) -> NodeId<AF> {
        NodeId::dangerously_new_with_id_as_is(
            <AF as FromZeros>::new_zeroed(),
            0,
        )
    }

    pub fn nodes_count(&self) -> usize {
        self.counters.nodes_count()
    }

    #[cfg(feature = "cli")]
    pub fn prefixes_count(&self) -> usize {
        self.counters.prefixes_count().iter().sum()
    }

    // len checking does it all
    #[allow(clippy::indexing_slicing)]
    pub fn prefixes_count_for_len(
        &self,
        len: u8,
    ) -> Result<usize, PrefixStoreError> {
        if len <= AF::BITS {
            Ok(self.counters.prefixes_count()[len as usize])
        } else {
            Err(PrefixStoreError::PrefixLengthInvalid)
        }
    }

    // Calculates the id of the node that COULD host a prefix in its
    // ptrbitarr.
    pub(crate) fn node_id_for_prefix(
        &self,
        prefix: &PrefixId<AF>,
    ) -> (NodeId<AF>, BitSpan) {
        trace!(
            "prefix id bits: {:032b} len: {}",
            prefix.bits(),
            prefix.len()
        );
        let mut acc = 0;
        loop {
            acc += STRIDE_SIZE;
            if acc >= prefix.len() {
                let node_len = acc - STRIDE_SIZE;
                return (
                    NodeId::new_with_cleaned_id(prefix.bits(), node_len),
                    // NOT THE HASHING FUNCTION!
                    // Do the right shift in a checked manner, for the sake
                    // of 0/0. A search for 0/0 will perform a 0 << MAX_LEN,
                    // which will panic in debug mode (undefined behaviour
                    // in prod).
                    BitSpan::new(
                        ((prefix.bits() << AF::from_u8(node_len))
                            .checked_shr_or_zero(
                                (AF::BITS - (prefix.len() - node_len)).into(),
                            ))
                        .dangerously_truncate_to_u32(),
                        prefix.len() - node_len,
                    ),
                );
            }
        }
    }

    // ------- THE HASHING FUNCTION -----------------------------------------

    // Ok, so hashing is really hard, but we're keeping it simple, and
    // because we're keeping it simple we're having lots of collisions, but
    // we don't care!
    //
    // We're using a part of bitarray representation of the address part of
    // a prefix the as the hash. Sounds complicated, but isn't.
    // Suppose we have an IPv4 prefix, say 130.24.55.0/24.
    // The address part is 130.24.55.0 or as a bitarray that would be:
    //
    // pos  0    4    8    12   16   20   24   28
    // bit  1000 0010 0001 1000 0011 0111 0000 0000
    //
    // First, we're discarding the bits after the length of the prefix, so
    // we'll have:
    //
    // pos  0    4    8    12   16   20
    // bit  1000 0010 0001 1000 0011 0111
    //
    // Now we're dividing this bitarray into one or more levels. A level can
    // be an arbitrary number of bits between 1 and the length of the prefix,
    // but the number of bits summed over all levels should be exactly the
    // prefix length. So in our case they should add up to 24. A possible
    // division could be: 4, 4, 4, 4, 4, 4. Another one would be: 12, 12. The
    // actual division being used is described in the function
    // `<NB>::get_bits_for_len` in the `rotonda-macros` crate. Each level has
    // its own hash, so for our example prefix this would be:
    //
    // pos   0    4    8    12   16   20
    // level 0              1
    // hash  1000 0010 0001 1000 0011 0111
    //
    // level 1 hash: 1000 0010 0001
    // level 2 hash: 1000 0011 0011
    //
    // The hash is now converted to a usize integer, by shifting it all the
    // way to the right in a u32 and then converting to a usize. Why a usize
    // you ask? Because the hash is used by the CustomAllocStorage as the
    // index to the array for that specific prefix length and level.
    // So for our example this means that the hash on level 1 is now 0x821
    // (decimal 2081) and the hash on level 2 is 0x833 (decimal 2099).
    // Now, if we only consider the hash on level 1 and that we're going to
    // use that as the index to the array that stores all prefixes, you'll
    // notice very quickly that all prefixes starting with 130.[16..31] will
    // cause a collision: they'll all point to the same array element. These
    // collisions are resolved by creating a linked list from each array
    // element, where each element in the list has an array of its own that
    // uses the hash function with the level incremented.

    pub(crate) fn hash_node_id(id: NodeId<AF>, level: u8) -> usize {
        let last_level = prev_node_size(id.len(), level);
        let size = nodeset_size(id.len(), level);

        // shifting left and right here should never overflow for inputs
        // (NodeId, level) that are valid for IPv4 and IPv6. In release
        // compiles this may NOT be noticable, because the undefined behaviour
        // is most probably the desired behaviour (saturating). But it's UB
        // for a reason, so we should not rely on it, and verify that we are
        // not hitting that behaviour.
        debug_assert!(id.bits().checked_shl(last_level as u32).is_some());
        debug_assert!((id.bits() << AF::from_u8(last_level))
            .checked_shr(u32::from((<AF>::BITS - size) % <AF>::BITS))
            .is_some());

        ((id.bits() << AF::from_u8(last_level))
            >> AF::from_u8((<AF>::BITS - size) % <AF>::BITS))
        .dangerously_truncate_to_u32() as usize
    }

    #[cfg(test)]
    fn test_valid_range() {
        for len in 0..128 {
            for lvl in 0..(len / 4) {
                let n_id = NodeId::<AF>::from((AF::from_u32(0), len));
                Self::hash_node_id(n_id, lvl);
            }
        }
    }
}

#[test]
fn test_hashing_node_id_valid_range() {
    TreeBitMap::<IPv6, 129>::test_valid_range()
}
// Partition for stride 4
//
// ptr bits never happen in the first half of the bitmap for the stride-size. Consequently the ptrbitarr can be an integer type
// half the size of the pfxbitarr.
//
// ptr bit arr (u16)                                                        0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15    x
// pfx bit arr (u32)   0 1 2  3  4  5  6   7   8   9  10  11  12  13  14   15   16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
// nibble              * 0 1 00 01 10 11 000 001 010 011 100 101 110 111 0000 0001 0010 0011 0100 0101 0110 0111 1000 1001 1010 1011 1100 1101 1110 1111    x
// nibble len offset   0 1    2            3                                4
//
// stride 3: 1 + 2 + 4 + 8                              =  15 bits. 2^4 - 1 (1 << 4) - 1. ptrbitarr starts at pos  7 (1 << 3) - 1
// stride 4: 1 + 2 + 4 + 8 + 16                         =  31 bits. 2^5 - 1 (1 << 5) - 1. ptrbitarr starts at pos 15 (1 << 4) - 1
// stride 5: 1 + 2 + 4 + 8 + 16 + 32 + 64               =  63 bits. 2^6 - 1
// stride 6: 1 + 2 + 4 + 8 + 16 + 32 + 64               = 127 bits. 2^7 - 1
// stride 7: 1 + 2 + 4 + 8 + 16 + 32 + 64 = 128         = 256 bits. 2^8 - 1126
// stride 8: 1 + 2 + 4 + 8 + 16 + 32 + 64 + 128 + 256   = 511 bits. 2^9 - 1
//
// Ex.:
// pfx            65.0.0.252/30                                             0100_0001_0000_0000_0000_0000_1111_1100
//
// nibble 1       (pfx << 0) >> 28                                          0000_0000_0000_0000_0000_0000_0000_0100
// bit_pos        (1 << nibble length) - 1 + nibble                         0000_0000_0000_0000_0000_1000_0000_0000
//
// nibble 2       (pfx << 4) >> 24                                          0000_0000_0000_0000_0000_0000_0000_0001
// bit_pos        (1 << nibble length) - 1 + nibble                         0000_0000_0000_0000_1000_0000_0000_0000
// ...
// nibble 8       (pfx << 28) >> 0                                          0000_0000_0000_0000_0000_0000_0000_1100
// bit_pos        (1 << nibble length) - 1 + nibble = (1 << 2) - 1 + 2 = 5  0000_0010_0000_0000_0000_0000_0000_0000
// 5 - 5 - 5 - 4 - 4 - [4] - 5
// startpos (2 ^ nibble length) - 1 + nibble as usize

// This implements the funky stats for a tree
#[cfg(feature = "cli")]
impl<AF: AddressFamily, const ROOT_SIZE: usize> std::fmt::Display
    for TreeBitMap<AF, ROOT_SIZE>
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(_f, "{} prefixes created", self.prefixes_count())?;
        writeln!(_f, "{} nodes created", self.nodes_count())?;
        writeln!(_f)?;

        writeln!(
            _f,
            "level\t[{}] prefixes-occupied/max-prefixes percentage_occupied",
            Colour::Green.paint("prefixes")
        )?;

        let bars = ["▏", "▎", "▍", "▌", "▋", "▊", "▉"];
        const SCALE: u32 = 5500;
        for crate::stats::CreatedNodes {
            depth_level: len,
            count: prefix_count,
        } in self.counters.prefix_stats()
        {
            let max_pfx = u128::overflowing_pow(2, len as u32);
            let n = (prefix_count as u32 / SCALE) as usize;

            write!(_f, "/{}\t", len)?;

            for _ in 0..n {
                write!(_f, "{}", Colour::Green.paint("█"))?;
            }

            write!(
                _f,
                "{}",
                Colour::Green.paint(
                    *bars
                        .get(
                            ((prefix_count as u32 % SCALE) / (SCALE / 7))
                                as usize
                        )
                        .unwrap_or(&"NaN")
                ) //  = scale / 7
            )?;

            write!(
                _f,
                " {}/{} {:.2}%",
                prefix_count,
                max_pfx.0,
                (prefix_count as f64 / max_pfx.0 as f64) * 100.0
            )?;

            writeln!(_f)?;
        }

        Ok(())
    }
}
