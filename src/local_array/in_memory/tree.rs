// ----------- THE STORE ----------------------------------------------------
//
// The CustomAllocStore provides in-memory storage for the BitTreeMapNodes
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
// Currently, the meta-data is an atomically stored value, that is required to
// implement the `Meta` and the `Clone` trait. New meta-data
// instances are stored atomically without further ado, but updates to a
// piece of meta-data are done by merging the previous meta-data with the new
// meta-data, through use of the `MergeUpdate` trait.
//
// The `upsert_prefix` methods retrieve only the most recent insert
// for a prefix (for now).
//
// Future work could have a user-configurable retention strategy that allows
// the meta-data to be stored as a linked-list of references, where each
// meta-data object has a reference to its predecessor.
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

use crate::local_array::bit_span::BitSpan;
use crate::local_array::in_memory::atomic_types::StoredNode;
use crate::prefix_record::Meta;
use crate::prelude::multi::{NodeSet, PrefixId};
use crossbeam_epoch::Atomic;
use crossbeam_utils::Backoff;
use log::{debug, log_enabled, trace};
use roaring::RoaringBitmap;

use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicU8};
use std::{fmt::Debug, marker::PhantomData};

use super::atomic_types::{
    NodeBuckets, PrefixBuckets, PrefixSet, StoredPrefix,
};
use crate::af::AddressFamily;
use crate::rib::Counters;
use crate::{
    impl_search_level, impl_search_level_for_mui, retrieve_node_mut_closure,
    store_node_closure,
};

use super::super::errors::PrefixStoreError;
pub(crate) use super::atomic_stride::*;

use super::node::TreeBitMapNode;

#[cfg(feature = "cli")]
use ansi_term::Colour;

//------------------- Sized Node Enums ------------------------------------

// No, no, NO, NO, no, no! We're not going to Box this, because that's slow!
// This enum is never used to store nodes/prefixes, it's only to be used in
// generic code.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum SizedStrideNode<AF: AddressFamily> {
    Stride3(TreeBitMapNode<AF, Stride3>),
    Stride4(TreeBitMapNode<AF, Stride4>),
    Stride5(TreeBitMapNode<AF, Stride5>),
}

impl<AF, S> Default for TreeBitMapNode<AF, S>
where
    AF: AddressFamily,
    S: Stride,
{
    fn default() -> Self {
        Self {
            ptrbitarr: <<S as Stride>::AtomicPtrSize as AtomicBitmap>::new(),
            pfxbitarr: <<S as Stride>::AtomicPfxSize as AtomicBitmap>::new(),
            _af: PhantomData,
        }
    }
}

impl<AF> Default for SizedStrideNode<AF>
where
    AF: AddressFamily,
{
    fn default() -> Self {
        SizedStrideNode::Stride3(TreeBitMapNode {
            ptrbitarr: AtomicStride2(AtomicU8::new(0)),
            pfxbitarr: AtomicStride3(AtomicU16::new(0)),
            _af: PhantomData,
        })
    }
}

// Used to create a public iterator over all nodes.
#[derive(Debug)]
pub enum SizedStrideRef<'a, AF: AddressFamily> {
    Stride3(&'a TreeBitMapNode<AF, Stride3>),
    Stride4(&'a TreeBitMapNode<AF, Stride4>),
    Stride5(&'a TreeBitMapNode<AF, Stride5>),
}

pub(crate) enum NewNodeOrIndex<AF: AddressFamily> {
    NewNode(SizedStrideNode<AF>),
    ExistingNode(StrideNodeId<AF>),
    NewPrefix,
    ExistingPrefix,
}

//--------------------- Per-Stride-Node-Id Type -----------------------------

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StrideNodeId<AF: AddressFamily>(Option<(AF, u8)>);

impl<AF: AddressFamily> StrideNodeId<AF> {
    pub fn empty() -> Self {
        Self(None)
    }

    pub fn dangerously_new_with_id_as_is(addr_bits: AF, len: u8) -> Self {
        Self(Some((addr_bits, len)))
    }

    #[inline]
    pub fn new_with_cleaned_id(addr_bits: AF, len: u8) -> Self {
        Self(Some((addr_bits.truncate_to_len(len), len)))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    pub fn get_id(&self) -> (AF, u8) {
        self.0.unwrap()
    }
    pub fn get_len(&self) -> u8 {
        self.0.unwrap().1
    }
    pub fn set_len(mut self, len: u8) -> Self {
        self.0.as_mut().unwrap().1 = len;
        self
    }

    pub fn add_to_len(mut self, len: u8) -> Self {
        self.0.as_mut().unwrap().1 += len;
        self
    }

    #[inline]
    pub fn truncate_to_len(self) -> Self {
        let (addr_bits, len) = self.0.unwrap();
        StrideNodeId::new_with_cleaned_id(addr_bits, len)
    }

    // clean out all bits that are set beyond the len. This function should
    // be used before doing any ORing to add a nibble.
    #[inline]
    pub fn unwrap_with_cleaned_id(&self) -> (AF, u8) {
        let (addr_bits, len) = self.0.unwrap();
        (addr_bits.truncate_to_len(len), len)
    }

    pub fn add_nibble(&self, nibble: u32, nibble_len: u8) -> Self {
        let (addr_bits, len) = self.unwrap_with_cleaned_id();
        let res = addr_bits.add_nibble(len, nibble, nibble_len);
        Self(Some(res))
    }

    pub fn into_inner(self) -> Option<(AF, u8)> {
        self.0
    }
}

impl<AF: AddressFamily> std::fmt::Display for StrideNodeId<AF> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .map(|x| format!("{}-{}", x.0, x.1))
                .unwrap_or_else(|| "-".to_string())
        )
    }
}

impl<AF: AddressFamily> std::convert::From<StrideNodeId<AF>>
    for PrefixId<AF>
{
    fn from(id: StrideNodeId<AF>) -> Self {
        let (addr_bits, len) = id.0.unwrap();
        PrefixId::new(addr_bits, len)
    }
}

//------------------------- Node Collections --------------------------------

// #[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Copy, Clone)]
// pub enum StrideType {
//     Stride3,
//     Stride4,
//     Stride5,
// }

// impl From<u8> for StrideType {
//     fn from(level: u8) -> Self {
//         match level {
//             3 => StrideType::Stride3,
//             4 => StrideType::Stride4,
//             5 => StrideType::Stride5,
//             _ => panic!("Invalid stride level {}", level),
//         }
//     }
// }

// impl std::fmt::Display for StrideType {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             StrideType::Stride3 => write!(f, "S3"),
//             StrideType::Stride4 => write!(f, "S4"),
//             StrideType::Stride5 => write!(f, "S5"),
//         }
//     }
// }

//--------------------- TreeBitMap ------------------------------------------

#[derive(Debug)]
pub struct TreeBitMap<
    AF: AddressFamily,
    M: Meta,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, M>,
> {
    pub(crate) node_buckets: NB,
    pub(crate) prefix_buckets: PB,
    counters: Counters,
    _af: PhantomData<AF>,
    _m: PhantomData<M>,
}

impl<
        AF: AddressFamily,
        M: Meta,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > TreeBitMap<AF, M, NB, PB>
{
    pub(crate) fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let tree_bitmap = Self {
            node_buckets: NodeBuckets::init(),
            prefix_buckets: PB::init(),
            counters: Counters::default(),
            _af: PhantomData,
            _m: PhantomData,
        };

        let root_node = match Self::get_first_stride_size() {
            3 => SizedStrideNode::Stride3(TreeBitMapNode {
                ptrbitarr: AtomicStride2(AtomicU8::new(0)),
                pfxbitarr: AtomicStride3(AtomicU16::new(0)),
                _af: PhantomData,
            }),
            4 => SizedStrideNode::Stride4(TreeBitMapNode {
                ptrbitarr: AtomicStride3(AtomicU16::new(0)),
                pfxbitarr: AtomicStride4(AtomicU32::new(0)),
                _af: PhantomData,
            }),
            5 => SizedStrideNode::Stride5(TreeBitMapNode {
                ptrbitarr: AtomicStride4(AtomicU32::new(0)),
                pfxbitarr: AtomicStride5(AtomicU64::new(0)),
                _af: PhantomData,
            }),
            unknown_stride_size => {
                panic!(
                    "unknown stride size {} encountered in STRIDES array",
                    unknown_stride_size
                );
            }
        };

        let _retry_count = tree_bitmap.store_node(
            StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0),
            0_u32,
            root_node,
        )?;

        Ok(tree_bitmap)
    }

    // Create a new node in the store with payload `next_node`.
    //
    // Next node will be ignored if a node with the same `id` already exists,
    // but the multi_uniq_id will be added to the rbm_index of the NodeSet.
    //
    // Returns: a tuple with the node_id of the created node and the number of
    // retry_count
    #[allow(clippy::type_complexity)]
    pub(crate) fn store_node(
        &self,
        id: StrideNodeId<AF>,
        multi_uniq_id: u32,
        next_node: SizedStrideNode<AF>,
    ) -> Result<(StrideNodeId<AF>, u32), PrefixStoreError> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                TreeBitMapNode<AF, S>,
                u32, // multi_uniq_id
                u8,  // the store level
                u32, // retry_count
            ) -> Result<
                (StrideNodeId<AF>, u32),
                PrefixStoreError,
            >,
        }

        let search_level_3 =
            store_node_closure![Stride3; id; guard; back_off;];
        let search_level_4 =
            store_node_closure![Stride4; id; guard; back_off;];
        let search_level_5 =
            store_node_closure![Stride5; id; guard; back_off;];

        if log_enabled!(log::Level::Trace) {
            debug!(
                "{} store: Store node {}: {:?} mui {}",
                std::thread::current().name().unwrap_or("unnamed-thread"),
                id,
                next_node,
                multi_uniq_id
            );
        }
        self.counters.inc_nodes_count();

        match next_node {
            SizedStrideNode::Stride3(new_node) => (search_level_3.f)(
                &search_level_3,
                self.node_buckets.get_store3(id),
                new_node,
                multi_uniq_id,
                0,
                0,
            ),
            SizedStrideNode::Stride4(new_node) => (search_level_4.f)(
                &search_level_4,
                self.node_buckets.get_store4(id),
                new_node,
                multi_uniq_id,
                0,
                0,
            ),
            SizedStrideNode::Stride5(new_node) => (search_level_5.f)(
                &search_level_5,
                self.node_buckets.get_store5(id),
                new_node,
                multi_uniq_id,
                0,
                0,
            ),
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_node_mut(
        &self,
        id: StrideNodeId<AF>,
        multi_uniq_id: u32,
    ) -> Option<SizedStrideRef<'_, AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, S>,
                &'a NodeSet<AF, S>,
                u8,
            )
                -> Option<SizedStrideRef<'a, AF>>,
        }

        let search_level_3 =
            retrieve_node_mut_closure![Stride3; id; multi_uniq_id;];
        let search_level_4 =
            retrieve_node_mut_closure![Stride4; id; multi_uniq_id;];
        let search_level_5 =
            retrieve_node_mut_closure![Stride5; id; multi_uniq_id;];

        if log_enabled!(log::Level::Trace) {
            trace!(
                "{} store: Retrieve node mut {} from l{}",
                std::thread::current().name().unwrap_or("unnamed-thread"),
                id,
                id.get_id().1
            );
        }

        match self.node_buckets.get_stride_for_id(id) {
            3 => (search_level_3.f)(
                &search_level_3,
                self.node_buckets.get_store3(id),
                0,
            ),

            4 => (search_level_4.f)(
                &search_level_4,
                self.node_buckets.get_store4(id),
                0,
            ),
            _ => (search_level_5.f)(
                &search_level_5,
                self.node_buckets.get_store5(id),
                0,
            ),
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_node(
        &self,
        id: StrideNodeId<AF>,
    ) -> Option<SizedStrideRef<'_, AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, S>,
                &'a NodeSet<AF, S>,
                u8,
            )
                -> Option<SizedStrideRef<'a, AF>>,
        }

        let search_level_3 = impl_search_level![Stride3; id;];
        let search_level_4 = impl_search_level![Stride4; id;];
        let search_level_5 = impl_search_level![Stride5; id;];

        if log_enabled!(log::Level::Trace) {
            trace!(
                "{} store: Retrieve node {} from l{}",
                std::thread::current().name().unwrap_or("unnamed-thread"),
                id,
                id.get_id().1
            );
        }

        match self.get_stride_for_id(id) {
            3 => (search_level_3.f)(
                &search_level_3,
                self.node_buckets.get_store3(id),
                0,
            ),
            4 => (search_level_4.f)(
                &search_level_4,
                self.node_buckets.get_store4(id),
                0,
            ),
            _ => (search_level_5.f)(
                &search_level_5,
                self.node_buckets.get_store5(id),
                0,
            ),
        }
    }

    // retrieve a node, but only its bitmap index contains the specified mui.
    // Used for iterators per mui.
    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_node_for_mui(
        &self,
        id: StrideNodeId<AF>,
        // The mui that is tested to be present in the nodes bitmap index
        mui: u32,
    ) -> Option<SizedStrideRef<'_, AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, S>,
                &'a NodeSet<AF, S>,
                u8,
            )
                -> Option<SizedStrideRef<'a, AF>>,
        }

        let search_level_3 = impl_search_level_for_mui![Stride3; id; mui;];
        let search_level_4 = impl_search_level_for_mui![Stride4; id; mui;];
        let search_level_5 = impl_search_level_for_mui![Stride5; id; mui;];

        if log_enabled!(log::Level::Trace) {
            trace!(
                "{} store: Retrieve node {} from l{} for mui {}",
                std::thread::current().name().unwrap_or("unnamed-thread"),
                id,
                id.get_id().1,
                mui
            );
        }

        match self.get_stride_for_id(id) {
            3 => (search_level_3.f)(
                &search_level_3,
                self.node_buckets.get_store3(id),
                0,
            ),
            4 => (search_level_4.f)(
                &search_level_4,
                self.node_buckets.get_store4(id),
                0,
            ),
            _ => (search_level_5.f)(
                &search_level_5,
                self.node_buckets.get_store5(id),
                0,
            ),
        }
    }

    pub(crate) fn get_stride_for_id(&self, id: StrideNodeId<AF>) -> u8 {
        self.node_buckets.get_stride_for_id(id)
    }

    pub(crate) fn get_root_node_id(&self) -> StrideNodeId<AF> {
        StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0)
    }

    pub fn get_nodes_count(&self) -> usize {
        self.counters.get_nodes_count()
    }

    // This function is used by the upsert_prefix function above.
    //
    // We're using a Chained Hash Table and this function returns one of:
    // - a StoredPrefix that already exists for this search_prefix_id
    // - the Last StoredPrefix in the chain.
    // - an error, if no StoredPrefix whatsoever can be found in the store.
    //
    // The error condition really shouldn't happen, because that basically
    // means the root node for that particular prefix length doesn't exist.
    #[allow(clippy::type_complexity)]
    pub(crate) fn non_recursive_retrieve_prefix_mut(
        &self,
        search_prefix_id: PrefixId<AF>,
    ) -> (&StoredPrefix<AF, M>, bool) {
        trace!("non_recursive_retrieve_prefix_mut_with_guard");
        let mut prefix_set = self
            .prefix_buckets
            .get_root_prefix_set(search_prefix_id.get_len());
        let mut level: u8 = 0;

        trace!("root prefix_set {:?}", prefix_set);
        loop {
            // HASHING FUNCTION
            let index = TreeBitMap::<AF, M, NB, PB>::hash_prefix_id(
                search_prefix_id,
                level,
            );

            // probe the slot with the index that's the result of the hashing.
            // let locked_prefix = prefix_set.0.get(index);
            let stored_prefix = match prefix_set.0.get(index) {
                Some(p) => {
                    trace!("prefix set found.");
                    (p, true)
                }
                None => {
                    // We're at the end of the chain and haven't found our
                    // search_prefix_id anywhere. Return the end-of-the-chain
                    // StoredPrefix, so the caller can attach a new one.
                    trace!(
                        "no record. returning last found record in level
                        {}, with index {}.",
                        level,
                        index
                    );
                    let index = TreeBitMap::<AF, M, NB, PB>::hash_prefix_id(
                        search_prefix_id,
                        level,
                    );
                    trace!("calculate next index {}", index);
                    let var_name = (
                        prefix_set
                            .0
                            .get_or_init(index, || {
                                StoredPrefix::new::<PB>(
                                    PrefixId::new(
                                        search_prefix_id.get_net(),
                                        search_prefix_id.get_len(),
                                    ),
                                    level,
                                )
                            })
                            .0,
                        false,
                    );
                    var_name
                }
            };

            if search_prefix_id == stored_prefix.0.prefix {
                // GOTCHA!
                // Our search-prefix is stored here, so we're returning
                // it, so its PrefixRecord can be updated by the caller.
                trace!("found requested prefix {:?}", search_prefix_id);
                return stored_prefix;
            } else {
                // A Collision. Follow the chain.
                level += 1;
                prefix_set = &stored_prefix.0.next_bucket;
                continue;
            }
        }
    }

    // This function is used by the match_prefix, and [more|less]_specifics
    // public methods on the TreeBitMap (indirectly).
    #[allow(clippy::type_complexity)]
    pub fn non_recursive_retrieve_prefix(
        &self,
        id: PrefixId<AF>,
    ) -> (
        Option<&StoredPrefix<AF, M>>,
        Option<(
            PrefixId<AF>,
            u8,
            &PrefixSet<AF, M>,
            [Option<(&PrefixSet<AF, M>, usize)>; 32],
            usize,
        )>,
    ) {
        let mut prefix_set =
            self.prefix_buckets.get_root_prefix_set(id.get_len());
        let mut parents = [None; 32];
        let mut level: u8 = 0;
        let backoff = Backoff::new();

        loop {
            // The index of the prefix in this array (at this len and
            // level) is calculated by performing the hash function
            // over the prefix.

            // HASHING FUNCTION
            let index =
                TreeBitMap::<AF, M, NB, PB>::hash_prefix_id(id, level);

            if let Some(stored_prefix) = prefix_set.0.get(index) {
                if id == stored_prefix.get_prefix_id() {
                    trace!("found requested prefix {:?}", id);
                    parents[level as usize] = Some((prefix_set, index));
                    return (
                        Some(stored_prefix),
                        Some((id, level, prefix_set, parents, index)),
                    );
                };

                // Advance to the next level.
                prefix_set = &stored_prefix.next_bucket;
                level += 1;
                backoff.spin();
                continue;
            }

            trace!("no prefix found for {:?}", id);
            parents[level as usize] = Some((prefix_set, index));
            return (None, Some((id, level, prefix_set, parents, index)));
        }
    }

    #[allow(dead_code)]
    fn remove_prefix(&mut self, index: PrefixId<AF>) -> Option<M> {
        match index.is_empty() {
            false => self.prefix_buckets.remove(index),
            true => None,
        }
    }

    pub fn get_prefixes_count(&self) -> usize {
        self.counters.get_prefixes_count().iter().sum()
    }

    pub fn get_prefixes_count_for_len(&self, len: u8) -> usize {
        self.counters.get_prefixes_count()[len as usize]
    }

    // Stride related methods

    pub fn get_stride_sizes(&self) -> &[u8] {
        self.node_buckets.get_stride_sizes()
    }

    pub(crate) fn get_first_stride_size() -> u8 {
        NB::get_first_stride_size()
    }

    // Calculates the id of the node that COULD host a prefix in its
    // ptrbitarr.
    pub(crate) fn get_node_id_for_prefix(
        &self,
        prefix: &PrefixId<AF>,
    ) -> (StrideNodeId<AF>, BitSpan) {
        let mut acc = 0;
        for i in self.get_stride_sizes() {
            acc += *i;
            if acc >= prefix.get_len() {
                let node_len = acc - i;
                return (
                    StrideNodeId::new_with_cleaned_id(
                        prefix.get_net(),
                        node_len,
                    ),
                    // NOT THE HASHING FUNCTION!
                    // Do the right shift in a checked manner, for the sake
                    // of 0/0. A search for 0/0 will perform a 0 << MAX_LEN,
                    // which will panic in debug mode (undefined behaviour
                    // in prod).
                    BitSpan::new(
                        ((prefix.get_net() << node_len).checked_shr_or_zero(
                            (AF::BITS - (prefix.get_len() - node_len)).into(),
                        ))
                        .dangerously_truncate_to_u32(),
                        prefix.get_len() - node_len,
                    ),
                );
            }
        }
        panic!("prefix length for {:?} is too long", prefix);
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

    pub(crate) fn hash_node_id(id: StrideNodeId<AF>, level: u8) -> usize {
        // And, this is all of our hashing function.
        let last_level = if level > 0 {
            <NB>::len_to_store_bits(id.get_id().1, level - 1)
        } else {
            0
        };
        let this_level = <NB>::len_to_store_bits(id.get_id().1, level);
        trace!("bits division {}", this_level);
        trace!(
            "calculated index ({} << {}) >> {}",
            id.get_id().0,
            last_level,
            ((<AF>::BITS - (this_level - last_level)) % <AF>::BITS) as usize
        );
        // HASHING FUNCTION
        ((id.get_id().0 << last_level)
            >> ((<AF>::BITS - (this_level - last_level)) % <AF>::BITS))
            .dangerously_truncate_to_u32() as usize
    }

    pub(crate) fn hash_prefix_id(id: PrefixId<AF>, level: u8) -> usize {
        // And, this is all of our hashing function.
        let last_level = if level > 0 {
            <PB>::get_bits_for_len(id.get_len(), level - 1)
        } else {
            0
        };
        let this_level = <PB>::get_bits_for_len(id.get_len(), level);
        trace!(
            "bits division {}; no of bits {}",
            this_level,
            this_level - last_level
        );
        trace!(
            "calculated index ({} << {}) >> {}",
            id.get_net(),
            last_level,
            ((<AF>::BITS - (this_level - last_level)) % <AF>::BITS) as usize
        );
        // HASHING FUNCTION
        ((id.get_net() << last_level)
            >> ((<AF>::BITS - (this_level - last_level)) % <AF>::BITS))
            .dangerously_truncate_to_u32() as usize
    }
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
impl<
        AF: AddressFamily,
        M: Meta,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > std::fmt::Display for TreeBitMap<AF, M, NB, PB>
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(_f, "{} prefixes created", self.get_prefixes_count())?;
        writeln!(_f, "{} nodes created", self.get_nodes_count())?;
        writeln!(_f)?;

        writeln!(
            _f,
            "stride division {:?}",
            self.get_stride_sizes()
                .iter()
                .map_while(|s| if s > &0 { Some(*s) } else { None })
                .collect::<Vec<_>>()
        )?;

        writeln!(
            _f,
            "level\t[{}] prefixes-occupied/max-prefixes percentage_occupied",
            Colour::Green.paint("prefixes")
        )?;

        let bars = ["▏", "▎", "▍", "▌", "▋", "▊", "▉"];
        const SCALE: u32 = 5500;

        trace!(
            "stride_sizes {:?}",
            self.get_stride_sizes()
                .iter()
                .map_while(|s| if s > &0 { Some(*s) } else { None })
                .enumerate()
                .collect::<Vec<(usize, u8)>>()
        );

        for crate::stats::CreatedNodes {
            depth_level: len,
            count: prefix_count,
        } in self.counters.get_prefix_stats()
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
                    bars[((prefix_count as u32 % SCALE) / (SCALE / 7))
                        as usize]
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