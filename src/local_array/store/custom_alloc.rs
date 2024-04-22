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
// does not report any memory leaks. Miri is persistent in reporting
// memory leaks in the mt-prefix-store. They both report the memory leaks
// in the same location: the init method of the node- and prefix-buckets.
//
// I have reasons to believe these reported memory leaks aren't real, or
// that crossbeam-epoch leaks a bit of memory when creating a new `Atomic`
// instance. Since neither prefix nor node buckets can or should be dropped
// this is not a big issue anyway, it just means that an `Atomic` occupies
// more memory than it could in an optimal situation. Since we're not storing
// the actual meta-data in an `Atomic` (it is stored in an `ArcSwap`), this
// means memory usage won't grow on updating the meta-data on a prefix,
// (unless the meta-data itself grows of course, but that's up to the user).
//
// To get a better understanding on the nature of the reported memory leaks
// I have created a branch (`vec_set`) that replaces the dynamically sized
// array with a (equally sparse) Vec, that is not filled with
// `Atomic:::null()`, but with `Option<StoredPrefix` instead, in order to
// see if this would eliminate the memory leaks reporting. It did not.
// Valgrind still reports the memory leaks at the same location, although
// they're now reported as `indirectly leaked`, instead of `directly`. Miri
// is unchanged. The attentive reader may now suspect that the `Atomic`
// inside the `AtomicStoredPrefix` may be the culprit, but:
// a. Both miri and valgrind report the leaks still in the same place:
//    the creation of the buckets, not the creation of the stored prefixes.
// b. Tests that look at the memory usage of the prefix stores under heavy
//    modification of existing prefixes do not exhibit memory leaking
//    behavior.
// c. Tests that look at memory usage under addition of new prefixes
//    exhibit linear incrementation of memory usage (which is expected).
// d. Tests that look at memory usage under contention do not exhibit
//    increased memory usage either.
//
// My strong suspicion is that both Miri and Valgrind report the sparse
// slots in the buckets as leaks, no matter whether they're `Atomic::null()`,
// or `None` values, probably as a result of the way `crossbeam-epoch`
// indexes into these, with pointer arithmetic (unsafe as hell).
//
// I would be super grateful if somebody would prove me wrong and can point
// to an actual memory leak in the mt-prefix-store (and even more if they
// can produce a fix for it).

use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use log::{debug, info, log_enabled, trace};

use crossbeam_epoch::{self as epoch, Atomic};
use crossbeam_utils::Backoff;
use epoch::{CompareExchangeError, Guard, Owned, Shared};

use roaring::RoaringBitmap;

use std::marker::PhantomData;

use crate::{local_array::{
    bit_span::BitSpan, store::errors::PrefixStoreError,
}, prefix_record::MergeUpdate};
use crate::{local_array::tree::*, stats::CreatedNodes};

// use crate::prefix_record::InternalPrefixRecord;
use crate::{
    impl_search_level, retrieve_node_mut_with_guard_closure,
    store_node_closure,
};

use super::atomic_types::*;
use crate::AddressFamily;

//------------ Counters -----------------------------------------------------

#[derive(Debug)]
pub struct Counters {
    nodes: AtomicUsize,
    prefixes: [AtomicUsize; 129],
}

impl Counters {
    pub fn get_nodes_count(&self) -> usize {
        self.nodes.load(Ordering::Relaxed)
    }

    pub fn inc_nodes_count(&self) {
        self.nodes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_prefixes_count(&self) -> Vec<usize> {
        self.prefixes
            .iter()
            .map(|pc| pc.load(Ordering::Relaxed))
            .collect::<Vec<_>>()
    }

    pub fn inc_prefixes_count(&self, len: u8) {
        self.prefixes[len as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_prefix_stats(&self) -> Vec<CreatedNodes> {
        self.prefixes
            .iter()
            .enumerate()
            .filter_map(|(len, count)| {
                let count = count.load(Ordering::Relaxed);
                if count != 0 {
                    Some(CreatedNodes {
                        depth_level: len as u8,
                        count,
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Default for Counters {
    fn default() -> Self {
        let mut prefixes: Vec<AtomicUsize> = Vec::with_capacity(129);
        for _ in 0..=128 {
            prefixes.push(AtomicUsize::new(0));
        }

        Self {
            nodes: AtomicUsize::new(0),
            prefixes: prefixes.try_into().unwrap(),
        }
    }
}

//------------ StoreStats ----------------------------------------------

#[derive(Debug)]
pub struct StoreStats {
    pub v4: Vec<CreatedNodes>,
    pub v6: Vec<CreatedNodes>,
}

// ----------- CustomAllocStorage -------------------------------------------
//
// CustomAllocStorage is a storage backend that uses a custom allocator, that
// consists of arrays that point to other arrays on collision.
#[derive(Debug)]
pub struct CustomAllocStorage<
    AF: AddressFamily,
    M: crate::prefix_record::Meta + MergeUpdate,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, M>,
> {
    pub(crate) buckets: NB,
    pub prefixes: PB,
    pub default_route_prefix_serial: AtomicUsize,
    pub counters: Counters,
    _m: PhantomData<M>,
    _af: PhantomData<AF>,
}

impl<
        'a,
        AF: AddressFamily,
        M: crate::prefix_record::Meta,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
    > CustomAllocStorage<AF, M, NB, PB>
{
    pub(crate) fn init(
        root_node: SizedStrideNode<AF>,
        // A node always gets created as an intermediary to create an actual
        // meta-data record. A meta-data record has an id that is unique in
        // the collection of Records, that is stored as a value in the tree.
        // This unique id is used to be able to decide to replace or add a
        // record to the meta-data collection in a multi-map. It is also added
        // to a bitmap index on each node that has children where the unique
        // id appears on a Record.
        // multi_uniq_id: u32,
        guard: &'a Guard,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        info!("store: initialize store");

        let store = CustomAllocStorage {
            buckets: NodeBuckets::<AF>::init(),
            prefixes: PrefixBuckets::<AF, M>::init(),
            default_route_prefix_serial: AtomicUsize::new(0),
            counters: Counters::default(),
            _af: PhantomData,
            _m: PhantomData,
        };

        let _retry_count = store.store_node(
            StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0),
            0_u32,
            root_node,
            guard,
        )?;

        Ok(store)
    }

    pub(crate) fn acquire_new_node_id(
        &self,
        (prefix_net, sub_prefix_len): (AF, u8),
    ) -> StrideNodeId<AF> {
        StrideNodeId::new_with_cleaned_id(prefix_net, sub_prefix_len)
    }

    // Create a new node in the store with payload `next_node`.
    //
    // Next node will be ignored if a node with the same `id` already exists.
    // Returns:
    // a tuple with the node_id of the created node and the number of retry_count
    #[allow(clippy::type_complexity)]
    pub(crate) fn store_node(
        &self,
        id: StrideNodeId<AF>,
        multi_uniq_id: u32,
        next_node: SizedStrideNode<AF>,
        guard: &Guard,
    ) -> Result<(StrideNodeId<AF>, u32), PrefixStoreError> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                TreeBitMapNode<AF, S>,
                u8,  // the store level
                u32, // retry_count
            ) -> Result<
                (StrideNodeId<AF>, u32),
                PrefixStoreError,
            >,
        }

        let back_off = crossbeam_utils::Backoff::new();

        let search_level_3 =
            store_node_closure![Stride3; id; multi_uniq_id; guard; back_off;];
        let search_level_4 =
            store_node_closure![Stride4; id; multi_uniq_id; guard; back_off;];
        let search_level_5 =
            store_node_closure![Stride5; id; multi_uniq_id; guard; back_off;];

        if log_enabled!(log::Level::Trace) {
            debug!(
                "{} store: Store node {}: {:?}",
                std::thread::current().name().unwrap(),
                id,
                next_node
            );
        }
        self.counters.inc_nodes_count();

        match next_node {
            SizedStrideNode::Stride3(new_node) => (search_level_3.f)(
                &search_level_3,
                self.buckets.get_store3(id),
                new_node,
                0,
                0,
            ),
            SizedStrideNode::Stride4(new_node) => (search_level_4.f)(
                &search_level_4,
                self.buckets.get_store4(id),
                new_node,
                0,
                0,
            ),
            SizedStrideNode::Stride5(new_node) => (search_level_5.f)(
                &search_level_5,
                self.buckets.get_store5(id),
                new_node,
                0,
                0,
            ),
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_node_with_guard(
        &'a self,
        id: StrideNodeId<AF>,
        guard: &'a Guard,
    ) -> Option<SizedStrideRef<'a, AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                u8,
                &'a Guard,
            )
                -> Option<SizedStrideRef<'a, AF>>,
        }

        let search_level_3 = impl_search_level![Stride3; id;];
        let search_level_4 = impl_search_level![Stride4; id;];
        let search_level_5 = impl_search_level![Stride5; id;];

        if log_enabled!(log::Level::Trace) {
            trace!(
                "{} store: Retrieve node {} from l{}",
                std::thread::current().name().unwrap(),
                id,
                id.get_id().1
            );
        }

        match self.get_stride_for_id(id) {
            3 => (search_level_3.f)(
                &search_level_3,
                self.buckets.get_store3(id),
                0,
                guard,
            ),
            4 => (search_level_4.f)(
                &search_level_4,
                self.buckets.get_store4(id),
                0,
                guard,
            ),
            _ => (search_level_5.f)(
                &search_level_5,
                self.buckets.get_store5(id),
                0,
                guard,
            ),
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_node_mut_with_guard(
        &'a self,
        id: StrideNodeId<AF>,
        multi_uniq_id: u32,
        guard: &'a Guard,
    ) -> Option<SizedStrideRefMut<'a, AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                // [u8; 10],
                u8,
                &'a Guard,
            )
                -> Option<SizedStrideRefMut<'a, AF>>,
        }

        let search_level_3 =
            retrieve_node_mut_with_guard_closure![Stride3; id; multi_uniq_id;];
        let search_level_4 =
            retrieve_node_mut_with_guard_closure![Stride4; id; multi_uniq_id;];
        let search_level_5 =
            retrieve_node_mut_with_guard_closure![Stride5; id; multi_uniq_id;];

        if log_enabled!(log::Level::Trace) {
            trace!(
                "{} store: Retrieve node {} from l{}",
                std::thread::current().name().unwrap(),
                id,
                id.get_id().1
            );
        }

        match self.buckets.get_stride_for_id(id) {
            3 => (search_level_3.f)(
                &search_level_3,
                self.buckets.get_store3(id),
                0,
                guard,
            ),

            4 => (search_level_4.f)(
                &search_level_4,
                self.buckets.get_store4(id),
                0,
                guard,
            ),
            _ => (search_level_5.f)(
                &search_level_5,
                self.buckets.get_store5(id),
                0,
                guard,
            ),
        }
    }

    pub(crate) fn get_root_node_id(&self) -> StrideNodeId<AF> {
        StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0)
    }

    pub fn get_nodes_count(&self) -> usize {
        self.counters.get_nodes_count()
    }

    // Prefixes related methods

    pub(crate) fn load_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    fn increment_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial
            .fetch_add(1, Ordering::SeqCst)
    }

    // THE CRITICAL SECTION
    //
    // CREATING OR UPDATING A PREFIX IN THE STORE
    //
    // YES, THE MAGIC HAPPENS HERE!
    //
    // This uses the TAG feature of crossbeam_utils::epoch to ensure that we
    // are not overwriting a prefix meta-data that already has been created
    // or was updated by another thread.
    //
    // Our plan:
    //
    // 1. LOAD
    //    Load the current prefix and meta-data from the store if any.
    // 2. INSERT
    //    If there is no current meta-data, create it.
    // 3. UPDATE
    //    If there is a prefix, meta-data combo, then load it and merge
    //    the existing meta-dat with our meta-data using the `MergeUpdate`
    //    trait (a so-called 'Read-Copy-Update').
    // 4. SUCCESS
    //    See if we can successfully store the updated meta-data in the store.
    // 5. DONE
    //    If Step 4 succeeded we're done!
    // 6. FAILURE - REPEAT
    //    If Step 4 failed we're going to do the whole thing again.

    pub(crate) fn upsert_prefix(
        &self,
        prefix: PrefixId<AF>,
        record: M,
        guard: &Guard,
        user_data: Option<&<M as MergeUpdate>::UserDataIn>,
    ) -> Result<(Upsert<<M as MergeUpdate>::UserDataOut>, u32), PrefixStoreError> {
        let mut retry_count = 0;
        let mut new_record = Arc::new(record);

        let (atomic_stored_prefix, level) = self
            .non_recursive_retrieve_prefix_mut_with_guard(
                PrefixId::new(prefix.get_net(), prefix.get_len()),
                guard,
            )?;

        let mut inner_stored_prefix =
            atomic_stored_prefix.0.load(Ordering::SeqCst, guard);

        loop {
            match inner_stored_prefix.is_null() {
                // There's no StoredPrefix at this location yet. Create a new
                // PrefixRecord with our record and try to store it in the
                // empty slot.
                true => {
                    if log_enabled!(log::Level::Debug) {
                        debug!(
                            "{} store: Create new prefix record",
                            std::thread::current().name().unwrap()
                        );
                    }
                    let new_stored_prefix = StoredPrefix::new::<PB>(
                        PrefixId::new(prefix.get_net(), prefix.get_len()),
                        new_record,
                        level,
                    );

                    // We're expecting an empty slot.
                    match atomic_stored_prefix.0.compare_exchange(
                        Shared::null(),
                        Owned::new(new_stored_prefix),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    ) {
                        // ...and we got an empty slot, the newly created
                        // StorePrefix is stored into it.
                        Ok(spfx) => {
                            if log_enabled!(log::Level::Info) {
                                let StoredPrefix {
                                    prefix,
                                    record: stored_record,
                                    ..
                                } = unsafe { spfx.deref() };
                                if log_enabled!(log::Level::Info) {
                                    info!(
                                        "{} store: Inserted new prefix record {}/{} with {:?}",
                                        std::thread::current().name().unwrap(),
                                        prefix.get_net().into_ipaddr(), prefix.get_len(),
                                        stored_record.get_meta_cloned()
                                    );
                                }
                            }

                            self.counters
                                .inc_prefixes_count(prefix.get_len());

                            return Ok((Upsert::Insert, retry_count));
                        }
                        // ...somebody beat us to it, the slot's not empty
                        // anymore, we'll have to do it again.
                        Err(CompareExchangeError { current, new }) => {
                            if log_enabled!(log::Level::Info) {
                                info!(
                                    "{} store: Prefix can't be inserted as new {:?}",
                                    std::thread::current().name().unwrap(),
                                    current
                                );
                            }
                            retry_count += 1;

                            new_record =
                                new.into_box().get_record_as_arc().clone();

                            // reuse the returned existing prefix_record,
                            // for the next iteration.
                            inner_stored_prefix = current;

                            continue;
                        }
                    }
                }
                // There already is a StoredPrefix with a record at this
                // location. Perform a Read-Copy-Update (RCU) on the record
                // to update its value.
                false => {
                    if log_enabled!(log::Level::Debug) {
                        debug!(
                            "{} store: Found existing prefix record for {}/{}",
                            std::thread::current().name().unwrap(),
                            prefix.get_net(),
                            prefix.get_len()
                        );
                    }

                    // We don't have to reload the prefix record at this
                    // location: Once it is created the prefix-record itself
                    // will not be changed anymore, only its record can be
                    // RCU'd.

                    // Prepare to invoke the ArcSwap read-copy-update cycle.
                    // The cycle will retry the closure until it is able to
                    // store the result produced by the last invocation. On
                    // each invocation the implementer of the MergeUpdate
                    // trait can choose to output some user defined UserDataOut
                    // which we store and return to them.
                    let mut user_data_out_final = None;

                    unsafe { inner_stored_prefix.deref() }
                        .record
                        .as_arc_swap()
                        .rcu(|meta| {
                            // Track how many times the RCU operation invoked
                            // us. We don't actually know if this is a retry
                            // or the first invocation, so our counter becomes
                            // one too high. We can't set it to -1 to start
                            // with as it is unsigned, so outside the closure
                            // below we correct the counter by subtracting
                            // one from it.
                            retry_count += 1;

                            // TODO: if retry_count > 1 then backoff?

                            let (res, user_data_out) = meta
                                .clone_merge_update(
                                    &new_record,
                                    user_data,
                                )
                                .unwrap();
                            user_data_out_final = Some(user_data_out);
                            res
                        });

                    // Correct the retry counter as it is off by one.
                    retry_count -= 1;

                    // The user data must have been set by now so this unwrap is safe.
                    let user_data = user_data_out_final.unwrap();

                    return Ok((Upsert::Update(user_data), retry_count));
                }
            }
        }
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
    fn non_recursive_retrieve_prefix_mut_with_guard(
        &'a self,
        search_prefix_id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> Result<(&'a AtomicStoredPrefix<AF, M>, u8), PrefixStoreError> {
        let mut prefix_set = self
            .prefixes
            .get_root_prefix_set(search_prefix_id.get_len());
        let mut level: u8 = 0;
        let mut stored_prefix = None;

        loop {
            // HASHING FUNCTION
            let index = Self::hash_prefix_id(search_prefix_id, level);

            let prefixes = prefix_set.0.load(Ordering::SeqCst, guard);
            trace!("prefixes at level {}? {:?}", level, !prefixes.is_null());

            // probe the slot with the index that's the result of the hashing.
            let prefix_probe = if !prefixes.is_null() {
                trace!("prefix set found.");
                unsafe { &prefixes.deref()[index] }
            } else {
                // We're at the end of the chain and haven't found our
                // search_prefix_id anywhere. Return the end-of-the-chain
                // StoredPrefix, so the caller can attach a new one.
                trace!("no prefix set.");
                return stored_prefix
                    .map(|sp| (sp, level))
                    .ok_or(PrefixStoreError::StoreNotReadyError);
            };

            stored_prefix = Some(unsafe { prefix_probe.assume_init_ref() });

            if let Some(StoredPrefix {
                prefix,
                next_bucket,
                ..
            }) = stored_prefix.unwrap().get_stored_prefix_mut(guard)
            {
                if search_prefix_id == *prefix {
                    // GOTCHA!
                    // Our search-prefix is stored here, so we're returning
                    // it, so its PrefixRecord can be updated by the caller.
                    trace!("found requested prefix {:?}", search_prefix_id);
                    return stored_prefix
                        .map(|sp| (sp, level))
                        .ok_or(PrefixStoreError::StoreNotReadyError);
                } else {
                    // A Collision. Follow the chain.
                    level += 1;
                    prefix_set = next_bucket;
                    continue;
                }
            }

            // No record at the deepest level, still we're returning a reference to it,
            // so the caller can insert a new record here.
            return stored_prefix
                .map(|sp| (sp, level))
                .ok_or(PrefixStoreError::StoreNotReadyError);
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn non_recursive_retrieve_prefix_with_guard(
        &'a self,
        id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> (
        Option<&StoredPrefix<AF, M>>,
        Option<(
            PrefixId<AF>,
            u8,
            &'a PrefixSet<AF, M>,
            [Option<(&'a PrefixSet<AF, M>, usize)>; 26],
            usize,
        )>,
    ) {
        let mut prefix_set = self.prefixes.get_root_prefix_set(id.get_len());
        let mut parents = [None; 26];
        let mut level: u8 = 0;
        let backoff = Backoff::new();

        loop {
            // The index of the prefix in this array (at this len and
            // level) is calculated by performing the hash function
            // over the prefix.

            // HASHING FUNCTION
            let index = Self::hash_prefix_id(id, level);

            let mut prefixes = prefix_set.0.load(Ordering::Acquire, guard);

            if !prefixes.is_null() {
                let prefix_ref = unsafe { &mut prefixes.deref_mut()[index] };
                if let Some(stored_prefix) =
                    unsafe { prefix_ref.assume_init_ref() }
                        .get_stored_prefix(guard)
                {
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
            }

            trace!("no prefix found for {:?}", id);
            parents[level as usize] = Some((prefix_set, index));
            return (None, Some((id, level, prefix_set, parents, index)));
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_prefix_with_guard(
        &'a self,
        prefix_id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> Option<(&StoredPrefix<AF, M>, &'a usize)> {
        struct SearchLevel<
            's,
            AF: AddressFamily,
            M: crate::prefix_record::Meta,
        > {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                u8,
                &'a Guard,
            ) -> Option<(
                &'a StoredPrefix<AF, M>,
                &'a usize,
            )>,
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, M>,
                 prefix_set: &PrefixSet<AF, M>,
                 mut level: u8,
                 guard: &Guard| {
                // HASHING FUNCTION
                let index = Self::hash_prefix_id(prefix_id, level);

                let prefixes = prefix_set.0.load(Ordering::SeqCst, guard);
                let prefix_ref = unsafe { &prefixes.deref()[index] };

                if let Some(stored_prefix) =
                    unsafe { prefix_ref.assume_init_ref() }
                        .get_stored_prefix(guard)
                {
                    if prefix_id == stored_prefix.prefix {
                        trace!("found requested prefix {:?}", prefix_id);
                        return Some((stored_prefix, &stored_prefix.serial));
                    };
                    level += 1;
                    (search_level.f)(
                        search_level,
                        &stored_prefix.next_bucket,
                        level,
                        guard,
                    );
                }
                None
            },
        };

        (search_level.f)(
            &search_level,
            self.prefixes.get_root_prefix_set(prefix_id.get_len()),
            0,
            guard,
        )
    }

    #[allow(dead_code)]
    fn remove_prefix(&mut self, index: PrefixId<AF>) -> Option<M> {
        match index.is_empty() {
            false => self.prefixes.remove(index),
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

    pub(crate) fn get_stride_for_id(&self, id: StrideNodeId<AF>) -> u8 {
        self.buckets.get_stride_for_id(id)
    }

    pub fn get_stride_sizes(&self) -> &[u8] {
        self.buckets.get_stride_sizes()
    }

    // pub(crate) fn get_strides_len() -> u8 {
    //     NB::get_strides_len()
    // }

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
        trace!("bits division {}", this_level);
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

//------------ Upsert -------------------------------------------------------
pub enum Upsert<T> {
    Insert,
    Update(T),
}

impl<T> std::fmt::Display for Upsert<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Upsert::Insert => write!(f, "Insert"),
            Upsert::Update(_) => write!(f, "Update"),
        }
    }
}
