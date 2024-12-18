use super::super::persist::lsm_tree::PersistTree;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use inetnum::addr::Prefix;
use log::{debug, error, info, log_enabled, trace};

use crossbeam_epoch as epoch;
use epoch::{Guard, Owned};
use roaring::RoaringBitmap;

use crate::local_array::in_memory::tree::{StrideNodeId, TreeBitMap};
use crate::local_array::types::PrefixId;
use crate::stats::CreatedNodes;
use crate::{
    local_array::errors::PrefixStoreError, prefix_record::PublicRecord,
};

use crate::local_array::in_memory::atomic_types::PrefixBuckets;
use crate::local_array::in_memory::atomic_types::{
    NodeBuckets, StoredPrefix,
};
use crate::local_array::in_memory::tree::{NewNodeOrIndex, SizedStrideRef};

// Make sure to also import the other methods for the Rib, so the proc macro
// create_store can use them.
pub use crate::local_array::iterators;
pub use crate::local_array::query;

use crate::{insert_match, IPv4, IPv6, MatchType, Meta, QueryResult};

use crate::AddressFamily;

//------------ StoreConfig ---------------------------------------------------

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PersistStrategy {
    WriteAhead,
    PersistHistory,
    MemoryOnly,
    PersistOnly,
}

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub persist_strategy: PersistStrategy,
    pub persist_path: String,
}

impl StoreConfig {
    pub fn persist_strategy(&self) -> PersistStrategy {
        self.persist_strategy
    }
}

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

//------------ UpsertReport --------------------------------------------------

#[derive(Debug)]
pub struct UpsertReport {
    // Indicates the number of Atomic Compare-and-Swap operations were
    // necessary to create/update the Record entry. High numbers indicate
    // contention.
    pub cas_count: usize,
    // Indicates whether this was the first mui record for this prefix was
    // created. So, the prefix did not exist before hand.
    pub prefix_new: bool,
    // Indicates whether this mui was new for this prefix. False means an old
    // value was overwritten.
    pub mui_new: bool,
    // The number of mui records for this prefix after the upsert operation.
    pub mui_count: usize,
}

// ----------- Rib -----------------------------------------------------------
//
// A Routing Information Base that consists of multiple different trees for
// in-memory and on-disk (persisted storage).
#[derive(Debug)]
pub struct Rib<
    AF: AddressFamily,
    M: Meta,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, M>,
    const PREFIX_SIZE: usize,
    const KEY_SIZE: usize,
> {
    pub config: StoreConfig,
    pub(crate) in_memory_tree: TreeBitMap<AF, M, NB, PB>,
    #[cfg(feature = "persist")]
    persist_tree: Option<PersistTree<AF, PREFIX_SIZE, KEY_SIZE>>,
    pub counters: Counters,
}

impl<
        'a,
        AF: AddressFamily,
        M: crate::prefix_record::Meta,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
        const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > Rib<AF, M, NB, PB, PREFIX_SIZE, KEY_SIZE>
{
    pub fn new(
        config: StoreConfig,
    ) -> Result<
        Rib<AF, M, NB, PB, PREFIX_SIZE, KEY_SIZE>,
        Box<dyn std::error::Error>,
    > {
        Rib::<AF, M, NB, PB, PREFIX_SIZE, KEY_SIZE>::init(config)
    }

    fn init(config: StoreConfig) -> Result<Self, Box<dyn std::error::Error>> {
        info!("store: initialize store {}", AF::BITS);

        let persist_tree = match config.persist_strategy {
            PersistStrategy::MemoryOnly => None,
            _ => {
                let persist_path = &Path::new(&config.persist_path);
                Some(PersistTree::new(persist_path))
            }
        };

        let store = Rib {
            config,
            in_memory_tree: TreeBitMap::<AF, M, NB, PB>::new()?,
            persist_tree,
            counters: Counters::default(),
        };

        Ok(store)
    }

    pub fn insert(
        &self,
        pfx: PrefixId<AF>,
        record: PublicRecord<M>,
        update_path_selections: Option<M::TBI>,
    ) -> Result<UpsertReport, PrefixStoreError> {
        let guard = &epoch::pin();

        if pfx.get_len() == 0 {
            let res = self.update_default_route_prefix_meta(record, guard)?;
            return Ok(res);
        }

        let mut stride_end: u8 = 0;
        let mut cur_i = self.in_memory_tree.get_root_node_id();
        let mut level: u8 = 0;
        let mut acc_retry_count = 0;

        loop {
            let stride =
                self.in_memory_tree.get_stride_sizes()[level as usize];
            stride_end += stride;
            let nibble_len = if pfx.get_len() < stride_end {
                stride + pfx.get_len() - stride_end
            } else {
                stride
            };

            let nibble = AF::get_nibble(
                pfx.get_net(),
                stride_end - stride,
                nibble_len,
            );
            let is_last_stride = pfx.get_len() <= stride_end;
            let stride_start = stride_end - stride;
            // let back_off = crossbeam_utils::Backoff::new();

            // insert_match! returns the node_id of the next node to be
            // traversed. It was created if it did not exist.
            let node_result = insert_match![
                // applicable to the whole outer match in the macro
                self;
                guard;
                nibble_len;
                nibble;
                is_last_stride;
                pfx;
                record;
                // perform an update for the paths in this record
                update_path_selections;
                // the length at the start of the stride a.k.a. start_bit
                stride_start;
                stride;
                cur_i;
                level;
                acc_retry_count;
                // Strides to create match arm for; stats level
                Stride3; 0,
                Stride4; 1,
                Stride5; 2
            ];

            match node_result {
                Ok((next_id, retry_count)) => {
                    cur_i = next_id;
                    level += 1;
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
        record: PublicRecord<M>,
        guard: &epoch::Guard,
    ) -> Result<UpsertReport, PrefixStoreError> {
        trace!("Updating the default route...");

        if let Some(root_node) = self.in_memory_tree.retrieve_node_mut(
            self.in_memory_tree.get_root_node_id(),
            record.multi_uniq_id,
        ) {
            match root_node {
                SizedStrideRef::Stride3(_) => {
                    self.in_memory_tree
                        .node_buckets
                        .get_store3(self.in_memory_tree.get_root_node_id())
                        .update_rbm_index(record.multi_uniq_id)?;
                }
                SizedStrideRef::Stride4(_) => {
                    self.in_memory_tree
                        .node_buckets
                        .get_store4(self.in_memory_tree.get_root_node_id())
                        .update_rbm_index(record.multi_uniq_id)?;
                }
                SizedStrideRef::Stride5(_) => {
                    self.in_memory_tree
                        .node_buckets
                        .get_store5(self.in_memory_tree.get_root_node_id())
                        .update_rbm_index(record.multi_uniq_id)?;
                }
            };
        };

        self.upsert_prefix(
            PrefixId::new(AF::zero(), 0),
            record,
            // Do not update the path selection for the default route.
            None,
            guard,
        )
    }

    // // Create a new node in the store with payload `next_node`.
    // //
    // // Next node will be ignored if a node with the same `id` already exists,
    // // but the multi_uniq_id will be added to the rbm_index of the NodeSet.
    // //
    // // Returns: a tuple with the node_id of the created node and the number of
    // // retry_count
    // #[allow(clippy::type_complexity)]
    // pub(crate) fn store_node(
    //     &self,
    //     id: StrideNodeId<AF>,
    //     multi_uniq_id: u32,
    //     next_node: SizedStrideNode<AF>,
    // ) -> Result<(StrideNodeId<AF>, u32), PrefixStoreError> {
    //     struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
    //         f: &'s dyn Fn(
    //             &SearchLevel<AF, S>,
    //             &NodeSet<AF, S>,
    //             TreeBitMapNode<AF, S>,
    //             u32, // multi_uniq_id
    //             u8,  // the store level
    //             u32, // retry_count
    //         ) -> Result<
    //             (StrideNodeId<AF>, u32),
    //             PrefixStoreError,
    //         >,
    //     }

    //     let search_level_3 =
    //         store_node_closure![Stride3; id; guard; back_off;];
    //     let search_level_4 =
    //         store_node_closure![Stride4; id; guard; back_off;];
    //     let search_level_5 =
    //         store_node_closure![Stride5; id; guard; back_off;];

    //     if log_enabled!(log::Level::Trace) {
    //         debug!(
    //             "{} store: Store node {}: {:?} mui {}",
    //             std::thread::current().name().unwrap_or("unnamed-thread"),
    //             id,
    //             next_node,
    //             multi_uniq_id
    //         );
    //     }
    //     self.counters.inc_nodes_count();

    //     match next_node {
    //         SizedStrideNode::Stride3(new_node) => (search_level_3.f)(
    //             &search_level_3,
    //             self.in_memory_tree.node_buckets.get_store3(id),
    //             new_node,
    //             multi_uniq_id,
    //             0,
    //             0,
    //         ),
    //         SizedStrideNode::Stride4(new_node) => (search_level_4.f)(
    //             &search_level_4,
    //             self.in_memory_tree.node_buckets.get_store4(id),
    //             new_node,
    //             multi_uniq_id,
    //             0,
    //             0,
    //         ),
    //         SizedStrideNode::Stride5(new_node) => (search_level_5.f)(
    //             &search_level_5,
    //             self.in_memory_tree.node_buckets.get_store5(id),
    //             new_node,
    //             multi_uniq_id,
    //             0,
    //             0,
    //         ),
    //     }
    // }

    // #[allow(clippy::type_complexity)]
    // pub(crate) fn retrieve_node(
    //     &'a self,
    //     id: StrideNodeId<AF>,
    // ) -> Option<SizedStrideRef<'a, AF>> {
    //     struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
    //         f: &'s dyn for<'a> Fn(
    //             &SearchLevel<AF, S>,
    //             &'a NodeSet<AF, S>,
    //             u8,
    //         )
    //             -> Option<SizedStrideRef<'a, AF>>,
    //     }

    //     let search_level_3 = impl_search_level![Stride3; id;];
    //     let search_level_4 = impl_search_level![Stride4; id;];
    //     let search_level_5 = impl_search_level![Stride5; id;];

    //     if log_enabled!(log::Level::Trace) {
    //         trace!(
    //             "{} store: Retrieve node {} from l{}",
    //             std::thread::current().name().unwrap_or("unnamed-thread"),
    //             id,
    //             id.get_id().1
    //         );
    //     }

    //     match self.get_stride_for_id(id) {
    //         3 => (search_level_3.f)(
    //             &search_level_3,
    //             self.in_memory_tree.node_buckets.get_store3(id),
    //             0,
    //         ),
    //         4 => (search_level_4.f)(
    //             &search_level_4,
    //             self.in_memory_tree.node_buckets.get_store4(id),
    //             0,
    //         ),
    //         _ => (search_level_5.f)(
    //             &search_level_5,
    //             self.in_memory_tree.node_buckets.get_store5(id),
    //             0,
    //         ),
    //     }
    // }

    // retrieve a node, but only its bitmap index contains the specified mui.
    // Used for iterators per mui.
    // #[allow(clippy::type_complexity)]
    // pub(crate) fn retrieve_node_for_mui(
    //     &'a self,
    //     id: StrideNodeId<AF>,
    //     // The mui that is tested to be present in the nodes bitmap index
    //     mui: u32,
    // ) -> Option<SizedStrideRef<'a, AF>> {
    //     struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
    //         f: &'s dyn for<'a> Fn(
    //             &SearchLevel<AF, S>,
    //             &'a NodeSet<AF, S>,
    //             u8,
    //         )
    //             -> Option<SizedStrideRef<'a, AF>>,
    //     }

    //     let search_level_3 = impl_search_level_for_mui![Stride3; id; mui;];
    //     let search_level_4 = impl_search_level_for_mui![Stride4; id; mui;];
    //     let search_level_5 = impl_search_level_for_mui![Stride5; id; mui;];

    //     if log_enabled!(log::Level::Trace) {
    //         trace!(
    //             "{} store: Retrieve node {} from l{} for mui {}",
    //             std::thread::current().name().unwrap_or("unnamed-thread"),
    //             id,
    //             id.get_id().1,
    //             mui
    //         );
    //     }

    //     match self.get_stride_for_id(id) {
    //         3 => (search_level_3.f)(
    //             &search_level_3,
    //             self.in_memory_tree.node_buckets.get_store3(id),
    //             0,
    //         ),
    //         4 => (search_level_4.f)(
    //             &search_level_4,
    //             self.in_memory_tree.node_buckets.get_store4(id),
    //             0,
    //         ),
    //         _ => (search_level_5.f)(
    //             &search_level_5,
    //             self.in_memory_tree.node_buckets.get_store5(id),
    //             0,
    //         ),
    //     }
    // }

    // #[allow(clippy::type_complexity)]
    // pub(crate) fn retrieve_node_mut(
    //     &'a self,
    //     id: StrideNodeId<AF>,
    //     multi_uniq_id: u32,
    // ) -> Option<SizedStrideRef<'a, AF>> {
    //     struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
    //         f: &'s dyn for<'a> Fn(
    //             &SearchLevel<AF, S>,
    //             &'a NodeSet<AF, S>,
    //             u8,
    //         )
    //             -> Option<SizedStrideRef<'a, AF>>,
    //     }

    //     let search_level_3 =
    //         retrieve_node_mut_closure![Stride3; id; multi_uniq_id;];
    //     let search_level_4 =
    //         retrieve_node_mut_closure![Stride4; id; multi_uniq_id;];
    //     let search_level_5 =
    //         retrieve_node_mut_closure![Stride5; id; multi_uniq_id;];

    //     if log_enabled!(log::Level::Trace) {
    //         trace!(
    //             "{} store: Retrieve node mut {} from l{}",
    //             std::thread::current().name().unwrap_or("unnamed-thread"),
    //             id,
    //             id.get_id().1
    //         );
    //     }

    //     match self.in_memory_tree.node_buckets.get_stride_for_id(id) {
    //         3 => (search_level_3.f)(
    //             &search_level_3,
    //             self.in_memory_tree.node_buckets.get_store3(id),
    //             0,
    //         ),

    //         4 => (search_level_4.f)(
    //             &search_level_4,
    //             self.in_memory_tree.node_buckets.get_store4(id),
    //             0,
    //         ),
    //         _ => (search_level_5.f)(
    //             &search_level_5,
    //             self.in_memory_tree.node_buckets.get_store5(id),
    //             0,
    //         ),
    //     }
    // }

    // pub(crate) fn get_root_node_id(&self) -> StrideNodeId<AF> {
    //     StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0)
    // }

    pub fn get_nodes_count(&self) -> usize {
        self.in_memory_tree.get_nodes_count()
    }

    // Prefixes related methods

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
        record: PublicRecord<M>,
        update_path_selections: Option<M::TBI>,
        guard: &Guard,
    ) -> Result<UpsertReport, PrefixStoreError> {
        let mut prefix_is_new = true;
        let mut mui_is_new = true;

        let (mui_count, cas_count) = match self
            .in_memory_tree
            .non_recursive_retrieve_prefix_mut(prefix)
        {
            // There's no StoredPrefix at this location yet. Create a new
            // PrefixRecord and try to store it in the empty slot.
            (locked_prefix, false) => {
                if log_enabled!(log::Level::Debug) {
                    debug!(
                        "{} store: Create new prefix record",
                        std::thread::current()
                            .name()
                            .unwrap_or("unnamed-thread")
                    );
                }

                let (mui_count, retry_count) =
                    locked_prefix.record_map.upsert_record(
                        prefix,
                        record,
                        &self.persist_tree,
                        self.config.persist_strategy,
                    )?;

                // See if someone beat us to creating the record.
                if mui_count.is_some() {
                    mui_is_new = false;
                    prefix_is_new = false;
                } else {
                    // No, we were the first, we created a new prefix
                    self.counters.inc_prefixes_count(prefix.get_len());
                }

                (mui_count, retry_count)
            }
            // There already is a StoredPrefix with a record at this
            // location.
            (stored_prefix, true) => {
                if log_enabled!(log::Level::Debug) {
                    debug!(
                        "{} store: Found existing prefix record for {}/{}",
                        std::thread::current()
                            .name()
                            .unwrap_or("unnamed-thread"),
                        prefix.get_net(),
                        prefix.get_len()
                    );
                }
                prefix_is_new = false;

                // Update the already existing record_map with our
                // caller's record.
                stored_prefix.set_ps_outdated(guard)?;

                let (mui_count, retry_count) =
                    stored_prefix.record_map.upsert_record(
                        prefix,
                        record,
                        &self.persist_tree,
                        self.config.persist_strategy,
                    )?;
                mui_is_new = mui_count.is_none();

                if let Some(tbi) = update_path_selections {
                    stored_prefix
                        .calculate_and_store_best_backup(&tbi, guard)?;
                }

                (mui_count, retry_count)
            }
        };

        Ok(UpsertReport {
            prefix_new: prefix_is_new,
            cas_count,
            mui_new: mui_is_new,
            mui_count: mui_count.unwrap_or(1),
        })
    }

    pub(crate) fn withdrawn_muis_bmin(
        &'a self,
        guard: &'a Guard,
    ) -> &'a RoaringBitmap {
        unsafe {
            self.in_memory_tree
                .withdrawn_muis_bmin
                .load(Ordering::Acquire, guard)
                .deref()
        }
    }

    // Change the status of the record for the specified (prefix, mui)
    // combination  to Withdrawn.
    pub fn mark_mui_as_withdrawn_for_prefix(
        &self,
        prefix: PrefixId<AF>,
        mui: u32,
    ) -> Result<(), PrefixStoreError> {
        let (stored_prefix, exists) = self
            .in_memory_tree
            .non_recursive_retrieve_prefix_mut(prefix);

        if !exists {
            return Err(PrefixStoreError::StoreNotReadyError);
        }

        stored_prefix.record_map.mark_as_withdrawn_for_mui(mui);

        Ok(())
    }

    // Change the status of the record for the specified (prefix, mui)
    // combination  to Active.
    pub fn mark_mui_as_active_for_prefix(
        &self,
        prefix: PrefixId<AF>,
        mui: u32,
    ) -> Result<(), PrefixStoreError> {
        let (stored_prefix, exists) = self
            .in_memory_tree
            .non_recursive_retrieve_prefix_mut(prefix);

        if !exists {
            return Err(PrefixStoreError::StoreNotReadyError);
        }

        stored_prefix.record_map.mark_as_active_for_mui(mui);

        Ok(())
    }

    // Change the status of the mui globally to Withdrawn. Iterators and match
    // functions will by default not return any records for this mui.
    pub fn mark_mui_as_withdrawn(
        &self,
        mui: u32,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        let current = self
            .in_memory_tree
            .withdrawn_muis_bmin
            .load(Ordering::Acquire, guard);

        let mut new = unsafe { current.as_ref() }.unwrap().clone();
        new.insert(mui);

        #[allow(clippy::assigning_clones)]
        loop {
            match self.in_memory_tree.withdrawn_muis_bmin.compare_exchange(
                current,
                Owned::new(new),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(updated) => {
                    new =
                        unsafe { updated.current.as_ref() }.unwrap().clone();
                }
            }
        }
    }

    // Change the status of the mui globally to Active. Iterators and match
    // functions will default to the status on the record itself.
    pub fn mark_mui_as_active(
        &self,
        mui: u32,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        let current = self
            .in_memory_tree
            .withdrawn_muis_bmin
            .load(Ordering::Acquire, guard);

        let mut new = unsafe { current.as_ref() }.unwrap().clone();
        new.remove(mui);

        #[allow(clippy::assigning_clones)]
        loop {
            match self.in_memory_tree.withdrawn_muis_bmin.compare_exchange(
                current,
                Owned::new(new),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(updated) => {
                    new =
                        unsafe { updated.current.as_ref() }.unwrap().clone();
                }
            }
        }
    }

    // Whether this mui is globally withdrawn. Note that this overrules
    // (by default) any (prefix, mui) combination in iterators and match
    // functions.
    pub fn mui_is_withdrawn(&self, mui: u32, guard: &Guard) -> bool {
        unsafe {
            self.in_memory_tree
                .withdrawn_muis_bmin
                .load(Ordering::Acquire, guard)
                .as_ref()
        }
        .unwrap()
        .contains(mui)
    }

    // Whether this mui is globally active. Note that the local statuses of
    // records (prefix, mui) may be set to withdrawn in iterators and match
    // functions.
    pub fn mui_is_active(&self, mui: u32, guard: &Guard) -> bool {
        !unsafe {
            self.in_memory_tree
                .withdrawn_muis_bmin
                .load(Ordering::Acquire, guard)
                .as_ref()
        }
        .unwrap()
        .contains(mui)
    }

    // Helper to filter out records that are not-active (Inactive or
    // Withdrawn), or whose mui appears in the global withdrawn index.
    pub(crate) fn get_filtered_records(
        &self,
        pfx: &StoredPrefix<AF, M>,
        mui: Option<u32>,
        guard: &Guard,
    ) -> Vec<PublicRecord<M>> {
        let bmin = self.withdrawn_muis_bmin(guard);

        pfx.record_map.get_filtered_records(mui, bmin)
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
    // #[allow(clippy::type_complexity)]
    // pub(crate) fn non_recursive_retrieve_prefix_mut(
    //     &'a self,
    //     search_prefix_id: PrefixId<AF>,
    // ) -> (&'a StoredPrefix<AF, M>, bool) {
    //     trace!("non_recursive_retrieve_prefix_mut_with_guard");
    //     let mut prefix_set = self
    //         .in_memory_tree
    //         .prefix_buckets
    //         .get_root_prefix_set(search_prefix_id.get_len());
    //     let mut level: u8 = 0;

    //     trace!("root prefix_set {:?}", prefix_set);
    //     loop {
    //         // HASHING FUNCTION
    //         let index = TreeBitMap::<AF, M, NB, PB>::hash_prefix_id(
    //             search_prefix_id,
    //             level,
    //         );

    //         // probe the slot with the index that's the result of the hashing.
    //         // let locked_prefix = prefix_set.0.get(index);
    //         let stored_prefix = match prefix_set.0.get(index) {
    //             Some(p) => {
    //                 trace!("prefix set found.");
    //                 (p, true)
    //             }
    //             None => {
    //                 // We're at the end of the chain and haven't found our
    //                 // search_prefix_id anywhere. Return the end-of-the-chain
    //                 // StoredPrefix, so the caller can attach a new one.
    //                 trace!(
    //                     "no record. returning last found record in level
    //                     {}, with index {}.",
    //                     level,
    //                     index
    //                 );
    //                 let index = TreeBitMap::<AF, M, NB, PB>::hash_prefix_id(
    //                     search_prefix_id,
    //                     level,
    //                 );
    //                 trace!("calculate next index {}", index);
    //                 let var_name = (
    //                     prefix_set
    //                         .0
    //                         .get_or_init(index, || {
    //                             StoredPrefix::new::<PB>(
    //                                 PrefixId::new(
    //                                     search_prefix_id.get_net(),
    //                                     search_prefix_id.get_len(),
    //                                 ),
    //                                 level,
    //                             )
    //                         })
    //                         .0,
    //                     false,
    //                 );
    //                 var_name
    //             }
    //         };

    //         if search_prefix_id == stored_prefix.0.prefix {
    //             // GOTCHA!
    //             // Our search-prefix is stored here, so we're returning
    //             // it, so its PrefixRecord can be updated by the caller.
    //             trace!("found requested prefix {:?}", search_prefix_id);
    //             return stored_prefix;
    //         } else {
    //             // A Collision. Follow the chain.
    //             level += 1;
    //             prefix_set = &stored_prefix.0.next_bucket;
    //             continue;
    //         }
    //     }
    // }

    // // This function is used by the match_prefix, and [more|less]_specifics
    // // public methods on the TreeBitMap (indirectly).
    // #[allow(clippy::type_complexity)]
    // pub fn non_recursive_retrieve_prefix(
    //     &'a self,
    //     id: PrefixId<AF>,
    // ) -> (
    //     Option<&'a StoredPrefix<AF, M>>,
    //     Option<(
    //         PrefixId<AF>,
    //         u8,
    //         &'a PrefixSet<AF, M>,
    //         [Option<(&'a PrefixSet<AF, M>, usize)>; 32],
    //         usize,
    //     )>,
    // ) {
    //     let mut prefix_set = self
    //         .in_memory_tree
    //         .prefix_buckets
    //         .get_root_prefix_set(id.get_len());
    //     let mut parents = [None; 32];
    //     let mut level: u8 = 0;
    //     let backoff = Backoff::new();

    //     loop {
    //         // The index of the prefix in this array (at this len and
    //         // level) is calculated by performing the hash function
    //         // over the prefix.

    //         // HASHING FUNCTION
    //         let index =
    //             TreeBitMap::<AF, M, NB, PB>::hash_prefix_id(id, level);

    //         if let Some(stored_prefix) = prefix_set.0.get(index) {
    //             if id == stored_prefix.get_prefix_id() {
    //                 trace!("found requested prefix {:?}", id);
    //                 parents[level as usize] = Some((prefix_set, index));
    //                 return (
    //                     Some(stored_prefix),
    //                     Some((id, level, prefix_set, parents, index)),
    //                 );
    //             };
    //             // Advance to the next level.

    //             prefix_set = &stored_prefix.next_bucket;
    //             level += 1;
    //             backoff.spin();
    //             continue;
    //         }

    //         trace!("no prefix found for {:?}", id);
    //         parents[level as usize] = Some((prefix_set, index));
    //         return (None, Some((id, level, prefix_set, parents, index)));
    //     }
    // }

    // #[allow(dead_code)]
    // fn remove_prefix(&mut self, index: PrefixId<AF>) -> Option<M> {
    //     match index.is_empty() {
    //         false => self.in_memory_tree.prefix_buckets.remove(index),
    //         true => None,
    //     }
    // }

    pub fn get_prefixes_count(&self) -> usize {
        self.counters.get_prefixes_count().iter().sum()
    }

    pub fn get_prefixes_count_for_len(&self, len: u8) -> usize {
        self.counters.get_prefixes_count()[len as usize]
    }

    // Stride related methods

    // pub(crate) fn get_stride_for_id(&self, id: StrideNodeId<AF>) -> u8 {
    //     self.in_memory_tree.node_buckets.get_stride_for_id(id)
    // }

    // Pass through the in_memory stride sizes, for printing purposes
    pub fn get_stride_sizes(&self) -> &[u8] {
        self.in_memory_tree.node_buckets.get_stride_sizes()
    }

    // pub(crate) fn get_strides_len() -> u8 {
    //     NB::get_strides_len()
    // }

    // pub(crate) fn get_first_stride_size() -> u8 {
    //     NB::get_first_stride_size()
    // }

    // Calculates the id of the node that COULD host a prefix in its
    // ptrbitarr.
    // pub(crate) fn get_node_id_for_prefix(
    //     &self,
    //     prefix: &PrefixId<AF>,
    // ) -> (StrideNodeId<AF>, BitSpan) {
    //     let mut acc = 0;
    //     for i in self.get_stride_sizes() {
    //         acc += *i;
    //         if acc >= prefix.get_len() {
    //             let node_len = acc - i;
    //             return (
    //                 StrideNodeId::new_with_cleaned_id(
    //                     prefix.get_net(),
    //                     node_len,
    //                 ),
    //                 // NOT THE HASHING FUNCTION!
    //                 // Do the right shift in a checked manner, for the sake
    //                 // of 0/0. A search for 0/0 will perform a 0 << MAX_LEN,
    //                 // which will panic in debug mode (undefined behaviour
    //                 // in prod).
    //                 BitSpan::new(
    //                     ((prefix.get_net() << node_len).checked_shr_or_zero(
    //                         (AF::BITS - (prefix.get_len() - node_len)).into(),
    //                     ))
    //                     .dangerously_truncate_to_u32(),
    //                     prefix.get_len() - node_len,
    //                 ),
    //             );
    //         }
    //     }
    //     panic!("prefix length for {:?} is too long", prefix);
    // }

    //-------- Persistence ---------------------------------------------------

    pub fn persist_strategy(&self) -> PersistStrategy {
        self.config.persist_strategy
    }

    pub fn match_prefix_in_persisted_store(
        &'a self,
        search_pfx: PrefixId<AF>,
        mui: Option<u32>,
    ) -> QueryResult<M> {
        let key: Vec<u8> = if let Some(mui) = mui {
            PersistTree::<AF,
        PREFIX_SIZE, KEY_SIZE>::prefix_mui_persistence_key(search_pfx, mui)
        } else {
            search_pfx.as_bytes::<PREFIX_SIZE>().to_vec()
        };

        if let Some(persist) = &self.persist_tree {
            QueryResult {
                prefix: Some(search_pfx.into_pub()),
                match_type: MatchType::ExactMatch,
                prefix_meta: persist
                    .get_records_for_key(&key)
                    .into_iter()
                    .map(|(_, rec)| rec)
                    .collect::<Vec<_>>(),
                less_specifics: None,
                more_specifics: None,
            }
        } else {
            QueryResult {
                prefix: None,
                match_type: MatchType::EmptyMatch,
                prefix_meta: vec![],
                less_specifics: None,
                more_specifics: None,
            }
        }
    }

    pub fn get_records_for_prefix(
        &self,
        prefix: &Prefix,
    ) -> Vec<PublicRecord<M>> {
        if let Some(p) = &self.persist_tree {
            p.get_records_for_prefix(PrefixId::from(*prefix))
        } else {
            vec![]
        }
    }

    pub fn flush_to_disk(&self) -> Result<(), PrefixStoreError> {
        if let Some(p) = &self.persist_tree {
            p.flush_to_disk()
                .map_err(|_| PrefixStoreError::PersistFailed)
        } else {
            Err(PrefixStoreError::PersistFailed)
        }
    }

    pub fn approx_persisted_items(&self) -> usize {
        if let Some(p) = &self.persist_tree {
            p.approximate_len()
        } else {
            0
        }
    }

    pub fn disk_space(&self) -> u64 {
        if let Some(p) = &self.persist_tree {
            p.disk_space()
        } else {
            0
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

    // pub(crate) fn hash_node_id(id: StrideNodeId<AF>, level: u8) -> usize {
    //     // And, this is all of our hashing function.
    //     let last_level = if level > 0 {
    //         <NB>::len_to_store_bits(id.get_id().1, level - 1)
    //     } else {
    //         0
    //     };
    //     let this_level = <NB>::len_to_store_bits(id.get_id().1, level);
    //     trace!("bits division {}", this_level);
    //     trace!(
    //         "calculated index ({} << {}) >> {}",
    //         id.get_id().0,
    //         last_level,
    //         ((<AF>::BITS - (this_level - last_level)) % <AF>::BITS) as usize
    //     );
    //     // HASHING FUNCTION
    //     ((id.get_id().0 << last_level)
    //         >> ((<AF>::BITS - (this_level - last_level)) % <AF>::BITS))
    //         .dangerously_truncate_to_u32() as usize
    // }

    // pub(crate) fn hash_prefix_id(id: PrefixId<AF>, level: u8) -> usize {
    //     // And, this is all of our hashing function.
    //     let last_level = if level > 0 {
    //         <PB>::get_bits_for_len(id.get_len(), level - 1)
    //     } else {
    //         0
    //     };
    //     let this_level = <PB>::get_bits_for_len(id.get_len(), level);
    //     trace!(
    //         "bits division {}; no of bits {}",
    //         this_level,
    //         this_level - last_level
    //     );
    //     trace!(
    //         "calculated index ({} << {}) >> {}",
    //         id.get_net(),
    //         last_level,
    //         ((<AF>::BITS - (this_level - last_level)) % <AF>::BITS) as usize
    //     );
    //     // HASHING FUNCTION
    //     ((id.get_net() << last_level)
    //         >> ((<AF>::BITS - (this_level - last_level)) % <AF>::BITS))
    //         .dangerously_truncate_to_u32() as usize
    // }
}

impl<
        M: Meta,
        NB: NodeBuckets<IPv4>,
        PB: PrefixBuckets<IPv4, M>,
        const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > std::fmt::Display for Rib<IPv4, M, NB, PB, PREFIX_SIZE, KEY_SIZE>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Rib<IPv4, {}>", std::any::type_name::<M>())
    }
}

impl<
        M: Meta,
        NB: NodeBuckets<IPv6>,
        PB: PrefixBuckets<IPv6, M>,
        const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > std::fmt::Display for Rib<IPv6, M, NB, PB, PREFIX_SIZE, KEY_SIZE>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Rib<IPv6, {}>", std::any::type_name::<M>())
    }
}
