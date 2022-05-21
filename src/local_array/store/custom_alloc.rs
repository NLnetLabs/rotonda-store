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
// go over all (nore|prefix) buckets until it matches the requested (node|
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
// The meta-data for a prefix is (also) stored as a linked-list of
// references, where each meta-data object has a reference to its
// predecessor. New meta-data instances are stored atomically without further
// ado, but updates to a piece of meta-data are done by merging the previous
// meta-data with the new meta-data, through use of the `MergeUpdate` trait.
//
// The `retrieve_prefix_*` methods retrieve only the most recent insert
// for a prefix (for now).
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
//
use std::{
    fmt::Debug,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::{self as epoch, Atomic};

use log::{info, trace, warn};

use epoch::{Guard, Owned};
use std::marker::PhantomData;

use crate::local_array::bit_span::BitSpan;
use crate::local_array::tree::*;

use crate::prefix_record::InternalPrefixRecord;
use crate::{impl_search_level, impl_search_level_mut, impl_write_level};

use crate::AddressFamily;
use routecore::record::MergeUpdate;
use routecore::record::Meta;
use super::atomic_types::*;

// ----------- CustomAllocStorage -------------------------------------------
//
// CustomAllocStorage is a storage backend that uses a custom allocator, that
// consitss of arrays that point to other arrays on collision.
#[derive(Debug)]
pub struct CustomAllocStorage<
    AF: AddressFamily,
    Meta: routecore::record::Meta + routecore::record::MergeUpdate,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, Meta>,
> {
    pub(crate) buckets: NB,
    pub prefixes: PB,
    pub default_route_prefix_serial: AtomicUsize,
    _m: PhantomData<Meta>,
    _af: PhantomData<AF>,
}

impl<
        'a,
        AF: AddressFamily,
        Meta: routecore::record::Meta,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, Meta>,
    > CustomAllocStorage<AF, Meta, NB, PB>
{
    pub(crate) fn init(root_node: SizedStrideNode<AF>) -> Self {
        trace!("initialize storage backend");

        let store = CustomAllocStorage {
            buckets: NodeBuckets::<AF>::init(),
            prefixes: PrefixBuckets::<AF, Meta>::init(),
            // len_to_stride_size,
            default_route_prefix_serial: AtomicUsize::new(0),
            _af: PhantomData,
            _m: PhantomData,
        };

        store.store_node(
            StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0),
            root_node,
        );
        store
    }

    pub(crate) fn acquire_new_node_id(
        &self,
        (prefix_net, sub_prefix_len): (AF, u8),
    ) -> StrideNodeId<AF> {
        StrideNodeId::new_with_cleaned_id(prefix_net, sub_prefix_len)
    }

    // Create a new node in the store with paylaod `next_node`.
    //
    // Next node will be ignored if a node with the same `id` already exists.
    #[allow(clippy::type_complexity)]
    pub(crate) fn store_node(
        &self,
        id: StrideNodeId<AF>,
        next_node: SizedStrideNode<AF>,
    ) -> Option<StrideNodeId<AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                TreeBitMapNode<AF, S>,
                u8,
            ) -> Option<StrideNodeId<AF>>,
        }

        let search_level_3 = impl_write_level![Stride3; id;];
        let search_level_4 = impl_write_level![Stride4; id;];
        let search_level_5 = impl_write_level![Stride5; id;];

        trace!("insert node {}: {:?}", id, next_node);
        match next_node {
            SizedStrideNode::Stride3(new_node) => (search_level_3.f)(
                &search_level_3,
                self.buckets.get_store3(id),
                new_node,
                0,
            ),
            SizedStrideNode::Stride4(new_node) => (search_level_4.f)(
                &search_level_4,
                self.buckets.get_store4(id),
                new_node,
                0,
            ),
            SizedStrideNode::Stride5(new_node) => (search_level_5.f)(
                &search_level_5,
                self.buckets.get_store5(id),
                new_node,
                0,
            ),
        }
    }

    #[allow(clippy::type_complexity, dead_code)]
    fn update_node(
        &self,
        id: StrideNodeId<AF>,
        updated_node: SizedStrideRefMut<AF>,
    ) {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                TreeBitMapNode<AF, S>,
                u8,
            ) -> Option<StrideNodeId<AF>>,
        }

        let search_level_3 = impl_write_level![Stride3; id;];
        let search_level_4 = impl_write_level![Stride4; id;];
        let search_level_5 = impl_write_level![Stride5; id;];

        match updated_node {
            SizedStrideRefMut::Stride3(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3(id),
                    new_node,
                    0,
                )
            }
            SizedStrideRefMut::Stride4(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4(id),
                    new_node,
                    0,
                )
            }
            SizedStrideRefMut::Stride5(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5(id),
                    new_node,
                    0,
                )
            }
        };
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

        match self.get_stride_for_id(id) {
            3 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3(id),
                    0,
                    guard,
                )
            }

            4 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4(id),
                    0,
                    guard,
                )
            }
            _ => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5(id),
                    0,
                    guard,
                )
            }
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_node_mut_with_guard(
        &'a self,
        id: StrideNodeId<AF>,
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

        let search_level_3 = impl_search_level_mut![Stride3; id;];
        let search_level_4 = impl_search_level_mut![Stride4; id;];
        let search_level_5 = impl_search_level_mut![Stride5; id;];

        match self.buckets.get_stride_for_id(id) {
            3 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3(id),
                    0,
                    guard,
                )
            }

            4 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4(id),
                    0,
                    guard,
                )
            }
            _ => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5(id),
                    0,
                    guard,
                )
            }
        }
    }

    pub(crate) fn get_root_node_id(&self) -> StrideNodeId<AF> {
        StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0)
    }

    pub fn get_nodes_len(&self) -> usize {
        0
    }

    // Prefixes related methods

    pub(crate) fn load_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial.load(Ordering::Acquire)
    }

    #[allow(dead_code)]
    fn increment_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial
            .fetch_add(1, Ordering::Acquire)
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
        record: InternalPrefixRecord<AF, Meta>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let guard = &epoch::pin();
        let (atomic_stored_prefix, level) = self
            .retrieve_prefix_mut_with_guard(
                PrefixId::new(record.net, record.len),
                guard,
            );

        match atomic_stored_prefix
            .0
            .load(Ordering::SeqCst, guard)
            .is_null()
        {
            true => {
                trace!("create new super-aggregated prefix record");
                let new_stored_prefix =
                    StoredPrefix::new::<PB>(record, level);

                match atomic_stored_prefix.0.compare_exchange(
                    atomic_stored_prefix.0.load(Ordering::SeqCst, guard),
                    Owned::new(new_stored_prefix),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    guard,
                ) {
                    Ok(pfx) => {
                        trace!("inserted new prefix record {:?}", &pfx);
                        return Ok(());
                    }
                    Err(stored_prefix) => {
                        trace!(
                            "prefix can't be inserted as new {:?}",
                            stored_prefix.current
                        );
                    }
                }
            }
            false => {
                trace!(
                    "existing super-aggregated prefix record for {}/{}",
                    record.net,
                    record.len
                );
                if let Some(inner_stored_prefix) =
                    atomic_stored_prefix.get_stored_prefix_mut(guard)
                {
                    let mut next_agg_record = inner_stored_prefix
                        .next_agg_record
                        .load(Ordering::SeqCst, guard);
                    match next_agg_record.is_null() {
                        true => {
                            trace!("no aggregation record. Create new aggregation record");
                            inner_stored_prefix
                                .atomic_update_aggregate(&record);
                        }
                        false => {
                            trace!("aggregation record exists. Update it");
                            let inner_next_agg_record =
                                unsafe { next_agg_record.deref_mut() };
                            let next_record = inner_next_agg_record
                                .next_record
                                .load(Ordering::SeqCst, guard);
                            let rec_hash_id = record.get_hash_id();
                            match next_record.is_null() {
                                true => {
                                    trace!("add record in the list (first entry).");
                                    inner_next_agg_record
                                        .atomic_tail_agg(record);
                                }
                                false => {
                                    trace!("look for matching unique routes list");
                                    let inner_next_record =
                                        unsafe { next_record.deref() };
                                    trace!(
                                        "next_record {:?}",
                                        inner_next_record
                                    );
                                    for next_rec in
                                        inner_next_record.iter(guard)
                                    {
                                        // Yes! You came to the right place! This is the
                                        // crux of the whole store.
                                        trace!(
                                            "{} == {}?",
                                            rec_hash_id,
                                            next_rec.get_hash_id()
                                        );
                                        match rec_hash_id
                                            == next_rec.get_hash_id()
                                        {
                                            // This is the same id, so we're going to prepend this record
                                            // to the linked-list of records.
                                            true => {
                                                trace!("found existing route for this record. prepend record to the list.");
                                                trace!(
                                                    "new record {}",
                                                    record
                                                );
                                                inner_next_agg_record
                                                    .atomic_prepend_record(
                                                        record,
                                                    );
                                                return Ok(());
                                            }

                                            false => {}
                                        }
                                    }

                                    trace!("Create new route list and add the record.");
                                    inner_next_agg_record
                                        .atomic_tail_agg(record);
                                }
                            }
                        }
                    }
                }
            }
        };

        Ok(())
    }

    // #[allow(clippy::type_complexity)]
    // pub(crate) fn upsert_prefix(
    //     &self,
    //     pfx_rec: InternalPrefixRecord<AF, Meta>,
    // ) -> Result<(), Box<dyn std::error::Error>> {
    //     let pfx_id = PrefixId::new(pfx_rec.net, pfx_rec.len);
    //     struct UpdateMeta<'s, AF: AddressFamily, M: routecore::record::Meta> {
    //         retry_record:
    //             &'s dyn for<'a> Fn(
    //                 &UpdateMeta<AF, M>,
    //                 &AtomicStoredPrefix<AF, M>,
    //                 Box<InternalPrefixRecord<AF, M>>,
    //                 u8,
    //             )
    //                 -> Result<(), Box<dyn std::error::Error>>,
    //     }

    //     let update_meta = UpdateMeta {
    //         retry_record: &|update_meta: &UpdateMeta<AF, Meta>,
    //                         // the memory location where we want to write our updated
    //                         // prefix
    //                         stored_prefix,
    //                         // the new record we want to write.
    //                         mut pfx_rec,
    //                         // the current level in this prefix-length set of arrays
    //                         level: u8| {
    //             // Load the prefix meta-data if any (Step 1)
    //             let guard = &epoch::pin();
    //             let inner_stored_prefix =
    //                 stored_prefix.0.load(Ordering::SeqCst, guard);
    //             let curr_prefix =
    //                 unsafe { inner_stored_prefix.into_owned().into_box() };
    //             // let curr_prefix = unsafe { unwrapped_curr_prefix.deref_mut() };
    //             let tag = inner_stored_prefix.tag();
    //             let pfx_rec = *pfx_rec;

    //             // fields for the new to-be-created StoredPrefix
    //             let mut prev_vert_list;
    //             let mut next_set = PrefixSet::empty();
    //             let mut prev_hor_list;
    //             let mut pfx_rec_hash = 0;
    //             let atomic_last_rec = curr_prefix
    //                 .super_agg_record
    //                 .load(Ordering::SeqCst, guard);
    //             let last_rec = unsafe { atomic_last_rec.deref() };

    //             match inner_stored_prefix.is_null() {
    //                 // There is no super_agg_record here, create it (Step 2).
    //                 // INSERT
    //                 true => {
    //                     // // start calculation size of next set
    //                     // let this_level =
    //                     //     PB::get_bits_for_len(pfx_id.get_len(), level);

    //                     // let next_level =
    //                     //     PB::get_bits_for_len(pfx_id.get_len(), level + 1);

    //                     // trace!(
    //                     //     "this level {} next level {}",
    //                     //     this_level,
    //                     //     next_level
    //                     // );
    //                     // let next_set = if next_level > 0 {
    //                     //     info!(
    //                     //         "INSERT with new bucket of size {} at prefix len {}",
    //                     //         1 << (next_level - this_level), pfx_id.get_len()
    //                     //     );
    //                     //     PrefixSet::init(
    //                     //         (1 << (next_level - this_level)) as usize,
    //                     //     )
    //                     // } else {
    //                     //     info!("INSERT at LAST LEVEL with empty bucket at prefix len {}", pfx_id.get_len());
    //                     //     PrefixSet::empty()
    //                     // };
    //                     // // End of calculation

    //                     // pfx_rec.meta = Some(
    //                     //     last_rec
    //                     //         .meta
    //                     //         .as_ref()
    //                     //         .unwrap()
    //                     //         .clone_merge_update(
    //                     //             &pfx_rec.meta.as_ref().unwrap(),
    //                     //         )?,
    //                     // );

    //                     match stored_prefix.0.compare_exchange(
    //                         inner_stored_prefix,
    //                         Owned::new(StoredPrefix {
    //                             serial: 1,
    //                             super_agg_record: Atomic::new(pfx_rec),
    //                             next_bucket: next_set,
    //                             next_agg_record: Atomic::new(StoredAggRecord::new(pfx_rec))
    //                             // next_agg_record: Atomic::new(
    //                             //     LinkedListRecord::new(pfx_rec),
    //                             // ),
    //                             // agg_list: Atomic::null(),
    //                         })
    //                         .with_tag(tag + 1),
    //                         Ordering::SeqCst,
    //                         Ordering::SeqCst,
    //                         guard,
    //                     ) {
    //                         Ok(_) => {
    //                             // SUCCESS! (Step 5)
    //                             // Nobody messed with our prefix meta-data in between
    //                             // us loading the tag and creating the entry with that
    //                             // tag.
    //                             trace!(
    //                                 "prefix successfully updated {:?}",
    //                                 pfx_id
    //                             );
    //                             Ok(())
    //                         }
    //                         Err(store_error) => {
    //                             // FAILURE (Step 6)
    //                             // Some other thread messed it up. Try again by
    //                             // upping a newly-read tag once more, reading
    //                             // the newly-current meta-data, updating it with
    //                             // our meta-data and see if it works then.
    //                             // rinse-repeat.
    //                             trace!(
    //                                 "Contention. Prefix update failed {:?}",
    //                                 pfx_id
    //                             );
    //                             // Try again. TODO: backoff neeeds to be implemented
    //                             // hers.
    //                             (update_meta.retry_record)(
    //                                 update_meta,
    //                                 store_error.current.into(),
    //                                 unsafe {
    //                                     store_error
    //                                         .new
    //                                         .super_agg_record
    //                                         .load(Ordering::SeqCst, guard)
    //                                         .into_owned()
    //                                         .into_box()
    //                                 },
    //                                 level,
    //                             )
    //                         }
    //                     };
    //                 }
    //                 // There is a prefix here, load the meta-data and merge
    //                 // it with our new data (Step 3)
    //                 // UPDATE
    //                 false => {
    //                     trace!("UPDATE");
    //                     // Check to see if we should be inserting in the
    //                     // "vertical" list, in case the hashes for the
    //                     // existing record and the newly created one are
    //                     // the same, or if we should be inserting into the
    //                     // "horizontal" list, in case of the hashes are
    //                     // different.

    //                     trace!(
    //                         "new hash {} exist hash {}",
    //                         pfx_rec.get_hash_id(),
    //                         last_rec.get_hash_id()
    //                     );
    //                     match pfx_rec.get_hash_id() == last_rec.get_hash_id()
    //                     {
    //                         // THE "VERTICAL" LIST
    //                         // Non-unique meta-data as defined by its hash.
    //                         // Create new record at the HEAD of the
    //                         // `prev_record` linked-list.
    //                         true => {
    //                             // Tuck the current record away on the heap.
    //                             // This doesn't have to be an atomic pointer, since
    //                             // we're doing this in one (atomic) transaction.
    //                             let next_list = curr_prefix
    //                                 .record_list
    //                                 .load(Ordering::SeqCst, guard);
    //                             let tag = next_list.tag();
    //                             match next_list.is_null() {
    //                                 true => {
    //                                     prev_vert_list =
    //                                         LinkedListRecord::new(
    //                                             pfx_rec, None,
    //                                         );
    //                                 }
    //                                 false => {
    //                                     prev_vert_list =
    //                                         unsafe { next_list.deref() }
    //                                             .prepend(pfx_rec);
    //                                 }
    //                             };
    //                             match curr_prefix
    //                                 .record_list
    //                                 .compare_exchange(
    //                                     next_list,
    //                                     Owned::new(prev_vert_list)
    //                                         .with_tag(tag + 1),
    //                                     Ordering::SeqCst,
    //                                     Ordering::SeqCst,
    //                                     guard,
    //                                 ) {
    //                                 Ok(_) => {
    //                                     // SUCCESS! (Step 5)
    //                                     // Nobody messed with our prefix meta-data in between
    //                                     // us loading the tag and creating the entry with that
    //                                     // tag.
    //                                     trace!("prefix successfully updated {:?}", pfx_id);
    //                                     return Ok(());
    //                                 }
    //                                 Err(store_error) => {
    //                                     // FAILURE (Step 6)
    //                                     // Some other thread messed it up. Try again by
    //                                     // upping a newly-read tag once more, reading
    //                                     // the newly-current meta-data, updating it with
    //                                     // our meta-data and see if it works then.
    //                                     // rinse-repeat.
    //                                     trace!(
    //                                             "Contention. Prefix update failed {:?}",
    //                                             pfx_id
    //                                         );
    //                                     // Try again. TODO: backoff neeeds to be implemented
    //                                     // hers.
    //                                     // return (update_meta.f)(
    //                                     //     update_meta,
    //                                     //     store_error.current.into(),
    //                                     //     store_error
    //                                     //         .new
    //                                     //         .record
    //                                     //         .clone()
    //                                     //         .unwrap(),
    //                                     //     level,
    //                                     // )
    //                                 }
    //                             }
    //                         }
    //                         // THE "HORIZONTAL" LIST
    //                         // Unique for the meta-data at this point.
    //                         // Move to the element in the `next_list`
    //                         // linked list, if any.
    //                         false => {
    //                             let next_list = curr_prefix
    //                                 .agg_list
    //                                 .load(Ordering::SeqCst, guard);
    //                             match next_list.is_null() {
    //                                 // No list exists, create the list and
    //                                 // fill the HEAD
    //                                 true => {
    //                                     trace!("create new horizontal list for unique hash_id {}", pfx_rec_hash);
    //                                     prev_hor_list = LinkedListRecord::new(
    //                                         pfx_rec, None,
    //                                     );
    //                                 }
    //                                 // A list exists, move to the HEAD of it,
    //                                 // and run this closure again.
    //                                 false => {
    //                                     // pfx_rec.meta = Some(
    //                                     //     curr_pfx_rec
    //                                     //         .meta
    //                                     //         .as_ref()
    //                                     //         .unwrap()
    //                                     //         .clone_merge_update(
    //                                     //             next_list
    //                                     //                 .record
    //                                     //                 .meta
    //                                     //                 .as_ref()
    //                                     //                 .unwrap(),
    //                                     //         )?,
    //                                     // );
    //                                     prev_hor_list =
    //                                         unsafe { next_list.deref() }
    //                                             .prepend(pfx_rec);
    //                                 }
    //                             };
    //                             match curr_prefix
    //                                 .record_list
    //                                 .compare_exchange(
    //                                     next_list,
    //                                     Owned::new(prev_vert_list)
    //                                         .with_tag(tag + 1),
    //                                     Ordering::SeqCst,
    //                                     Ordering::SeqCst,
    //                                     guard,
    //                                 ) {
    //                                 Ok(_) => {
    //                                     // SUCCESS! (Step 5)
    //                                     // Nobody messed with our prefix meta-data in between
    //                                     // us loading the tag and creating the entry with that
    //                                     // tag.
    //                                     trace!("prefix successfully updated {:?}", pfx_id);
    //                                     return Ok(());
    //                                 }
    //                                 Err(store_error) => {
    //                                     // FAILURE (Step 6)
    //                                     // Some other thread messed it up. Try again by
    //                                     // upping a newly-read tag once more, reading
    //                                     // the newly-current meta-data, updating it with
    //                                     // our meta-data and see if it works then.
    //                                     // rinse-repeat.
    //                                     trace!(
    //                                             "Contention. Prefix update failed {:?}",
    //                                             pfx_id
    //                                         );
    //                                     // Try again. TODO: backoff neeeds to be implemented
    //                                     // hers.
    //                                     // return (update_meta.f)(
    //                                     //     update_meta,
    //                                     //     store_error.current.into(),
    //                                     //     store_error
    //                                     //         .new
    //                                     //         .record
    //                                     //         .clone()
    //                                     //         .unwrap(),
    //                                     //     level,
    //                                     // )
    //                                 }
    //                             }
    //                         }
    //                     };
    //                 }
    //             };

    //             // START

    //             let atomic_stored_prefix =
    //                 stored_prefix.0.load(Ordering::SeqCst, guard);

    //             if atomic_stored_prefix.is_null() {
    //                 // start calculation size of next set
    //                 let this_level =
    //                     PB::get_bits_for_len(pfx_id.get_len(), level);

    //                 let next_level =
    //                     PB::get_bits_for_len(pfx_id.get_len(), level + 1);

    //                 trace!(
    //                     "this level {} next level {}",
    //                     this_level,
    //                     next_level
    //                 );
    //                 let next_set = if next_level > 0 {
    //                     info!(
    //                             "INSERT with new bucket of size {} at prefix len {}",
    //                             1 << (next_level - this_level), pfx_id.get_len()
    //                         );
    //                     PrefixSet::init(
    //                         (1 << (next_level - this_level)) as usize,
    //                     )
    //                 } else {
    //                     info!("INSERT at LAST LEVEL with empty bucket at prefix len {}", pfx_id.get_len());
    //                     PrefixSet::empty()
    //                 };
    //                 // End of calculation

    //                 // pfx_rec.meta = Some(
    //                 //     last_rec.meta.as_ref().unwrap().clone_merge_update(
    //                 //         &pfx_rec.meta.as_ref().unwrap(),
    //                 //     )?,
    //                 // );

    //                 match stored_prefix.0.compare_exchange(
    //                     atomic_stored_prefix,
    //                     Owned::new(StoredPrefix {
    //                         serial: 1,
    //                         hash_id: pfx_rec.get_hash_id(),
    //                         super_agg_record: Atomic::new(pfx_rec),
    //                         next_bucket: next_set,
    //                         record_list: Atomic::new(LinkedListRecord::new(
    //                             pfx_rec, None,
    //                         )),
    //                         agg_list: Atomic::null(),
    //                     })
    //                     .with_tag(tag + 1),
    //                     Ordering::SeqCst,
    //                     Ordering::SeqCst,
    //                     guard,
    //                 ) {
    //                     Ok(_) => {
    //                         // SUCCESS! (Step 5)
    //                         // Nobody messed with our prefix meta-data in between
    //                         // us loading the tag and creating the entry with that
    //                         // tag.
    //                         trace!(
    //                             "prefix successfully updated {:?}",
    //                             pfx_id
    //                         );
    //                         Ok(())
    //                     }
    //                     Err(store_error) => {
    //                         // FAILURE (Step 6)
    //                         // Some other thread messed it up. Try again by
    //                         // upping a newly-read tag once more, reading
    //                         // the newly-current meta-data, updating it with
    //                         // our meta-data and see if it works then.
    //                         // rinse-repeat.
    //                         trace!(
    //                             "Contention. Prefix update failed {:?}",
    //                             pfx_id
    //                         );
    //                         // Try again. TODO: backoff neeeds to be implemented
    //                         // hers.
    //                         (update_meta.retry_record)(
    //                             update_meta,
    //                             store_error.current.into(),
    //                             unsafe {
    //                                 store_error
    //                                     .new
    //                                     .super_agg_record
    //                                     .load(Ordering::SeqCst, guard)
    //                                     .into_owned()
    //                                     .into_box()
    //                             },
    //                             level,
    //                         )
    //                     }
    //                 };
    //             };

    //             // END OF THE START

    //             // The Atomic magic, see if we'll be able to save this new
    //             // meta-data in our store without some other thread having
    //             // updated it in the meantime (Step 4)
    //             match curr_prefix.super_agg_record.compare_exchange(
    //                 atomic_last_rec,
    //                 Owned::new(pfx_rec).with_tag(tag + 1),
    //                 Ordering::SeqCst,
    //                 Ordering::SeqCst,
    //                 guard,
    //             ) {
    //                 Ok(_) => {
    //                     // SUCCESS! (Step 5)
    //                     // Nobody messed with our prefix meta-data in between
    //                     // us loading the tag and creating the entry with that
    //                     // tag.
    //                     trace!("prefix successfully updated {:?}", pfx_id);
    //                     Ok(())
    //                 }
    //                 Err(store_error) => {
    //                     // FAILURE (Step 6)
    //                     // Some other thread messed it up. Try again by
    //                     // upping a newly-read tag once more, reading
    //                     // the newly-current meta-data, updating it with
    //                     // our meta-data and see if it works then.
    //                     // rinse-repeat.
    //                     trace!(
    //                         "Contention. Prefix update failed {:?}",
    //                         pfx_id
    //                     );
    //                     // Try again. TODO: backoff neeeds to be implemented
    //                     // hers.
    //                     (update_meta.retry_record)(
    //                         update_meta,
    //                         store_error.current.into(),
    //                         store_error.new.into_box(),
    //                         level,
    //                     )
    //                 }
    //             }
    //         },
    //     };

    //     let guard = &epoch::pin();
    //     trace!("UPSERT PREFIX {:?}", pfx_rec);

    //     let (stored_prefix, level) =
    //         self.retrieve_prefix_mut_with_guard(pfx_id, guard);

    //     (update_meta.retry_record)(
    //         &update_meta,
    //         stored_prefix,
    //         Box::new(pfx_rec),
    //         level,
    //     )
    // }

    #[allow(clippy::type_complexity)]
    fn retrieve_prefix_mut_with_guard(
        &'a self,
        id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> (&'a AtomicStoredPrefix<AF, Meta>, u8) {
        struct SearchLevel<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                u8,
                &'a Guard,
            )
                -> (&'a AtomicStoredPrefix<AF, M>, u8),
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, Meta>,
                 prefix_set: &PrefixSet<AF, Meta>,
                 mut level: u8,
                 guard: &Guard| {
                // HASHING FUNCTION
                let index = Self::hash_prefix_id(id, level);

                trace!("retrieve prefix with guard");

                let prefixes =
                    prefix_set.0.load(Ordering::Relaxed, guard);
                trace!(
                    "prefixes at level {}? {:?}",
                    level,
                    !prefixes.is_null()
                );
                let prefix_ref = unsafe { &prefixes.deref()[index] };
                let stored_prefix = unsafe { prefix_ref.assume_init_ref() };

                if let Some(StoredPrefix {
                    super_agg_record: pfx_rec,
                    prefix,
                    next_bucket,
                    ..
                }) = stored_prefix.get_stored_prefix(guard)
                {
                    if let Some(pfx_rec) = pfx_rec.get_record(guard) {
                        if id == pfx_rec.into() {
                            trace!("found requested prefix {:?}", id);
                            return (stored_prefix, level);
                        } else {
                            level += 1;
                            return (search_level.f)(
                                search_level,
                                next_bucket,
                                level,
                                guard,
                            );
                        }
                    };
                }
                // No record at the deepest level, still we're returning a reference to it,
                // so the caller can insert a new record here.
                (stored_prefix, level)
            },
        };

        (search_level.f)(
            &search_level,
            self.prefixes.get_root_prefix_set(id.get_len()),
            0,
            guard,
        )
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn retrieve_prefix(
        &self,
        id: PrefixId<AF>,
    ) -> Option<InternalPrefixRecord<AF, Meta>> {
        let guard = epoch::pin();
        struct SearchLevel<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                u8,
                &'a Guard,
            )
                -> Option<InternalPrefixRecord<AF, M>>,
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, Meta>,
                 prefix_set: &PrefixSet<AF, Meta>,
                 mut level: u8,
                 guard: &Guard| {
                // HASHING FUNCTION
                let index = Self::hash_prefix_id(id, level);

                let mut prefixes =
                    prefix_set.0.load(Ordering::Relaxed, guard);
                let prefix_ref = unsafe { &mut prefixes.deref_mut()[index] };

                if let Some(StoredPrefix {
                    super_agg_record: pfx_rec,
                    next_bucket: next_set,
                    ..
                }) = unsafe { prefix_ref.assume_init_ref() }
                    .get_stored_prefix(guard)
                {
                    if let Some(pfx_rec) = pfx_rec.get_record(guard) {
                        if id == PrefixId::from(pfx_rec) {
                            trace!("found requested prefix {:?}", id);
                            return Some(pfx_rec.clone());
                        };
                        level += 1;
                        return (search_level.f)(
                            search_level,
                            &next_set,
                            level,
                            guard,
                        );
                    }
                }

                None
            },
        };

        (search_level.f)(
            &search_level,
            self.prefixes.get_root_prefix_set(id.get_len()),
            0,
            &guard,
        )
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn non_recursive_retrieve_prefix_with_guard(
        &'a self,
        id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> (
        Option<(&InternalPrefixRecord<AF, Meta>, &'a usize)>,
        Option<(
            PrefixId<AF>,
            u8,
            &'a PrefixSet<AF, Meta>,
            [Option<(&'a PrefixSet<AF, Meta>, usize)>; 26],
            usize,
        )>,
    ) {
        let mut prefix_set = self.prefixes.get_root_prefix_set(id.get_len());
        let mut parents = [None; 26];
        let mut level: u8 = 0;

        loop {
            // The index of the prefix in this array (at this len and
            // level) is calculated by performing the hash function
            // over the prefix.

            // HASHING FUNCTION
            let index = Self::hash_prefix_id(id, level);

            let mut prefixes = prefix_set.0.load(Ordering::Relaxed, guard);
            let prefix_ref = unsafe { &mut prefixes.deref_mut()[index] };
            if let Some(StoredPrefix {
                serial,
                super_agg_record: pfx_rec,
                next_bucket: next_set,
                ..
            }) = unsafe { prefix_ref.assume_init_ref() }
                .get_stored_prefix(guard)
            {
                if let Some(pfx_rec) = pfx_rec.get_record(guard) {
                    if id == PrefixId::new(pfx_rec.net, pfx_rec.len) {
                        trace!("found requested prefix {:?}", id);
                        parents[level as usize] = Some((prefix_set, index));
                        return (
                            Some((pfx_rec, serial)),
                            Some((id, level, prefix_set, parents, index)),
                        );
                    };
                    // Advance to the next level.
                    prefix_set = next_set;
                    level += 1;
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
        id: PrefixId<AF>,
        guard: &'a Guard,
    ) -> Option<(&InternalPrefixRecord<AF, Meta>, &'a usize)> {
        struct SearchLevel<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                u8,
                &'a Guard,
            ) -> Option<(
                &'a InternalPrefixRecord<AF, M>,
                &'a usize,
            )>,
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, Meta>,
                 prefix_set: &PrefixSet<AF, Meta>,
                 mut level: u8,
                 guard: &Guard| {
                // HASHING FUNCTION
                let index = Self::hash_prefix_id(id, level);

                let prefixes = prefix_set.0.load(Ordering::Relaxed, guard);
                // trace!("nodes {:?}", unsafe { unwrapped_nodes.deref_mut().len() });
                let prefix_ref = unsafe { &prefixes.deref()[index] };
                if let Some(StoredPrefix {
                    serial,
                    super_agg_record: pfx_rec,
                    next_bucket: next_set,
                    ..
                }) = unsafe { prefix_ref.assume_init_ref() }
                    .get_stored_prefix(guard)
                {
                    if let Some(pfx_rec) = pfx_rec.get_record(guard) {
                        if id == PrefixId::new(pfx_rec.net, pfx_rec.len) {
                            trace!("found requested prefix {:?}", id);
                            return Some((pfx_rec, serial));
                        };
                        level += 1;
                        (search_level.f)(
                            search_level,
                            next_set,
                            level,
                            guard,
                        );
                    };
                }
                None
            },
        };

        (search_level.f)(
            &search_level,
            self.prefixes.get_root_prefix_set(id.get_len()),
            0,
            guard,
        )
    }

    #[allow(dead_code)]
    fn remove_prefix(&mut self, index: PrefixId<AF>) -> Option<Meta> {
        match index.is_empty() {
            false => self.prefixes.remove(index),
            true => None,
        }
    }

    pub fn get_prefixes_len(&self) -> usize {
        (0..=AF::BITS)
            .map(|pfx_len| -> usize {
                self.prefixes
                    .get_root_prefix_set(pfx_len)
                    .get_len_recursive()
            })
            .sum()
    }

    // Stride related methods

    pub(crate) fn get_stride_for_id(&self, id: StrideNodeId<AF>) -> u8 {
        self.buckets.get_stride_for_id(id)
    }

    pub fn get_stride_sizes(&self) -> &[u8] {
        self.buckets.get_stride_sizes()
    }

    pub(crate) fn get_strides_len() -> u8 {
        NB::get_strides_len()
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
                    BitSpan::new(
                        ((prefix.get_net() << node_len)
                            >> (AF::BITS - (prefix.get_len() - node_len)))
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
    // because we're keeping we're having lots of collisions, but we don't
    // care!
    //
    // We're using a part of bitarray representation of the address part of
    // a prefixas the as the hash. Sounds complicated, but isn't.
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
    // you ask? Because the hash is used by teh CustomAllocStorage as the
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
        // Aaaaand, this is all of our hashing function.
        // I'll explain later.
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
        // Aaaaand, this is all of our hashing function.
        // I'll explain later.
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
