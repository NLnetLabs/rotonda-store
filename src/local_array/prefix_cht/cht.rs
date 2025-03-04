use crossbeam_epoch::Guard;
use crossbeam_utils::Backoff;
use inetnum::addr::Prefix;
use log::{debug, log_enabled, trace};
use roaring::RoaringBitmap;

use crate::{
    local_array::in_memory::atomic_types::{
        bits_for_len, Cht, MultiMapValue, PrefixSet, StoredPrefix,
    },
    prelude::multi::{PrefixId, PrefixStoreError},
    rib::UpsertReport,
    AddressFamily, Meta, PublicRecord,
};

#[derive(Debug)]
pub(crate) struct PrefixCht<
    AF: AddressFamily,
    M: Meta,
    const ROOT_SIZE: usize,
>(Cht<PrefixSet<AF, M>, ROOT_SIZE, 1>);

impl<AF: AddressFamily, M: Meta, const ROOT_SIZE: usize>
    PrefixCht<AF, M, ROOT_SIZE>
{
    pub(crate) fn init() -> Self {
        Self(<Cht<PrefixSet<AF, M>, ROOT_SIZE, 1>>::init())
    }

    pub(crate) fn get_records_for_prefix(
        &self,
        prefix: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Vec<PublicRecord<M>>> {
        let mut prefix_set = self.0.root_for_len(prefix.get_len());
        let mut level: u8 = 0;
        let backoff = Backoff::new();

        loop {
            // The index of the prefix in this array (at this len and
            // level) is calculated by performing the hash function
            // over the prefix.

            // HASHING FUNCTION
            let index = Self::hash_prefix_id(prefix, level);

            if let Some(stored_prefix) = prefix_set.0.get(index) {
                if prefix == stored_prefix.get_prefix_id() {
                    if log_enabled!(log::Level::Trace) {
                        trace!(
                            "found requested prefix {} ({:?})",
                            Prefix::from(prefix),
                            prefix
                        );
                    }

                    return stored_prefix.record_map.get_filtered_records(
                        mui,
                        include_withdrawn,
                        bmin,
                    );
                };

                // Advance to the next level.
                prefix_set = &stored_prefix.next_bucket;
                level += 1;
                backoff.spin();
                continue;
            }

            trace!("no prefix found for {:?}", prefix);
            return None;
        }
    }

    pub(crate) fn upsert_prefix(
        &self,
        prefix: PrefixId<AF>,
        record: PublicRecord<M>,
        update_path_selections: Option<M::TBI>,
        guard: &Guard,
    ) -> Result<(UpsertReport, Option<MultiMapValue<M>>), PrefixStoreError>
    {
        let mut prefix_is_new = true;
        let mut mui_is_new = true;

        let (mui_count, cas_count) =
            match self.non_recursive_retrieve_prefix_mut(prefix) {
                // There's no StoredPrefix at this location yet. Create a new
                // PrefixRecord and try to store it in the empty slot.
                (stored_prefix, false) => {
                    if log_enabled!(log::Level::Debug) {
                        debug!(
                            "{} store: Create new prefix record",
                            std::thread::current()
                                .name()
                                .unwrap_or("unnamed-thread")
                        );
                    }

                    let (mui_count, retry_count) =
                        stored_prefix.record_map.upsert_record(record)?;

                    // See if someone beat us to creating the record.
                    if mui_count.is_some() {
                        mui_is_new = false;
                        prefix_is_new = false;
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
                        stored_prefix.record_map.upsert_record(record)?;
                    mui_is_new = mui_count.is_none();

                    if let Some(tbi) = update_path_selections {
                        stored_prefix
                            .calculate_and_store_best_backup(&tbi, guard)?;
                    }

                    (mui_count, retry_count)
                }
            };

        let count = mui_count.as_ref().map(|m| m.1).unwrap_or(1);
        Ok((
            UpsertReport {
                prefix_new: prefix_is_new,
                cas_count,
                mui_new: mui_is_new,
                mui_count: count,
            },
            mui_count.map(|m| m.0),
        ))
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
    pub(crate) fn non_recursive_retrieve_prefix_mut(
        &self,
        search_prefix_id: PrefixId<AF>,
    ) -> (&StoredPrefix<AF, M>, bool) {
        trace!("non_recursive_retrieve_prefix_mut_with_guard");
        let mut prefix_set = self.0.root_for_len(search_prefix_id.get_len());
        let mut level: u8 = 0;

        trace!("root prefix_set {:?}", prefix_set);
        loop {
            // HASHING FUNCTION
            let index = Self::hash_prefix_id(search_prefix_id, level);

            // probe the slot with the index that's the result of the hashing.
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
                    let index = Self::hash_prefix_id(search_prefix_id, level);
                    trace!("calculate next index {}", index);
                    let var_name = (
                        prefix_set
                            .0
                            .get_or_init(index, || {
                                StoredPrefix::new(
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
                if log_enabled!(log::Level::Trace) {
                    trace!(
                        "found requested prefix {} ({:?})",
                        Prefix::from(search_prefix_id),
                        search_prefix_id
                    );
                }
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
        let mut prefix_set = self.0.root_for_len(id.get_len());
        let mut parents = [None; 32];
        let mut level: u8 = 0;
        let backoff = Backoff::new();

        loop {
            // The index of the prefix in this array (at this len and
            // level) is calculated by performing the hash function
            // over the prefix.

            // HASHING FUNCTION
            let index = Self::hash_prefix_id(id, level);

            if let Some(stored_prefix) = prefix_set.0.get(index) {
                if id == stored_prefix.get_prefix_id() {
                    if log_enabled!(log::Level::Trace) {
                        trace!(
                            "found requested prefix {} ({:?})",
                            Prefix::from(id),
                            id
                        );
                    }
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

    pub(crate) fn hash_prefix_id(id: PrefixId<AF>, level: u8) -> usize {
        // And, this is all of our hashing function.
        let last_level = if level > 0 {
            bits_for_len(id.get_len(), level - 1)
        } else {
            0
        };
        let this_level = bits_for_len(id.get_len(), level);
        // trace!(
        //     "bits division {}; no of bits {}",
        //     this_level,
        //     this_level - last_level
        // );
        // trace!(
        //     "calculated index ({} << {}) >> {}",
        //     id.get_net(),
        //     last_level,
        //     ((<AF>::BITS - (this_level - last_level)) % <AF>::BITS) as usize
        // );
        // HASHING FUNCTION
        ((id.get_net() << AF::from_u32(last_level as u32))
            >> AF::from_u8(
                (<AF>::BITS - (this_level - last_level)) % <AF>::BITS,
            ))
        .dangerously_truncate_to_u32() as usize
    }
}
