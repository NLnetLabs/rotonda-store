use crossbeam_epoch::{self as epoch};
use epoch::Guard;

use crate::af::AddressFamily;
use crate::local_array::in_memory::atomic_types::{
    NodeBuckets, PrefixBuckets,
};
use crate::prefix_record::PublicRecord;
use crate::rib::{PersistStrategy, Rib};
use inetnum::addr::Prefix;

use crate::{Meta, QueryResult};

use crate::{MatchOptions, MatchType};

use super::errors::PrefixStoreError;
use super::types::{PrefixId, RouteStatus};

//------------ Prefix Matching ----------------------------------------------

impl<'a, AF, M, NB, PB, const PREFIX_SIZE: usize, const KEY_SIZE: usize>
    Rib<AF, M, NB, PB, PREFIX_SIZE, KEY_SIZE>
where
    AF: AddressFamily,
    M: Meta,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, M>,
{
    pub fn more_specifics_from(
        &'a self,
        prefix_id: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        let result =
            self.in_memory_tree.non_recursive_retrieve_prefix(prefix_id);
        let prefix = result.0;
        let more_specifics_vec = self.more_specific_prefix_iter_from(
            prefix_id,
            mui,
            include_withdrawn,
            guard,
        );

        QueryResult {
            prefix: if let Some(pfx) = prefix {
                Prefix::new(
                    pfx.prefix.get_net().into_ipaddr(),
                    pfx.prefix.get_len(),
                )
                .ok()
            } else {
                None
            },
            prefix_meta: prefix
                .map(|r| self.get_filtered_records(r, mui, guard))
                .unwrap_or_default(),
            match_type: MatchType::EmptyMatch,
            less_specifics: None,
            more_specifics: Some(more_specifics_vec.collect()),
        }
    }

    pub fn less_specifics_from(
        &'a self,
        prefix_id: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        let result =
            self.in_memory_tree.non_recursive_retrieve_prefix(prefix_id);

        let prefix = result.0;
        let less_specifics_vec = result.1.map(
            |(prefix_id, _level, _cur_set, _parents, _index)| {
                self.less_specific_prefix_iter(
                    prefix_id,
                    mui,
                    include_withdrawn,
                    guard,
                )
            },
        );

        QueryResult {
            prefix: if let Some(pfx) = prefix {
                Prefix::new(
                    pfx.prefix.get_net().into_ipaddr(),
                    pfx.prefix.get_len(),
                )
                .ok()
            } else {
                None
            },
            prefix_meta: prefix
                .map(|r| self.get_filtered_records(r, mui, guard))
                .unwrap_or_default(),
            match_type: MatchType::EmptyMatch,
            less_specifics: less_specifics_vec.map(|iter| iter.collect()),
            more_specifics: None,
        }
    }

    pub fn more_specifics_iter_from(
        &'a self,
        prefix_id: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> Result<
        impl Iterator<Item = (PrefixId<AF>, Vec<PublicRecord<M>>)> + 'a,
        PrefixStoreError,
    > {
        let bmin = self.withdrawn_muis_bmin(guard);

        if mui.is_some() && bmin.contains(mui.unwrap()) {
            Err(PrefixStoreError::PrefixNotFound)
        } else {
            Ok(self.more_specific_prefix_iter_from(
                prefix_id,
                mui,
                include_withdrawn,
                guard,
            ))
        }
    }

    pub fn match_prefix(
        &'a self,
        search_pfx: PrefixId<AF>,
        options: &MatchOptions,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        match self.config.persist_strategy() {
            PersistStrategy::PersistOnly => {
                self.match_prefix_in_persisted_store(search_pfx, options.mui)
            }
            _ => self.match_prefix_in_memory(search_pfx, options, guard),
        }
    }

    fn match_prefix_in_memory(
        &'a self,
        search_pfx: PrefixId<AF>,
        options: &MatchOptions,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        // `non_recursive_retrieve_prefix` returns an exact match only, so no
        // longest matching prefix!
        let mut stored_prefix = self
            .in_memory_tree
            .non_recursive_retrieve_prefix(search_pfx)
            .0
            .map(|pfx| {
                (
                    pfx.prefix,
                    if !options.include_withdrawn {
                        // Filter out all the withdrawn records, both
                        // with globally withdrawn muis, and with local
                        // statuses
                        // set to Withdrawn.
                        self.get_filtered_records(pfx, options.mui, guard)
                            .into_iter()
                            .collect()
                    } else {
                        // Do no filter out any records, but do rewrite
                        // the local statuses of the records with muis
                        // that appear in the specified bitmap index.
                        pfx.record_map.as_records_with_rewritten_status(
                            self.withdrawn_muis_bmin(guard),
                            RouteStatus::Withdrawn,
                        )
                    },
                )
            });

        // Check if we have an actual exact match, if not then fetch the
        // first lesser-specific with the greatest length, that's the Longest
        // matching prefix, but only if the user requested a longest match or
        // empty match.
        let match_type = match (&options.match_type, &stored_prefix) {
            // we found an exact match, we don't need to do anything.
            (_, Some((_pfx, meta))) if !meta.is_empty() => {
                MatchType::ExactMatch
            }
            // we didn't find an exact match, but the user requested it
            // so we need to find the longest matching prefix.
            (MatchType::LongestMatch | MatchType::EmptyMatch, _) => {
                stored_prefix = self
                    .less_specific_prefix_iter(
                        search_pfx,
                        options.mui,
                        options.include_withdrawn,
                        guard,
                    )
                    .max_by(|p0, p1| p0.0.get_len().cmp(&p1.0.get_len()));
                if stored_prefix.is_some() {
                    MatchType::LongestMatch
                } else {
                    MatchType::EmptyMatch
                }
            }
            // We got an empty match, but the user requested an exact match,
            // even so, we're going to look for more and/or less specifics if
            // the user asked for it.
            (MatchType::ExactMatch, _) => MatchType::EmptyMatch,
        };

        QueryResult {
            prefix: stored_prefix.as_ref().map(|p| p.0.into_pub()),
            prefix_meta: stored_prefix
                .as_ref()
                .map(|pfx| pfx.1.clone())
                .unwrap_or_default(),
            less_specifics: if options.include_less_specifics {
                Some(
                    self.less_specific_prefix_iter(
                        if let Some(ref pfx) = stored_prefix {
                            pfx.0
                        } else {
                            search_pfx
                        },
                        options.mui,
                        options.include_withdrawn,
                        guard,
                    )
                    .collect(),
                )
            } else {
                None
            },
            more_specifics: if options.include_more_specifics {
                Some(
                    self.more_specific_prefix_iter_from(
                        if let Some(pfx) = stored_prefix {
                            pfx.0
                        } else {
                            search_pfx
                        },
                        options.mui,
                        options.include_withdrawn,
                        guard,
                    )
                    .collect(),
                )
                // The user requested more specifics, but there aren't any, so
                // we need to return an empty vec, not a None.
            } else {
                None
            },
            match_type,
        }
    }

    pub fn best_path(
        &'a self,
        search_pfx: PrefixId<AF>,
        guard: &Guard,
    ) -> Option<Result<PublicRecord<M>, PrefixStoreError>> {
        self.in_memory_tree
            .non_recursive_retrieve_prefix(search_pfx)
            .0
            .map(|p_rec| {
                p_rec.get_path_selections(guard).best().map_or_else(
                    || Err(PrefixStoreError::BestPathNotFound),
                    |mui| {
                        p_rec
                            .record_map
                            .get_record_for_active_mui(mui)
                            .ok_or(PrefixStoreError::StoreNotReadyError)
                    },
                )
            })
    }

    pub fn calculate_and_store_best_and_backup_path(
        &self,
        search_pfx: PrefixId<AF>,
        tbi: &<M as Meta>::TBI,
        guard: &Guard,
    ) -> Result<(Option<u32>, Option<u32>), PrefixStoreError> {
        self.in_memory_tree
            .non_recursive_retrieve_prefix(search_pfx)
            .0
            .map_or(Err(PrefixStoreError::StoreNotReadyError), |p_rec| {
                p_rec.calculate_and_store_best_backup(tbi, guard)
            })
    }

    pub fn is_ps_outdated(
        &self,
        search_pfx: PrefixId<AF>,
        guard: &Guard,
    ) -> Result<bool, PrefixStoreError> {
        self.in_memory_tree
            .non_recursive_retrieve_prefix(search_pfx)
            .0
            .map_or(Err(PrefixStoreError::StoreNotReadyError), |p| {
                Ok(p.is_ps_outdated(guard))
            })
    }
}
