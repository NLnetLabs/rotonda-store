use crossbeam_epoch::{self as epoch};
use epoch::Guard;

use crate::af::AddressFamily;
use crate::local_array::in_memory::atomic_types::{
    NodeBuckets, PrefixBuckets,
};
use crate::prefix_record::PublicRecord;
use crate::rib::{PersistStrategy, Rib};
use inetnum::addr::Prefix;

use crate::{IncludeHistory, Meta, QueryResult};

use crate::{MatchOptions, MatchType};

use super::errors::PrefixStoreError;
use super::types::PrefixId;

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
        let more_specifics_vec =
            self.in_memory_tree.more_specific_prefix_iter_from(
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
                .map(|r| {
                    r.record_map.get_filtered_records(
                        mui,
                        self.in_memory_tree.withdrawn_muis_bmin(guard),
                    )
                })
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
                self.in_memory_tree.less_specific_prefix_iter(
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
                .map(|r| {
                    r.record_map.get_filtered_records(
                        mui,
                        self.in_memory_tree.withdrawn_muis_bmin(guard),
                    )
                })
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
        let bmin = self.in_memory_tree.withdrawn_muis_bmin(guard);

        if mui.is_some() && bmin.contains(mui.unwrap()) {
            Err(PrefixStoreError::PrefixNotFound)
        } else {
            Ok(self.in_memory_tree.more_specific_prefix_iter_from(
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
            // Everything is in memory only, so look there. There can be no
            // historical records for this variant, so we just return the
            // stuff found in memeory. A request for  historical records
            // willbe ignored.
            PersistStrategy::MemoryOnly => self
                .in_memory_tree
                .match_prefix(search_pfx, options, guard)
                .into(),
            // All the records are persisted, they have never been committed
            // to memory. However the in-memory-tree is still used to indicate
            // which (prefix, mui) tuples have been created.
            PersistStrategy::PersistOnly => {
                if options.match_type == MatchType::ExactMatch
                    && !self.contains(search_pfx, options.mui)
                {
                    return QueryResult::empty();
                }

                if let Some(persist_tree) = &self.persist_tree {
                    persist_tree
                        .match_prefix(
                            self.in_memory_tree
                                .match_prefix_by_tree_traversal(
                                    search_pfx, options,
                                ),
                            options,
                        )
                        .into()
                } else {
                    QueryResult::empty()
                }
            }
            // We have the current records in memory, additionally they may be
            // encriched with historical data from the persisted data.
            PersistStrategy::WriteAhead => {
                let mut res = self
                    .in_memory_tree
                    .match_prefix(search_pfx, options, guard);
                if let Some(persist_tree) = &self.persist_tree {
                    match options.include_history {
                        IncludeHistory::All => {
                            if self.contains(search_pfx, options.mui) {
                                res.prefix_meta.extend(
                                    persist_tree.get_records_for_prefix(
                                        search_pfx,
                                        options.mui,
                                    ),
                                );
                            }

                            if let Some(ls) = &mut res.less_specifics {
                                for rec in ls {
                                    if self.contains(rec.0, options.mui) {
                                        // write ahead has the current record,
                                        // as well as the historical ones, so
                                        // we just override the recs from the
                                        // in-memory tree.
                                        rec.1 = persist_tree
                                            .get_records_for_prefix(
                                                rec.0,
                                                options.mui,
                                            );
                                    }
                                }
                            };

                            if let Some(rs) = &mut res.more_specifics {
                                for rec in rs {
                                    if self.contains(rec.0, options.mui) {
                                        rec.1 = persist_tree
                                            .get_records_for_prefix(
                                                rec.0,
                                                options.mui,
                                            );
                                    }
                                }
                            };

                            res.into()
                        }
                        IncludeHistory::SearchPrefix => {
                            if self.contains(search_pfx, options.mui) {
                                res.prefix_meta.extend(
                                    persist_tree.get_records_for_prefix(
                                        search_pfx,
                                        options.mui,
                                    ),
                                );
                            }

                            res.into()
                        }
                        IncludeHistory::None => res.into(),
                    }
                } else {
                    res.into()
                }
            }
            // All current info is in memory so look there. If the user has
            // requested historical records, we will ask the persist_tree to
            // add those to the intermedidate result of the in-memory query.
            PersistStrategy::PersistHistory => {
                let mut res = self
                    .in_memory_tree
                    .match_prefix(search_pfx, options, guard);

                if let Some(persist_tree) = &self.persist_tree {
                    match options.include_history {
                        IncludeHistory::All => {
                            res.prefix_meta.extend(
                                persist_tree.get_records_for_prefix(
                                    search_pfx,
                                    options.mui,
                                ),
                            );

                            if let Some(ls) = &mut res.less_specifics {
                                for rec in ls {
                                    if self.contains(rec.0, options.mui) {
                                        // the persisted tree only has the
                                        // historical records, so we extend
                                        // the vec. It should already hold the
                                        // current record.
                                        rec.1.extend(
                                            persist_tree
                                                .get_records_for_prefix(
                                                    rec.0,
                                                    options.mui,
                                                ),
                                        );
                                    }
                                }
                            };

                            if let Some(rs) = &mut res.more_specifics {
                                for rec in rs {
                                    if self.contains(rec.0, options.mui) {
                                        rec.1.extend(
                                            persist_tree
                                                .get_records_for_prefix(
                                                    rec.0,
                                                    options.mui,
                                                ),
                                        );
                                    }
                                }
                            };

                            res.into()
                        }
                        IncludeHistory::SearchPrefix => {
                            if self.contains(search_pfx, options.mui) {
                                res.prefix_meta.extend(
                                    persist_tree.get_records_for_prefix(
                                        search_pfx,
                                        options.mui,
                                    ),
                                );
                            }

                            res.into()
                        }
                        IncludeHistory::None => res.into(),
                    }
                } else {
                    res.into()
                }
            }
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

pub(crate) struct TreeQueryResult<AF: AddressFamily> {
    pub match_type: MatchType,
    pub prefix: Option<PrefixId<AF>>,
    pub less_specifics: Option<Vec<PrefixId<AF>>>,
    pub more_specifics: Option<Vec<PrefixId<AF>>>,
}

pub(crate) type FamilyRecord<AF, M> =
    Vec<(PrefixId<AF>, Vec<PublicRecord<M>>)>;

pub(crate) struct FamilyQueryResult<AF: AddressFamily, M: Meta> {
    pub match_type: MatchType,
    pub prefix: Option<PrefixId<AF>>,
    pub prefix_meta: Vec<PublicRecord<M>>,
    pub less_specifics: Option<FamilyRecord<AF, M>>,
    pub more_specifics: Option<FamilyRecord<AF, M>>,
}

impl<AF: AddressFamily, M: Meta> From<FamilyQueryResult<AF, M>>
    for QueryResult<M>
{
    fn from(value: FamilyQueryResult<AF, M>) -> Self {
        QueryResult {
            match_type: value.match_type,
            prefix: value.prefix.map(|p| p.into()),
            prefix_meta: value.prefix_meta,
            less_specifics: value
                .less_specifics
                .map(|ls| ls.into_iter().collect()),
            more_specifics: value
                .more_specifics
                .map(|ms| ms.into_iter().collect()),
        }
    }
}
