use crossbeam_epoch::{self as epoch};
use epoch::Guard;
use log::{log_enabled, trace};

use crate::af::AddressFamily;
use crate::local_array::in_memory::atomic_types::{
    NodeBuckets, PrefixBuckets,
};
use crate::rib::{PersistStrategy, Rib};
use crate::{PublicRecord, RecordSet};
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
    pub fn get_value(
        &'a self,
        prefix_id: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> Option<Vec<PublicRecord<M>>> {
        match self.persist_strategy() {
            PersistStrategy::PersistOnly => {
                self.persist_tree.as_ref().map(|tree| {
                    tree.get_records_for_prefix(
                        prefix_id,
                        mui,
                        include_withdrawn,
                        self.in_memory_tree.withdrawn_muis_bmin(guard),
                    )
                })
            }
            _ => self.prefix_cht.get_records_for_prefix(
                prefix_id,
                mui,
                include_withdrawn,
                self.in_memory_tree.withdrawn_muis_bmin(guard),
            ),
        }
    }

    pub fn more_specifics_from(
        &'a self,
        prefix_id: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        let prefix = if !self.contains(prefix_id, mui) {
            Some(Prefix::from(prefix_id))
        } else {
            None
        };
        let more_specifics = self
            .in_memory_tree
            .more_specific_prefix_iter_from(prefix_id)
            .map(|p| {
                self.get_value(prefix_id, mui, include_withdrawn, guard)
                    .map(|v| (p, v))
            })
            .collect();

        QueryResult {
            prefix,
            prefix_meta: prefix
                .map(|_pfx| {
                    self.get_value(prefix_id, mui, include_withdrawn, guard)
                        .unwrap_or_default()
                })
                .unwrap_or(vec![]),
            match_type: MatchType::EmptyMatch,
            less_specifics: None,
            more_specifics,
        }
    }

    pub fn less_specifics_from(
        &'a self,
        prefix_id: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        let prefix = if !self.contains(prefix_id, mui) {
            Some(Prefix::from(prefix_id))
        } else {
            None
        };

        let less_specifics = self
            .in_memory_tree
            .less_specific_prefix_iter(prefix_id)
            .map(|p| {
                self.get_value(prefix_id, mui, include_withdrawn, guard)
                    .map(|v| (p, v))
            })
            .collect();

        QueryResult {
            prefix,
            prefix_meta: self
                .get_value(prefix_id, mui, include_withdrawn, guard)
                .unwrap_or_default(),
            match_type: MatchType::EmptyMatch,
            less_specifics,
            more_specifics: None,
        }
    }

    // pub fn more_specifics_keys_from(
    //     &self,
    //     prefix_id: PrefixId<AF>,
    // ) -> impl Iterator<Item = PrefixId<AF>> + '_ {
    //     self.in_memory_tree
    //         .more_specific_prefix_iter_from(prefix_id)
    // }

    pub fn more_specifics_iter_from(
        &'a self,
        prefix_id: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> impl Iterator<Item = (PrefixId<AF>, Vec<PublicRecord<M>>)> + 'a {
        println!("more_specifics_iter_from fn");
        // If the user wanted a specific mui and not withdrawn prefixes, we
        // may return early if the mui is globally withdrawn.
        (if mui.is_some_and(|m| {
            !include_withdrawn && self.mui_is_withdrawn(m, guard)
        }) {
            None
        } else {
            Some(
                self.in_memory_tree
                    .more_specific_prefix_iter_from(prefix_id)
                    .filter_map(move |p| {
                        self.get_value(p, mui, include_withdrawn, guard)
                            .map(|v| (p, v))
                    }),
            )
        })
        .into_iter()
        .flatten()
        .chain(
            (if mui.is_some_and(|m| {
                self.config.persist_strategy == PersistStrategy::WriteAhead
                    || (!include_withdrawn && self.mui_is_withdrawn(m, guard))
            }) {
                None
            } else {
                let global_withdrawn_bmin =
                    self.in_memory_tree.withdrawn_muis_bmin(guard);
                self.persist_tree.as_ref().map(|persist_tree| {
                    persist_tree.more_specific_prefix_iter_from(
                        prefix_id,
                        vec![],
                        mui,
                        global_withdrawn_bmin,
                        include_withdrawn,
                    )
                })
            })
            .into_iter()
            .flatten(),
        )
    }

    pub fn less_specifics_iter_from(
        &'a self,
        prefix_id: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> impl Iterator<Item = (PrefixId<AF>, Vec<PublicRecord<M>>)> + 'a {
        match self.config.persist_strategy() {
            PersistStrategy::MemoryOnly
            | PersistStrategy::WriteAhead
            | PersistStrategy::PersistHistory => {
                // if mui.is_some_and(|m| self.mui_is_withdrawn(m, guard)) {
                //     return None;
                // }

                self.in_memory_tree
                    .less_specific_prefix_iter(prefix_id)
                    .filter_map(move |p| {
                        self.get_value(p, mui, include_withdrawn, guard)
                            .map(|v| (p, v))
                    })

                // (if mui.is_some_and(|m| self.mui_is_withdrawn(m, guard)) {
                //     None
                // } else {
                //     Some(
                //         self.in_memory_tree
                //             .less_specific_prefix_iter(
                //                 prefix_id,
                //                 // mui,
                //                 // include_withdrawn,
                //                 // guard,
                //             )
                //             .map(|p| {
                //                 self.get_value(
                //                     p,
                //                     mui.clone(),
                //                     include_withdrawn,
                //                     guard,
                //                 )
                //                 .map(|v| (p, v))
                //                 .unwrap()
                //             }),
                //     )
                // })
                // .into_iter()
                // .flatten()
            }
            PersistStrategy::PersistOnly => unimplemented!(),
        }
    }

    pub fn match_prefix(
        &'a self,
        search_pfx: PrefixId<AF>,
        options: &MatchOptions,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        trace!("match_prefix rib {:?} {:?}", search_pfx, options);
        let res = self.in_memory_tree.match_prefix(search_pfx, options);

        trace!("res {:?}", res);
        let mut res = QueryResult::from(res);

        if log_enabled!(log::Level::Trace) {
            let ms = self
                .in_memory_tree
                .more_specific_prefix_iter_from(search_pfx)
                .collect::<Vec<_>>();
            trace!("more specifics!!! {:?}", ms);
            trace!("len {}", ms.len());
        }

        if let Some(Some(m)) = res.prefix.map(|p| {
            self.get_value(
                p.into(),
                options.mui,
                options.include_withdrawn,
                guard,
            )
            .and_then(|v| if v.is_empty() { None } else { Some(v) })
        }) {
            trace!("got value {:?}", m);
            res.prefix_meta = m;
        } else {
            res.prefix = None;
            res.match_type = MatchType::EmptyMatch;
        }

        if options.include_more_specifics {
            res.more_specifics = res.more_specifics.map(|p| {
                p.iter()
                    .filter_map(|mut r| {
                        if let Some(m) = self.get_value(
                            r.prefix.into(),
                            options.mui,
                            options.include_withdrawn,
                            guard,
                        ) {
                            r.meta = m;
                            Some(r)
                        } else {
                            None
                        }
                    })
                    .collect()
            });
        }
        if options.include_less_specifics {
            res.less_specifics = res.less_specifics.map(|p| {
                p.iter()
                    .filter_map(|mut r| {
                        if let Some(m) = self.get_value(
                            r.prefix.into(),
                            options.mui,
                            options.include_withdrawn,
                            guard,
                        ) {
                            r.meta = m;
                            Some(r)
                        } else {
                            None
                        }
                    })
                    .collect()
            });
        }

        res
    }

    pub fn match_prefix_legacy(
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
            PersistStrategy::MemoryOnly => {
                let mut res: QueryResult<M> = self
                    .in_memory_tree
                    .match_prefix_by_tree_traversal(search_pfx, options)
                    .into();

                res.prefix_meta = res
                    .prefix
                    .map(|p| {
                        self.get_value(
                            p.into(),
                            options.mui,
                            options.include_withdrawn,
                            guard,
                        )
                        .unwrap_or_default()
                    })
                    .unwrap_or_default();
                res
            }
            // All the records are persisted, they have never been committed
            // to memory. However the in-memory-tree is still used to indicate
            // which (prefix, mui) tuples have been created.
            PersistStrategy::PersistOnly => {
                // If no withdawn should be included and a specific mui was
                // requested, then return early, if the mui lives in the
                // global withdrawn muis.
                if !options.include_withdrawn {
                    if let Some(mui) = options.mui {
                        let withdrawn_muis_bmin =
                            self.in_memory_tree.withdrawn_muis_bmin(guard);
                        if withdrawn_muis_bmin.contains(mui) {
                            return QueryResult::empty();
                        }
                    }
                }

                // if options.match_type == MatchType::ExactMatch
                //     && !self.contains(search_pfx, options.mui)
                // {
                //     return QueryResult::empty();
                // }

                if let Some(persist_tree) = &self.persist_tree {
                    let withdrawn_muis_bmin =
                        self.in_memory_tree.withdrawn_muis_bmin(guard);
                    println!("persist store found");
                    println!("mem record {:#?}", search_pfx);
                    let tbm_result = self
                        .in_memory_tree
                        .match_prefix_by_tree_traversal(search_pfx, options);
                    println!("found by traversal {:#?}", tbm_result);

                    persist_tree
                        .match_prefix(
                            tbm_result,
                            options,
                            withdrawn_muis_bmin,
                        )
                        .into()
                } else {
                    println!("no persist store");
                    QueryResult::empty()
                }
            }
            // We have the current records in memory, additionally they may be
            // encriched with historical data from the persisted data.
            PersistStrategy::WriteAhead => {
                let mut res: FamilyQueryResult<AF, M> = self
                    .in_memory_tree
                    .match_prefix_by_tree_traversal(search_pfx, options)
                    .into();
                if let Some(persist_tree) = &self.persist_tree {
                    match options.include_history {
                        IncludeHistory::All => {
                            if self.contains(search_pfx, options.mui) {
                                res.prefix_meta.extend(
                                    persist_tree.get_records_for_prefix(
                                        search_pfx,
                                        options.mui,
                                        options.include_withdrawn,
                                        self.in_memory_tree
                                            .withdrawn_muis_bmin(guard),
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
                                                options.include_withdrawn,
                                                self.in_memory_tree
                                                    .withdrawn_muis_bmin(
                                                        guard,
                                                    ),
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
                                                options.include_withdrawn,
                                                self.in_memory_tree
                                                    .withdrawn_muis_bmin(
                                                        guard,
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
                                        options.include_withdrawn,
                                        self.in_memory_tree
                                            .withdrawn_muis_bmin(guard),
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
                let mut res: FamilyQueryResult<AF, M> = self
                    .in_memory_tree
                    .match_prefix_by_tree_traversal(search_pfx, options)
                    .into();

                if let Some(persist_tree) = &self.persist_tree {
                    match options.include_history {
                        IncludeHistory::All => {
                            res.prefix_meta.extend(
                                persist_tree.get_records_for_prefix(
                                    search_pfx,
                                    options.mui,
                                    options.include_withdrawn,
                                    self.in_memory_tree
                                        .withdrawn_muis_bmin(guard),
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
                                                    options.include_withdrawn,
                                                    self.in_memory_tree
                                                        .withdrawn_muis_bmin(
                                                            guard,
                                                        ),
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
                                                    options.include_withdrawn,
                                                    self.in_memory_tree
                                                        .withdrawn_muis_bmin(
                                                            guard,
                                                        ),
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
                                        options.include_withdrawn,
                                        self.in_memory_tree
                                            .withdrawn_muis_bmin(guard),
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
        self.prefix_cht
            .non_recursive_retrieve_prefix(search_pfx)
            .0
            .map(|p_rec| {
                p_rec.get_path_selections(guard).best().map_or_else(
                    || Err(PrefixStoreError::BestPathNotFound),
                    |mui| {
                        p_rec
                            .record_map
                            .get_record_for_mui(mui, false)
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
        self.prefix_cht
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
        self.prefix_cht
            .non_recursive_retrieve_prefix(search_pfx)
            .0
            .map_or(Err(PrefixStoreError::StoreNotReadyError), |p| {
                Ok(p.is_ps_outdated(guard))
            })
    }
}

#[derive(Debug)]
pub(crate) struct TreeQueryResult<AF: AddressFamily> {
    pub match_type: MatchType,
    pub prefix: Option<PrefixId<AF>>,
    pub less_specifics: Option<Vec<PrefixId<AF>>>,
    pub more_specifics: Option<Vec<PrefixId<AF>>>,
}

impl<AF: AddressFamily, M: Meta> From<TreeQueryResult<AF>>
    for QueryResult<M>
{
    fn from(value: TreeQueryResult<AF>) -> Self {
        Self {
            match_type: value.match_type,
            prefix: value.prefix.map(|p| p.into()),
            prefix_meta: vec![],
            less_specifics: value
                .less_specifics
                .map(|ls| ls.into_iter().map(|p| (p, vec![])).collect()),
            more_specifics: value
                .more_specifics
                .map(|ms| ms.into_iter().map(|p| (p, vec![])).collect()),
        }
    }
}

impl<AF: AddressFamily, M: Meta> From<TreeQueryResult<AF>>
    for FamilyQueryResult<AF, M>
{
    fn from(value: TreeQueryResult<AF>) -> Self {
        Self {
            match_type: value.match_type,
            prefix: value.prefix,
            prefix_meta: vec![],
            less_specifics: None,
            more_specifics: None,
        }
    }
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
