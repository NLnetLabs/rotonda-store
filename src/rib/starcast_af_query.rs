use crossbeam_epoch::{self as epoch};
use epoch::Guard;
use log::trace;
use zerocopy::TryFromBytes;

use crate::rib::starcast_af::{Config, PersistStrategy, StarCastAfRib};
use crate::types::prefix_record::ZeroCopyRecord;
use crate::types::PublicRecord;
use crate::AddressFamily;
use inetnum::addr::Prefix;

use crate::{Meta, QueryResult};

use crate::{MatchOptions, MatchType};

use crate::types::errors::PrefixStoreError;
use crate::types::PrefixId;

//------------ Prefix Matching ----------------------------------------------

impl<
        'a,
        AF: AddressFamily,
        M: Meta,
        const N_ROOT_SIZE: usize,
        const P_ROOT_SIZE: usize,
        C: Config,
        const KEY_SIZE: usize,
    > StarCastAfRib<AF, M, N_ROOT_SIZE, P_ROOT_SIZE, C, KEY_SIZE>
{
    pub(crate) fn get_value(
        &'a self,
        prefix_id: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> Option<Vec<PublicRecord<M>>> {
        match self.persist_strategy() {
            PersistStrategy::PersistOnly => {
                trace!("get value from persist_store for {:?}", prefix_id);
                self.persist_tree.as_ref().and_then(|tree| {
                    tree.get_records_for_prefix(
                        prefix_id,
                        mui,
                        include_withdrawn,
                        self.tree_bitmap.withdrawn_muis_bmin(guard),
                    )
                    .map(|v| {
                        v.iter()
                            .map(|bytes| {
                                let record: &ZeroCopyRecord<AF> =
                                    ZeroCopyRecord::try_ref_from_bytes(bytes)
                                        .unwrap();
                                PublicRecord::<M> {
                                    multi_uniq_id: record.multi_uniq_id,
                                    ltime: record.ltime,
                                    status: record.status,
                                    meta: <Vec<u8>>::from(
                                        record.meta.as_ref(),
                                    )
                                    .into(),
                                }
                            })
                            .collect::<Vec<_>>()
                    })
                })
            }
            _ => self.prefix_cht.get_records_for_prefix(
                prefix_id,
                mui,
                include_withdrawn,
                self.tree_bitmap.withdrawn_muis_bmin(guard),
            ),
        }
    }

    pub(crate) fn more_specifics_from(
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
            .tree_bitmap
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

    pub(crate) fn less_specifics_from(
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
            .tree_bitmap
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

    pub(crate) fn more_specifics_iter_from(
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
                self.tree_bitmap
                    .more_specific_prefix_iter_from(prefix_id)
                    .filter_map(move |p| {
                        self.get_value(p, mui, include_withdrawn, guard)
                            .map(|v| (p, v))
                    }),
            )
        })
        .into_iter()
        .flatten()
        // .chain(
        //     (if mui.is_some_and(|m| {
        //         self.config.persist_strategy == PersistStrategy::WriteAhead
        //             || (!include_withdrawn && self.mui_is_withdrawn(m, guard))
        //     }) {
        //         None
        //     } else {
        //         let global_withdrawn_bmin =
        //             self.in_memory_tree.withdrawn_muis_bmin(guard);
        //         self.persist_tree.as_ref().map(|persist_tree| {
        //             persist_tree.more_specific_prefix_iter_from(
        //                 prefix_id,
        //                 vec![],
        //                 mui,
        //                 global_withdrawn_bmin,
        //                 include_withdrawn,
        //             )
        //         })
        //     })
        //     .into_iter()
        //     .flatten(),
        // )
    }

    pub(crate) fn less_specifics_iter_from(
        &'a self,
        prefix_id: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> impl Iterator<Item = (PrefixId<AF>, Vec<PublicRecord<M>>)> + 'a {
        self.tree_bitmap
            .less_specific_prefix_iter(prefix_id)
            .filter_map(move |p| {
                self.get_value(p, mui, include_withdrawn, guard)
                    .map(|v| (p, v))
            })
    }

    pub(crate) fn match_prefix(
        &'a self,
        search_pfx: PrefixId<AF>,
        options: &MatchOptions,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        trace!("match_prefix rib {:?} {:?}", search_pfx, options);
        let res = self.tree_bitmap.match_prefix(search_pfx, options);

        trace!("res {:?}", res);
        let mut res = QueryResult::from(res);

        if let Some(Some(m)) = res.prefix.map(|p| {
            self.get_value(
                p.into(),
                options.mui,
                options.include_withdrawn,
                guard,
            )
            .and_then(|v| if v.is_empty() { None } else { Some(v) })
        }) {
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

    pub(crate) fn best_path(
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

    pub(crate) fn calculate_and_store_best_and_backup_path(
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

    pub(crate) fn is_ps_outdated(
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
