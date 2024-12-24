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
            PersistStrategy::PersistOnly => {
                if let Some(persist_tree) = &self.persist_tree {
                    persist_tree.match_prefix(
                        self.in_memory_tree.match_prefix_by_tree_traversal(
                            search_pfx, options,
                        ),
                        options,
                    )
                } else {
                    QueryResult::empty()
                }
            }
            _ => self.in_memory_tree.match_prefix(search_pfx, options, guard),
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

pub struct TreeQueryResult<AF: AddressFamily> {
    pub match_type: MatchType,
    pub prefix: Option<PrefixId<AF>>,
    pub less_specifics: Option<Vec<PrefixId<AF>>>,
    pub more_specifics: Option<Vec<PrefixId<AF>>>,
}
