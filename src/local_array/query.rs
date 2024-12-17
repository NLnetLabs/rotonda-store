use std::sync::atomic::Ordering;

use crossbeam_epoch::{self as epoch};
use epoch::Guard;

use crate::af::AddressFamily;
use crate::local_array::in_memory::atomic_types::{
    NodeBuckets, PrefixBuckets,
};
use crate::prefix_record::{Meta, PublicRecord};
use crate::rib::{PersistStrategy, PersistTree, Rib};
use inetnum::addr::Prefix;

use crate::QueryResult;

use crate::local_array::in_memory::node::TreeBitMapNode;
use crate::{MatchOptions, MatchType};

use super::errors::PrefixStoreError;
use super::in_memory::atomic_types::StoredPrefix;
use super::in_memory::tree::{SizedStrideRef, StrideNodeId};
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
        let result = self.non_recursive_retrieve_prefix(prefix_id);
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
        let result = self.non_recursive_retrieve_prefix(prefix_id);

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
        let bmin = self.in_memory_tree.withdrawn_muis_bmin(guard);

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
        let mut stored_prefix =
            self.non_recursive_retrieve_prefix(search_pfx).0.map(|pfx| {
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
                            self.in_memory_tree.withdrawn_muis_bmin(guard),
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

    fn match_prefix_in_persisted_store(
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

    pub fn best_path(
        &'a self,
        search_pfx: PrefixId<AF>,
        guard: &Guard,
    ) -> Option<Result<PublicRecord<M>, PrefixStoreError>> {
        self.non_recursive_retrieve_prefix(search_pfx)
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
        self.non_recursive_retrieve_prefix(search_pfx)
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
        self.non_recursive_retrieve_prefix(search_pfx)
            .0
            .map_or(Err(PrefixStoreError::StoreNotReadyError), |p| {
                Ok(p.is_ps_outdated(guard))
            })
    }

    // In a LMP search we have to go over all the nibble lengths in the
    // stride up until the value of the actual nibble length were looking for
    // (until we reach stride length for all strides that aren't the last)
    // and see if the prefix bit in that position is set. Note that this does
    // not search for prefixes with length 0 (which would always match).
    // So for matching a nibble 1010, we have to search for 1, 10, 101 and
    // 1010 on resp. position 1, 5, 12 and 25:
    //                       ↓          ↓                         ↓
    // nibble              * 0 1 00 01 10 11 000 001 010 011 100 101 110 111
    // nibble len offset   0 1    2            3
    //
    // (contd.)
    // pfx bit arr (u32)     15   16   17   18   19   20   21   22   23   24
    // nibble              0000 0001 0010 0011 0100 0101 0110 0111 1000 1001
    // nibble len offset      4
    //
    // (contd.)               ↓
    // pfx bit arr (u32)     25   26   27   28   29   30   31
    // nibble              1010 1011 1100 1101 1110 1111    x
    // nibble len offset      4(contd.)

    #[deprecated]
    pub fn match_prefix_by_tree_traversal(
        &'a self,
        search_pfx: PrefixId<AF>,
        options: &MatchOptions,
        // guard: &'a Guard,
    ) -> QueryResult<M> {
        // --- The Default Route Prefix -------------------------------------

        // The Default Route Prefix unfortunately does not fit in tree as we
        // have it. There's no room for it in the pfxbitarr of the root node,
        // since that can only contain serial numbers for prefixes that are
        // children of the root node. We, however, want the default prefix
        // which lives on the root node itself! We are *not* going to return
        // all of the prefixes in the tree as more-specifics.
        if search_pfx.get_len() == 0 {
            // match self.load_default_route_prefix_serial() {
            //     0 => {
            //         return QueryResult {
            //             prefix: None,
            //             prefix_meta: vec![],
            //             match_type: MatchType::EmptyMatch,
            //             less_specifics: None,
            //             more_specifics: None,
            //         };
            //     }

            //     _serial => {
            let prefix_meta = self
                .retrieve_prefix(PrefixId::new(AF::zero(), 0))
                .map(|sp| sp.0.record_map.as_records())
                .unwrap_or_default();
            return QueryResult {
                prefix: Prefix::new(
                    search_pfx.get_net().into_ipaddr(),
                    search_pfx.get_len(),
                )
                .ok(),
                prefix_meta,
                match_type: MatchType::ExactMatch,
                less_specifics: None,
                more_specifics: None,
            };
            // }
            // }
        }

        let mut stride_end = 0;

        let root_node_id = self.get_root_node_id();
        let mut node = match self.get_stride_for_id(root_node_id) {
            3 => self.retrieve_node(root_node_id).unwrap(),
            4 => self.retrieve_node(root_node_id).unwrap(),
            _ => self.retrieve_node(root_node_id).unwrap(),
        };

        let mut nibble;
        let mut nibble_len;

        //---- result values ------------------------------------------------

        // These result values are kept in mutable variables, and assembled
        // at the end into a QueryResult struct. This proved to result in the
        // most efficient code, where we don't have to match on
        // SizedStrideNode over and over. The `match_type` field in the
        // QueryResult is computed at the end.

        // The final prefix
        let mut match_prefix_idx: Option<PrefixId<AF>> = None;

        // The indexes of the less-specifics
        let mut less_specifics_vec = if options.include_less_specifics {
            Some(Vec::<PrefixId<AF>>::new())
        } else {
            None
        };

        // The indexes of the more-specifics.
        let mut more_specifics_vec = if options.include_more_specifics {
            Some(Vec::<PrefixId<AF>>::new())
        } else {
            None
        };

        //---- Stride Processing --------------------------------------------

        // We're going to iterate over all the strides in the treebitmap (so
        // up to the last bit in the max prefix length for that tree). When
        // a final prefix is found or we get to the end of the strides,
        // depending on the options.match_type (the type requested by the
        // user). we ALWAYS break out of the loop. WE ALWAYS BREAK OUT OF THE
        // LOOP. Just before breaking some processing is done inside the loop
        // before the break (looking up more-specifics mainly), which looks a
        // bit repetitious, but again it's been done like that to avoid
        // having to match over a SizedStrideNode again in the
        // `post-processing` section.

        for stride in self.get_stride_sizes() {
            stride_end += stride;

            let last_stride = search_pfx.get_len() < stride_end;

            nibble_len = if last_stride {
                stride + search_pfx.get_len() - stride_end
            } else {
                *stride
            };

            // Shift left and right to set the bits to zero that are not
            // in the nibble we're handling here.
            nibble = AddressFamily::get_nibble(
                search_pfx.get_net(),
                stride_end - stride,
                nibble_len,
            );

            match node {
                SizedStrideRef::Stride3(current_node) => {
                    let search_fn = match options.match_type {
                        MatchType::ExactMatch => {
                            if options.include_less_specifics {
                                TreeBitMapNode::search_stride_for_exact_match_with_less_specifics_at
                            } else {
                                TreeBitMapNode::search_stride_for_exact_match_at
                            }
                        }
                        MatchType::LongestMatch => {
                            TreeBitMapNode::search_stride_for_longest_match_at
                        }
                        MatchType::EmptyMatch => {
                            TreeBitMapNode::search_stride_for_longest_match_at
                        }
                    };

                    // This whole match assumes that:
                    // - if the first value in the return tuple of
                    //   `search_fn` holds a value, then we need to continue
                    //   searching by following the node contained in the
                    //   value.
                    // - The second value in the tuple holds the prefix that
                    //   was found.
                    // The less_specifics_vec is mutated by `search_fn` to
                    // hold the prefixes found along the way, in the cases
                    // where `include_less_specifics` was requested by the
                    // user.
                    match search_fn(
                        current_node,
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut less_specifics_vec,
                    ) {
                        // This and the next match will handle all
                        // intermediary nodes, but they might also handle
                        // exit nodes.
                        (Some(n), Some(pfx_idx)) => {
                            match_prefix_idx = Some(pfx_idx);
                            node = self.retrieve_node(n).unwrap();

                            if last_stride {
                                if options.include_more_specifics {
                                    more_specifics_vec = self
                                        .get_all_more_specifics_from_nibble(
                                            current_node,
                                            nibble,
                                            nibble_len,
                                            StrideNodeId::new_with_cleaned_id(
                                                search_pfx.get_net(),
                                                stride_end - stride,
                                            ),
                                        );
                                }
                                break;
                            }
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();

                            if last_stride {
                                if options.include_more_specifics {
                                    more_specifics_vec = self
                                        .get_all_more_specifics_from_nibble(
                                            current_node,
                                            nibble,
                                            nibble_len,
                                            StrideNodeId::new_with_cleaned_id(
                                                search_pfx.get_net(),
                                                stride_end - stride,
                                            ),
                                        );
                                }
                                break;
                            }
                        }
                        // This handles exact and longest matches: there are
                        // no more children, but there is a prefix on this
                        // node.
                        (None, Some(pfx_idx)) => {
                            if options.include_more_specifics {
                                more_specifics_vec = self
                                    .get_all_more_specifics_from_nibble(
                                        current_node,
                                        nibble,
                                        nibble_len,
                                        StrideNodeId::new_with_cleaned_id(
                                            search_pfx.get_net(),
                                            stride_end - stride,
                                        ),
                                    );
                            }
                            match_prefix_idx = Some(pfx_idx);
                            break;
                        }
                        // This handles cases where there's no prefix (and no
                        // child) for exact match or longest match, the empty
                        // match - which doesn't care about actually finding
                        // a prefix - just continues in search of
                        // more-specifics.
                        (None, None) => {
                            match options.match_type {
                                MatchType::EmptyMatch => {
                                    // To make sure we don't process this
                                    // match arm more then once, we return
                                    // early here.
                                    more_specifics_vec = self
                                        .get_all_more_specifics_from_nibble(
                                            current_node,
                                            nibble,
                                            nibble_len,
                                            StrideNodeId::new_with_cleaned_id(
                                                search_pfx.get_net(),
                                                stride_end - stride,
                                            ),
                                        );

                                    match_prefix_idx = None;
                                    break;
                                }
                                MatchType::LongestMatch => {}
                                MatchType::ExactMatch => {
                                    match_prefix_idx = None;
                                }
                            }
                            break;
                        }
                    }
                }
                //---- From here only repetitions for all strides -----------
                // For comments see the code above for the Stride3 arm.
                SizedStrideRef::Stride4(current_node) => {
                    let search_fn = match options.match_type {
                        MatchType::ExactMatch => {
                            if options.include_less_specifics {
                                TreeBitMapNode::search_stride_for_exact_match_with_less_specifics_at
                            } else {
                                TreeBitMapNode::search_stride_for_exact_match_at
                            }
                        }
                        MatchType::LongestMatch => {
                            TreeBitMapNode::search_stride_for_longest_match_at
                        }
                        MatchType::EmptyMatch => {
                            TreeBitMapNode::search_stride_for_longest_match_at
                        }
                    };
                    match search_fn(
                        current_node,
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut less_specifics_vec,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            match_prefix_idx = Some(pfx_idx);
                            node = self.retrieve_node(n).unwrap();

                            if last_stride {
                                if options.include_more_specifics {
                                    more_specifics_vec = self
                                        .get_all_more_specifics_from_nibble(
                                            current_node,
                                            nibble,
                                            nibble_len,
                                            StrideNodeId::new_with_cleaned_id(
                                                search_pfx.get_net(),
                                                stride_end - stride,
                                            ),
                                        );
                                }
                                break;
                            }
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();

                            if last_stride {
                                if options.include_more_specifics {
                                    more_specifics_vec = self
                                        .get_all_more_specifics_from_nibble(
                                            current_node,
                                            nibble,
                                            nibble_len,
                                            StrideNodeId::new_with_cleaned_id(
                                                search_pfx.get_net(),
                                                stride_end - stride,
                                            ),
                                        );
                                }
                                break;
                            }
                        }
                        (None, Some(pfx_idx)) => {
                            if options.include_more_specifics {
                                more_specifics_vec = self
                                    .get_all_more_specifics_from_nibble(
                                        current_node,
                                        nibble,
                                        nibble_len,
                                        StrideNodeId::new_with_cleaned_id(
                                            search_pfx.get_net(),
                                            stride_end - stride,
                                        ),
                                    );
                            }
                            match_prefix_idx = Some(pfx_idx);
                            break;
                        }
                        (None, None) => {
                            match options.match_type {
                                MatchType::EmptyMatch => {
                                    // To make sure we don't process this match arm more then once, we
                                    // return early here.
                                    more_specifics_vec = self
                                        .get_all_more_specifics_from_nibble(
                                            current_node,
                                            nibble,
                                            nibble_len,
                                            StrideNodeId::new_with_cleaned_id(
                                                search_pfx.get_net(),
                                                stride_end - stride,
                                            ),
                                        );

                                    match_prefix_idx = None;
                                    break;
                                }
                                MatchType::LongestMatch => {}
                                MatchType::ExactMatch => {
                                    match_prefix_idx = None;
                                }
                            }
                            break;
                        }
                    }
                }
                SizedStrideRef::Stride5(current_node) => {
                    let search_fn = match options.match_type {
                        MatchType::ExactMatch => {
                            if options.include_less_specifics {
                                TreeBitMapNode::search_stride_for_exact_match_with_less_specifics_at
                            } else {
                                TreeBitMapNode::search_stride_for_exact_match_at
                            }
                        }
                        MatchType::LongestMatch => {
                            TreeBitMapNode::search_stride_for_longest_match_at
                        }
                        MatchType::EmptyMatch => {
                            TreeBitMapNode::search_stride_for_longest_match_at
                        }
                    };
                    match search_fn(
                        current_node,
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut less_specifics_vec,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            match_prefix_idx = Some(pfx_idx);
                            node = self.retrieve_node(n).unwrap();

                            if last_stride {
                                if options.include_more_specifics {
                                    more_specifics_vec = self
                                        .get_all_more_specifics_from_nibble(
                                            current_node,
                                            nibble,
                                            nibble_len,
                                            StrideNodeId::new_with_cleaned_id(
                                                search_pfx.get_net(),
                                                stride_end - stride,
                                            ),
                                        );
                                }
                                break;
                            }
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();

                            if last_stride {
                                if options.include_more_specifics {
                                    more_specifics_vec = self
                                        .get_all_more_specifics_from_nibble(
                                            current_node,
                                            nibble,
                                            nibble_len,
                                            StrideNodeId::new_with_cleaned_id(
                                                search_pfx.get_net(),
                                                stride_end - stride,
                                            ),
                                        );
                                }
                                break;
                            }
                        }
                        (None, Some(pfx_idx)) => {
                            if options.include_more_specifics {
                                more_specifics_vec = self
                                    .get_all_more_specifics_from_nibble(
                                        current_node,
                                        nibble,
                                        nibble_len,
                                        StrideNodeId::new_with_cleaned_id(
                                            search_pfx.get_net(),
                                            stride_end - stride,
                                        ),
                                    );
                            }
                            match_prefix_idx = Some(pfx_idx);
                            break;
                        }
                        (None, None) => {
                            match options.match_type {
                                MatchType::EmptyMatch => {
                                    more_specifics_vec = self
                                        .get_all_more_specifics_from_nibble(
                                            current_node,
                                            nibble,
                                            nibble_len,
                                            StrideNodeId::new_with_cleaned_id(
                                                search_pfx.get_net(),
                                                stride_end - stride,
                                            ),
                                        );

                                    match_prefix_idx = None;
                                    break;
                                }
                                MatchType::LongestMatch => {}
                                MatchType::ExactMatch => {
                                    match_prefix_idx = None;
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }
        //------------------ end of Stride branch arm repetition ------------

        //------------------ post-processing --------------------------------

        // If the above loop finishes (so not hitting a break) we have
        // processed all strides and have found a child node and maybe a
        // prefix. Now we will look up more-specifics for longest-matching
        // prefixes that were found in the last stride only. Note that still
        // any of the match_types (as specified by the user, not the return
        // type) may end up here.

        let mut match_type: MatchType = MatchType::EmptyMatch;
        let prefix = None;
        if let Some(pfx_idx) = match_prefix_idx {
            match_type = match self.retrieve_prefix(pfx_idx) {
                Some(prefix) => {
                    if prefix.0.prefix.get_len() == search_pfx.get_len() {
                        MatchType::ExactMatch
                    } else {
                        MatchType::LongestMatch
                    }
                }
                None => MatchType::EmptyMatch,
            };
        };

        QueryResult {
            prefix: prefix.map(|pfx: (&StoredPrefix<AF, M>, usize)| {
                pfx.0.prefix.into_pub()
            }),
            prefix_meta: prefix
                .map(|pfx| pfx.0.record_map.as_records())
                .unwrap_or_default(),
            match_type,
            less_specifics: if options.include_less_specifics {
                less_specifics_vec
                    .unwrap()
                    .iter()
                    .filter_map(move |p| {
                        self.retrieve_prefix(*p).map(|p| {
                            Some((p.0.prefix, p.0.record_map.as_records()))
                        })
                    })
                    .collect()
            } else {
                None
            },
            more_specifics: if options.include_more_specifics {
                more_specifics_vec.map(|vec| {
                    vec.into_iter()
                        .map(|p| {
                            self.retrieve_prefix(p)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "more specific {:?} does not exist",
                                        p
                                    )
                                })
                                .0
                        })
                        .map(|sp| (sp.prefix, sp.record_map.as_records()))
                        .collect()
                })
            } else {
                None
            },
        }
    }

    // Helper to filter out records that are not-active (Inactive or
    // Withdrawn), or whose mui appears in the global withdrawn index.
    fn get_filtered_records(
        &self,
        pfx: &StoredPrefix<AF, M>,
        mui: Option<u32>,
        guard: &Guard,
    ) -> Vec<PublicRecord<M>> {
        let bmin = self.in_memory_tree.withdrawn_muis_bmin(guard);

        pfx.record_map.get_filtered_records(mui, bmin)
    }
}
