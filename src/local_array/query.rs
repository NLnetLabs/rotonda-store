use crate::af::AddressFamily;
use routecore::addr::Prefix;
use routecore::bgp::RecordSet;
use routecore::record::NoMeta;

use crate::local_array::storage_backend::*;
use crate::prefix_record::InternalPrefixRecord;
use crate::QueryResult;

use crate::local_array::node::TreeBitMapNode;
use crate::local_array::tree::TreeBitMap;
use crate::{MatchOptions, MatchType};

use super::node::{PrefixId, SizedStrideRef, StrideNodeId};

//------------ Longest Matching Prefix  -------------------------------------

impl<'a, Store> TreeBitMap<Store>
where
    Store: StorageBackend,
{
    // In a LMP search we have to go over all the nibble lengths in the
    // stride up until the value of the actual nibble length were looking for
    // (until we reach stride length for all strides that aren't the last)
    // and see if the prefix bit in that posision is set. Note that this does
    // not search for prefixes with length 0 (which would always match).
    // So for matching a nibble 1010, we have to search for 1, 10, 101 and
    // 1010 on resp. position 1, 5, 12 and 25:
    //                       ↓          ↓                         ↓                                                              ↓
    // pfx bit arr (u32)   0 1 2  3  4  5  6   7   8   9  10  11  12  13  14   15   16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
    // nibble              * 0 1 00 01 10 11 000 001 010 011 100 101 110 111 0000 0001 0010 0011 0100 0101 0110 0111 1000 1001 1010 1011 1100 1101 1110 1111    x
    // nibble len offset   0 1    2            3                                4

    pub(crate) fn match_prefix(
        &'a self,
        search_pfx: &InternalPrefixRecord<Store::AF, NoMeta>,
        options: &MatchOptions,
    ) -> QueryResult<'a, Store::Meta> {
        let mut stride_end = 0;
        let mut level: usize = 0;

        let mut node = self.retrieve_node(self.get_root_node_id()).unwrap();
        let mut nibble;
        let mut nibble_len;

        //---- result values ------------------------------------------------

        // These result values are kept in mutable variables, and assembled
        // at the end into a QueryResult struct. This proved to result in the
        // most efficient code, where we don't have to match on
        // SizedStrideNode over and over. The `match_type` field in the
        // QueryResult is computed at the end.

        // The final prefix
        let mut match_prefix_idx: Option<PrefixId<Store::AF>> = None;

        // The indexes of the less-specifics
        let mut less_specifics_vec = if options.include_less_specifics {
            Some(Vec::<PrefixId<Store::AF>>::new())
        } else {
            None
        };

        // The indexes of the more-specifics.
        let mut more_specifics_vec = if options.include_more_specifics {
            Some(Vec::<PrefixId<Store::AF>>::new())
        } else {
            None
        };

        //---- Stride Processing --------------------------------------------

        // We're going to iterate over all the strides in the treebitmap (so
        // up to the last bit in the max prefix lentgth for that tree). When
        // a final prefix is found or we get to the end of the strides,
        // depending on the options.match_type (the type requested by the
        // user). we ALWAYS break out of the loop. WE ALWAYS BREAK OUT OF THE
        // LOOP. Just before breaking some processing is done inside the loop
        // before the break (looking up more-specifics mainly), which looks a
        // bit repetitious, but again it's been done like that to avoid
        // having to match over a SizedStrideNode again in the
        // `post-processing` section.

        for stride in self.strides.iter() {
            print!("xxx start search at root node");
            stride_end += stride;

            let last_stride = search_pfx.len < stride_end;

            nibble_len = if last_stride {
                stride + search_pfx.len - stride_end
            } else {
                *stride
            };
            level += 1;

            // Shift left and right to set the bits to zero that are not
            // in the nibble we're handling here.
            nibble = AddressFamily::get_nibble(
                search_pfx.net,
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
                            node = self.store.retrieve_node(n).unwrap();
                            if last_stride {
                                if options.include_more_specifics {
                                    more_specifics_vec = self
                                        .get_all_more_specifics_from_nibble(
                                            current_node,
                                            nibble,
                                            nibble_len,
                                            StrideNodeId::new_with_cleaned_id(
                                                search_pfx.net,
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
                                                search_pfx.net,
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
                                            search_pfx.net,
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
                                                search_pfx.net,
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
                                                search_pfx.net,
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
                                                search_pfx.net,
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
                                            search_pfx.net,
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
                                                search_pfx.net,
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
                                                search_pfx.net,
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
                                                search_pfx.net,
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
                                            search_pfx.net,
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
                                                search_pfx.net,
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
                } // SizedStrideNode::Stride6(current_node) => {
                  //     let search_fn = match options.match_type {
                  //         MatchType::ExactMatch => {
                  //             if options.include_less_specifics {
                  //                 TreeBitMapNode::search_stride_for_exact_match_with_less_specifics_at
                  //             } else {
                  //                 TreeBitMapNode::search_stride_for_exact_match_at
                  //             }
                  //         }
                  //         MatchType::LongestMatch => {
                  //             TreeBitMapNode::search_stride_for_longest_match_at
                  //         }
                  //         MatchType::EmptyMatch => {
                  //             TreeBitMapNode::search_stride_for_longest_match_at
                  //         }
                  //     };

                  //     match search_fn(
                  //         &current_node,
                  //         search_pfx,
                  //         nibble,
                  //         nibble_len,
                  //         stride_end - stride,
                  //         &mut less_specifics_vec,
                  //     ) {
                  //         (Some(n), Some(pfx_idx)) => {
                  //             match_prefix_idx = Some(pfx_idx.get_part());
                  //             node = self.retrieve_node(n).unwrap();
                  //             if last_stride {
                  //                 if options.include_more_specifics {
                  //                     more_specifics_vec = self
                  //                         .get_all_more_specifics_from_nibble(
                  //                             &current_node,
                  //                             nibble,
                  //                             nibble_len,
                  //                         );
                  //                 }
                  //                 break;
                  //             }
                  //         }
                  //         (Some(n), None) => {
                  //             node = self.retrieve_node(n).unwrap();
                  //             if last_stride {
                  //                 if options.include_more_specifics {
                  //                     more_specifics_vec = self
                  //                         .get_all_more_specifics_from_nibble(
                  //                             &current_node,
                  //                             nibble,
                  //                             nibble_len,
                  //                         );
                  //                 }
                  //                 break;
                  //             }
                  //         }
                  //         (None, Some(pfx_idx)) => {
                  //             if options.include_more_specifics {
                  //                 more_specifics_vec = self
                  //                     .get_all_more_specifics_from_nibble(
                  //                         &current_node,
                  //                         nibble,
                  //                         nibble_len,
                  //                     );
                  //             }
                  //             match_prefix_idx = Some(pfx_idx.get_part());
                  //             break;
                  //         }
                  //         (None, None) => {
                  //             match options.match_type {
                  //                 MatchType::EmptyMatch => {
                  //                     // To make sure we don't process this match arm more then once, we
                  //                     // return early here.
                  //                     more_specifics_vec = self
                  //                         .get_all_more_specifics_from_nibble(
                  //                             &current_node,
                  //                             nibble,
                  //                             nibble_len,
                  //                         );

                  //                     match_prefix_idx = None;
                  //                     break;
                  //                 }
                  //                 MatchType::LongestMatch => {}
                  //                 MatchType::ExactMatch => {
                  //                     match_prefix_idx = None;
                  //                 }
                  //             }
                  //             break;
                  //         }
                  //     }
                  // }
                  // SizedStrideNode::Stride7(current_node) => {
                  //     let search_fn = match options.match_type {
                  //         MatchType::ExactMatch => {
                  //             if options.include_less_specifics {
                  //                 TreeBitMapNode::search_stride_for_exact_match_with_less_specifics_at
                  //             } else {
                  //                 TreeBitMapNode::search_stride_for_exact_match_at
                  //             }
                  //         }
                  //         MatchType::LongestMatch => {
                  //             TreeBitMapNode::search_stride_for_longest_match_at
                  //         }
                  //         MatchType::EmptyMatch => {
                  //             TreeBitMapNode::search_stride_for_longest_match_at
                  //         }
                  //     };
                  //     match search_fn(
                  //         &current_node,
                  //         search_pfx,
                  //         nibble,
                  //         nibble_len,
                  //         stride_end - stride,
                  //         &mut less_specifics_vec,
                  //     ) {
                  //         (Some(n), Some(pfx_idx)) => {
                  //             match_prefix_idx = Some(pfx_idx.get_part());
                  //             node = self.retrieve_node(n).unwrap();
                  //             if last_stride {
                  //                 if options.include_more_specifics {
                  //                     more_specifics_vec = self
                  //                         .get_all_more_specifics_from_nibble(
                  //                             &current_node,
                  //                             nibble,
                  //                             nibble_len,
                  //                         );
                  //                 }
                  //                 break;
                  //             }
                  //         }
                  //         (Some(n), None) => {
                  //             node = self.retrieve_node(n).unwrap();
                  //             if last_stride {
                  //                 if options.include_more_specifics {
                  //                     more_specifics_vec = self
                  //                         .get_all_more_specifics_from_nibble(
                  //                             &current_node,
                  //                             nibble,
                  //                             nibble_len,
                  //                         );
                  //                 }
                  //                 break;
                  //             }
                  //         }
                  //         (None, Some(pfx_idx)) => {
                  //             if options.include_more_specifics {
                  //                 more_specifics_vec = self
                  //                     .get_all_more_specifics_from_nibble(
                  //                         &current_node,
                  //                         nibble,
                  //                         nibble_len,
                  //                     );
                  //             }
                  //             match_prefix_idx = Some(pfx_idx.get_part());
                  //             break;
                  //         }
                  //         (None, None) => {
                  //             match options.match_type {
                  //                 MatchType::EmptyMatch => {
                  //                     more_specifics_vec = self
                  //                         .get_all_more_specifics_from_nibble(
                  //                             &current_node,
                  //                             nibble,
                  //                             nibble_len,
                  //                         );

                  //                     match_prefix_idx = None;
                  //                     break;
                  //                 }
                  //                 MatchType::LongestMatch => {}
                  //                 MatchType::ExactMatch => {
                  //                     match_prefix_idx = None;
                  //                 }
                  //             }
                  //             break;
                  //         }
                  //     }
                  // }
                  // SizedStrideNode::Stride8(current_node) => {
                  //     let search_fn = match options.match_type {
                  //         MatchType::ExactMatch => {
                  //             if options.include_less_specifics {
                  //                 TreeBitMapNode::search_stride_for_exact_match_with_less_specifics_at
                  //             } else {
                  //                 TreeBitMapNode::search_stride_for_exact_match_at
                  //             }
                  //         }
                  //         MatchType::LongestMatch => {
                  //             TreeBitMapNode::search_stride_for_longest_match_at
                  //         }
                  //         MatchType::EmptyMatch => {
                  //             TreeBitMapNode::search_stride_for_longest_match_at
                  //         }
                  //     };
                  //     match search_fn(
                  //         &current_node,
                  //         search_pfx,
                  //         nibble,
                  //         nibble_len,
                  //         stride_end - stride,
                  //         &mut less_specifics_vec,
                  //     ) {
                  //         (Some(n), Some(pfx_idx)) => {
                  //             match_prefix_idx = Some(pfx_idx.get_part());
                  //             node = self.retrieve_node(n).unwrap();
                  //             if last_stride {
                  //                 if options.include_more_specifics {
                  //                     more_specifics_vec = self
                  //                         .get_all_more_specifics_from_nibble(
                  //                             &current_node,
                  //                             nibble,
                  //                             nibble_len,
                  //                         );
                  //                 }
                  //                 break;
                  //             }
                  //         }
                  //         (Some(n), None) => {
                  //             node = self.retrieve_node(n).unwrap();
                  //             if last_stride {
                  //                 if options.include_more_specifics {
                  //                     more_specifics_vec = self
                  //                         .get_all_more_specifics_from_nibble(
                  //                             &current_node,
                  //                             nibble,
                  //                             nibble_len,
                  //                         );
                  //                 }
                  //                 break;
                  //             }
                  //         }
                  //         (None, Some(pfx_idx)) => {
                  //             if options.include_more_specifics {
                  //                 more_specifics_vec = self
                  //                     .get_all_more_specifics_from_nibble(
                  //                         &current_node,
                  //                         nibble,
                  //                         nibble_len,
                  //                     );
                  //             }
                  //             match_prefix_idx = Some(pfx_idx.get_part());
                  //             break;
                  //         }
                  //         (None, None) => {
                  //             match options.match_type {
                  //                 MatchType::EmptyMatch => {
                  //                     more_specifics_vec = self
                  //                         .get_all_more_specifics_from_nibble(
                  //                             &current_node,
                  //                             nibble,
                  //                             nibble_len,
                  //                         );

                  //                     match_prefix_idx = None;
                  //                     break;
                  //                 }
                  //                 MatchType::LongestMatch => {}
                  //                 MatchType::ExactMatch => {
                  //                     match_prefix_idx = None;
                  //                 }
                  //             }
                  //             break;
                  //         }
                  //     }
                  // }
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
        let mut prefix = None;
        if let Some(pfx_idx) = match_prefix_idx {
            prefix = self.retrieve_prefix(pfx_idx);
            match_type = if prefix.unwrap().len == search_pfx.len {
                MatchType::ExactMatch
            } else {
                MatchType::LongestMatch
            }
        };

        QueryResult {
            prefix: if let Some(pfx) = prefix {
                Prefix::new(pfx.net.into_ipaddr(), pfx.len).ok()
            } else {
                None
            },
            prefix_meta: if let Some(pfx) = prefix {
                pfx.meta.as_ref()
            } else {
                None
            },
            match_type,
            less_specifics: if options.include_less_specifics {
                less_specifics_vec.map(|vec| {
                    vec.iter()
                        .map(|p| self.retrieve_prefix(*p).unwrap())
                        .collect::<RecordSet<'a, Store::Meta>>()
                })
            } else {
                None
            },
            more_specifics: if options.include_more_specifics {
                more_specifics_vec.map(|vec| {
                    vec.iter()
                        .map(|p| self.retrieve_prefix(*p).unwrap())
                        .collect()
                })
            } else {
                None
            },
        }
    }
}
