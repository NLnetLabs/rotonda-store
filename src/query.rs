use crate::common::{AddressFamily, NoMeta, Prefix};
use crate::{SizedStrideNode, SortableNodeId, StorageBackend, TreeBitMap, TreeBitMapNode};
use std::fmt::Debug;

pub struct MatchOptions {
    pub match_type: MatchType,
    pub include_less_specifics: bool,
    pub include_more_specifics: bool,
}

#[derive(Debug)]
pub enum MatchType {
    ExactMatch,
    LongestMatch,
}

#[derive(Debug)]
pub struct QueryResult<'a, Store>
where
    Store: StorageBackend,
{
    pub prefix: Option<&'a Prefix<Store::AF, Store::Meta>>,
    // pub match_type: MatchType,
    pub less_specifics: Option<Vec<&'a Prefix<Store::AF, Store::Meta>>>,
    pub more_specifics: Option<Vec<&'a Prefix<Store::AF, Store::Meta>>>,
}

//------------ Longest Matching Prefix  --------------------------------------------------------

impl<'a, Store> TreeBitMap<Store>
where
    Store: StorageBackend,
{
    // In a LMP search we have to go over all the nibble lengths in the stride up
    // until the value of the actual nibble length were looking for (until we reach
    // stride length for all strides that aren't the last) and see if the
    // prefix bit in that posision is set.
    // Note that this does not search for prefixes with length 0 (which would always
    // match).
    // So for matching a nibble 1010, we have to search for 1, 10, 101 and 1010 on
    // resp. position 1, 5, 12 and 25:
    //                       ↓          ↓                         ↓                                                              ↓
    // pfx bit arr (u32)   0 1 2  3  4  5  6   7   8   9  10  11  12  13  14   15   16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
    // nibble              * 0 1 00 01 10 11 000 001 010 011 100 101 110 111 0000 0001 0010 0011 0100 0101 0110 0111 1000 1001 1010 1011 1100 1101 1110 1111    x
    // nibble len offset   0 1    2            3                                4

    pub fn match_prefix(
        &'a self,
        search_pfx: &Prefix<Store::AF, NoMeta>,
        options: &MatchOptions,
    ) -> QueryResult<'a, Store> {
        let mut stride_end = 0;
        let mut found_pfx_idx: Option<
            <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
        > = None;
        let mut node = self.retrieve_node(self.get_root_node_id()).unwrap();
        let mut more_specifics_vec: Vec<Store::NodeType> = vec![];
        let mut last_child_node = None;
        let mut nibble = 0;
        let mut nibble_len = 0;

        let mut less_specifics_vec = if options.include_less_specifics {
            Some(Vec::<Store::NodeType>::new())
        } else {
            None
        };

        for stride in self.strides.iter() {
            stride_end += stride;

            nibble_len = if search_pfx.len < stride_end {
                stride + search_pfx.len - stride_end
            } else {
                *stride
            };

            // Shift left and right to set the bits to zero that are not
            // in the nibble we're handling here.
            nibble = AddressFamily::get_nibble(search_pfx.net, stride_end - stride, nibble_len);

            match node {
                SizedStrideNode::Stride3(current_node) => {
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
                    };

                    // This whole match assumes that:
                    // - if the first value in the return tuple of `search_fn` holds a value, then we need to continue
                    //   searching by following the node contained in the value.
                    // - The second value in the tuple holds the prefix that was found.
                    // The less_specifics_vec is mutated by `search_fn` to hold the prefixes found along the way, in the
                    // cases where `include_less_specifics` was requested by the user.
                    match search_fn(
                        current_node,
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut less_specifics_vec,
                    ) {
                        // This and the next match will handle intermediary nodes.
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        // This handles exact matches.
                        (None, Some(pfx_idx)) => {
                            if options.include_more_specifics {
                                let (cnvec, msvec) =
                                    current_node.add_more_specifics_at(nibble, nibble_len);
                                more_specifics_vec.extend(msvec);

                                for child_node in cnvec.iter() {
                                    self.get_all_more_specifics_for_node(
                                        self.retrieve_node(*child_node).unwrap(),
                                        &mut more_specifics_vec,
                                    );
                                }
                            }
                            let pfx = self.retrieve_prefix(pfx_idx.get_part());
                            return QueryResult {
                                prefix: pfx,
                                // match_type: match pfx {
                                //     Some(pfx) if pfx.len == search_pfx.len => MatchType::ExactMatch,
                                //     _ => MatchType::LongestMatch,
                                // },
                                more_specifics: Some(
                                    more_specifics_vec
                                        .iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect(),
                                ),
                                less_specifics: less_specifics_vec.map(|vec| {
                                    vec.iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect()
                                }),
                            };
                        }
                        // This handles cases where there's no prefix, either exact or longest matching.
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride4(current_node) => {
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
                    };

                    // This whole match assumes that:
                    // - if the first value in the return tuple of `search_fn` holds a value, then we need to continue
                    //   searching by following the node contained in the value.
                    // - The second value in the tuple holds the prefix that was found.
                    // The less_specifics_vec is mutated by `search_fn` to hold the prefixes found along the way, in the
                    // cases where `include_less_specifics` was requested by the user.
                    match search_fn(
                        current_node,
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut less_specifics_vec,
                    ) {
                        // This and the next match will handle intermediary nodes.
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        // This handles exact matches.
                        (None, Some(pfx_idx)) => {
                            if options.include_more_specifics {
                                let (cnvec, msvec) =
                                    current_node.add_more_specifics_at(nibble, nibble_len);
                                more_specifics_vec.extend(msvec);

                                for child_node in cnvec.iter() {
                                    self.get_all_more_specifics_for_node(
                                        self.retrieve_node(*child_node).unwrap(),
                                        &mut more_specifics_vec,
                                    );
                                }
                            }
                            let pfx = self.retrieve_prefix(pfx_idx.get_part());
                            return QueryResult {
                                prefix: pfx,
                                // match_type: match pfx {
                                //     Some(pfx) if pfx.len == search_pfx.len => MatchType::ExactMatch,
                                //     _ => MatchType::LongestMatch,
                                // },
                                more_specifics: Some(
                                    more_specifics_vec
                                        .iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect(),
                                ),
                                less_specifics: less_specifics_vec.map(|vec| {
                                    vec.iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect()
                                }),
                            };
                        }
                        // This handles cases where there's no prefix, either exact or longest matching.
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride5(current_node) => {
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
                    };

                    // This whole match assumes that:
                    // - if the first value in the return tuple of `search_fn` holds a value, then we need to continue
                    //   searching by following the node contained in the value.
                    // - The second value in the tuple holds the prefix that was found.
                    // The less_specifics_vec is mutated by `search_fn` to hold the prefixes found along the way, in the
                    // cases where `include_less_specifics` was requested by the user.
                    match search_fn(
                        current_node,
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut less_specifics_vec,
                    ) {
                        // This and the next match will handle intermediary nodes.
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        // This handles exact matches.
                        (None, Some(pfx_idx)) => {
                            if options.include_more_specifics {
                                let (cnvec, msvec) =
                                    current_node.add_more_specifics_at(nibble, nibble_len);
                                more_specifics_vec.extend(msvec);

                                for child_node in cnvec.iter() {
                                    self.get_all_more_specifics_for_node(
                                        self.retrieve_node(*child_node).unwrap(),
                                        &mut more_specifics_vec,
                                    );
                                }
                            }
                            let pfx = self.retrieve_prefix(pfx_idx.get_part());
                            return QueryResult {
                                prefix: pfx,
                                // match_type: match pfx {
                                //     Some(pfx) if pfx.len == search_pfx.len => MatchType::ExactMatch,
                                //     _ => MatchType::LongestMatch,
                                // },
                                more_specifics: Some(
                                    more_specifics_vec
                                        .iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect(),
                                ),
                                less_specifics: less_specifics_vec.map(|vec| {
                                    vec.iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect()
                                }),
                            };
                        }
                        // This handles cases where there's no prefix, either exact or longest matching.
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride6(current_node) => {
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
                    };

                    // This whole match assumes that:
                    // - if the first value in the return tuple of `search_fn` holds a value, then we need to continue
                    //   searching by following the node contained in the value.
                    // - The second value in the tuple holds the prefix that was found.
                    // The less_specifics_vec is mutated by `search_fn` to hold the prefixes found along the way, in the
                    // cases where `include_less_specifics` was requested by the user.
                    match search_fn(
                        current_node,
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut less_specifics_vec,
                    ) {
                        // This and the next match will handle intermediary nodes.
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        // This handles exact matches.
                        (None, Some(pfx_idx)) => {
                            if options.include_more_specifics {
                                let (cnvec, msvec) =
                                    current_node.add_more_specifics_at(nibble, nibble_len);
                                more_specifics_vec.extend(msvec);

                                for child_node in cnvec.iter() {
                                    self.get_all_more_specifics_for_node(
                                        self.retrieve_node(*child_node).unwrap(),
                                        &mut more_specifics_vec,
                                    );
                                }
                            }
                            let pfx = self.retrieve_prefix(pfx_idx.get_part());
                            return QueryResult {
                                prefix: pfx,
                                // match_type: match pfx {
                                //     Some(pfx) if pfx.len == search_pfx.len => MatchType::ExactMatch,
                                //     _ => MatchType::LongestMatch,
                                // },
                                more_specifics: Some(
                                    more_specifics_vec
                                        .iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect(),
                                ),
                                less_specifics: less_specifics_vec.map(|vec| {
                                    vec.iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect()
                                }),
                            };
                        }
                        // This handles cases where there's no prefix, either exact or longest matching.
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride7(current_node) => {
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
                    };

                    // This whole match assumes that:
                    // - if the first value in the return tuple of `search_fn` holds a value, then we need to continue
                    //   searching by following the node contained in the value.
                    // - The second value in the tuple holds the prefix that was found.
                    // The less_specifics_vec is mutated by `search_fn` to hold the prefixes found along the way, in the
                    // cases where `include_less_specifics` was requested by the user.
                    match search_fn(
                        current_node,
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut less_specifics_vec,
                    ) {
                        // This and the next match will handle intermediary nodes.
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        // This handles exact matches.
                        (None, Some(pfx_idx)) => {
                            if options.include_more_specifics {
                                let (cnvec, msvec) =
                                    current_node.add_more_specifics_at(nibble, nibble_len);
                                more_specifics_vec.extend(msvec);

                                for child_node in cnvec.iter() {
                                    self.get_all_more_specifics_for_node(
                                        self.retrieve_node(*child_node).unwrap(),
                                        &mut more_specifics_vec,
                                    );
                                }
                            }
                            let pfx = self.retrieve_prefix(pfx_idx.get_part());
                            return QueryResult {
                                prefix: pfx,
                                // match_type: match pfx {
                                //     Some(pfx) if pfx.len == search_pfx.len => MatchType::ExactMatch,
                                //     _ => MatchType::LongestMatch,
                                // },
                                more_specifics: Some(
                                    more_specifics_vec
                                        .iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect(),
                                ),
                                less_specifics: less_specifics_vec.map(|vec| {
                                    vec.iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect()
                                }),
                            };
                        }
                        // This handles cases where there's no prefix, either exact or longest matching.
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride8(current_node) => {
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
                    };

                    // This whole match assumes that:
                    // - if the first value in the return tuple of `search_fn` holds a value, then we need to continue
                    //   searching by following the node contained in the value.
                    // - The second value in the tuple holds the prefix that was found.
                    // The less_specifics_vec is mutated by `search_fn` to hold the prefixes found along the way, in the
                    // cases where `include_less_specifics` was requested by the user.
                    match search_fn(
                        current_node,
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut less_specifics_vec,
                    ) {
                        // This and the next match will handle intermediary nodes.
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                            last_child_node = Some(n);
                        }
                        // This handles exact matches.
                        (None, Some(pfx_idx)) => {
                            if options.include_more_specifics {
                                let (cnvec, msvec) =
                                    current_node.add_more_specifics_at(nibble, nibble_len);
                                more_specifics_vec.extend(msvec);

                                for child_node in cnvec.iter() {
                                    self.get_all_more_specifics_for_node(
                                        self.retrieve_node(*child_node).unwrap(),
                                        &mut more_specifics_vec,
                                    );
                                }
                            }
                            let pfx = self.retrieve_prefix(pfx_idx.get_part());
                            return QueryResult {
                                prefix: pfx,
                                // match_type: match pfx {
                                //     Some(pfx) if pfx.len == search_pfx.len => MatchType::ExactMatch,
                                //     _ => MatchType::LongestMatch,
                                // },
                                more_specifics: Some(
                                    more_specifics_vec
                                        .iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect(),
                                ),
                                less_specifics: less_specifics_vec.map(|vec| {
                                    vec.iter()
                                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                        .collect()
                                }),
                            };
                        }
                        // This handles cases where there's no prefix, either exact or longest matching.
                        (None, None) => {
                            break;
                        }
                    }
                }
            }
        }

        // This looks up more-specifics for longest-matching prefixes,
        // exact matches will never come here (they already returned in the above match).
        // Note that a MatchType::LongestMatch (the intent of the user) may or may not come here,
        // since it can also match exactly, i.e. longest match == exact match.
        if let MatchType::LongestMatch = options.match_type {
            if options.include_more_specifics {
                if let Some(child_node) = last_child_node {
                    let (cnvec, msvec) = match self.retrieve_node(child_node) {
                        Some(SizedStrideNode::Stride3(n)) => {
                            n.add_more_specifics_at(nibble, nibble_len)
                        }
                        Some(SizedStrideNode::Stride4(n)) => {
                            n.add_more_specifics_at(nibble, nibble_len)
                        }
                        Some(SizedStrideNode::Stride5(n)) => {
                            n.add_more_specifics_at(nibble, nibble_len)
                        }
                        Some(SizedStrideNode::Stride6(n)) => {
                            n.add_more_specifics_at(nibble, nibble_len)
                        }
                        Some(SizedStrideNode::Stride7(n)) => {
                            n.add_more_specifics_at(nibble, nibble_len)
                        }
                        Some(SizedStrideNode::Stride8(n)) => {
                            n.add_more_specifics_at(nibble, nibble_len)
                        }
                        None => (vec![], vec![]),
                    };
                    more_specifics_vec.extend(msvec);

                    for child_node in cnvec.iter() {
                        self.get_all_more_specifics_for_node(
                            self.retrieve_node(*child_node).unwrap(),
                            &mut more_specifics_vec,
                        );
                    }
                };
            }
        }

        if let Some(pfx_idx) = found_pfx_idx {
            QueryResult {
                prefix: self.retrieve_prefix(pfx_idx),
                more_specifics: Some(
                    more_specifics_vec
                        .iter()
                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                        .collect(),
                ),
                less_specifics: less_specifics_vec.map(|vec| {
                    vec.iter()
                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                        .collect()
                }),
            }
        } else {
            QueryResult {
                prefix: None,
                more_specifics: Some(
                    more_specifics_vec
                        .iter()
                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                        .collect(),
                ),
                less_specifics: less_specifics_vec.map(|vec| {
                    vec.iter()
                        .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                        .collect()
                }),
            }
        }
    }
}
