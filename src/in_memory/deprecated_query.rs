use log::trace;

use crate::af::AddressFamily;
use crate::prelude::multi::PrefixSet;
use inetnum::addr::Prefix;

use crate::{Meta, QueryResult};

use crate::local_array::in_memory::node::{SizedStrideRef, TreeBitMapNode};
use crate::{MatchOptions, MatchType};

use super::super::in_memory::atomic_types::StoredPrefix;
use super::super::in_memory::tree::Stride;
use super::super::types::PrefixId;
use super::atomic_types::{NodeBuckets, PrefixBuckets};
use super::node::StrideNodeId;
use super::tree::TreeBitMap;

#[allow(dead_code)]
impl<'a, AF, M, NB, PB> TreeBitMap<AF, M, NB, PB>
where
    AF: AddressFamily,
    M: Meta,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, M>,
{
    #[allow(clippy::type_complexity)]
    fn retrieve_prefix(
        &'a self,
        prefix_id: PrefixId<AF>,
    ) -> Option<(&'a StoredPrefix<AF, M>, usize)> {
        struct SearchLevel<
            's,
            AF: AddressFamily,
            M: crate::prefix_record::Meta,
        > {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &'a PrefixSet<AF, M>,
                u8,
            )
                -> Option<(&'a StoredPrefix<AF, M>, usize)>,
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, M>,
                 prefix_set: &PrefixSet<AF, M>,
                 mut level: u8| {
                // HASHING FUNCTION
                let index =
                    crate::local_array::in_memory::tree::TreeBitMap::<
                        AF,
                        M,
                        NB,
                        PB,
                    >::hash_prefix_id(prefix_id, level);

                if let Some(stored_prefix) = prefix_set.0.get(index) {
                    if prefix_id == stored_prefix.prefix {
                        trace!("found requested prefix {:?}", prefix_id,);
                        return Some((stored_prefix, 0));
                    };
                    level += 1;

                    (search_level.f)(
                        search_level,
                        &stored_prefix.next_bucket,
                        level,
                    );
                }
                None
            },
        };

        (search_level.f)(
            &search_level,
            self.prefix_buckets.get_root_prefix_set(prefix_id.get_len()),
            0,
        )
    }

    // This function assembles all entries in the `pfx_vec` of all child nodes
    // of the `start_node` into one vec, starting from itself and then
    // recursively assembling adding all `pfx_vec`s of its children.
    fn _get_all_more_specifics_for_node(
        &self,
        start_node_id: StrideNodeId<AF>,
        found_pfx_vec: &mut Vec<PrefixId<AF>>,
    ) {
        trace!("{:?}", self.retrieve_node(start_node_id));
        match self.retrieve_node(start_node_id) {
            Some(SizedStrideRef::Stride3(n)) => {
                found_pfx_vec.extend(
                    n.pfx_iter(start_node_id).collect::<Vec<PrefixId<AF>>>(),
                );

                for child_node in n.ptr_iter(start_node_id) {
                    self._get_all_more_specifics_for_node(
                        child_node,
                        found_pfx_vec,
                    );
                }
            }
            Some(SizedStrideRef::Stride4(n)) => {
                found_pfx_vec.extend(
                    n.pfx_iter(start_node_id).collect::<Vec<PrefixId<AF>>>(),
                );

                for child_node in n.ptr_iter(start_node_id) {
                    self._get_all_more_specifics_for_node(
                        child_node,
                        found_pfx_vec,
                    );
                }
            }
            Some(SizedStrideRef::Stride5(n)) => {
                found_pfx_vec.extend(
                    n.pfx_iter(start_node_id).collect::<Vec<PrefixId<AF>>>(),
                );

                for child_node in n.ptr_iter(start_node_id) {
                    self._get_all_more_specifics_for_node(
                        child_node,
                        found_pfx_vec,
                    );
                }
            }
            _ => {
                panic!("can't find node {}", start_node_id);
            }
        }
    }

    // This function assembles the prefixes of a child node starting on a
    // specified bit position in a ptr_vec of `current_node` into a vec,
    // then adds all prefixes of these children recursively into a vec and
    // returns that.
    fn _get_all_more_specifics_from_nibble<S: Stride>(
        &self,
        current_node: &TreeBitMapNode<AF, S>,
        nibble: u32,
        nibble_len: u8,
        base_prefix: StrideNodeId<AF>,
    ) -> Option<Vec<PrefixId<AF>>> {
        let (cnvec, mut msvec) = current_node.add_more_specifics_at(
            nibble,
            nibble_len,
            base_prefix,
        );

        for child_node in cnvec.iter() {
            self._get_all_more_specifics_for_node(*child_node, &mut msvec);
        }
        Some(msvec)
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

    fn _match_prefix_by_tree_traversal(
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
            return QueryResult {
                prefix: None,
                prefix_meta: vec![],
                match_type: MatchType::EmptyMatch,
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
                                        ._get_all_more_specifics_from_nibble(
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
                                        ._get_all_more_specifics_from_nibble(
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
                                    ._get_all_more_specifics_from_nibble(
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
                                        ._get_all_more_specifics_from_nibble(
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
                                        ._get_all_more_specifics_from_nibble(
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
                                        ._get_all_more_specifics_from_nibble(
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
                                    ._get_all_more_specifics_from_nibble(
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
                                        ._get_all_more_specifics_from_nibble(
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
                                        ._get_all_more_specifics_from_nibble(
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
                                        ._get_all_more_specifics_from_nibble(
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
                                    ._get_all_more_specifics_from_nibble(
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
                                        ._get_all_more_specifics_from_nibble(
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
}
