use crate::common::{AddressFamily, NoMeta, Prefix};
use crate::{SizedStrideNode, SortableNodeId, StorageBackend, TreeBitMap};

//------------ Longest Matching Prefix  --------------------------------------------------------

impl<'a, Store> TreeBitMap<Store>
where
    Store: StorageBackend,
{
    pub fn match_longest_prefix(
        &'a self,
        search_pfx: &Prefix<Store::AF, NoMeta>,
    ) -> Option<&'a Prefix<Store::AF, Store::Meta>> {
        let mut stride_end = 0;
        let mut found_pfx_idx: Option<
            <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
        > = None;
        let mut node = self.retrieve_node(self.get_root_node_id()).unwrap();

        for stride in self.strides.iter() {
            stride_end += stride;

            let nibble_len = if search_pfx.len < stride_end {
                stride + search_pfx.len - stride_end
            } else {
                *stride
            };

            // Shift left and right to set the bits to zero that are not
            // in the nibble we're handling here.
            let nibble = AddressFamily::get_nibble(search_pfx.net, stride_end - stride, nibble_len);

            // let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
            // let mut offset: u32 = (1_u32 << nibble_len) - 1;
            // let mut bit_pos: u32 = 0x1 << (Self::BITS - offset as u8 - nibble as u8 - 1);

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
            match node {
                SizedStrideNode::Stride3(current_node) => {
                    match current_node.search_stride_at_lmp_only(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride4(current_node) => {
                    match current_node.search_stride_at_lmp_only(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride5(current_node) => {
                    match current_node.search_stride_at_lmp_only(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride6(current_node) => {
                    match current_node.search_stride_at_lmp_only(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride7(current_node) => {
                    match current_node.search_stride_at_lmp_only(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride8(current_node) => {
                    match current_node.search_stride_at_lmp_only(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
            };
        }

        if let Some(pfx_idx) = found_pfx_idx {
            Some(self.retrieve_prefix(pfx_idx).unwrap())
        } else {
            None
        }
    }

    pub fn match_longest_prefix_with_less_specifics(
        &'a self,
        search_pfx: &Prefix<Store::AF, NoMeta>,
    ) -> Vec<&'a Prefix<Store::AF, Store::Meta>> {
        let mut stride_end = 0;
        let mut found_pfx_idxs: Vec<Store::NodeType> = vec![];
        let mut node = self.retrieve_node(self.get_root_node_id()).unwrap();

        for stride in self.strides.iter() {
            stride_end += stride;

            let nibble_len = if search_pfx.len < stride_end {
                stride + search_pfx.len - stride_end
            } else {
                *stride
            };

            // Shift left and right to set the bits to zero that are not
            // in the nibble we're handling here.
            let nibble = AddressFamily::get_nibble(search_pfx.net, stride_end - stride, nibble_len);

            // In a LMP search we have to go over all the nibble lengths in the stride up
            // until the value of the actual nibble length were looking for (until we reach
            // stride length for all strides that aren't the last) and see if the
            // prefix bit in that position is set.
            // Note that this does not search for prefixes with length 0 (which would always
            // match).
            // So for matching a nibble 1010, we have to search for 1, 10, 101 and 1010 on
            // resp. position 1, 5, 12 and 25:
            //                       ↓          ↓                         ↓                                                              ↓
            // pfx bit arr (u32)   0 1 2  3  4  5  6   7   8   9  10  11  12  13  14   15   16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
            // nibble              * 0 1 00 01 10 11 000 001 010 011 100 101 110 111 0000 0001 0010 0011 0100 0101 0110 0111 1000 1001 1010 1011 1100 1101 1110 1111    x
            // nibble len offset   0 1    2            3                                4
            match node {
                SizedStrideNode::Stride3(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        None => {
                            return found_pfx_idxs
                                .into_iter()
                                .map(|i| self.retrieve_prefix(i.get_part()).unwrap())
                                .collect();
                        }
                    }
                }
                SizedStrideNode::Stride4(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        None => {
                            return found_pfx_idxs
                                .iter()
                                .map(|i| self.retrieve_prefix(i.get_part()).unwrap())
                                .collect();
                        }
                    }
                }
                SizedStrideNode::Stride5(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        None => {
                            return found_pfx_idxs
                                .iter()
                                .map(|i| self.retrieve_prefix(i.get_part()).unwrap())
                                .collect();
                        }
                    }
                }
                SizedStrideNode::Stride6(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        None => {
                            return found_pfx_idxs
                                .iter()
                                .map(|i| self.retrieve_prefix(i.get_part()).unwrap())
                                .collect();
                        }
                    }
                }
                SizedStrideNode::Stride7(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        None => {
                            return found_pfx_idxs
                                .iter()
                                .map(|i| self.retrieve_prefix(i.get_part()).unwrap())
                                .collect();
                        }
                    }
                }
                SizedStrideNode::Stride8(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        None => {
                            return found_pfx_idxs
                                .iter()
                                .map(|i| self.retrieve_prefix(i.get_part()).unwrap())
                                .collect();
                        }
                    }
                }
            };
        }

        found_pfx_idxs
            .iter()
            .map(|i| self.retrieve_prefix(i.get_part()).unwrap())
            .collect()
    }

    pub fn match_longest_prefix_with_guard(
        &'a self,
        search_pfx: &Prefix<Store::AF, NoMeta>,
    ) -> Vec<Prefix<Store::AF, Store::Meta>>
    where
        <Store as StorageBackend>::AF: AddressFamily + 'static,
        <Store as StorageBackend>::Meta: Copy,
    {
        let mut stride_end = 0;
        let mut found_pfx_idxs: Vec<Store::NodeType> = vec![];
        let mut node_g = self.retrieve_node_with_guard(self.get_root_node_id());
        let mut node: &SizedStrideNode<Store::AF, Store::NodeType> = &node_g;

        for stride in self.strides.iter() {
            stride_end += stride;

            let nibble_len = if search_pfx.len < stride_end {
                stride + search_pfx.len - stride_end
            } else {
                *stride
            };

            // Shift left and right to set the bits to zero that are not
            // in the nibble we're handling here.
            let nibble = AddressFamily::get_nibble(search_pfx.net, stride_end - stride, nibble_len);

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
            match node {
                SizedStrideNode::Stride3(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            drop(node_g);
                            node_g = self.retrieve_node_with_guard(n);
                            node = &node_g;
                        }
                        None => {
                            return found_pfx_idxs
                                .into_iter()
                                .map(move |i| {
                                    let pfx: &Prefix<Store::AF, Store::Meta> =
                                        &self.store.retrieve_prefix_with_guard(i);
                                    *pfx
                                })
                                .collect::<Vec<Prefix<Store::AF, Store::Meta>>>();
                        }
                    }
                }
                SizedStrideNode::Stride4(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            drop(node_g);
                            node_g = self.retrieve_node_with_guard(n);
                            node = &node_g;
                        }
                        None => {
                            return found_pfx_idxs
                                .into_iter()
                                .map(move |i| {
                                    let pfx: &Prefix<Store::AF, Store::Meta> =
                                        &self.store.retrieve_prefix_with_guard(i);
                                    *pfx
                                })
                                .collect::<Vec<Prefix<Store::AF, Store::Meta>>>();
                        }
                    }
                }
                SizedStrideNode::Stride5(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            drop(node_g);
                            node_g = self.retrieve_node_with_guard(n);
                            node = &node_g;
                        }
                        None => {
                            return found_pfx_idxs
                                .into_iter()
                                .map(move |i| {
                                    let pfx: &Prefix<Store::AF, Store::Meta> =
                                        &self.store.retrieve_prefix_with_guard(i);
                                    *pfx
                                })
                                .collect::<Vec<Prefix<Store::AF, Store::Meta>>>();
                        }
                    }
                }
                SizedStrideNode::Stride6(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            drop(node_g);
                            node_g = self.retrieve_node_with_guard(n);
                            node = &node_g;
                        }
                        None => {
                            return found_pfx_idxs
                                .into_iter()
                                .map(move |i| {
                                    let pfx: &Prefix<Store::AF, Store::Meta> =
                                        &self.store.retrieve_prefix_with_guard(i);
                                    *pfx
                                })
                                .collect::<Vec<Prefix<Store::AF, Store::Meta>>>();
                        }
                    }
                }
                SizedStrideNode::Stride7(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            drop(node_g);
                            node_g = self.retrieve_node_with_guard(n);
                            node = &node_g;
                        }
                        None => {
                            return found_pfx_idxs
                                .into_iter()
                                .map(move |i| {
                                    let pfx: &Prefix<Store::AF, Store::Meta> =
                                        &self.store.retrieve_prefix_with_guard(i);
                                    *pfx
                                })
                                .collect::<Vec<Prefix<Store::AF, Store::Meta>>>();
                        }
                    }
                }
                SizedStrideNode::Stride8(current_node) => {
                    match current_node.search_stride_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                        &mut found_pfx_idxs,
                    ) {
                        Some(n) => {
                            drop(node_g);
                            node_g = self.store.retrieve_node_with_guard(n);
                            node = &node_g;
                        }
                        None => {
                            return found_pfx_idxs
                                .into_iter()
                                .map(move |i| {
                                    let pfx: &Prefix<Store::AF, Store::Meta> =
                                        &self.store.retrieve_prefix_with_guard(i);
                                    *pfx
                                })
                                .collect::<Vec<Prefix<Store::AF, Store::Meta>>>();
                        }
                    }
                }
            };
        }

        found_pfx_idxs
            .into_iter()
            .map(move |i| {
                let pfx: &Prefix<Store::AF, Store::Meta> =
                    &self.store.retrieve_prefix_with_guard(i);
                *pfx
            })
            .collect::<Vec<Prefix<Store::AF, Store::Meta>>>()
    }

    //------------ Exactly Matching Prefix  --------------------------------------------------------

    pub fn match_exact_prefix(
        &'a self,
        search_pfx: &Prefix<Store::AF, NoMeta>,
    ) -> Option<&'a Prefix<Store::AF, Store::Meta>> {
        let mut stride_end = 0;
        let mut found_pfx_idx: Option<
            <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
        > = None;
        let mut node = self.retrieve_node(self.get_root_node_id()).unwrap();

        for stride in self.strides.iter() {
            stride_end += stride;

            let nibble_len = if search_pfx.len < stride_end {
                stride + search_pfx.len - stride_end
            } else {
                *stride
            };

            // Shift left and right to set the bits to zero that are not
            // in the nibble we're handling here.
            let nibble = AddressFamily::get_nibble(search_pfx.net, stride_end - stride, nibble_len);

            match node {
                SizedStrideNode::Stride3(current_node) => {
                    match current_node.search_stride_for_exact_match_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride4(current_node) => {
                    match current_node.search_stride_for_exact_match_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride5(current_node) => {
                    match current_node.search_stride_for_exact_match_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride6(current_node) => {
                    match current_node.search_stride_for_exact_match_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride7(current_node) => {
                    match current_node.search_stride_for_exact_match_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride8(current_node) => {
                    match current_node.search_stride_for_exact_match_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx)) => {
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None) => {
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx)) => {
                            return Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap())
                        }
                        (None, None) => {
                            break;
                        }
                    }
                }
            }
        }

        if let Some(pfx_idx) = found_pfx_idx {
            Some(self.retrieve_prefix(pfx_idx).unwrap())
        } else {
            None
        }
    }

    pub fn match_exact_prefix_with_more_specifics(
        &'a self,
        search_pfx: &Prefix<Store::AF, NoMeta>,
    ) -> (
        Option<&'a Prefix<Store::AF, Store::Meta>>,
        Vec<&'a Prefix<Store::AF, Store::Meta>>,
    ) {
        let mut stride_end = 0;
        let mut found_pfx_idx: Option<
            <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
        > = None;
        let mut node = self.retrieve_node(self.get_root_node_id()).unwrap();
        let mut more_specifics_vec: Vec<Store::NodeType> = vec![];
        let mut child_nodes_vec: Vec<Store::NodeType> = vec![];

        for stride in self.strides.iter() {
            stride_end += stride;

            let nibble_len = if search_pfx.len < stride_end {
                stride + search_pfx.len - stride_end
            } else {
                *stride
            };

            // Shift left and right to set the bits to zero that are not
            // in the nibble we're handling here.
            let nibble = AddressFamily::get_nibble(search_pfx.net, stride_end - stride, nibble_len);

            match node {
                SizedStrideNode::Stride3(current_node) => {
                    match current_node.search_stride_for_more_specifics_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);

                            for child_node in child_nodes_vec.iter() {
                                self.get_all_more_specifics_for_node(
                                    self.retrieve_node(*child_node).unwrap(),
                                    &mut more_specifics_vec,
                                );
                            }

                            return (
                                Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap()),
                                more_specifics_vec
                                    .iter()
                                    .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                    .collect(),
                            );
                        }
                        (None, None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride4(current_node) => {
                    match current_node.search_stride_for_more_specifics_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);

                            for child_node in child_nodes_vec.iter() {
                                self.get_all_more_specifics_for_node(
                                    self.retrieve_node(*child_node).unwrap(),
                                    &mut more_specifics_vec,
                                );
                            }

                            return (
                                Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap()),
                                more_specifics_vec
                                    .iter()
                                    .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                    .collect(),
                            );
                        }
                        (None, None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride5(current_node) => {
                    match current_node.search_stride_for_more_specifics_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);

                            for child_node in child_nodes_vec.iter() {
                                self.get_all_more_specifics_for_node(
                                    self.retrieve_node(*child_node).unwrap(),
                                    &mut more_specifics_vec,
                                );
                            }

                            return (
                                Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap()),
                                more_specifics_vec
                                    .iter()
                                    .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                    .collect(),
                            );
                        }
                        (None, None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride6(current_node) => {
                    match current_node.search_stride_for_more_specifics_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);

                            for child_node in child_nodes_vec.iter() {
                                self.get_all_more_specifics_for_node(
                                    self.retrieve_node(*child_node).unwrap(),
                                    &mut more_specifics_vec,
                                );
                            }

                            return (
                                Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap()),
                                more_specifics_vec
                                    .iter()
                                    .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                    .collect(),
                            );
                        }
                        (None, None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride7(current_node) => {
                    match current_node.search_stride_for_more_specifics_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);

                            for child_node in child_nodes_vec.iter() {
                                self.get_all_more_specifics_for_node(
                                    self.retrieve_node(*child_node).unwrap(),
                                    &mut more_specifics_vec,
                                );
                            }

                            return (
                                Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap()),
                                more_specifics_vec
                                    .iter()
                                    .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                    .collect(),
                            );
                        }
                        (None, None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            break;
                        }
                    }
                }
                SizedStrideNode::Stride8(current_node) => {
                    match current_node.search_stride_for_more_specifics_at(
                        search_pfx,
                        nibble,
                        nibble_len,
                        stride_end - stride,
                    ) {
                        (Some(n), Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            found_pfx_idx = Some(pfx_idx.get_part());
                            node = self.retrieve_node(n).unwrap();
                        }
                        (Some(n), None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            node = self.retrieve_node(n).unwrap();
                        }
                        (None, Some(pfx_idx), cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);

                            for child_node in child_nodes_vec.iter() {
                                self.get_all_more_specifics_for_node(
                                    self.retrieve_node(*child_node).unwrap(),
                                    &mut more_specifics_vec,
                                );
                            }

                            return (
                                Some(self.retrieve_prefix(pfx_idx.get_part()).unwrap()),
                                more_specifics_vec
                                    .iter()
                                    .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                                    .collect(),
                            );
                        }
                        (None, None, cnvec, msvec) => {
                            child_nodes_vec.extend(cnvec);
                            more_specifics_vec.extend(msvec);
                            break;
                        }
                    }
                }
            }
        }

        for child_node in child_nodes_vec.iter() {
            self.get_all_more_specifics_for_node(
                self.retrieve_node(*child_node).unwrap(),
                &mut more_specifics_vec,
            );
        }

        if let Some(pfx_idx) = found_pfx_idx {
            (
                Some(self.retrieve_prefix(pfx_idx).unwrap()),
                more_specifics_vec
                    .iter()
                    .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                    .collect(),
            )
        } else {
            (
                None,
                more_specifics_vec
                    .iter()
                    .map(|p| self.retrieve_prefix(p.get_part()).unwrap())
                    .collect(),
            )
        }
    }
}
