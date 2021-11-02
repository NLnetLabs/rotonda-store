use crate::common::InternalPrefixRecord;
use crate::node_id::SortableNodeId;
use crate::synth_int::{U256, U512, Zero};
pub use crate::stride::*;

use std::{
    fmt::{Binary, Debug},
    marker::PhantomData,
};

use crate::local_vec::tree::{NewNodeOrIndex, SizedStrideNode};

use routecore::record::NoMeta;
use routecore::addr::AddressFamily;

pub struct TreeBitMapNode<AF: AddressFamily, S, NodeId>
where
    S: Stride,
    <S as Stride>::PtrSize: Debug + Binary + Copy,
    AF: AddressFamily,
    NodeId: SortableNodeId + Copy,
{
    pub ptrbitarr: <S as Stride>::PtrSize,
    pub pfxbitarr: S,
    // The vec of prefixes hosted by this node,
    // referenced by (bit_id, global prefix index)
    // This is the exact same type as for the NodeIds,
    // so we reuse that.
    pub pfx_vec: Vec<NodeId>,
    // The vec of child nodes hosted by this
    // node, referenced by (ptrbitarr_index, global vec index)
    // We need the u16 (ptrbitarr_index) to sort the
    // vec that's stored in the node.
    pub ptr_vec: Vec<NodeId>,
    pub _af: PhantomData<AF>,
}

impl<AF, S, NodeId> Debug for TreeBitMapNode<AF, S, NodeId>
where
    AF: AddressFamily,
    S: Stride,
    <S as Stride>::PtrSize: Debug + Binary + Copy,
    NodeId: SortableNodeId + Copy,
    // <NodeId as SortableNodeId>::Part: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeBitMapNode")
            .field("ptrbitarr", &self.ptrbitarr)
            .field("pfxbitarr", &self.pfxbitarr)
            .field("ptr_vec", &self.ptr_vec)
            .field("pfx_vec", &self.pfx_vec)
            .finish()
    }
}

impl<AF, S, NodeId> TreeBitMapNode<AF, S, NodeId>
where
    AF: AddressFamily,
    S: Stride + std::ops::BitAnd<Output = S> + std::ops::BitOr<Output = S> + Zero,
    <S as Stride>::PtrSize:
        Debug + Binary + Copy + std::ops::BitAnd<Output = S::PtrSize> + PartialOrd + Zero,
    NodeId: SortableNodeId + Copy,
{
    // Inspects the stride (nibble, nibble_len) to see it there's
    // already a child node (if not at the last stride) or a prefix
    //  (if it's the last stride).
    //
    // Returns one of:
    // - A newly created child node.
    // - The index of the existing child node in the global `nodes` vec
    // - A newly created Prefix
    // - The index of the existing prefix in the global `prefixes` vec
    pub(crate) fn eval_node_or_prefix_at(
        &mut self,
        nibble: u32,
        nibble_len: u8,
        next_stride: Option<&u8>,
        is_last_stride: bool,
    ) -> NewNodeOrIndex<AF, NodeId> {
        let bit_pos = S::get_bit_pos(nibble, nibble_len);
        let new_node: SizedStrideNode<AF, NodeId>;

        // Check that we're not at the last stride (pfx.len <= stride_end),
        // Note that next_stride may have a value, but we still don't want to
        // continue, because we've exceeded the length of the prefix to
        // be inserted.
        // Also note that a nibble_len < S::BITS (a smaller than full nibble)
        // does indeed indicate the last stride has been reached, but the
        // reverse is *not* true, i.e. a full nibble can also be the last
        // stride. Hence the `is_last_stride` argument
        if !is_last_stride {
            // We are not at the last stride
            // Check it the ptr bit is already set in this position
            if (S::into_stride_size(self.ptrbitarr) & bit_pos) == S::zero() {
                // Nope, set it and create a child node
                self.ptrbitarr =
                    S::into_ptrbitarr_size(bit_pos | S::into_stride_size(self.ptrbitarr));

                match next_stride.unwrap() {
                    3_u8 => {
                        new_node = SizedStrideNode::Stride3(TreeBitMapNode {
                            ptrbitarr: <Stride3 as Stride>::PtrSize::zero(),
                            pfxbitarr: Stride3::zero(),
                            pfx_vec: vec![],
                            ptr_vec: vec![],
                            _af: PhantomData,
                        });
                    }
                    4_u8 => {
                        new_node = SizedStrideNode::Stride4(TreeBitMapNode {
                            ptrbitarr: <Stride4 as Stride>::PtrSize::zero(),
                            pfxbitarr: Stride4::zero(),
                            pfx_vec: vec![],
                            ptr_vec: vec![],
                            _af: PhantomData,
                        });
                    }
                    5_u8 => {
                        new_node = SizedStrideNode::Stride5(TreeBitMapNode {
                            ptrbitarr: <Stride5 as Stride>::PtrSize::zero(),
                            pfxbitarr: Stride5::zero(),
                            pfx_vec: vec![],
                            ptr_vec: vec![],
                            _af: PhantomData,
                        });
                    }
                    6_u8 => {
                        new_node = SizedStrideNode::Stride6(TreeBitMapNode {
                            ptrbitarr: <Stride6 as Stride>::PtrSize::zero(),
                            pfxbitarr: Stride6::zero(),
                            pfx_vec: vec![],
                            ptr_vec: vec![],
                            _af: PhantomData,
                        });
                    }
                    7_u8 => {
                        new_node = SizedStrideNode::Stride7(TreeBitMapNode {
                            ptrbitarr: 0_u128,
                            pfxbitarr: U256(0_u128, 0_u128),
                            pfx_vec: vec![],
                            ptr_vec: vec![],
                            _af: PhantomData,
                        });
                    }
                    8_u8 => {
                        new_node = SizedStrideNode::Stride8(TreeBitMapNode {
                            ptrbitarr: U256(0_u128, 0_u128),
                            pfxbitarr: U512(0_u128, 0_u128, 0_u128, 0_u128),
                            pfx_vec: vec![],
                            ptr_vec: vec![],
                            _af: PhantomData,
                        });
                    }
                    _ => {
                        panic!("can't happen");
                    }
                };

                // we can return bit_pos.leading_zeros() since bit_pos is the bitmap that
                // points to the current bit in ptrbitarr (it's **not** the prefix of the node!),
                // so the number of zeros in front of it should always be unique and describes
                // the index of this node in the ptrbitarr.
                // ex.:
                // In a stride3 (ptrbitarr lenght is 8):
                // bit_pos 0001 0000
                // so this is the fourth bit, so points to index = 3
                return NewNodeOrIndex::NewNode(new_node, (bit_pos.leading_zeros() as u16).into());
            }
        } else {
            // only at the last stride do we create the bit in the prefix bitmap,
            // and only if it doesn't exist already
            if self.pfxbitarr & bit_pos == <S as std::ops::BitAnd>::Output::zero() {
                self.pfxbitarr = bit_pos | self.pfxbitarr;
                return NewNodeOrIndex::NewPrefix;
            }
            return NewNodeOrIndex::ExistingPrefix(
                self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, nibble_len)].get_part(),
            );
        }

        NewNodeOrIndex::ExistingNode(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)])
    }

    //-------------------- Search nibble functions -------------------------------

    // This function looks for the longest marching prefix in the provided nibble,
    // by iterating over all the bits in it and comparing that with the appriopriate
    // bytes from the requested prefix.
    // It mutates the `less_specifics_vec` that was passed in to hold all the prefixes
    // found along the way.
    pub fn search_stride_for_longest_match_at(
        &self,
        search_pfx: &InternalPrefixRecord<AF, NoMeta>,
        mut nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        less_specifics_vec: &mut Option<Vec<NodeId>>,
    ) -> (Option<NodeId>, Option<NodeId>) {
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;

        for n_l in 1..(nibble_len + 1) {
            // Move the bit in the right position.
            nibble = AddressFamily::get_nibble(search_pfx.net, start_bit, n_l);
            bit_pos = S::get_bit_pos(nibble, n_l);

            // Check if the prefix has been set, if so select the prefix. This is not
            // necessarily the final prefix that will be returned.

            // Check it there's a prefix matching in this bitmap for this nibble
            if self.pfxbitarr & bit_pos > S::zero() {
                let f_pfx = self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, n_l)];

                // Receiving a less_specifics_vec means that the user wants to have
                // all the last-specific prefixes returned, so add the found prefix.
                if let Some(ls_vec) = less_specifics_vec {
                    if !(search_pfx.len <= start_bit + nibble_len
                        || (S::into_stride_size(self.ptrbitarr)
                            & S::get_bit_pos(nibble, nibble_len))
                            == S::zero())
                    {
                        ls_vec.push(f_pfx);
                    }
                }

                found_pfx = Some(f_pfx);
            }
        }

        // Check if this the last stride, or if they're no more children to go to,
        // if so return what we found up until now.
        if search_pfx.len <= start_bit + nibble_len
            || (S::into_stride_size(self.ptrbitarr) & bit_pos) == S::zero()
        // No children or at the end, return the definitive LMP we found.
        {
            return (
                None,      /* no more children */
                found_pfx, /* The definitive LMP if any */
            );
        }

        // There's another child, return it together with the preliminary LMP we found.
        (
            Some(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)]), /* The node that has children the next stride */
            found_pfx,
        )
    }

    // This function looks for the exactly matching prefix in the provided nibble.
    // It doesn't needd to iterate over anything it just compares the complete nibble, with
    // the appropriate bits in the requested prefix.
    // Although this is rather efficient, there's no way to collect less-specific prefixes from
    // the search prefix.
    pub fn search_stride_for_exact_match_at(
        &self,
        search_pfx: &InternalPrefixRecord<AF, NoMeta>,
        nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        _: &mut Option<Vec<NodeId>>,
    ) -> (Option<NodeId>, Option<NodeId>) {
        // This is an exact match, so we're only considering the position of the full nibble.
        let bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;
        let mut found_child = None;

        // Is this the last nibble?
        // Otherwise we're not looking for a prefix (exact matching only lives at last nibble)
        match search_pfx.len <= start_bit + nibble_len {
            // We're at the last nibble.
            true => {
                // Check for an actual prefix at the right position, i.e. consider the complete nibble
                if self.pfxbitarr & bit_pos > S::zero() {
                    found_pfx =
                        Some(self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, nibble_len)]);
                }
            }
            // We're not at the last nibble.
            false => {
                // Check for a child node at the right position, i.e. consider the complete nibble.
                if (S::into_stride_size(self.ptrbitarr) & bit_pos) > S::zero() {
                    found_child = Some(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)]);
                }
            }
        }

        (
            found_child, /* The node that has children in the next stride, if any */
            found_pfx,   /* The exactly matching prefix, if any */
        )
    }

    // This function looks for the exactly matching prefix in the provided nibble,
    // just like the one above, but this *does* iterate over all the bytes in the nibble to collect
    // the less-specific prefixes of the the search prefix.
    // This is of course slower, so it should only be used when the user explicitly requests less-specifics.
    pub fn search_stride_for_exact_match_with_less_specifics_at(
        &self,
        search_pfx: &InternalPrefixRecord<AF, NoMeta>,
        mut nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        less_specifics_vec: &mut Option<Vec<NodeId>>,
    ) -> (Option<NodeId>, Option<NodeId>) {
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;

        let ls_vec = less_specifics_vec
            .as_mut()
            .expect("You shouldn't call this function without a `less_specifics_vec` buffer. Supply one when calling this function or use `search_stride_for_exact_match_at`");

        for n_l in 1..(nibble_len + 1) {
            // Move the bit in the right position.
            nibble = AddressFamily::get_nibble(search_pfx.net, start_bit, n_l);
            bit_pos = S::get_bit_pos(nibble, n_l);

            // Check if the prefix has been set, if so select the prefix. This is not
            // necessarily the final prefix that will be returned.

            // Check it there's a prefix matching in this bitmap for this nibble,

            if self.pfxbitarr & bit_pos > S::zero() {
                // since we want an exact match only, we will fill the prefix field only
                // if we're exactly at the last bit of the nibble
                if n_l == nibble_len {
                    found_pfx = Some(self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, n_l)]);
                }

                // Receiving a less_specifics_vec means that the user wants to have
                // all the last-specific prefixes returned, so add the found prefix.
                ls_vec.push(self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, n_l)]);
            }
        }

        if found_pfx.is_none() {
            // no prefix here, clear out all of the prefixes we found along the way,
            // since it doesn't make sense to return less-specifics if we don't have a exact match.
            ls_vec.clear();
        }

        // Check if this the last stride, or if they're no more children to go to,
        // if so return what we found up until now.
        match search_pfx.len <= start_bit + nibble_len
            || (S::into_stride_size(self.ptrbitarr) & bit_pos)
                == <S as std::ops::BitAnd>::Output::zero()
        {
            // No children or at the end, return the definitive LMP we found.
            true => (
                None,      /* no more children */
                found_pfx, /* The definitive LMP if any */
            ),
            // There's another child, we won't return the found_pfx, since we're not
            // at the last nibble and we want an exact match only.
            false => (
                Some(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)]), /* The node that has children the next stride */
                None,
            ),
        }
    }

    // Search a stride for more-specific prefixes and child nodes containing
    // more specifics for `search_prefix`.
    pub fn add_more_specifics_at(
        &self,
        nibble: u32,
        nibble_len: u8,
    ) -> (
        // Option<NodeId>, /* the node with children in the next stride  */
        Vec<NodeId>, /* child nodes with more more-specifics in this stride */
        Vec<NodeId>, /* more-specific prefixes in this stride */
    ) {
        let mut found_children_with_more_specifics = vec![];
        let mut found_more_specifics_vec: Vec<NodeId> = vec![];

        // This is an exact match, so we're only considering the position of the full nibble.
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_child = None;

        // Is there also a child node here?
        // Note that even without a child node, there may be more specifics further up in this
        // pfxbitarr or children in this ptrbitarr.
        if (S::into_stride_size(self.ptrbitarr) & bit_pos) > S::zero() {
            found_child = Some(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)]);
        }

        if let Some(child) = found_child {
            found_children_with_more_specifics.push(child);
        }

        // println!("{}..{}", nibble_len + start_bit, S::STRIDE_LEN + start_bit);
        // println!("start nibble: {:032b}", nibble);
        // println!("extra bit: {}", (S::STRIDE_LEN - nibble_len));

        // We're expanding the search for more-specifics bit-by-bit.
        // `ms_nibble_len` is the number of bits including the original nibble we're considering,
        // e.g. if our prefix has a length of 25 and we've all strides sized 4,
        // We would end up with a last nibble_len of 1.
        // `ms_nibble_len` will expand then from 2 up and till 4.
        // ex.:
        // nibble: 1 , (nibble_len: 1)
        // Iteration:
        // ms_nibble_len=1,n_l=0: 10, n_l=1: 11
        // ms_nibble_len=2,n_l=0: 100, n_l=1: 101, n_l=2: 110, n_l=3: 111
        // ms_nibble_len=3,n_l=0: 1000, n_l=1: 1001, n_l=2: 1010, ..., n_l=7: 1111

        for ms_nibble_len in nibble_len + 1..S::STRIDE_LEN + 1 {
            // iterate over all the possible values for this `ms_nibble_len`,
            // e.g. two bits can have 4 different values.
            for n_l in 0..(1 << (ms_nibble_len - nibble_len)) {
                // move the nibble left with the amount of bits we're going to loop over.
                // e.g. a stride of size 4 with a nibble 0000 0000 0000 0011 becomes 0000 0000 0000 1100
                // then it will iterate over ...1100,...1101,...1110,...1111
                let ms_nibble = (nibble << (ms_nibble_len - nibble_len)) + n_l as u32;
                bit_pos = S::get_bit_pos(ms_nibble, ms_nibble_len);

                // println!("nibble:    {:032b}", ms_nibble);
                // println!("ptrbitarr: {:032b}", self.ptrbitarr);
                // println!("bitpos:    {:032b}", bit_pos);

                if (S::into_stride_size(self.ptrbitarr) & bit_pos) > S::zero() {
                    found_children_with_more_specifics
                        .push(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, ms_nibble)]);
                }

                if self.pfxbitarr & bit_pos > S::zero() {
                    found_more_specifics_vec.push(
                        self.pfx_vec[S::get_pfx_index(self.pfxbitarr, ms_nibble, ms_nibble_len)],
                    );
                }
            }
        }

        (
            // We're done here, the caller should now go over all nodes in found_children_with_more_specifics vec and add
            // ALL prefixes found in there.
            found_children_with_more_specifics,
            found_more_specifics_vec,
        )
    }
}