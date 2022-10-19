use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicU8};
use std::{
    fmt::Debug,
    marker::PhantomData,
};

use crossbeam_utils::Backoff;
use log::trace;

// pub use super::atomic_stride::*;
use super::bit_span::BitSpan;
use super::store::iterators::SizedNodeMoreSpecificIter;
use crate::local_array::store::iterators::SizedPrefixIter;
pub use crate::local_array::query::*;
pub use crate::local_array::tree::*;
use crate::af::Zero;
use crate::af::AddressFamily;

//------------ TreeBitMap Node ----------------------------------------------

// The treebitmap turned into a triebitmap, really. A Node in the treebitmap
// now only holds a ptrbitarr bitmap and a pfxbitarr bitmap, that indicate
// whether a node or a prefix exists in that spot. The corresponding node Ids
// and prefix ids are calcaluted from their position in the array. Nodes do
// *NOT* have a clue where they are in the tree, so they don't know the node
// id they represent. Instead, the node id is calculated from the position in
// the tree. That's why several methods take a `base_prefix` as a an argument:
// it represents the ID of the node itself.
//
// The elision of both the collection of children nodes and the prefix nodes in
// a treebitmap node is enabled by the storage backend for the multi-threaded
// store, since holds its entries keyed on the [node|prefix] id. (in contrast
// with arrays or vecs, that have 
pub struct TreeBitMapNode<
    AF,
    S,
> where
    Self: Sized,
    S: Stride,
    AF: AddressFamily,
{
    pub ptrbitarr: <S as Stride>::AtomicPtrSize,
    pub pfxbitarr: <S as Stride>::AtomicPfxSize,
    pub _af: PhantomData<AF>,
}

impl<AF, S> Debug
    for TreeBitMapNode<AF, S>
where
    AF: AddressFamily,
    S: Stride,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeBitMapNode")
            .field("ptrbitarr", &self.ptrbitarr.load())
            .field("pfxbitarr", &self.pfxbitarr.load())
            .finish()
    }
}

impl<AF, S>
    std::fmt::Display for TreeBitMapNode<AF, S>
where
    AF: AddressFamily,
    S: Stride
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TreeBitMapNode {{ ptrbitarr: {:?}, pfxbitarr: {:?} }}",
            self.ptrbitarr.load(),
            self.pfxbitarr.load(),
        )
    }
}

impl<AF, S>
    TreeBitMapNode<AF, S>
where
    AF: AddressFamily,
    S: Stride
{
    // ------- Iterators ----------------------------------------------------

    // Iterate over all the child node of this node
    pub(crate) fn ptr_iter(&self, base_prefix: StrideNodeId<AF>) -> 
        NodeChildIter<AF,S> {
            NodeChildIter::<AF,S> {
                base_prefix,
                ptrbitarr: self.ptrbitarr.load(),
                bit_span: BitSpan::new(0, 1),
                _af: PhantomData,
            }
    }

    // Iterate over all the prefix ids contained in this node.
    // Note that this function is *not* used by the iterator that iterates
    // over all prefixes. That one doesn't have to use the tree at all, but
    // uses the store directly.
    pub(crate) fn pfx_iter(&self, base_prefix: StrideNodeId<AF>) -> 
        NodePrefixIter<AF, S> {
        NodePrefixIter::<AF, S> {
            pfxbitarr: self.pfxbitarr.load(),
            base_prefix,
            bit_span: BitSpan::new(0,1 ),
            _af: PhantomData,
            _s: PhantomData,
        }
    }

    // Iterate over the more specific prefixes ids contained in this node
    pub(crate) fn more_specific_pfx_iter(&self, base_prefix: StrideNodeId<AF>, 
    start_bit_span: BitSpan, skip_self: bool) -> 
        NodeMoreSpecificsPrefixIter<AF, S> {
        NodeMoreSpecificsPrefixIter::<AF, S> {
            pfxbitarr: self.pfxbitarr.load(),
            base_prefix,
            start_bit_span,
            cursor: start_bit_span,
            skip_self,
            _s: PhantomData,
        }
    }

    // Iterate over the nodes that contain more specifics for the requested
    // base_prefix and corresponding bit_span.
    pub(crate) fn more_specific_ptr_iter(&self, base_prefix: StrideNodeId<AF>, 
    start_bit_span: BitSpan) -> 
        NodeMoreSpecificChildIter<AF, S> {
        NodeMoreSpecificChildIter::<AF, S> {
            ptrbitarr: self.ptrbitarr.load(),
            base_prefix,
            start_bit_span,
            cursor: None,
            _af: PhantomData,
        }
    }


    // ------- Search by Traversal methods -----------------------------------

    // Inspects the stride (nibble, nibble_len) to see it there's already a 
    // child node (if not at the last stride) or a prefix (if it's the last
    // stride).
    //
    // Returns a tuple of which the first element is one of:
    // - A newly created child node.
    // - The index of the existing child node in the global `nodes` vec
    // - A newly created Prefix
    // - The index of the existing prefix in the global `prefixes` vec
    // and the second element is the number of accumulated retries for the
    // compare_exchange of both ptrbitarr and pfxbitarr.
    pub(crate) fn eval_node_or_prefix_at(
        &mut self,
        nibble: u32,
        nibble_len: u8,
        // all the bits of the search prefix, but with the length set to
        // the length of this stride. So bits are set beyond its length.
        base_prefix: StrideNodeId<AF>,
        stride_len: u8,
        next_stride: Option<&u8>,
        is_last_stride: bool,
    ) -> (NewNodeOrIndex<AF>, u32) {

        // THE CRIICAL SECTION
        //
        // UPDATING ptrbitarr & pfxbitarr
        //
        // This section is not as critical as creating/updating a
        // a prefix. We need to set one bit only, and if somebody
        // beat us to it that's fine, we'll figure that out when
        // we try to write the prefix's serial number later on.
        // The one thing that can go wrong here is that we are
        // using an old ptrbitarr and overwrite bits set in the
        // meantime elsewhere in the bitarray.
        let mut retry_count = 0;
        let ptrbitarr = self.ptrbitarr.load();
        let pfxbitarr = self.pfxbitarr.load();
        let bit_pos = S::get_bit_pos(nibble, nibble_len);
        let new_node: SizedStrideNode<AF>;

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
            if (S::into_stride_size(ptrbitarr) & bit_pos) == <<<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType>::zero() {
                // Nope, set it and create a child node

                match next_stride.unwrap() {
                    3_u8 => {
                        new_node = SizedStrideNode::Stride3(TreeBitMapNode {
                            ptrbitarr: AtomicStride2(AtomicU8::new(0)),
                            pfxbitarr: AtomicStride3(AtomicU16::new(0)),
                            // pfx_vec: PrefixSet::empty(14),
                            _af: PhantomData,
                        });
                    }
                    4_u8 => {
                        new_node = SizedStrideNode::Stride4(TreeBitMapNode {
                            ptrbitarr: AtomicStride3(AtomicU16::new(0)),
                            pfxbitarr: AtomicStride4(AtomicU32::new(0)),
                            // pfx_vec: PrefixSet::empty(30),
                            _af: PhantomData,
                        });
                    }
                    5_u8 => {
                        new_node = SizedStrideNode::Stride5(TreeBitMapNode {
                            ptrbitarr: AtomicStride4(AtomicU32::new(0)),
                            pfxbitarr: AtomicStride5(AtomicU64::new(0)),
                            // pfx_vec: PrefixSet::empty(62),
                            _af: PhantomData,
                        });
                    }
                    _ => {
                        panic!("can't happen");
                    }
                };



                // THE CRIICAL SECTION
                //
                // UPDATING pfxbitarr
                //
                // preventing using an old ptrbitarr and overwrite bits set
                // in the meantime elsewhere in the bitarray.
                let mut a_ptrbitarr = self.ptrbitarr.compare_exchange(ptrbitarr,
                    S::into_ptrbitarr_size(
                    bit_pos | S::into_stride_size(ptrbitarr),
                ));
                let backoff = Backoff::new();
                loop {
                    match a_ptrbitarr {
                        CasResult(Ok(_)) => {
                            break;
                        }
                        CasResult(Err(newer_array)) => {
                            // Someone beat us to it, so we need to use the
                            // newer array.
                            retry_count += 1;
                            a_ptrbitarr = self.ptrbitarr.compare_exchange(newer_array,
                                S::into_ptrbitarr_size(
                                bit_pos | S::into_stride_size(newer_array),
                            ));
                        }
                    };
                    backoff.spin();
                }

                return (NewNodeOrIndex::NewNode(
                    new_node
                ), retry_count);
            }
        } else {
            // only at the last stride do we create the bit in the prefix
            // bitmap, and only if it doesn't exist already
            if pfxbitarr & bit_pos
                == <<<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType as std::ops::BitAnd>::Output::zero()
            {

                // THE CRIICAL SECTION
                //
                // UPDATING pfxbitarr
                //
                // preventing using an old pfxbitarr and overwrite bits set
                // in the meantime elsewhere in the bitarray.
                let mut a_pfxbitarr = 
                self.pfxbitarr.compare_exchange(
                    pfxbitarr, bit_pos | pfxbitarr
                );
                let backoff = Backoff::new();

                loop {
                    match a_pfxbitarr {
                        CasResult(Ok(_)) => {
                            break;
                        }
                        CasResult(Err(newer_array)) => {
                            // Someone beat us to it, so we need to use the
                            // newer array.
                            retry_count += 1;
                            a_pfxbitarr = self.pfxbitarr.compare_exchange(
                                newer_array, bit_pos | newer_array
                            );
                        }
                    };
                    backoff.spin();
                }

                return (NewNodeOrIndex::NewPrefix, retry_count);
            }
            return (NewNodeOrIndex::ExistingPrefix, retry_count);
        }

        // Nodes always live at the last length of a stride (i.e. the last 
        // nibble), so we add the stride length to the length of the
        // base_prefix (which is always the start length of the stride).
        (NewNodeOrIndex::ExistingNode(
            base_prefix.add_to_len(stride_len).truncate_to_len()
        ), retry_count)
    }

    //-------- Search nibble functions --------------------------------------

    // This function looks for the longest marching prefix in the provided
    // nibble, by iterating over all the bits in it and comparing that with
    // the appriopriate bytes from the requested prefix. It mutates the 
    // `less_specifics_vec` that was passed in to hold all the prefixes found
    // along the way.
    pub(crate) fn search_stride_for_longest_match_at(
        &self,
        search_pfx: PrefixId<AF>,
        mut nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        less_specifics_vec: &mut Option<Vec<PrefixId<AF>>>,
    ) -> (Option<StrideNodeId<AF>>, Option<PrefixId<AF>>) {
        let pfxbitarr = self.pfxbitarr.load();
        let ptrbitarr = self.ptrbitarr.load();
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;

        trace!("start longest_match search");
        for n_l in 1..(nibble_len + 1) {
            // Move the bit in the right position.
            nibble =
                AddressFamily::get_nibble(search_pfx.get_net(), start_bit, n_l);
            bit_pos = S::get_bit_pos(nibble, n_l);

            // Check if the prefix has been set, if so select the prefix.
            // This is not necessarily the final prefix that will be
            // returned.

            // Check it there's a prefix matching in this bitmap for this 
            // nibble.
            trace!("pfxbitarr {:032b}", pfxbitarr);

            if pfxbitarr & bit_pos > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero() {
                let f_pfx = PrefixId::new(search_pfx.get_net().truncate_to_len(start_bit + n_l), start_bit + n_l);
                // f_pfx.set_serial(self.get_pfx_serial(f_pfx, nibble, n_l, guard).load(Ordering::Relaxed));

                // Receiving a less_specifics_vec means that the user wants
                // to have all the last-specific prefixes returned, so add
                // the found prefix.
                trace!("gather pfx in less_specifics {:?}", f_pfx);
                trace!("ls_vec {:?}", less_specifics_vec);
                if let Some(ls_vec) = less_specifics_vec {
                    trace!("len {}", search_pfx.get_len());
                    trace!("start_bit {}", start_bit);
                    trace!("n_l {}", n_l);
                    trace!("smaller length? {}", search_pfx.get_len() > start_bit + n_l);
                    trace!("{}", (S::into_stride_size(ptrbitarr)
                            & bit_pos)
                            == <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero());
                    if search_pfx.get_len() > start_bit + n_l
                        && (S::into_stride_size(ptrbitarr)
                            & bit_pos)
                            == <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero()
                    {
                        ls_vec.push(f_pfx);
                    }
                }

                found_pfx = Some(f_pfx);
            }
        }

        let base_prefix =
            StrideNodeId::new_with_cleaned_id(search_pfx.get_net(), start_bit);

        // Check if this the last stride, or if they're no more children to
        // go to, if so return what we found up until now.
        if search_pfx.get_len() <= start_bit + nibble_len
            || (S::into_stride_size(ptrbitarr) & bit_pos) == <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero()
        // No children or at the end, return the definitive LMP we found.
        {
            return (
                None,      /* no more children */
                found_pfx, /* The definitive LMP if any */
            );
        }

        // There's another child, return it together with the preliminary LMP
        // we found.
        (
            // The identifier of the node that has children of the next
            // stride.
            Some(base_prefix.add_nibble(nibble, nibble_len)),
            found_pfx,
        )
    }

    // This function looks for the exactly matching prefix in the provided
    // nibble. It doesn't need to iterate over anything it just compares
    // the complete nibble, with the appropriate bits in the requested
    // prefix. Although this is rather efficient, there's no way to collect
    // less-specific prefixes from the search prefix.
    pub(crate) fn search_stride_for_exact_match_at(
        &'_ self,
        search_pfx: PrefixId<AF>,
        nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        _: &mut Option<Vec<PrefixId<AF>>>,
    ) -> (Option<StrideNodeId<AF>>, Option<PrefixId<AF>>) {
        let pfxbitarr = self.pfxbitarr.load();
        let ptrbitarr = self.ptrbitarr.load();
        // This is an exact match, so we're only considering the position of
        // the full nibble.
        let bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;
        let mut found_child = None;

        // Is this the last nibble?
        // Otherwise we're not looking for a prefix (exact matching only
        // lives at last nibble)
        match search_pfx.get_len() <= start_bit + nibble_len {
            // We're at the last nibble.
            true => {
                // Check for an actual prefix at the right position, i.e. 
                // consider the complete nibble.
                if pfxbitarr & bit_pos > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero() {
                    let f_pfx = PrefixId::new(search_pfx.get_net().truncate_to_len(start_bit + nibble_len), start_bit + nibble_len);
                    found_pfx = Some(f_pfx);
                }
            }
            // We're not at the last nibble.
            false => {
                // Check for a child node at the right position, i.e.
                // consider the complete nibble.
                if (S::into_stride_size(ptrbitarr) & bit_pos) > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero()
                {
                    found_child = Some(
                        StrideNodeId::new_with_cleaned_id(search_pfx.get_net(), start_bit + nibble_len)
                    );
                }
            }
        }

        (
            found_child, /* The node that has children in the next stride, if
                            any */
            found_pfx,   /* The exactly matching prefix, if any */
        )
    }

    // This function looks for the exactly matching prefix in the provided
    // nibble, just like the one above, but this *does* iterate over all the
    // bytes in the nibble to collect the less-specific prefixes of the the 
    // search prefix. This is of course slower, so it should only be used 
    // when the user explicitly requests less-specifics.
    pub(crate) fn search_stride_for_exact_match_with_less_specifics_at(
        &self,
        search_pfx: PrefixId<AF>,
        mut nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        less_specifics_vec: &mut Option<Vec<PrefixId<AF>>>,
    ) -> (Option<StrideNodeId<AF>>, Option<PrefixId<AF>>) {
        let pfxbitarr = self.pfxbitarr.load();
        let ptrbitarr = self.ptrbitarr.load();
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;

        let ls_vec = less_specifics_vec
            .as_mut()
            .expect(concat!("You shouldn't call this function without",
            "a `less_specifics_vec` buffer. Supply one when calling this function",
            "or use `search_stride_for_exact_match_at`"));

        for n_l in 1..(nibble_len + 1) {
            // Move the bit in the right position.
            nibble =
                AddressFamily::get_nibble(search_pfx.get_net(), start_bit, n_l);
            bit_pos = S::get_bit_pos(nibble, n_l);

            // Check if the prefix has been set, if so select the prefix.
            // This is not necessarily the final prefix that will be
            // returned.

            // Check it there's a prefix matching in this bitmap for this 
            // nibble.
            if pfxbitarr & bit_pos > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero() {
                // since we want an exact match only, we will fill the prefix
                // field only if we're exactly at the last bit of the nibble
                if n_l == nibble_len {
                    let f_pfx =
                        PrefixId::new(
                            search_pfx.get_net().truncate_to_len(start_bit + n_l), start_bit + n_l);
                        found_pfx = Some(f_pfx);
                }

                // Receiving a less_specifics_vec means that the user wants to
                // have all the last-specific prefixes returned, so add the
                // found prefix.
                let f_pfx = PrefixId::new(search_pfx.get_net().truncate_to_len(start_bit + n_l), start_bit + n_l);
                ls_vec.push(f_pfx);
            }
        }

        if found_pfx.is_none() {
            // no prefix here, clear out all of the prefixes we found along
            // the way, since it doesn't make sense to return less-specifics
            // if we don't have a exact match.
            ls_vec.clear();
        }

        // Check if this the last stride, or if they're no more children to 
        // go to, if so return what we found up until now.
        match search_pfx.get_len() <= start_bit + nibble_len
            || (S::into_stride_size(ptrbitarr) & bit_pos)
                == <<<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType as std::ops::BitAnd>::Output::zero()
        {
            // No children or at the end, return the definitive LMP we found.
            true => (
                None,      /* no more children */
                found_pfx, /* The definitive LMP if any */
            ),
            // There's another child, we won't return the found_pfx, since
            // we're not at the last nibble and we want an exact match only.
            false => (
                Some(StrideNodeId::new_with_cleaned_id(search_pfx.get_net(), start_bit + nibble_len)),
                None,
            ),
        }
    }

    // Search a stride for more-specific prefixes and child nodes containing
    // more specifics for `search_prefix`.
    pub(crate) fn add_more_specifics_at(
        &self,
        nibble: u32,
        nibble_len: u8,
        base_prefix: StrideNodeId<AF>,
    ) -> (
        Vec<StrideNodeId<AF>>, /* child nodes with more more-specifics in
                                  this stride */
        Vec<PrefixId<AF>>,     /* more-specific prefixes in this stride */
    ) {
        trace!("start adding more specifics");
        let pfxbitarr = self.pfxbitarr.load();
        let ptrbitarr = self.ptrbitarr.load();
        trace!("ptrbitarr {:032b}", ptrbitarr);
        trace!("pfxbitarr {:032b}", pfxbitarr);
        let mut found_children_with_more_specifics = vec![];
        let mut found_more_specifics_vec: Vec<PrefixId<AF>> = vec![];

        // This is an exact match, so we're only considering the position of
        // the full nibble.
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_child = None;

        // Is there also a child node here?
        // Note that even without a child node, there may be more specifics 
        // further up in this pfxbitarr or children in this ptrbitarr.
        if (S::into_stride_size(ptrbitarr) & bit_pos)
            > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero(
            )
        {
            found_child = Some(base_prefix.add_nibble(nibble, nibble_len));
        }

        if let Some(child) = found_child {
            found_children_with_more_specifics.push(child);
        }

        // We're expanding the search for more-specifics bit-by-bit.
        // `ms_nibble_len` is the number of bits including the original
        // nibble we're considering, e.g. if our prefix has a length of 25
        // and we've all strides sized 4, we would end up with a last
        // nibble_len of 1. `ms_nibble_len` will expand then from 2 up and
        // till 4.
        // 
        // ex.:
        // nibble: 1 , (nibble_len: 1)
        // Iteration:
        // ms_nibble_len=1,n_l=0: 10, n_l=1: 11
        // ms_nibble_len=2,n_l=0: 100, n_l=1: 101, n_l=2: 110, n_l=3: 111
        // ms_nibble_len=3,n_l=0: 1000, n_l=1: 1001, n_l=2: 1010, ..., 
        // n_l=7: 1111

        for ms_nibble_len in nibble_len + 1..=S::STRIDE_LEN {
            // iterate over all the possible values for this `ms_nibble_len`,
            // e.g. two bits can have 4 different values.
            for n_l in 0..(1 << (ms_nibble_len - nibble_len)) {
                // move the nibble left with the amount of bits we're going
                // to loop over. e.g. a stride of size 4 with a nibble 0000
                // 0000 0000 0011 becomes 0000 0000 0000 1100, then it will
                // iterate over ...1100,...1101,...1110,...1111
                let ms_nibble =
                    (nibble << (ms_nibble_len - nibble_len)) + n_l as u32;
                bit_pos = S::get_bit_pos(ms_nibble, ms_nibble_len);

                if (S::into_stride_size(ptrbitarr) & bit_pos) > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero()
                {
                    found_children_with_more_specifics.push(
                    base_prefix.add_nibble(ms_nibble, ms_nibble_len)
                    );
                }

                if pfxbitarr & bit_pos > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero() {
                    found_more_specifics_vec.push(
                        base_prefix.add_nibble(ms_nibble, ms_nibble_len).into()                    )
                }
            }
        }

        trace!("found_children_with_more_specifics {:?}", found_children_with_more_specifics);
        trace!("found_more_specifics_vec {:?}", found_more_specifics_vec);

        (
            // We're done here, the caller should now go over all nodes in
            // found_children_with_more_specifics vec and add ALL prefixes
            // found in there.
            found_children_with_more_specifics,
            found_more_specifics_vec,
        )
    }
}


// ------------ Iterator methods --------------------------------------------

// ----------- NodeChildIter ------------------------------------------------

// create an iterator over all child nodes id
//
// we don't have a collection of local nodes anymore, since the id of the
// node are deterministically generated, as the prefix+len they represent
// in the treebitmap. This has both the advantage of using less memory,
// and being easier to use in a concurrently updated tree. The
// disadvantage is that we have to look up the child nodes on the fly
// when we want to iterate over all children of a node.
//
// ptr child nodes only exist at the last nibble of the stride size
// (`child_len`). Since children  in the first nibbles are leaf nodes.
// leaf nodes will only be prefixes. So if we have a first stride of
// size 5, all ptr nodes wil have StrideNodeIds with len = 5.
//
// Ex.:
//
// Stride no.          1       2       3      4       5       6       7       
// StrideSize          5       5       4      3       3       3       3      
// child pfxs len      /1-5   /5-10    /10-14 /15-17  /18-20  /21-23  /24-26
// child Nodes len     /5      /10     /14    /17     /20     /23     /26 
//
// Stride no.          8      9
// StrideSize          3      3
// child pfxs len      /27-29 /30-32
// child Nodes len     /29    /32

#[derive(Debug, Copy, Clone)]
pub(crate) struct NodeChildIter<AF: AddressFamily, S: Stride> {
   base_prefix: StrideNodeId<AF>,
   ptrbitarr: <<S as Stride>::AtomicPtrSize as AtomicBitmap>::InnerType,
   bit_span: BitSpan, // start with 0
   _af: PhantomData<AF>,
}

impl<AF: AddressFamily, S: Stride> std::iter::Iterator for
    NodeChildIter<AF, S>
{
    type Item = StrideNodeId<AF>;
    fn next(&mut self) -> Option<Self::Item> {
        // iterate over all the possible values for this stride length, e.g.
        // two bits can have 4 different values.
        for cursor in self.bit_span.bits..(1 << S::STRIDE_LEN) {
            // move the bit_span left with the amount of bits we're going to
            // loop over.
            // e.g. a stride of size 4 with a nibble 0000 0000 0000 0011
            // becomes 0000 0000 0000 1100, then it will iterate over 
            // ...1100,...1101,...1110,...1111
            let bit_pos = S::get_bit_pos(cursor, S::STRIDE_LEN);
            if (S::into_stride_size(self.ptrbitarr) & bit_pos) >
                <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero()
            {
                self.bit_span.bits = cursor + 1;
                return Some(self.base_prefix.add_nibble(cursor, S::STRIDE_LEN));
            }    
            
        }
        None
    }
}

// ----------- NodeMoreSpecificChildIter ------------------------------------

// Create an iterator over all the child nodes that hold a more specific
// prefixes of the specified start_bit_span. This basically the same Iterator
// as the ChildNodeIter, except that it stops (potentially) earlier, to avoid
// including nodes with adjacent prefixes. Starting an iterator with a 
// `start_bit_span` of { bits: 0, len: 0 } will return all child nodes of 
// this node. In that case you could also use the `NodeChildIter` instead.
//
// inputs
//
// `base_prefix`
// This iterator take a `base_prefix` since the nodes themselves have no
// knowledge of their own prefixes, those are inferred by their position in
// the tree (therefore, it's actually a Trie). Note that `base_prefix` +
// `bit_span` define the actual starting prefix for this iteratator.
//
// `ptrbitarr` 
// is the bitmap that holds the slots that have child nodes. 
//
// `start_bit_span` 
// is the bit span that is going to be used as a starting point for the 
// iterator.
// 
// `cursor`
// holds the current cursor offset from the start_bit_span.bits, the sum of
// these describe the current position in the bitmap. Used for re-entry into
// the iterator. A new iterator should start with None.
//
// How this works
//
// The iterator starts at the start_bit_span.bits position in the bitmap and
// advances until it reaches either a one in the bitmap, or the maximum
// position for the particular more-specifics for this bit_span.
//
// e.x.
// The stride size is 5 and the starting bit span is {bits: 2, len: 4} (0010)
// The starting point is therefore the bit_array 0010. The iterator will go
// over 0010 0 and 0010 1. The next bits to consider would be 0011 0 which
// would not fit our starting bit_span of 0010. So we have to stop after 2
// iterations. This means that the number of iterations is determined by the
// difference between the number of bits in the stride size (5) and the the
// number of bits in the start_bit_span (4). The number of iterations in the
// above example is therefore 1 << (5 - 4) = 2. Remember that a ptrbitarr
// holds only one stride size (the largest for its stride size), so we're
// done now.
#[derive(Debug, Copy, Clone)]
pub(crate) struct NodeMoreSpecificChildIter<AF: AddressFamily, S: Stride> {
   base_prefix: StrideNodeId<AF>,
   ptrbitarr: <<S as Stride>::AtomicPtrSize as AtomicBitmap>::InnerType,
   start_bit_span: BitSpan,
   cursor: Option<u32>,
   _af: PhantomData<AF>,
}

impl<AF: AddressFamily, S: Stride> std::iter::Iterator for
    NodeMoreSpecificChildIter<AF, S>
{
    type Item = StrideNodeId<AF>;
    fn next(&mut self) -> Option<Self::Item> {
        // Early exits

        // Empty bitmap
        if self.ptrbitarr == <<S as Stride>::AtomicPtrSize as AtomicBitmap>::InnerType::zero() {
            trace!("empty ptrbitrarr. this iterator is done.");
            return None;
        }

        // Previous iteration incremented the cursor beyond the stride size.
        if let Some(cursor) = self.cursor { 
            if cursor >= (1 << (S::STRIDE_LEN - self.start_bit_span.len)) {
                trace!("cursor.bits >= (1 << (S::STRIDE_LEN - self.start_bit_span.len))");
                trace!("cursor: {}", cursor);
                trace!("start_bit_span: {} {}", self.start_bit_span.bits, self.start_bit_span.len);
                return None;
            }
        }

        // No early exits, we're in business.
        trace!("NodeMoreSpecificChildIter");
        trace!("base_prefix {}", self.base_prefix);
        trace!("stride_size {}", S::STRIDE_LEN);
        trace!("start_bit_span bits {} len {} ", self.start_bit_span.bits, self.start_bit_span.len);
        trace!("cursor bits {:?}", self.cursor);
        trace!("          x1  4   8  12  16  20  24  28  32");
        trace!("ptrbitarr {:032b}", self.ptrbitarr);

        let start = if let Some(bits) = self.cursor { bits } else { self.start_bit_span.bits };
        // We either stop if we have reached the maximum number of bits that
        // we should check for this bit_span or we stop at the end of the
        // stride (in case of a start_bit_span.bits == 0).
        let stop = <u32>::min((1 << (S::STRIDE_LEN - self.start_bit_span.len)) + start, (1 << S::STRIDE_LEN) - 1); 

        trace!("start {:?} stop {}", start, stop);
        for cursor in start..=stop {
            // move the bit_span left with the amount of bits we're going to loop over.
            // e.g. a stride of size 4 with a nibble 0000 0000 0000 0011
            // becomes 0000 0000 0000 1100, then it will iterate over 
            // ...1100,...1101,...1110,...1111
            let bit_pos = 
                S::get_bit_pos(
                    cursor, S::STRIDE_LEN);
            trace!("cmpbitarr x{:032b} {}", bit_pos, stop - start);
            if (S::into_stride_size(self.ptrbitarr) & bit_pos) >
                <<S as Stride>::AtomicPfxSize as AtomicBitmap
                >::InnerType::zero()
            {
                trace!("bingo!");
                self.cursor = Some(cursor + 1);

                trace!("next bit_span {} {} with cursor {:?}", self.start_bit_span.bits, self.start_bit_span.len, self.cursor);
                return Some(self.base_prefix.add_nibble(cursor, S::STRIDE_LEN));
            }
        }
        trace!("No more nodes. End of the iterator.");
        None
    }
}

impl<AF: AddressFamily> NodeMoreSpecificChildIter<AF, Stride3> {
    pub fn wrap(self) -> SizedNodeMoreSpecificIter<AF> {
        SizedNodeMoreSpecificIter::<AF>::Stride3(self)
    }
}

impl<AF: AddressFamily> NodeMoreSpecificChildIter<AF, Stride4> {
    pub fn wrap(self) -> SizedNodeMoreSpecificIter<AF> {
        SizedNodeMoreSpecificIter::<AF>::Stride4(self)
    }
}

impl<AF: AddressFamily> NodeMoreSpecificChildIter<AF, Stride5> {
    pub fn wrap(self) -> SizedNodeMoreSpecificIter<AF> {
        SizedNodeMoreSpecificIter::<AF>::Stride5(self)
    }
}


// ----------- NodePrefixIter -----------------------------------------------

// Create an iterator of all prefix ids hosted by this node.

// Partition for stride 3
//
// pfxbitarr (AF::BITS)  0 1 2  3  4  5  6   7   8   9  10  11  12  13  14 
// bit_span (binary)     * 0 1 00 01 10 11 000 001 010 011 100 101 110 111 * *
// bit_span (dec.)       * 0 1  0  1  2  3   0   1   2   3   4   5   6   7 * *
// len                   0 1   2           3                               
//
// pfxbitarr (example)   1 0 0 0  0  0  1  1   1   0   0   0   0   0   0   0 0
// pos (example)         0 0 0 0  0  0  0  0   1   0   0   0   0   0   0   0 0                 
//
// Ex.:
// `pos` describes the bit that is currently under consideration. 
// 
// `pfxbitarr` is the bitmap that contains the prefixes. Every 1 in the
// bitmap means that the prefix is hosted by this node. Moreover, the
// position in the bitmap describes the address part of the prefix, given
// a `base prefix`. The descibed prefix is the bits of the `base_prefix`
// bitmap appended by the `bit span` bits.
//
// The length of the prefix is
// described by sum of the length of the base_prefix and the `len`
// variable.
//
// The `bit_span` variable starts counting at every new prefix length.
pub(crate) struct NodePrefixIter<AF: AddressFamily, S: Stride> {
    base_prefix: StrideNodeId<AF>,
    pfxbitarr: <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType,
    bit_span: BitSpan, // start with 0
    _af: PhantomData<AF>,
    _s: PhantomData<S>,
}

impl<AF: AddressFamily, S: Stride> std::iter::Iterator for 
    NodePrefixIter<AF, S> {
        type Item = PrefixId<AF>;

        fn next(&mut self) -> Option<Self::Item> {
        // iterate over all the possible values for this stride length, e.g.
        // two bits can have 4 different values.
        for cursor in self.bit_span.bits..(1 << S::STRIDE_LEN) {
            
            let bit_pos = S::get_bit_pos(cursor, S::STRIDE_LEN);
            if self.pfxbitarr & bit_pos >
                <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero()
            {
                self.bit_span.bits = cursor + 1;
                return Some(self.base_prefix.add_nibble(cursor, S::STRIDE_LEN).into());
            }    
            
        }
        None
    }
}

// Creates an Iterator that returns all prefixes that exist in a node that
// are a more-specific prefix of the `base_prefix` + `start_bit_span`.
//
// Inputs
// 
// `base_prefix`
// This iterator take a `base_prefix` since the nodes themselves have no
// knowledge of their own prefixes, those are inferred by their position in
// the tree (therefore, it's actually a Trie). Note that `base_prefix` +
// `bit_span` define the actual starting prefix for this iteratator.
//
// `pfxbitarr` 
// is the bitmap that holds the slots that have prefixes.
//
// `start_bit_span` 
// is the bit span that is going to be used as a starting point for the 
// iterator.
// 
// `cursor`
// holds the current cursor offset from the start_bit_span.bits, the sum of
// these describe the current position in the bitmap. Used for re-entry into
// the iterator. A new iterator should start with None.
//
// How this works
//
// The iterator starts at the start_bit_span.bits position in the bitmap and
// advances until it reaches either a one in the bitmap, or the maximum
// position for the particular more-specifics for this bit_span. When it
// reaches the maximum position it determines whether there are more stride-
// sizes available in this bitmap. If there are, it advances to the next
// stride-size in the first position. If not it terminates the iterator.
//
// e.x.
// The stride size is 5 and the starting bit span is {bits: 1, len: 3} (001)
// This means that the stride size that we have to consider are 4 and 5. 3 
// being the size of the current bit_span and 5 being the size of the total
// stride. 
// The starting point is therefore the bit_array 001. The iterator will go
// over 001 00, 001 01, 001 10 and 001 11. The next bits to consider would be
//  010 00 which would not fit our starting bit_span of 0010. So we have to
// stop after 2 iterations. This means that the number of iterations is
// determined by the difference between the number of bits in the stride size
// (5) and the the number of bits in the start_bit_span (4). The number of 
// iterations in the above example is therefore 1 << (5 - 3) = 4.
// Unlike the MoreSpecificPrefixIter, we will have to consider more lengths
// than just the bit_span len. We will have to jump a few pfxbitarr birs and
// move to the next stride size in the bitmap, starting at bit_array 0010, or
// the bit_span { bits: 2, len: 3 }, a.k.a. 0010 << 1. But now we will have
// to go over a different amount of 1 << (5 - 4) = 2 iterations to reap the
// next bit_spans of 0010 0 and 0010 1.

pub(crate) struct NodeMoreSpecificsPrefixIter<AF: AddressFamily, S: Stride> {
    // immutables
    base_prefix: StrideNodeId<AF>,
    pfxbitarr: <<S as crate::local_array::atomic_stride::Stride>::AtomicPfxSize as crate::local_array::atomic_stride::AtomicBitmap>::InnerType,
    // we need to keep around only the `bits` part of the `bit_span`
    // technically, (it needs resetting the current state to it after each
    // prefix-length), but we'll keep the start-length as well for clarity
    // and increment it on a different field ('cur_len').
    start_bit_span: BitSpan,
    cursor: BitSpan,
    skip_self: bool,
    _s: PhantomData<S>,
}

impl<AF: AddressFamily, S: Stride> std::iter::Iterator for 
    NodeMoreSpecificsPrefixIter<AF, S> {
        type Item = PrefixId<AF>;

        fn next(&mut self) -> Option<Self::Item> {

            // Easy early exit conditions

            // Empty bitmap
            if self.pfxbitarr == <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero() {
                trace!("empty pfxbitarr. This iterator is done.");
                return None;
            }

            // No early exits, We're in business.
            trace!("len_offset {}", ((1<< self.cursor.len) - 1));
            trace!("start_bit {}", self.start_bit_span.bits);
            trace!("number of check bits in len {}", 
                (1 << (self.cursor.len - self.start_bit_span.len)));

            trace!("next more specifics prefix iter start bits {} len {}",
                self.start_bit_span.bits, self.start_bit_span.len);

            let mut res = None;

            // Move to the next len if we're at the first prefix-length that matches,
            // if `skip_self` is set. Typically this flag is set for the first stride.
            // In the consecutive strides, we don't want to skip the base prefix, since
            // that base_prefix is a more specific prefix of the one requested in the
            // first stride.
            if self.skip_self && (1 << (self.cursor.len - self.start_bit_span.len)) == 1 {
                // self.cursor.len += (self.start_bit_span.bits & 1) as u8;
                trace!("skipping self");
                self.start_bit_span.bits <<= 1;
                self.cursor.bits = self.start_bit_span.bits;
                self.cursor.len += 1;
                self.skip_self = false;
                trace!("new start bits {} len {}", self.start_bit_span.bits, self.start_bit_span.len);
                trace!("new cursor bits {} len {}", self.cursor.bits, self.cursor.len);
            }

            // Previous iteration or the skip_self routine may have
            // incremented the cursor beyond the end of the stride size.
            if self.cursor.len > S::STRIDE_LEN {
                trace!("cursor.len > S::STRIDE_LEN. This iterator is done.");
                return None;
            }
            
            loop {
                trace!("          x1  4   8  12  16  20  24  28  32  36  40  44  48  52  56  60  64");
                trace!("cmpnibble {:064b} ({} + {}) len {} stride_size {}", 
                    S::get_bit_pos(self.cursor.bits, self.cursor.len), 
                    (1<< self.cursor.len) - 1, 
                    self.cursor.bits, 
                    self.cursor.len + self.base_prefix.get_len(),
                    S::STRIDE_LEN
                );

                trace!("pfxbitarr {:064b}", self.pfxbitarr);

                if (S::get_bit_pos(self.cursor.bits, self.cursor.len) | self.pfxbitarr) == self.pfxbitarr {
                    trace!("found prefix with len {} at pos {} pfx len {}",
                        self.cursor.len, 
                        self.cursor.bits,
                        self.base_prefix.get_len() + self.cursor.len,
                    );
                    res = Some(self.base_prefix
                        .add_nibble(self.cursor.bits, self.cursor.len).into());
                    trace!("found prefix {:?}", res);
                }

                // Determine when we're at the end of the bits applicable to
                // this combo of this start_bit_span.
                // bitspan offset: 
                // self.start_bit_span.bits
                // number of matches in this length:
                // 1 << (self.cursor.len - self.start_bit_span.len)
                let max_pos_offset =  
                    self.start_bit_span.bits + 
                    (1 << (self.cursor.len - self.start_bit_span.len)) - 1;

                trace!("max_pos_offset {} > cursor bit_pos {}?", max_pos_offset, self.cursor.bits);
                trace!("number of check bits in len {}", (1 << (self.cursor.len - self.start_bit_span.len)));

                // case 1. At the beginning or inside a prefix-length.
                if max_pos_offset > self.cursor.bits {
                    self.cursor.bits += 1;
                } 
                // case 2. At the end of a prefix-lengths in this stride.
                else if self.cursor.len < S::STRIDE_LEN {
                    trace!("move len to {}", self.cursor.len + 1);
                    self.start_bit_span.bits <<= 1;
                    self.cursor.bits = self.start_bit_span.bits;
                    self.cursor.len += 1;
                }
                // case 4. At the end of a prefix-length, but not at the end of the pfxbitarr.
                else {
                    self.start_bit_span.bits <<= 1;
                    self.cursor.bits = self.start_bit_span.bits;
                    self.cursor.len += 1;
                    trace!("end of stride, next cursor bits {} len {}", self.cursor.bits, self.cursor.len);
                    return res;
                }

                trace!("some res {:?}", res);
                if res.is_some() { return res; }
            }
        }    
}

impl<AF: AddressFamily> NodeMoreSpecificsPrefixIter<AF, Stride3> {
    pub fn wrap(self) -> SizedPrefixIter<AF> {
        SizedPrefixIter::<AF>::Stride3(self)
    }
}

impl<AF: AddressFamily> NodeMoreSpecificsPrefixIter<AF, Stride4> {
    pub fn wrap(self) -> SizedPrefixIter<AF> {
        SizedPrefixIter::<AF>::Stride4(self)
    }
}

impl<AF: AddressFamily> NodeMoreSpecificsPrefixIter<AF, Stride5> {
    pub fn wrap(self) -> SizedPrefixIter<AF> {
        SizedPrefixIter::Stride5(self)
    }
}
