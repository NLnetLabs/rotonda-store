use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering, AtomicUsize};
use std::{
    fmt::Debug,
    marker::PhantomData,
};

use crossbeam_epoch::Guard;
use routecore::record::NoMeta;

pub use super::atomic_stride::*;
pub use crate::local_array::query::*;
pub use crate::local_array::tree::*;
use crate::prefix_record::InternalPrefixRecord;
use crate::af::Zero;
use crate::af::AddressFamily;

//------------ TreeBitMap Node ----------------------------------------------

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
    // The vec of prefixes hosted by this node, referenced by (bit_id, global
    // prefix index). This is the exact same type as for the NodeIds, so we
    // reuse that.
    // pub pfx_vec: PrefixSet<AF>,
    // The vec of child nodes hosted by this node, referenced by
    // (ptrbitarr_index, global vec index). We need the u16 (ptrbitarr_index)
    // to sort the vec that's stored in the node.
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
            // .field("pfx_vec", &self.pfx_vec)
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
            // self.pfx_vec
        )
    }
}

impl<AF, S>
    TreeBitMapNode<AF, S>
where
    AF: AddressFamily,
    S: Stride
{
    // create a vec of all child nodes id
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

    pub(crate) fn ptr_vec(
        &self,
        base_prefix: StrideNodeId<AF>,
    ) -> Vec<StrideNodeId<AF>> {
        let mut child_node_ids = Vec::new();
        // CRITICAL SECTION ALERT
        let ptrbitarr = self.ptrbitarr.load();
        // iterate over all the possible values for this `nibble_len`, e.g.
        // two bits can have 4 different values.
        for nibble in 0..(1 << S::STRIDE_LEN) {
            // move the nibble left with the amount of bits we're going to
            // loop over.
            // e.g. a stride of size 4 with a nibble 0000 0000 0000 0011
            // becomes 0000 0000 0000 1100, then it will iterate over 
            // ...1100,...1101,...1110,...1111
            let bit_pos = S::get_bit_pos(nibble, S::STRIDE_LEN);

            // CRITICAL SECTION ALERT!
            if (S::into_stride_size(ptrbitarr) & bit_pos) > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero()
            {
                child_node_ids.push(
                    base_prefix.add_nibble(nibble, S::STRIDE_LEN)
                );
            }
        }

        child_node_ids
    }

    pub(crate) fn pfx_vec(
        &self,
        base_prefix: StrideNodeId<AF>
    ) -> Vec<PrefixId<AF>> {
        let mut prefix_ids: Vec<PrefixId<AF>> = Vec::new();
        let pfxbitarr = self.pfxbitarr.load();
        for nibble in 0..S::STRIDE_LEN as u32 {
            let bit_pos = S::get_bit_pos(nibble, S::STRIDE_LEN);
            if (pfxbitarr & bit_pos) > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero()
            {
                prefix_ids.push(
                    base_prefix.add_nibble(nibble,S::STRIDE_LEN).into()
                );
            }
        }
        prefix_ids
    }

    pub(crate) fn get_pfx_serial<'a>(&'a self, base_prefix: PrefixId<AF>, nibble: u32, nibble_len: u8, guard: &'a Guard) -> &mut AtomicUsize {
        let index = <S as Stride>::get_pfx_index(nibble, nibble_len);
        todo!()
    }

    // Inspects the stride (nibble, nibble_len) to see it there's already a 
    // child node (if not at the last stride) or a prefix (if it's the last
    // stride).
    //
    // Returns one of:
    // - A newly created child node.
    // - The index of the existing child node in the global `nodes` vec
    // - A newly created Prefix
    // - The index of the existing prefix in the global `prefixes` vec
    pub(crate) fn eval_node_or_prefix_at<'a>(
        &'a mut self,
        nibble: u32,
        nibble_len: u8,
        // all the bits of the search prefix, but with the length set to
        // the length of this stride. So bits are set beyond its length.
        base_prefix: StrideNodeId<AF>,
        stride_len: u8,
        next_stride: Option<&u8>,
        is_last_stride: bool,
        guard: &'a Guard,
    ) -> NewNodeOrIndex<AF> {

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
                loop {
                    match a_ptrbitarr {
                        CasResult(Ok(_)) => {
                            break;
                        }
                        CasResult(Err(newer_array)) => {
                            // Someone beat us to it, so we need to use the
                            // newer array.
                            a_ptrbitarr = self.ptrbitarr.compare_exchange(newer_array,
                                S::into_ptrbitarr_size(
                                bit_pos | S::into_stride_size(newer_array),
                            ));
                        }
                    };
                }

                return NewNodeOrIndex::NewNode(
                    new_node
                );
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
                loop {
                    match a_pfxbitarr {
                        CasResult(Ok(_)) => {
                            break;
                        }
                        CasResult(Err(newer_array)) => {
                            // Someone beat us to it, so we need to use the
                            // newer array.
                            a_pfxbitarr = self.pfxbitarr.compare_exchange(
                                newer_array, bit_pos | newer_array
                            );
                        }
                    };
                }

                // self.pfxbitarr.compare_exchange(pfxbitarr, bit_pos | pfxbitarr);
                // CHECK THE RETURN VALUE HERE AND ACT ACCORDINGLY!!!!
                return NewNodeOrIndex::NewPrefix(self.get_pfx_serial(base_prefix.into(), nibble, nibble_len, guard));
            }
            // A prefix exists as a child of the base_prefix, so create the
            // PrefixId with the right offset from the base prefix and cut
            // it off at that point.
            let pfx: PrefixId<AF> = base_prefix.add_to_len(nibble_len).truncate_to_len().into();
            return NewNodeOrIndex::ExistingPrefix(
                    pfx, 
                    self.get_pfx_serial(base_prefix.into(), nibble, nibble_len, guard)
                );
        }

        // Nodes always live at the last length of a stride (i.e. the last 
        // nibble), so we add the stride length to the length of the
        // base_prefix (which is always the start length of the stride).
        NewNodeOrIndex::ExistingNode(base_prefix.add_to_len(stride_len).truncate_to_len())
    }

    //-------- Search nibble functions --------------------------------------

    // This function looks for the longest marching prefix in the provided
    // nibble, by iterating over all the bits in it and comparing that with
    // the appriopriate bytes from the requested prefix. It mutates the 
    // `less_specifics_vec` that was passed in to hold all the prefixes found
    // along the way.
    pub(crate) fn search_stride_for_longest_match_at<'a>(
        &'a self,
        search_pfx: &InternalPrefixRecord<AF, NoMeta>,
        mut nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        less_specifics_vec: &mut Option<Vec<PrefixId<AF>>>,
        guard: &'a Guard
    ) -> (Option<StrideNodeId<AF>>, Option<PrefixId<AF>>) {
        let pfxbitarr = self.pfxbitarr.load();
        let ptrbitarr = self.ptrbitarr.load();
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;

        for n_l in 1..(nibble_len + 1) {
            // Move the bit in the right position.
            nibble =
                AddressFamily::get_nibble(search_pfx.net, start_bit, n_l);
            bit_pos = S::get_bit_pos(nibble, n_l);

            // Check if the prefix has been set, if so select the prefix.
            // This is not necessarily the final prefix that will be
            // returned.

            // Check it there's a prefix matching in this bitmap for this 
            // nibble.
            if pfxbitarr & bit_pos > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero() {
                let f_pfx = PrefixId::new(search_pfx.net.truncate_to_len(start_bit + n_l), start_bit + n_l);
                f_pfx.set_serial(self.get_pfx_serial(f_pfx, nibble, n_l, guard).load(Ordering::Relaxed));

                // Receiving a less_specifics_vec means that the user wants
                // to have all the last-specific prefixes returned, so add
                // the found prefix.
                if let Some(ls_vec) = less_specifics_vec {
                    if !(search_pfx.len <= start_bit + nibble_len
                        || (S::into_stride_size(ptrbitarr)
                            & S::get_bit_pos(nibble, nibble_len))
                            == <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero())
                    {
                        ls_vec.push(f_pfx);
                    }
                }

                found_pfx = Some(f_pfx);
            }
        }

        let base_prefix =
            StrideNodeId::new_with_cleaned_id(search_pfx.net, start_bit);

        // Check if this the last stride, or if they're no more children to
        // go to, if so return what we found up until now.
        if search_pfx.len <= start_bit + nibble_len
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
    pub(crate) fn search_stride_for_exact_match_at<'a>(
        &'a self,
        search_pfx: &InternalPrefixRecord<AF, NoMeta>,
        nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        _: &mut Option<Vec<PrefixId<AF>>>,
        guard: &'a Guard
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
        match search_pfx.len <= start_bit + nibble_len {
            // We're at the last nibble.
            true => {
                // Check for an actual prefix at the right position, i.e. 
                // consider the complete nibble.
                if pfxbitarr & bit_pos > <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType::zero() {
                    let f_pfx = PrefixId::new(search_pfx.net.truncate_to_len(start_bit + nibble_len), start_bit + nibble_len);
                    f_pfx.set_serial(
                        self.get_pfx_serial(f_pfx, nibble, nibble_len, guard).load(Ordering::Acquire)
                    );
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
                        StrideNodeId::new_with_cleaned_id(search_pfx.net, start_bit + nibble_len)
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
    pub(crate) fn search_stride_for_exact_match_with_less_specifics_at<'a>(
        &'a self,
        search_pfx: &InternalPrefixRecord<AF, NoMeta>,
        mut nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        less_specifics_vec: &mut Option<Vec<PrefixId<AF>>>,
        guard: &'a Guard
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
                AddressFamily::get_nibble(search_pfx.net, start_bit, n_l);
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
                            search_pfx.net.truncate_to_len(start_bit + n_l), start_bit + n_l);
                        f_pfx
                        .set_serial(self.get_pfx_serial(f_pfx,nibble, n_l, guard).load(std::sync::atomic::Ordering::Acquire));
                        found_pfx = Some(f_pfx);
                        // self.pfx_vec.to_vec()
                            // [S::get_pfx_index(nibble, n_l)],
                    // );
                }

                // Receiving a less_specifics_vec means that the user wants to
                // have all the last-specific prefixes returned, so add the
                // found prefix.
                let f_pfx = PrefixId::new(search_pfx.net.truncate_to_len(start_bit + n_l), start_bit + n_l);
                f_pfx.set_serial(self.get_pfx_serial(f_pfx, nibble,n_l,guard).load(Ordering::Acquire));
                ls_vec.push(f_pfx);
                    // self.pfx_vec.to_vec()[S::get_pfx_index(nibble, n_l)],
                // );
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
        match search_pfx.len <= start_bit + nibble_len
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
                Some(StrideNodeId::new_with_cleaned_id(search_pfx.net, start_bit + nibble_len)),
                None,
            ),
        }
    }

    // Search a stride for more-specific prefixes and child nodes containing
    // more specifics for `search_prefix`.
    pub(crate) fn add_more_specifics_at<'a>(
        &'a self,
        nibble: u32,
        nibble_len: u8,
        base_prefix: StrideNodeId<AF>,
        guard: &'a Guard
    ) -> (
        Vec<StrideNodeId<AF>>, /* child nodes with more more-specifics in
                                  this stride */
        Vec<PrefixId<AF>>,     /* more-specific prefixes in this stride */
    ) {
        let pfxbitarr = self.pfxbitarr.load();
        let ptrbitarr = self.ptrbitarr.load();
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

        for ms_nibble_len in nibble_len + 1..S::STRIDE_LEN + 1 {
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
                        PrefixId::new(
                            base_prefix.get_id().0
                            .add_nibble(
                                base_prefix.get_id().1, ms_nibble, ms_nibble_len
                            ).0, 
                            base_prefix.get_id().1 + ms_nibble_len)
                            .set_serial(self.get_pfx_serial(base_prefix.into(), ms_nibble, ms_nibble_len, guard).load(Ordering::Acquire))
                    );
                }
            }
        }

        (
            // We're done here, the caller should now go over all nodes in
            // found_children_with_more_specifics vec and add ALL prefixes
            // found in there.
            found_children_with_more_specifics,
            found_more_specifics_vec,
        )
    }
}
