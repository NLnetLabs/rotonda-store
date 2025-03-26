use std::sync::atomic::{AtomicU16, AtomicU32};
use std::{fmt::Debug, marker::PhantomData};

use log::{log_enabled, trace};
use parking_lot_core::SpinWait;

use crate::tree_bitmap::atomic_bitmap::{
    AtomicBitmap, AtomicPfxBitArr, AtomicPtrBitArr, CasResult,
};
use crate::types::BitSpan;

use crate::rib::{BIT_SPAN_SIZE, STRIDE_SIZE};
use crate::types::AddressFamily;
use crate::types::PrefixId;

//------------ TreeBitMap Node ----------------------------------------------

// The treebitmap turned into a "trie-bitmap", really. A Node in the
// treebitmap now only holds a ptrbitarr bitmap and a pfxbitarr bitmap, that
// indicate whether a node or a prefix exists in that spot. The corresponding
// node Ids and prefix ids are calculated from their position in the array.
// Nodes do *NOT* have a clue where they are in the tree, so they don't know
// the node id they represent. Instead, the node id is calculated from the
// position in the tree. That's why several methods take a `base_prefix` as a
// an argument: it represents the ID of the node itself.
//
// The elision of both the collection of children nodes and the prefix nodes
// in a treebitmap node is enabled by the storage backend for the
// multi-threaded store, since holds its entries keyed on the [node|prefix]
// id. (in contrast with arrays or `vec`s, that have
pub(crate) struct TreeBitMapNode<AF>
where
    Self: Sized,
    AF: AddressFamily,
{
    pub ptrbitarr: AtomicPtrBitArr,
    pub pfxbitarr: AtomicPfxBitArr,
    pub _af: PhantomData<AF>,
}

impl<AF> Debug for TreeBitMapNode<AF>
where
    AF: AddressFamily,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeBitMapNode")
            .field("ptrbitarr", &self.ptrbitarr.load())
            .field("pfxbitarr", &self.pfxbitarr.load())
            .finish()
    }
}

impl<AF> std::fmt::Display for TreeBitMapNode<AF>
where
    AF: AddressFamily,
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

impl<AF> TreeBitMapNode<AF>
where
    AF: AddressFamily,
{
    pub(crate) fn new() -> Self {
        TreeBitMapNode {
            ptrbitarr: AtomicPtrBitArr::new(),
            pfxbitarr: AtomicPfxBitArr::new(),
            _af: PhantomData,
        }
    }

    // ------- Iterators ----------------------------------------------------

    // Iterate over the more specific prefixes ids contained in this node
    pub(crate) fn more_specific_pfx_iter(
        &self,
        base_prefix: NodeId<AF>,
        start_bs: BitSpan,
    ) -> NodeMoreSpecificsPrefixIter<AF> {
        debug_assert!(start_bs.check());
        NodeMoreSpecificsPrefixIter::<AF> {
            pfxbitarr: self.pfxbitarr.ms_pfx_mask(start_bs),
            base_prefix,
        }
    }

    // Iterate over the nodes that contain more specifics for the requested
    // base_prefix and corresponding bit_span.
    pub(crate) fn more_specific_ptr_iter(
        &self,
        base_prefix: NodeId<AF>,
        start_bs: BitSpan,
    ) -> NodeMoreSpecificChildIter<AF> {
        debug_assert!(start_bs.check());
        // let ptrbitarr = self.ptrbitarr.load();
        let (bitrange, start_cursor) = self.ptrbitarr.ptr_range(start_bs);

        NodeMoreSpecificChildIter::<AF> {
            bitrange,
            base_prefix,
            start_bs,
            start_cursor,
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
        &self,
        bit_span: BitSpan,
        // all the bits of the search prefix, but with the length set to
        // the length of this stride. So bits are set beyond its length.
        base_prefix: NodeId<AF>,
        // stride_len: u8,
        is_last_stride: bool,
    ) -> (NewNodeOrIndex<AF>, u32) {
        // THE CRITICAL SECTION
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
        let bit_pos = bit_span.into_bit_pos();
        let new_node: TreeBitMapNode<AF>;

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
            if (self.ptrbitarr.as_stride_size() & bit_pos) == 0 {
                // Nope, set it and create a child node
                new_node = TreeBitMapNode {
                    ptrbitarr: AtomicPtrBitArr(AtomicU16::new(0)),
                    pfxbitarr: AtomicPfxBitArr(AtomicU32::new(0)),
                    _af: PhantomData,
                };

                // THE CRITICAL SECTION
                //
                // UPDATING pfxbitarr
                //
                // preventing using an old ptrbitarr and overwrite bits set
                // in the meantime elsewhere in the bitarray.
                let mut a_ptrbitarr = self.ptrbitarr.compare_exchange(
                    ptrbitarr,
                    into_ptrbitarr(bit_pos | into_pfxbitarr(ptrbitarr)),
                );
                let mut spinwait = SpinWait::new();
                loop {
                    match a_ptrbitarr {
                        CasResult(Ok(_)) => {
                            break;
                        }
                        CasResult(Err(newer_array)) => {
                            // Someone beat us to it, so we need to use the
                            // newer array.
                            retry_count += 1;
                            a_ptrbitarr = self.ptrbitarr.compare_exchange(
                                newer_array,
                                into_ptrbitarr(
                                    bit_pos | into_pfxbitarr(newer_array),
                                ),
                            );
                        }
                    };
                    spinwait.spin_no_yield();
                }

                return (NewNodeOrIndex::NewNode(new_node), retry_count);
            }
        } else {
            // only at the last stride do we create the bit in the prefix
            // bitmap, and only if it doesn't exist already
            if pfxbitarr & bit_pos == 0 {
                // THE CRITICAL SECTION
                //
                // UPDATING pfxbitarr
                //
                // preventing using an old pfxbitarr and overwrite bits set
                // in the meantime elsewhere in the bitarray.
                let mut a_pfxbitarr = self
                    .pfxbitarr
                    .compare_exchange(pfxbitarr, bit_pos | pfxbitarr);
                let mut spinwait = SpinWait::new();

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
                                newer_array,
                                bit_pos | newer_array,
                            );
                        }
                    };
                    spinwait.spin_no_yield();
                }

                return (NewNodeOrIndex::NewPrefix, retry_count);
            }
            return (NewNodeOrIndex::ExistingPrefix, retry_count);
        }

        // Nodes always live at the last length of a stride (i.e. the last
        // nibble), so we add the stride length to the length of the
        // base_prefix (which is always the start length of the stride).
        (
            NewNodeOrIndex::ExistingNode(
                base_prefix.add_to_len(STRIDE_SIZE).truncate_to_len(),
            ),
            retry_count,
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

type PtrBitArr = u16;

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
// `bit_span` define the actual starting prefix for this iterator.
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
pub(crate) struct NodeMoreSpecificChildIter<AF: AddressFamily> {
    base_prefix: NodeId<AF>,
    bitrange: PtrBitArr,
    start_bs: BitSpan,
    start_cursor: u8,
}

impl<AF: AddressFamily> std::iter::Iterator
    for NodeMoreSpecificChildIter<AF>
{
    type Item = NodeId<AF>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.bitrange == 0 {
            trace!("empty ptrbitarr. This iterator is done.");
            return None;
        }

        let cursor = self.bitrange.leading_zeros() as u8 + 15;
        trace!("LZCNT {}", self.bitrange.leading_zeros());

        // if self.bitrange.leading_zeros() == 0 {
        //     trace!("bitrange   {:032b}", self.bitrange);
        //     panic!("empty bitrange. This iterator is done.");
        //     return None;
        // }

        trace!(
            "base_prefix {}, start bit span {:?} start-stop cursor {}-{}",
            self.base_prefix,
            self.start_bs,
            self.start_cursor,
            <u8>::min(
                (1 << (STRIDE_SIZE - self.start_bs.len)) + self.start_cursor,
                BIT_SPAN_SIZE - 2
            )
        );

        trace!("bitrange  {:032b}", self.bitrange);

        self.bitrange ^= ptr_bit_pos_from_index(cursor);

        trace!("mask      {:032b}", ptr_bit_pos_from_index(cursor));
        trace!("next br   {:032b}", self.bitrange);

        let bs = BitSpan::from_bit_pos_index(cursor);
        if log_enabled!(log::Level::Trace) {
            let bit_pos = ptr_bit_pos_from_index(cursor);
            trace!(
                "{:02}: {:05b} {:032b} bit_span: {:04b} ({:02}) (len: {})",
                cursor,
                cursor - 1,
                bit_pos,
                bs.bits,
                bs.bits,
                bs.len
            );
            trace!(
                ">> found node with more specific prefixes for
                    base prefix {:?} bit span {:?} (cursor {})",
                self.base_prefix,
                bs,
                cursor
            );
        }

        let pfx = self.base_prefix.add_bit_span(BitSpan {
            bits: bs.bits,
            len: STRIDE_SIZE,
        });
        Some(pfx)
    }
}

// ----------- NodePrefixIter -----------------------------------------------

// Create an iterator of all prefix ids hosted by this node.

// Partition for stride 3
//
// pfxbitarr (AF::BITS)  0 1 2  3  4  5  6   7   8   9  10  11  12  13  14
// bit_span (binary)     * 0 1 00 01 10 11 000 001 010 011 100 101 110 111 *
// bit_span (dec.)       * 0 1  0  1  2  3   0   1   2   3   4   5   6   7 *
// len                   0 1   2           3
//
// pfxbitarr (example)   1 0 0 0  0  0  1  1   1   0   0   0   0   0   0   0
// pos (example)         0 0 0 0  0  0  0  0   1   0   0   0   0   0   0   0
//
// Ex.:
// `pos` describes the bit that is currently under consideration.
//
// `pfxbitarr` is the bitmap that contains the prefixes. Every 1 in the
// bitmap means that the prefix is hosted by this node. Moreover, the
// position in the bitmap describes the address part of the prefix, given
// a `base prefix`. The described prefix is the bits of the `base_prefix`
// bitmap appended by the `bit span` bits.
//
// The length of the prefix is
// described by sum of the length of the base_prefix and the `len`
// variable.
//
// The `bit_span` variable starts counting at every new prefix length.
// pub(crate) struct NodePrefixIter<AF: AddressFamily, S: Stride> {
//     base_prefix: StrideNodeId<AF>,
//     pfxbitarr: <<S as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType,
//     cursor: u8,
//     _af: PhantomData<AF>,
//     _s: PhantomData<S>,
// }

#[allow(clippy::indexing_slicing)]
pub const fn ms_prefix_mask_arr(bs: BitSpan) -> u32 {
    [
        0b_01111111111111111111111111111110, // bits = 0, len = 0
        0b_01011001111000011111111000000000, // bits = 0, len = 1
        0b_00100110000111100000000111111110, // bits = 1, len = 1
        0b_00010001100000011110000000000000, // bits = 0, len = 2
        0b_00001000011000000001111000000000, // bits = 1, len = 2
        0b_00000100000110000000000111100000, // bits = 2, len = 2
        0b_00000010000001100000000000011110, // bits = 3, len = 2
        0b_00000001000000011000000000000000, // bits = 0, len = 3
        0b_00000000100000000110000000000000, // bits = 1, len = 3
        0b_00000000010000000001100000000000, // bits = 2, len = 3
        0b_00000000001000000000011000000000, // bits = 3, len = 3
        0b_00000000000100000000000110000000, // bits = 4, len = 3
        0b_00000000000010000000000001100000, // bits = 5, len = 3
        0b_00000000000001000000000000011000, // bits = 6, len = 3
        0b_00000000000000100000000000000110, // bits = 7, len = 3
        0b_00000000000000010000000000000000, // bits = 0, len = 4
        0b_00000000000000001000000000000000, // bits = 1, len = 4
        0b_00000000000000000100000000000000, // bits = 2, len = 4
        0b_00000000000000000010000000000000, // bits = 3, len = 4
        0b_00000000000000000001000000000000, // bits = 4, len = 4
        0b_00000000000000000000100000000000, // bits = 5, len = 4
        0b_00000000000000000000010000000000, // bits = 6, len = 4
        0b_00000000000000000000001000000000, // bits = 7, len = 4
        0b_00000000000000000000000100000000, // bits = 8, len = 4
        0b_00000000000000000000000010000000, // bits = 9, len = 4
        0b_00000000000000000000000001000000, // bits =10, len = 4
        0b_00000000000000000000000000100000, // bits =11, len = 4
        0b_00000000000000000000000000010000, // bits =12, len = 4
        0b_00000000000000000000000000001000, // bits =13, len = 4
        0b_00000000000000000000000000000100, // bits =14, len = 4
        0b_00000000000000000000000000000010, // bits =15, len = 4
        0b_00000000000000000000000000000000, // padding
    ][(1 << bs.len) - 1 + bs.bits as usize]
}

fn into_ptrbitarr(bitmap: u32) -> u16 {
    (bitmap >> 1) as u16
}

fn into_pfxbitarr(bitmap: u16) -> u32 {
    (bitmap as u32) << 1
}

fn bit_pos_from_index(i: u8) -> u32 {
    1_u32.rotate_right(1) >> i
}

fn ptr_bit_pos_from_index(i: u8) -> u16 {
    // trace!("pfx {} ptr {} strlen {}",
    // <$pfxsize>::BITS, <$ptrsize>::BITS, Self::STRIDE_LEN);
    trace!("PTR_BIT_POS_FROM_INDEX {i}");
    1_u16.rotate_right(i as u32 + 2)
}

pub(crate) fn ptr_range(ptrbitarr: u16, bs: BitSpan) -> (u16, u8) {
    let start: u8 = (bs.bits << (4 - bs.len)) as u8;
    let stop: u8 = start + (1 << (4 - bs.len));
    let mask: u16 = (((1_u32 << (stop as u32 - start as u32)) - 1)
        .rotate_right(stop as u32)
        >> 16) as u16;
    if log_enabled!(log::Level::Trace) {
        trace!("- mask      {:032b}", mask);
        trace!("- ptrbitarr {:032b}", ptrbitarr);
        trace!("- shl bitar {:032b}", ptrbitarr & mask);
    }

    (ptrbitarr & mask, start)
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
// `bit_span` define the actual starting prefix for this iterator.
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
// than just the bit_span len. We will have to jump a few pfxbitarr bits and
// move to the next stride size in the bitmap, starting at bit_array 0010, or
// the bit_span { bits: 2, len: 3 }, a.k.a. 0010 << 1. But now we will have
// to go over a different amount of 1 << (5 - 4) = 2 iterations to reap the
// next bit_spans of 0010 0 and 0010 1.
pub(crate) struct NodeMoreSpecificsPrefixIter<AF: AddressFamily> {
    base_prefix: NodeId<AF>,
    pfxbitarr: u32,
}

impl<AF: AddressFamily> std::iter::Iterator
    for NodeMoreSpecificsPrefixIter<AF>
{
    type Item = PrefixId<AF>;

    fn next(&mut self) -> Option<Self::Item> {
        // Empty bitmap
        if self.pfxbitarr == 0 {
            trace!("empty pfxbitarr. This iterator is done.");
            return None;
        }

        let cursor = self.pfxbitarr.leading_zeros() as u8;
        let bs = BitSpan::from_bit_pos_index(cursor);
        trace!(
            "ms prefix iterator start_bs {:?} start cursor {}",
            bs,
            bs.cursor_from_bit_span()
        );
        trace!("pfx {:032b}", self.pfxbitarr);
        let bit_pos = bs.into_bit_pos();
        let prefix_id: PrefixId<AF> = self
            .base_prefix
            .add_bit_span(BitSpan::from_bit_pos_index(
                bit_pos.leading_zeros() as u8,
            ))
            .into();
        self.pfxbitarr ^= bit_pos_from_index(cursor);
        Some(prefix_id)
    }
}

impl<AF> Default for TreeBitMapNode<AF>
where
    AF: AddressFamily,
{
    fn default() -> Self {
        Self {
            ptrbitarr: AtomicPtrBitArr::new(),
            pfxbitarr: AtomicPfxBitArr::new(),
            _af: PhantomData,
        }
    }
}

pub(crate) enum NewNodeOrIndex<AF: AddressFamily> {
    NewNode(TreeBitMapNode<AF>),
    ExistingNode(NodeId<AF>),
    NewPrefix,
    ExistingPrefix,
}

//--------------------- NodeId -----------------------------------------------

// The type that acts as the id for a node in the treebitmap and the node CHT.
// Its data structure is the same as [PrefixId], but its behaviour is subtly
// different from PrefixId, i.e. a NodeId only exists at a stride boundary,
// so it always stores multiples of 4 bits. It cannot be converted to/from
// a Prefix.

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct NodeId<AF: AddressFamily> {
    bits: AF,
    len: u8,
}

impl<AF: AddressFamily> NodeId<AF> {
    pub(crate) fn dangerously_new_with_id_as_is(
        addr_bits: AF,
        len: u8,
    ) -> Self {
        Self {
            bits: addr_bits,
            len,
        }
    }

    #[inline]
    pub(crate) fn new_with_cleaned_id(addr_bits: AF, len: u8) -> Self {
        Self {
            bits: addr_bits.truncate_to_len(len),
            len,
        }
    }

    pub(crate) fn len(&self) -> u8 {
        self.len
    }

    pub(crate) fn bits(&self) -> AF {
        self.bits
    }

    pub fn set_len(mut self, len: u8) -> Self {
        self.len = len;
        self
    }

    pub(crate) fn add_to_len(mut self, len: u8) -> Self {
        self.len += len;
        self
    }

    #[inline]
    pub(crate) fn truncate_to_len(self) -> Self {
        NodeId::new_with_cleaned_id(self.bits, self.len)
    }

    // clean out all bits that are set beyond the len. This function should
    // be used before doing any ORing to add a nibble.
    #[inline]
    pub(crate) fn with_cleaned_id(&self) -> (AF, u8) {
        (self.bits.truncate_to_len(self.len), self.len)
    }

    pub(crate) fn add_bit_span(&self, bs: BitSpan) -> Self {
        let (addr_bits, len) = self.with_cleaned_id();
        let res = addr_bits.add_bit_span(len, bs);
        res.into()
    }
}

impl<AF: AddressFamily> std::fmt::Display for NodeId<AF> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.bits, self.len)
    }
}

impl<AF: AddressFamily> std::convert::From<NodeId<AF>> for PrefixId<AF> {
    fn from(id: NodeId<AF>) -> Self {
        PrefixId::new(id.bits, id.len)
    }
}

impl<AF: AddressFamily> From<(AF, u8)> for NodeId<AF> {
    fn from(value: (AF, u8)) -> Self {
        NodeId {
            bits: value.0,
            len: value.1,
        }
    }
}
