use crate::common::{AddressFamily, MergeUpdate, NoMeta, Prefix};
use crate::impl_primitive_stride;
use crate::match_node_for_strides;
use crate::synth_int::{U256, U512};
use std::io::{Error, ErrorKind};
use std::ops::Deref;
use std::{
    fmt::{Binary, Debug},
    marker::PhantomData,
};

type Stride3 = u16;
type Stride4 = u32;
type Stride5 = u64;
type Stride6 = u128;
type Stride7 = U256;
type Stride8 = U512;

pub trait Stride: Sized + Debug + Binary + Eq + PartialOrd + PartialEq + Copy {
    type PtrSize;
    const BITS: u8;
    const STRIDE_LEN: u8;

    // Get the bit position of the start of the given nibble.
    // The nibble is defined as a `len` number of bits set from the right.

    // `<Self as Stride>::BITS`
    // is the whole length of the bitmap, since we are shifting to the left,
    // we have to start at the end of the bitmap.
    // `((1 << len) - 1)`
    // is the offset for this nibble length in the bitmap.
    // `nibble`
    // shifts to the right position withing the bit range for this nibble
    // length, this follows from the fact that the `nibble` value represents
    // *both* the bitmap part, we're considering here *and* the position
    // relative to the nibble length offset in the bitmap.
    fn get_bit_pos(nibble: u32, len: u8) -> Self;

    // Clear the bitmap to the right of the pointer and count the number of ones.
    // This numbder represents the index to the corresponding prefix in the pfx_vec.

    // Clearing is performed by shifting to the right until we have the nibble
    // all the way at the right.

    // `(<Self as Stride>::BITS >> 1)`
    // The end of the bitmap (this bitmap is half the size of the pfx bitmap)

    // `nibble`
    // The bit position relative to the offset for the nibble length, this index
    // is only used at the last (relevant) stride, so the offset is always 0.
    fn get_pfx_index(bitmap: Self, nibble: u32, len: u8) -> usize;

    // Clear the bitmap to the right of the pointer and count the number of ones.
    // This number represents the index to the corresponding child node in the ptr_vec.

    // Clearing is performed by shifting to the right until we have the nibble
    // all the way at the right.

    // For ptrbitarr the only index we want is the one for a full-length nibble
    // (stride length) at the last stride, so we don't need the length of the nibble

    // `(<Self as Stride>::BITS >> 1)`
    // The end of the bitmap (this bitmap is half the size of the pfx bitmap),
    // ::BITS is the size of the pfx bitmap.

    // `nibble`
    // The bit position relative to the offset for the nibble length, this index
    // is only used at the last (relevant) stride, so the offset is always 0.
    fn get_ptr_index(bitmap: Self::PtrSize, nibble: u32) -> usize;

    // Convert a ptrbitarr into a pfxbitarr sized bitmap,
    // so we can do bitwise operations with a pfxbitarr sized
    // bitmap on them.
    // Since the last bit in the pfxbitarr isn't used, but the
    // full ptrbitarr *is* used, the prtbitarr should be shifted
    // one bit to the left.
    fn into_stride_size(bitmap: Self::PtrSize) -> Self;

    // Convert a pfxbitarr sized bitmap into a ptrbitarr sized
    // Note that bitwise operators align bits of unsigend types with different
    // sizes to the right, so we don't have to do anything to pad the smaller sized
    // type. We do have to shift one bit to the left, to accomodate the unused pfxbitarr's
    // last bit.
    fn into_ptrbitarr_size(bitmap: Self) -> Self::PtrSize;

    fn zero() -> Self;
    fn one() -> Self;
    fn leading_zeros(self) -> u32;
}

trait Zero {
    fn zero() -> Self;
}

trait One {
    fn one() -> Self;
}

impl Zero for u8 {
    fn zero() -> u8 {
        0
    }
}

impl One for u8 {
    fn one() -> u8 {
        1
    }
}

impl_primitive_stride![3; 16; u16; u8];
impl_primitive_stride![4; 32; u32; u16];
impl_primitive_stride![5; 64; u64; u32];
impl_primitive_stride![6; 128; u128; u64];

impl Stride for Stride7 {
    type PtrSize = u128;
    const BITS: u8 = 255;
    const STRIDE_LEN: u8 = 7;

    fn get_bit_pos(nibble: u32, len: u8) -> Self {
        match 256 - ((1 << len) - 1) as u16 - nibble as u16 - 1 {
            n if n < 128 => U256(0, 1 << n),
            n => U256(1 << (n as u16 - 128), 0),
        }
    }

    fn get_pfx_index(bitmap: Self, nibble: u32, len: u8) -> usize {
        let n = 256 - ((1 << len) - 1) as u16 - nibble as u16 - 1;
        match n {
            // if we move less than 128 bits to the right,
            // all of bitmap.0 and a part of bitmap.1 will be used for counting zeros
            // ex.
            // ...1011_1010... >> 2 => ...0010_111010...
            //    ____ ====                 -- --====
            n if n < 128 => {
                bitmap.0.count_ones() as usize + (bitmap.1 >> n).count_ones() as usize - 1
            }
            // if we move more than 128 bits to the right,
            // all of bitmap.1 wil be shifted out of sight,
            // so we only have to count bitmap.0 zeroes than (after) shifting of course).
            n => (bitmap.0 >> (n - 128)).count_ones() as usize - 1,
        }
    }

    fn get_ptr_index(bitmap: Self::PtrSize, nibble: u32) -> usize {
        (bitmap >> ((256 >> 1) - nibble as u16 - 1) as usize).count_ones() as usize - 1
    }

    fn into_stride_size(bitmap: Self::PtrSize) -> Self {
        // One bit needs to move into the self.0 u128,
        // since the last bit of the *whole* bitmap isn't used.
        U256(bitmap >> 127, bitmap << 1)
    }

    fn into_ptrbitarr_size(bitmap: Self) -> Self::PtrSize {
        // TODO expand:
        // self.ptrbitarr =
        // S::into_ptrbitarr_size(bit_pos | S::into_stride_size(self.ptrbitarr));
        (bitmap.0 << 127 | bitmap.1 >> 1) as u128
    }

    #[inline]
    fn zero() -> Self {
        U256(0, 0)
    }

    #[inline]
    fn one() -> Self {
        U256(0, 1)
    }

    #[inline]
    fn leading_zeros(self) -> u32 {
        let lz = self.0.leading_zeros();
        let r = if lz == 128 {
            lz + self.1.leading_zeros()
        } else {
            lz
        };
        r
    }
}

impl Stride for Stride8 {
    type PtrSize = U256;
    const BITS: u8 = 255; // bogus
    const STRIDE_LEN: u8 = 8;

    fn get_bit_pos(nibble: u32, len: u8) -> Self {
        match 512 - ((1 << len) - 1) as u16 - nibble as u16 - 1 {
            n if n < 128 => U512(0, 0, 0, 1 << n),
            n if n < 256 => U512(0, 0, 1 << (n as u16 - 128), 0),
            n if n < 384 => U512(0, 1 << (n as u16 - 256), 0, 0),
            n => U512(1 << (n as u16 - 384), 0, 0, 0),
        }
    }

    fn get_pfx_index(bitmap: Self, nibble: u32, len: u8) -> usize {
        let n = 512 - ((1 << len) - 1) as u16 - nibble as u16 - 1;
        match n {
            // if we move less than 128 bits to the right,
            // all of bitmap.2 and a part of bitmap.3 will be used for counting zeros
            // ex.
            // ...1011_1010... >> 2 => ...0010_111010...
            //    ____ ====                 -- --====
            n if n < 128 => {
                bitmap.0.count_ones() as usize
                    + bitmap.1.count_ones() as usize
                    + bitmap.2.count_ones() as usize
                    + (bitmap.3 >> n).count_ones() as usize
                    - 1
            }

            n if n < 256 => {
                bitmap.0.count_ones() as usize
                    + bitmap.1.count_ones() as usize
                    + (bitmap.2 >> (n - 128)).count_ones() as usize
                    - 1
            }

            n if n < 384 => {
                bitmap.0.count_ones() as usize + (bitmap.1 >> (n - 256)).count_ones() as usize - 1
            }

            // if we move more than 384 bits to the right,
            // all of bitmap.[1,2,3] will be shifted out of sight,
            // so we only have to count bitmap.0 zeroes then (after shifting of course).
            n => (bitmap.0 >> (n - 384)).count_ones() as usize - 1,
        }
    }

    fn get_ptr_index(bitmap: Self::PtrSize, nibble: u32) -> usize {
        let n = (512 >> 1) - nibble as u16 - 1;
        match n {
            // if we move less than 256 bits to the right,
            // all of bitmap.0 and a part of bitmap.1 will be used for counting zeros
            // ex.
            // ...1011_1010... >> 2 => ...0010_111010...
            //    ____ ====                 -- --====
            n if n < 128 => {
                bitmap.0.count_ones() as usize + (bitmap.1 >> n).count_ones() as usize - 1
            }
            // if we move more than 256 bits to the right,
            // all of bitmap.1 wil be shifted out of sight,
            // so we only have to count bitmap.0 zeroes than (after) shifting of course).
            n => (bitmap.0 >> (n - 128)).count_ones() as usize - 1,
        }
    }

    fn into_stride_size(bitmap: Self::PtrSize) -> Self {
        // One bit needs to move into the self.0 u128,
        // since the last bit of the *whole* bitmap isn't used.
        U512(
            0,
            bitmap.0 >> 127,
            (bitmap.0 << 1) | (bitmap.1 >> 127),
            bitmap.1 << 1,
        )
    }

    fn into_ptrbitarr_size(bitmap: Self) -> Self::PtrSize {
        // TODO expand:
        // self.ptrbitarr =
        // S::into_ptrbitarr_size(bit_pos | S::into_stride_size(self.ptrbitarr));
        U256(
            (bitmap.1 << 127 | bitmap.2 >> 1) as u128,
            (bitmap.2 << 127 | bitmap.3 >> 1) as u128,
        )
    }

    #[inline]
    fn zero() -> Self {
        U512(0, 0, 0, 0)
    }

    #[inline]
    fn one() -> Self {
        U512(0, 0, 0, 1)
    }

    #[inline]
    fn leading_zeros(self) -> u32 {
        let mut lz = self.0.leading_zeros();
        if lz == 128 {
            lz += self.1.leading_zeros();
            if lz == 256 {
                lz += self.2.leading_zeros();
                if lz == 384 {
                    lz += self.3.leading_zeros();
                }
            }
        }
        lz
    }
}

#[derive(Debug)]
pub enum SizedStrideNode<AF: AddressFamily, NodeId: SortableNodeId + Copy> {
    Stride3(TreeBitMapNode<AF, Stride3, NodeId>),
    Stride4(TreeBitMapNode<AF, Stride4, NodeId>),
    Stride5(TreeBitMapNode<AF, Stride5, NodeId>),
    Stride6(TreeBitMapNode<AF, Stride6, NodeId>),
    Stride7(TreeBitMapNode<AF, Stride7, NodeId>),
    Stride8(TreeBitMapNode<AF, Stride8, NodeId>),
}

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

impl<AF, NodeId> Default for SizedStrideNode<AF, NodeId>
where
    AF: AddressFamily,
    NodeId: SortableNodeId + Copy,
{
    fn default() -> Self {
        SizedStrideNode::Stride3(TreeBitMapNode {
            ptrbitarr: 0,
            pfxbitarr: 0,
            pfx_vec: vec![],
            ptr_vec: vec![],
            _af: PhantomData,
        })
    }
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

pub trait SortableNodeId
where
    Self: std::cmp::Ord + std::fmt::Debug + Sized,
    Self::Sort: std::cmp::Ord + std::convert::From<u16>,
    Self::Part: std::cmp::Ord + std::convert::From<u16> + std::marker::Copy + Debug,
{
    type Part;
    type Sort;
    fn sort(self: &Self, other: &Self) -> std::cmp::Ordering;
    fn new(sort: &Self::Sort, part: &Self::Part) -> Self;
    fn get_sort(self: &Self) -> Self::Sort;
    fn get_part(self: &Self) -> Self::Part;
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug, Copy, Clone)]
pub struct InMemNodeId(u16, u32);

// This works for both IPv4 and IPv6 up to a certain point.
// the u16 for Sort is used for ordering the local vecs
// inside the nodes.
// The u32 Part is used as an index to the backing global vecs,
// so you CANNOT store all IPv6 prefixes that could exist!
// If you really want that you should implement your own type with trait
// SortableNodeId, e.g., Sort = u16, Part = u128.
impl SortableNodeId for InMemNodeId {
    type Sort = u16;
    type Part = u32;

    fn sort(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }

    fn new(sort: &Self::Sort, part: &Self::Part) -> InMemNodeId {
        InMemNodeId(*sort, *part)
    }

    fn get_sort(&self) -> Self::Sort {
        self.0
    }

    fn get_part(&self) -> Self::Part {
        self.1
    }
}

pub trait StorageBackend
where
    Self::NodeType: SortableNodeId + Copy,
{
    type NodeType;
    type AF: AddressFamily;
    type Meta: Debug + MergeUpdate;

    fn init(start_node: Option<SizedStrideNode<Self::AF, Self::NodeType>>) -> Self;
    // store_node should return an index with the associated type `Part` of the associated type
    // of this trait.
    // `id` is optional, since a vec uses the indexes as the ids of the nodes,
    // other storage data-structures may use unordered lists, where the id is in the
    // record, e.g., dynamodb
    fn store_node(
        &mut self,
        id: Option<Self::NodeType>,
        next_node: SizedStrideNode<Self::AF, Self::NodeType>,
    ) -> Option<Self::NodeType>;
    fn retrieve_node(
        &self,
        index: Self::NodeType,
    ) -> Option<&SizedStrideNode<Self::AF, Self::NodeType>>;
    fn retrieve_node_mut(
        &mut self,
        index: Self::NodeType,
    ) -> Result<&mut SizedStrideNode<Self::AF, Self::NodeType>, Box<dyn std::error::Error>>;
    fn get_root_node_id(&self) -> Self::NodeType;
    fn get_root_node_mut(&mut self) -> Option<&mut SizedStrideNode<Self::AF, Self::NodeType>>;
    fn get_nodes_len(&self) -> usize;
    fn store_prefix(
        &mut self,
        next_node: Prefix<Self::AF, Self::Meta>,
    ) -> Result<
        <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
        Box<dyn std::error::Error>,
    >;
    fn retrieve_prefix(
        &self,
        index: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&Prefix<Self::AF, Self::Meta>>;
    fn retrieve_prefix_mut(
        &mut self,
        index: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&mut Prefix<Self::AF, Self::Meta>>;
    fn get_prefixes_len(&self) -> usize;
    fn prefixes_iter(
        &self,
    ) -> Result<std::slice::Iter<'_, Prefix<Self::AF, Self::Meta>>, Box<dyn std::error::Error>>;
    fn prefixes_iter_mut(
        &mut self,
    ) -> Result<std::slice::IterMut<'_, Prefix<Self::AF, Self::Meta>>, Box<dyn std::error::Error>>;
}

pub struct InMemStorage<AF: AddressFamily, Meta: Debug> {
    nodes: Vec<SizedStrideNode<AF, InMemNodeId<Sort = u16, Part = u32>>>,
    pub prefixes: Vec<Prefix<AF, Meta>>,
}

impl<AF: AddressFamily, Meta: Debug + MergeUpdate> StorageBackend for InMemStorage<AF, Meta> {
    type NodeType = InMemNodeId<Sort = u16, Part = u32>;
    type AF = AF;
    type Meta = Meta;

    fn init(
        start_node: Option<SizedStrideNode<Self::AF, Self::NodeType>>,
    ) -> InMemStorage<AF, Meta> {
        let mut nodes = vec![];
        if let Some(n) = start_node {
            nodes = vec![n];
        }
        InMemStorage {
            nodes,
            prefixes: vec![],
        }
    }

    fn store_node(
        &mut self,
        _id: Option<Self::NodeType>,
        next_node: SizedStrideNode<Self::AF, Self::NodeType>,
    ) -> Option<Self::NodeType> {
        let id = self.nodes.len() as u32;
        self.nodes.push(next_node);
        //Store::NodeType::new(&bit_id, &i.into())
        //Store::NodeType::new(&((1 << $nibble_len) + $nibble as u16).into(), &i)
        Some(InMemNodeId::new(&0, &id))
    }

    fn retrieve_node(
        &self,
        id: Self::NodeType,
    ) -> Option<&SizedStrideNode<Self::AF, Self::NodeType>> {
        self.nodes.get(id.get_part() as usize)
    }

    fn retrieve_node_mut(
        &mut self,
        index: Self::NodeType,
    ) -> Result<&mut SizedStrideNode<Self::AF, Self::NodeType>, Box<dyn std::error::Error>> {
        self.nodes
            .get_mut(index.get_part() as usize)
            .ok_or(Box::new(Error::new(
                ErrorKind::Other,
                "Retrieve Node Error",
            )))
    }

    fn get_root_node_id(&self) -> Self::NodeType {
        InMemNodeId(0, 0)
    }

    fn get_root_node_mut(&mut self) -> Option<&mut SizedStrideNode<Self::AF, Self::NodeType>> {
        Some(&mut self.nodes[0])
    }

    fn get_nodes_len(&self) -> usize {
        self.nodes.len()
    }

    fn store_prefix(
        &mut self,
        next_node: Prefix<Self::AF, Self::Meta>,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let id = self.prefixes.len() as u32;
        self.prefixes.push(next_node);
        Ok(id)
    }

    fn retrieve_prefix(&self, index: u32) -> Option<&Prefix<Self::AF, Self::Meta>> {
        self.prefixes.get(index as usize)
    }

    fn retrieve_prefix_mut(&mut self, index: u32) -> Option<&mut Prefix<Self::AF, Self::Meta>> {
        self.prefixes.get_mut(index as usize)
    }

    fn get_prefixes_len(&self) -> usize {
        self.prefixes.len()
    }

    fn prefixes_iter(
        &self,
    ) -> Result<std::slice::Iter<'_, Prefix<AF, Meta>>, Box<dyn std::error::Error>> {
        Ok(self.prefixes.iter())
    }

    fn prefixes_iter_mut(
        &mut self,
    ) -> Result<std::slice::IterMut<'_, Prefix<AF, Meta>>, Box<dyn std::error::Error>> {
        Ok(self.prefixes.iter_mut())
    }
}

enum NewNodeOrIndex<AF: AddressFamily, NodeId: SortableNodeId + Copy> {
    NewNode(SizedStrideNode<AF, NodeId>, NodeId::Sort), // New Node and bit_id of the new node
    ExistingNode(NodeId),
    NewPrefix,
    ExistingPrefix(NodeId::Part),
}

impl<AF, S, NodeId> TreeBitMapNode<AF, S, NodeId>
where
    AF: AddressFamily,
    S: Stride + std::ops::BitAnd<Output = S> + std::ops::BitOr<Output = S>,
    <S as Stride>::PtrSize: Debug + Binary + Copy,
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
    fn eval_node_or_prefix_at(
        self: &mut Self,
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
            if (S::into_stride_size(self.ptrbitarr) & bit_pos)
                == <S as std::ops::BitAnd>::Output::zero()
            {
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

    fn search_stride_at<'b>(
        self: &Self,
        search_pfx: &Prefix<AF, NoMeta>,
        mut nibble: u32,
        nibble_len: u8,
        start_bit: u8,
        found_pfx: &'b mut Vec<NodeId>,
    ) -> Option<NodeId> {
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);

        for n_l in 1..(nibble_len + 1) {
            // Move the bit in the right position.
            nibble = AddressFamily::get_nibble(search_pfx.net, start_bit, n_l);

            bit_pos = S::get_bit_pos(nibble, n_l);

            // Check if the prefix has been set, if so select the prefix. This is not
            // necessarily the final prefix that will be returned.

            // Check it there's an prefix matching in this bitmap for this nibble
            if self.pfxbitarr & bit_pos > S::zero() {
                found_pfx.push(self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, n_l)]);
            }
        }

        // If we are at the end of the prefix length or if there are no more
        // children we're returning what we found so far.

        // Check if this the last stride, or if they're no more children to go to,
        // if so return what we found up until now.
        if search_pfx.len < start_bit
            || (S::into_stride_size(self.ptrbitarr) & bit_pos)
                == <S as std::ops::BitAnd>::Output::zero()
        {
            return None;
        }

        Some(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)])
    }

    fn search_stride_at_lmp_only<'b>(
        self: &Self,
        search_pfx: &Prefix<AF, NoMeta>,
        mut nibble: u32,
        nibble_len: u8,
        start_bit: u8,
    ) -> (Option<NodeId>, Option<NodeId>) {
        let mut bit_pos = S::get_bit_pos(nibble, nibble_len);
        let mut found_pfx = None;

        for n_l in 1..(nibble_len + 1) {
            // Move the bit in the right position.
            nibble = AddressFamily::get_nibble(search_pfx.net, start_bit, n_l);
            bit_pos = S::get_bit_pos(nibble, n_l);

            // Check if the prefix has been set, if so select the prefix. This is not
            // necessarily the final prefix that will be returned.

            // Check it there's an prefix matching in this bitmap for this nibble
            if self.pfxbitarr & bit_pos > S::zero() {
                found_pfx = Some(self.pfx_vec[S::get_pfx_index(self.pfxbitarr, nibble, n_l)]);
            }
        }

        // Check if this the last stride, or if they're no more children to go to,
        // if so return what we found up until now.
        if search_pfx.len < start_bit
            || (S::into_stride_size(self.ptrbitarr) & bit_pos)
                == <S as std::ops::BitAnd>::Output::zero()
        {
            return (None, found_pfx);
        }

        (
            Some(self.ptr_vec[S::get_ptr_index(self.ptrbitarr, nibble)]),
            found_pfx,
        )
    }
}
pub struct TreeBitMap<Store>
where
    Store: StorageBackend,
{
    pub strides: Vec<u8>,
    pub stats: Vec<StrideStats>,
    pub store: Store,
}

impl<'a, Store> TreeBitMap<Store>
where
    Store: StorageBackend,
{
    pub fn new(_strides_vec: Vec<u8>) -> TreeBitMap<Store> {
        // Check if the strides division makes sense
        let mut strides = vec![];
        let mut strides_sum = 0;
        for s in _strides_vec.iter().cycle() {
            strides.push(*s);
            strides_sum += s;
            if strides_sum >= Store::AF::BITS - 1 {
                break;
            }
        }
        assert_eq!(
            strides.iter().fold(0, |acc, s| { acc + s }),
            Store::AF::BITS
        );

        let mut stride_stats: Vec<StrideStats> = vec![
            StrideStats::new(SizedStride::Stride3, strides.len() as u8), // 0
            StrideStats::new(SizedStride::Stride4, strides.len() as u8), // 1
            StrideStats::new(SizedStride::Stride5, strides.len() as u8), // 2
            StrideStats::new(SizedStride::Stride6, strides.len() as u8), // 3
            StrideStats::new(SizedStride::Stride7, strides.len() as u8), // 4
            StrideStats::new(SizedStride::Stride8, strides.len() as u8), // 5
        ];

        let node: SizedStrideNode<
            <Store as StorageBackend>::AF,
            <Store as StorageBackend>::NodeType,
        >;

        match strides[0] {
            3 => {
                node = SizedStrideNode::Stride3(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[0].inc(0);
            }
            4 => {
                node = SizedStrideNode::Stride4(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[1].inc(0);
            }
            5 => {
                node = SizedStrideNode::Stride5(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[2].inc(0);
            }
            6 => {
                node = SizedStrideNode::Stride6(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: 0,
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[3].inc(0);
            }
            7 => {
                node = SizedStrideNode::Stride7(TreeBitMapNode {
                    ptrbitarr: 0,
                    pfxbitarr: U256(0, 0),
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[4].inc(0);
            }
            8 => {
                node = SizedStrideNode::Stride8(TreeBitMapNode {
                    ptrbitarr: U256(0, 0),
                    pfxbitarr: U512(0, 0, 0, 0),
                    ptr_vec: vec![],
                    pfx_vec: vec![],
                    _af: PhantomData,
                });
                stride_stats[5].inc(0);
            }
            _ => {
                panic!("unknown stride size encountered in STRIDES array");
            }
        };

        TreeBitMap {
            strides,
            stats: stride_stats,
            store: Store::init(Some(node)),
        }
    }

    // Partition for stride 4
    //
    // ptr bits never happen in the first half of the bitmap for the stride-size. Consequently the ptrbitarr can be an integer type
    // half the size of the pfxbitarr.
    //
    // ptr bit arr (u16)                                                        0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15    x
    // pfx bit arr (u32)   0 1 2  3  4  5  6   7   8   9  10  11  12  13  14   15   16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
    // nibble              * 0 1 00 01 10 11 000 001 010 011 100 101 110 111 0000 0001 0010 0011 0100 0101 0110 0111 1000 1001 1010 1011 1100 1101 1110 1111    x
    // nibble len offset   0 1    2            3                                4
    //
    // stride 3: 1 + 2 + 4 + 8                              =  15 bits. 2^4 - 1 (1 << 4) - 1. ptrbitarr starts at pos  7 (1 << 3) - 1
    // stride 4: 1 + 2 + 4 + 8 + 16                         =  31 bits. 2^5 - 1 (1 << 5) - 1. ptrbitarr starts at pos 15 (1 << 4) - 1
    // stride 5: 1 + 2 + 4 + 8 + 16 + 32 + 64               =  63 bits. 2^6 - 1
    // stride 6: 1 + 2 + 4 + 8 + 16 + 32 + 64               = 127 bits. 2^7 - 1
    // stride 7: 1 + 2 + 4 + 8 + 16 + 32 + 64 = 128         = 256 bits. 2^8 - 1126
    // stride 8: 1 + 2 + 4 + 8 + 16 + 32 + 64 + 128 + 256   = 511 bits. 2^9 - 1
    //
    // Ex.:
    // pfx            65.0.0.252/30                                             0100_0001_0000_0000_0000_0000_1111_1100
    //
    // nibble 1       (pfx << 0) >> 28                                          0000_0000_0000_0000_0000_0000_0000_0100
    // bit_pos        (1 << nibble length) - 1 + nibble                         0000_0000_0000_0000_0000_1000_0000_0000
    //
    // nibble 2       (pfx << 4) >> 24                                          0000_0000_0000_0000_0000_0000_0000_0001
    // bit_pos        (1 << nibble length) - 1 + nibble                         0000_0000_0000_0000_1000_0000_0000_0000
    // ...
    // nibble 8       (pfx << 28) >> 0                                          0000_0000_0000_0000_0000_0000_0000_1100
    // bit_pos        (1 << nibble length) - 1 + nibble = (1 << 2) - 1 + 2 = 5  0000_0010_0000_0000_0000_0000_0000_0000
    // 5 - 5 - 5 - 4 - 4 - [4] - 5
    // startpos (2 ^ nibble length) - 1 + nibble as usize

    pub fn insert(
        &mut self,
        pfx: Prefix<Store::AF, Store::Meta>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stride_end: u8 = 0;
        let mut cur_i: Store::NodeType = Store::NodeType::new(&0_u16.into(), &0_u16.into());
        let mut level: u8 = 0;

        loop {
            let stride = self.strides[level as usize];
            stride_end += stride;
            let nibble_len = if pfx.len < stride_end {
                stride + pfx.len - stride_end
            } else {
                stride
            };

            let nibble = Store::AF::get_nibble(pfx.net, stride_end - stride, nibble_len);
            let is_last_stride = pfx.len <= stride_end;

            let next_node_idx = match_node_for_strides![
                // applicable to the whole outer match in the marco
                self;
                nibble_len;
                nibble;
                is_last_stride;
                pfx;
                cur_i;
                level;
                // Strides to create match arm for; stats level
                Stride3; 0,
                Stride4; 1,
                Stride5; 2,
                Stride6; 3,
                Stride7; 4,
                Stride8; 5
            ];

            if let Some(i) = next_node_idx {
                cur_i = i;
                level += 1;
            } else {
                return Ok(());
            }
        }
    }

    pub fn store_node(
        &mut self,
        id: Option<Store::NodeType>,
        next_node: SizedStrideNode<Store::AF, Store::NodeType>,
    ) -> Option<Store::NodeType> {
        self.store.store_node(id, next_node)
    }

    #[inline]
    pub fn retrieve_node(
        &self,
        id: Store::NodeType,
    ) -> Option<&SizedStrideNode<Store::AF, Store::NodeType>> {
        self.store.retrieve_node(id)
    }

    pub fn get_root_node_id(&self) -> Store::NodeType {
        self.store.get_root_node_id()
    }

    #[inline]
    pub fn retrieve_node_mut(
        &mut self,
        index: Store::NodeType,
    ) -> Result<&mut SizedStrideNode<Store::AF, Store::NodeType>, Box<dyn std::error::Error>> {
        self.store.retrieve_node_mut(index)
    }

    pub fn store_prefix(
        &mut self,
        next_node: Prefix<Store::AF, Store::Meta>,
    ) -> Result<
        <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
        Box<dyn std::error::Error>,
    > {
        // let id = self.prefixes.len() as u32;
        self.store.store_prefix(next_node)
        // id
    }

    fn update_prefix_meta(
        &mut self,
        update_node_idx: <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
        meta: Store::Meta,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self.store.retrieve_prefix_mut(update_node_idx) {
            Some(update_pfx) => match update_pfx.meta.as_mut() {
                Some(exist_meta) => <Store::Meta>::merge_update(exist_meta, meta),
                None => {
                    update_pfx.meta = Some(meta);
                    Ok(())
                }
            },
            // TODO
            // Use/create proper error types
            None => Err("Prefix not found".into()),
        }
    }

    #[inline]
    pub fn retrieve_prefix(
        &'a self,
        index: <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&'a Prefix<Store::AF, Store::Meta>> {
        self.store.retrieve_prefix(index)
    }

    #[inline]
    pub fn retrieve_prefix_mut(
        &mut self,
        index: <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&mut Prefix<Store::AF, Store::Meta>> {
        self.store.retrieve_prefix_mut(index)
    }

    pub fn match_longest_prefix(
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

    pub fn match_longest_prefix_only(
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
}

#[derive(Debug, Copy, Clone)]
pub enum SizedStride {
    Stride3,
    Stride4,
    Stride5,
    Stride6,
    Stride7,
    Stride8,
}
pub struct StrideStats {
    pub stride_type: SizedStride,
    pub stride_size: usize,
    pub stride_len: u8,
    pub node_size: usize,
    pub created_nodes: Vec<CreatedNodes>,
    pub prefixes_num: Vec<CreatedNodes>,
}

impl StrideStats {
    pub fn new(stride_type: SizedStride, num_depth_levels: u8) -> Self {
        match stride_type {
            SizedStride::Stride3 => Self {
                stride_type: SizedStride::Stride3,
                stride_size: 16,
                stride_len: 3,
                node_size: std::mem::size_of::<Stride3>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
            SizedStride::Stride4 => Self {
                stride_type: SizedStride::Stride4,
                stride_size: 32,
                stride_len: 4,
                node_size: std::mem::size_of::<Stride4>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
            SizedStride::Stride5 => Self {
                stride_type: SizedStride::Stride5,
                stride_size: 64,
                stride_len: 5,
                node_size: std::mem::size_of::<Stride5>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
            SizedStride::Stride6 => Self {
                stride_type: SizedStride::Stride6,
                stride_size: 128,
                stride_len: 6,
                node_size: std::mem::size_of::<Stride6>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
            SizedStride::Stride7 => Self {
                stride_type: SizedStride::Stride7,
                stride_size: 256,
                stride_len: 7,
                node_size: std::mem::size_of::<Stride7>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
            SizedStride::Stride8 => Self {
                stride_type: SizedStride::Stride8,
                stride_size: 512,
                stride_len: 8,
                node_size: std::mem::size_of::<Stride8>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
        }
    }

    pub fn mem_usage(&self) -> usize {
        self.stride_size
            * self.created_nodes.iter().fold(0, |mut acc, c| {
                acc += c.count;
                acc
            })
    }

    fn nodes_vec(num_depth_levels: u8) -> Vec<CreatedNodes> {
        let mut vec: Vec<CreatedNodes> = vec![];
        for n in 0..num_depth_levels {
            vec.push(CreatedNodes {
                depth_level: n,
                count: 0,
            })
        }
        vec
    }

    fn inc(&mut self, depth_level: u8) {
        self.created_nodes[depth_level as usize].count += 1;
    }

    fn inc_prefix_count(&mut self, depth_level: u8) {
        self.prefixes_num[depth_level as usize].count += 1;
    }
}

impl Debug for StrideStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{:>8?} {:?} ({}k)",
            &self.stride_type,
            &self.created_nodes.iter().fold(0, |mut a, n| {
                a += n.count;
                a
            }),
            &self.created_nodes,
            &self.mem_usage() / 1024
        )
    }
}

#[derive(Copy, Clone)]
pub struct CreatedNodes {
    pub depth_level: u8,
    pub count: usize,
}

impl CreatedNodes {
    pub fn add(mut self, num: usize) {
        self.count += num;
    }
}

impl Debug for CreatedNodes {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", &self.count))
    }
}
