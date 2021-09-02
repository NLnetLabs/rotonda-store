use crate::common::{AddressFamily, MergeUpdate, NoMeta, Prefix};
use crate::impl_primitive_stride;
use crate::match_node_for_strides;
use crate::synth_int::{U256, U512};
use std::io::{Error, ErrorKind};
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
    // bit_pos always has only one bit set in the complete array.
    // e.g.:
    // len: 4
    // nibble: u16  =  0b0000 0000 0000 0111
    // bit_pos: u16 =  0b0000 0000 0000 1000

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

    // fn zero() -> Self;
    fn one() -> Self;
    fn leading_zeros(self) -> u32;
}

pub trait Zero {
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

impl Zero for u16 {
    fn zero() -> u16 {
        0
    }
}

impl Zero for u32 {
    fn zero() -> u32 {
        0
    }
}

impl Zero for u64 {
    fn zero() -> u64 {
        0
    }
}

impl Zero for u128 {
    fn zero() -> u128 {
        0
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

    // #[inline]
    // fn zero() -> Self {
    //     U256(0, 0)
    // }

    #[inline]
    fn one() -> Self {
        U256(0, 1)
    }

    #[inline]
    fn leading_zeros(self) -> u32 {
        let lz = self.0.leading_zeros();
        if lz == 128 {
            lz + self.1.leading_zeros()
        } else {
            lz
        }
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

    // #[inline]
    // fn zero() -> Self {
    //     U512(0, 0, 0, 0)
    // }

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
    fn sort(&self, other: &Self) -> std::cmp::Ordering;
    fn new(sort: &Self::Sort, part: &Self::Part) -> Self;
    fn get_sort(&self) -> Self::Sort;
    fn get_part(&self) -> Self::Part;
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
    fn acquire_new_node_id(
        &self,
        sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        part: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> <Self as StorageBackend>::NodeType;
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
    fn update_node(
        &mut self,
        current_node_id: Self::NodeType,
        updated_node: SizedStrideNode<Self::AF, Self::NodeType>,
    );
    fn retrieve_node(
        &self,
        index: Self::NodeType,
    ) -> Option<&SizedStrideNode<Self::AF, Self::NodeType>>;
    fn retrieve_node_mut(
        &mut self,
        index: Self::NodeType,
    ) -> Result<&mut SizedStrideNode<Self::AF, Self::NodeType>, Box<dyn std::error::Error>>;
    fn retrieve_node_with_guard(
        &self,
        index: Self::NodeType,
    ) -> CacheGuard<Self::AF, Self::NodeType>;
    fn get_root_node_id(&self) -> Self::NodeType;
    fn get_root_node_mut(&mut self) -> Option<&mut SizedStrideNode<Self::AF, Self::NodeType>>;
    fn get_nodes_len(&self) -> usize;
    fn acquire_new_prefix_id(
        &self,
        sort: &<<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        part: &Prefix<Self::AF, Self::Meta>,
    ) -> <Self as StorageBackend>::NodeType;
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
    fn retrieve_prefix_with_guard(
        &self,
        index: Self::NodeType,
    ) -> PrefixCacheGuard<Self::AF, Self::Meta>;
    fn get_prefixes_len(&self) -> usize;
    fn prefixes_iter(
        &self,
    ) -> Result<std::slice::Iter<'_, Prefix<Self::AF, Self::Meta>>, Box<dyn std::error::Error>>;
    fn prefixes_iter_mut(
        &mut self,
    ) -> Result<std::slice::IterMut<'_, Prefix<Self::AF, Self::Meta>>, Box<dyn std::error::Error>>;
}

#[derive(Debug)]
pub struct InMemStorage<AF: AddressFamily, Meta: Debug> {
    pub nodes: Vec<SizedStrideNode<AF, InMemNodeId>>,
    pub prefixes: Vec<Prefix<AF, Meta>>,
    _node_with_guard: std::cell::RefCell<SizedStrideNode<AF, InMemNodeId>>,
}

impl<AF: AddressFamily, Meta: Debug + MergeUpdate> StorageBackend for InMemStorage<AF, Meta> {
    type NodeType = InMemNodeId;
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
            _node_with_guard: std::cell::RefCell::new(SizedStrideNode::Stride8(TreeBitMapNode {
                ptrbitarr: U256(0, 0),
                pfxbitarr: U512(0, 0, 0, 0),
                pfx_vec: vec![],
                ptr_vec: vec![],
                _af: PhantomData,
            })),
        }
    }

    fn acquire_new_node_id(
        &self,
        sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        _part: <<Self as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> <Self as StorageBackend>::NodeType {
        // We're ignoring the part parameter here, because we want to store
        // the index into the global self.nodes vec in the local vec.
        InMemNodeId(sort, self.nodes.len() as u32)
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

    fn update_node(
        &mut self,
        current_node_id: Self::NodeType,
        updated_node: SizedStrideNode<Self::AF, Self::NodeType>,
    ) {
        let _default_val = std::mem::replace(
            self.retrieve_node_mut(current_node_id).unwrap(),
            updated_node,
        );
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
            .ok_or_else(|| Box::new(Error::new(ErrorKind::Other, "Retrieve Node Error")).into())
    }

    // Don't use this function, this is just a placeholder and a really
    // inefficient implementation.
    fn retrieve_node_with_guard(
        &self,
        _id: Self::NodeType,
    ) -> CacheGuard<Self::AF, Self::NodeType> {
        panic!("Not Implemented for InMeMStorage");
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

    fn acquire_new_prefix_id(
        &self,
        sort: &<<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        _part: &Prefix<<Self as StorageBackend>::AF, <Self as StorageBackend>::Meta>,
    ) -> <Self as StorageBackend>::NodeType {
        // We're ignoring the part parameter here, because we want to store
        // the index into the global self.prefixes vec in the local vec.
        InMemNodeId(*sort, self.prefixes.len() as u32)
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

    fn retrieve_prefix_with_guard(
        &self,
        _index: Self::NodeType,
    ) -> PrefixCacheGuard<Self::AF, Self::Meta> {
        panic!("nOt ImPlEmEnTed for InMemNode");
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

pub struct CacheGuard<'a, AF: 'static + AddressFamily, NodeId: SortableNodeId + Copy> {
    pub guard: std::cell::Ref<'a, SizedStrideNode<AF, NodeId>>,
}

impl<'a, AF: 'static + AddressFamily, NodeId: SortableNodeId + Copy> std::ops::Deref
    for CacheGuard<'a, AF, NodeId>
{
    type Target = SizedStrideNode<AF, NodeId>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

pub struct PrefixCacheGuard<'a, AF: 'static + AddressFamily, Meta: Debug> {
    pub guard: std::cell::Ref<'a, Prefix<AF, Meta>>,
}

impl<'a, AF: 'static + AddressFamily, Meta: Debug> std::ops::Deref
    for PrefixCacheGuard<'a, AF, Meta>
{
    type Target = Prefix<AF, Meta>;

    fn deref(&self) -> &Self::Target {
        &self.guard
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
    fn eval_node_or_prefix_at(
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
        search_pfx: &Prefix<AF, NoMeta>,
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
        search_pfx: &Prefix<AF, NoMeta>,
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
        search_pfx: &Prefix<AF, NoMeta>,
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
        assert_eq!(strides.iter().sum::<u8>(), Store::AF::BITS);

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
        let mut cur_i = self.store.get_root_node_id();
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

    #[inline]
    pub fn retrieve_node_with_guard(
        &self,
        id: Store::NodeType,
    ) -> CacheGuard<Store::AF, Store::NodeType> {
        self.store.retrieve_node_with_guard(id)
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
        &self,
        index: <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&Prefix<Store::AF, Store::Meta>> {
        self.store.retrieve_prefix(index)
    }

    #[inline]
    pub fn retrieve_prefix_mut(
        &mut self,
        index: <<Store as StorageBackend>::NodeType as SortableNodeId>::Part,
    ) -> Option<&mut Prefix<Store::AF, Store::Meta>> {
        self.store.retrieve_prefix_mut(index)
    }

    // This function assembles all entries in the `pfx_vec` of all child nodes of the
    // `start_node` into one vec, starting from iself and then recursively assembling
    // adding all `pfx_vec`s of its children.
    fn get_all_more_specifics_for_node(
        &self,
        start_node: &SizedStrideNode<Store::AF, Store::NodeType>,
        found_pfx_vec: &mut Vec<Store::NodeType>,
    ) {
        match start_node {
            SizedStrideNode::Stride3(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride4(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride5(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride6(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride7(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
            SizedStrideNode::Stride8(n) => {
                found_pfx_vec.extend_from_slice(&n.pfx_vec);

                for nn in n.ptr_vec.iter() {
                    self.get_all_more_specifics_for_node(
                        self.retrieve_node(*nn).unwrap(),
                        found_pfx_vec,
                    );
                }
            }
        }
    }

    // This function assembles the prefixes of a child node starting on a specified bit position in a ptr_vec of
    // `current_node` into a vec, then adds all prefixes of these children recursively into a vec and returns that.
    pub fn get_all_more_specifics_from_nibble<S: Stride>(
        &self,
        current_node: &TreeBitMapNode<Store::AF, S, Store::NodeType>,
        nibble: u32,
        nibble_len: u8,
    ) -> Option<Vec<Store::NodeType>>
    where
        S: Stride + std::ops::BitAnd<Output = S> + std::ops::BitOr<Output = S> + Zero,
        <S as Stride>::PtrSize:
            Debug + Binary + Copy + std::ops::BitAnd<Output = S::PtrSize> + PartialOrd + Zero,
    {
        let (cnvec, mut msvec) = current_node.add_more_specifics_at(nibble, nibble_len);

        for child_node in cnvec.iter() {
            self.get_all_more_specifics_for_node(
                self.retrieve_node(*child_node).unwrap(),
                &mut msvec,
            );
        }
        Some(msvec)
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
