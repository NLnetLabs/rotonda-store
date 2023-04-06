use std::fmt::{Binary, Debug};
use log::trace;
use std::sync::atomic::{
    AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering,
};

use crate::af::Zero;
use crate::synth_int::AtomicU128;
use crate::{impl_primitive_atomic_stride, AddressFamily};

pub type Stride3 = u16;
pub type Stride4 = u32;
pub type Stride5 = u64;

pub struct AtomicStride2(pub AtomicU8);
pub struct AtomicStride3(pub AtomicU16);
pub struct AtomicStride4(pub AtomicU32);
pub struct AtomicStride5(pub AtomicU64);
pub struct AtomicStride6(pub AtomicU128);

pub struct CasResult<InnerType>(pub Result<InnerType, InnerType>);

impl<InnerType> CasResult<InnerType> {
    fn new(value: InnerType) -> Self {
        CasResult(Ok(value))
    }
}

pub trait AtomicBitmap {
    type InnerType: Binary
        + Copy
        + Debug
        + Zero
        + PartialOrd
        + std::ops::BitAnd<Output = Self::InnerType>
        + std::ops::BitOr<Output = Self::InnerType>;

    fn new() -> Self;
    fn inner(self) -> Self::InnerType;
    fn is_set(&self, index: usize) -> bool;
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> CasResult<Self::InnerType>;
    fn load(&self) -> Self::InnerType;
    fn to_u64(&self) -> u64;
    fn to_u32(&self) -> u32;
}

impl AtomicBitmap for AtomicStride2 {
    type InnerType = u8;

    fn new() -> Self {
        AtomicStride2(AtomicU8::new(0))
    }
    fn inner(self) -> Self::InnerType {
        self.0.into_inner()
    }
    fn is_set(&self, bit: usize) -> bool {
        self.load() & (1 << bit) != 0
    }
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> CasResult<Self::InnerType> {
        CasResult(self.0.compare_exchange(
            current,
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ))
    }
    fn load(&self) -> Self::InnerType {
        self.0.load(Ordering::SeqCst)
    }

    fn to_u32(&self) -> u32 {
        self.0.load(Ordering::SeqCst) as u32
    }

    fn to_u64(&self) -> u64 {
        self.0.load(Ordering::SeqCst) as u64
    }
}

impl Zero for AtomicStride2 {
    fn zero() -> Self {
        AtomicStride2(AtomicU8::new(0))
    }

    fn is_zero(&self) -> bool {
        self.0.load(Ordering::SeqCst) == 0
    }
}

impl AtomicBitmap for AtomicStride3 {
    type InnerType = u16;

    fn new() -> Self {
        AtomicStride3(AtomicU16::new(0))
    }
    fn inner(self) -> Self::InnerType {
        self.0.into_inner()
    }
    fn is_set(&self, bit: usize) -> bool {
        self.load() & (1 << bit) != 0
    }
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> CasResult<Self::InnerType> {
        CasResult(self.0.compare_exchange(
            current,
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ))
    }

    fn load(&self) -> Self::InnerType {
        self.0.load(Ordering::SeqCst)
    }

    fn to_u32(&self) -> u32 {
        self.0.load(Ordering::SeqCst) as u32
    }

    fn to_u64(&self) -> u64 {
        self.0.load(Ordering::SeqCst) as u64
    }
}

impl Zero for AtomicStride3 {
    fn zero() -> Self {
        AtomicStride3(AtomicU16::new(0))
    }

    fn is_zero(&self) -> bool {
        self.0.load(Ordering::SeqCst) == 0
    }
}

impl AtomicBitmap for AtomicStride4 {
    type InnerType = u32;

    fn new() -> Self {
        AtomicStride4(AtomicU32::new(0))
    }
    fn inner(self) -> Self::InnerType {
        self.0.into_inner()
    }
    fn is_set(&self, bit: usize) -> bool {
        self.load() & (1 << bit) != 0
    }
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> CasResult<Self::InnerType> {
        CasResult(self.0.compare_exchange(
            current,
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ))
    }
    fn load(&self) -> Self::InnerType {
        self.0.load(Ordering::SeqCst)
    }

    fn to_u32(&self) -> u32 {
        self.0.load(Ordering::SeqCst)
    }

    fn to_u64(&self) -> u64 {
        self.0.load(Ordering::SeqCst) as u64
    }
}

impl Zero for AtomicStride4 {
    fn zero() -> Self {
        AtomicStride4(AtomicU32::new(0))
    }

    fn is_zero(&self) -> bool {
        self.0.load(Ordering::SeqCst) == 0
    }
}

impl AtomicBitmap for AtomicStride5 {
    type InnerType = u64;

    fn new() -> Self {
        AtomicStride5(AtomicU64::new(0))
    }
    fn inner(self) -> Self::InnerType {
        self.0.into_inner()
    }
    fn is_set(&self, bit: usize) -> bool {
        self.load() & (1 << bit) != 0
    }
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> CasResult<Self::InnerType> {
        CasResult(self.0.compare_exchange(
            current,
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ))
    }
    fn load(&self) -> Self::InnerType {
        self.0.load(Ordering::SeqCst)
    }

    fn to_u32(&self) -> u32 {
        self.0.load(Ordering::SeqCst) as u32
    }

    fn to_u64(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Zero for AtomicStride5 {
    fn zero() -> Self {
        AtomicStride5(AtomicU64::new(0))
    }

    fn is_zero(&self) -> bool {
        self.0.load(Ordering::SeqCst) == 0
    }
}

impl AtomicBitmap for AtomicStride6 {
    type InnerType = u128;

    fn new() -> Self {
        AtomicStride6(AtomicU128::new(0))
    }
    fn inner(self) -> Self::InnerType {
        let hi = self.0 .0.into_inner().to_be_bytes();
        let lo = self.0 .1.into_inner().to_be_bytes();

        u128::from_be_bytes([
            hi[0], hi[1], hi[2], hi[3], hi[4], hi[5], hi[6], hi[7], lo[0],
            lo[1], lo[2], lo[3], lo[4], lo[5], lo[6], lo[7],
        ])
    }
    fn is_set(&self, bit: usize) -> bool {
        self.load() & (1 << bit) != 0
    }
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> CasResult<Self::InnerType> {
        // TODO TODO
        // This is not actually thread-safe, it actually
        // needs a memory fence, since we're writing
        // to two different memory locations.
        (
            self.0 .0.compare_exchange(
                ((current << 64) >> 64) as u64,
                ((new >> 64) << 64) as u64,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ),
            self.0 .1.compare_exchange(
                ((current << 64) >> 64) as u64,
                ((new >> 64) << 64) as u64,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ),
        )
            .into()
    }
    fn load(&self) -> Self::InnerType {
        let hi = self.0 .0.load(Ordering::SeqCst).to_be_bytes();
        let lo = self.0 .1.load(Ordering::SeqCst).to_be_bytes();
        u128::from_be_bytes([
            hi[0], hi[1], hi[2], hi[3], hi[4], hi[5], hi[6], hi[7], lo[0],
            lo[1], lo[2], lo[3], lo[4], lo[5], lo[6], lo[7],
        ])
    }

    fn to_u32(&self) -> u32 {
        unimplemented!()
    }

    fn to_u64(&self) -> u64 {
        unimplemented!()
    }
}

impl Zero for AtomicStride6 {
    fn zero() -> Self {
        AtomicStride6(AtomicU128::new(0))
    }

    fn is_zero(&self) -> bool {
        self.0 .0.load(Ordering::SeqCst) == 0
            && self.0 .1.load(Ordering::SeqCst) == 0
    }
}

impl From<(Result<u64, u64>, Result<u64, u64>)> for CasResult<u128> {
    fn from(r: (Result<u64, u64>, Result<u64, u64>)) -> Self {
        match r {
            (Ok(hi), Ok(lo)) => CasResult::new(u128::from_be_bytes([
                hi.to_be_bytes()[0],
                hi.to_be_bytes()[1],
                hi.to_be_bytes()[2],
                hi.to_be_bytes()[3],
                hi.to_be_bytes()[4],
                hi.to_be_bytes()[5],
                hi.to_be_bytes()[6],
                hi.to_be_bytes()[7],
                lo.to_be_bytes()[0],
                lo.to_be_bytes()[1],
                lo.to_be_bytes()[2],
                lo.to_be_bytes()[3],
                lo.to_be_bytes()[4],
                lo.to_be_bytes()[5],
                lo.to_be_bytes()[6],
                lo.to_be_bytes()[7],
            ])),
            (Err(hi), Ok(lo)) => CasResult(Err(u128::from_be_bytes([
                hi.to_be_bytes()[0],
                hi.to_be_bytes()[1],
                hi.to_be_bytes()[2],
                hi.to_be_bytes()[3],
                hi.to_be_bytes()[4],
                hi.to_be_bytes()[5],
                hi.to_be_bytes()[6],
                hi.to_be_bytes()[7],
                lo.to_be_bytes()[0],
                lo.to_be_bytes()[1],
                lo.to_be_bytes()[2],
                lo.to_be_bytes()[3],
                lo.to_be_bytes()[4],
                lo.to_be_bytes()[5],
                lo.to_be_bytes()[6],
                lo.to_be_bytes()[7],
            ]))),
            (Ok(hi), Err(lo)) => CasResult(Err(u128::from_be_bytes([
                hi.to_be_bytes()[0],
                hi.to_be_bytes()[1],
                hi.to_be_bytes()[2],
                hi.to_be_bytes()[3],
                hi.to_be_bytes()[4],
                hi.to_be_bytes()[5],
                hi.to_be_bytes()[6],
                hi.to_be_bytes()[7],
                lo.to_be_bytes()[0],
                lo.to_be_bytes()[1],
                lo.to_be_bytes()[2],
                lo.to_be_bytes()[3],
                lo.to_be_bytes()[4],
                lo.to_be_bytes()[5],
                lo.to_be_bytes()[6],
                lo.to_be_bytes()[7],
            ]))),
            (Err(hi), Err(lo)) => CasResult(Err(u128::from_be_bytes([
                hi.to_be_bytes()[0],
                hi.to_be_bytes()[1],
                hi.to_be_bytes()[2],
                hi.to_be_bytes()[3],
                hi.to_be_bytes()[4],
                hi.to_be_bytes()[5],
                hi.to_be_bytes()[6],
                hi.to_be_bytes()[7],
                lo.to_be_bytes()[0],
                lo.to_be_bytes()[1],
                lo.to_be_bytes()[2],
                lo.to_be_bytes()[3],
                lo.to_be_bytes()[4],
                lo.to_be_bytes()[5],
                lo.to_be_bytes()[6],
                lo.to_be_bytes()[7],
            ]))),
        }
    }
}
pub trait Stride:
    Sized
    + Debug
    + Eq
    + Binary
    + PartialOrd
    + PartialEq
    + Zero
    + std::ops::BitAnd<Output = Self>
    + std::ops::BitOr<Output = Self>
where
    Self::AtomicPtrSize: AtomicBitmap,
    Self::AtomicPfxSize: AtomicBitmap,
    Self::PtrSize: Zero
        + Binary
        + Copy
        + Debug
        + std::ops::BitAnd<Output = Self::PtrSize>
        + PartialOrd
        + Zero,
{
    type AtomicPfxSize;
    type AtomicPtrSize;
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
    fn get_bit_pos(
        nibble: u32,
        len: u8,
    ) -> <<Self as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType;

    fn get_bit_pos_as_u8(
        nibble: u32,
        len: u8,
    ) -> u8;

    // Clear the bitmap to the right of the pointer and count the number of
    // ones. This numbder represents the index to the corresponding prefix in
    // the pfx_vec.

    // Clearing is performed by shifting to the right until we have the
    // nibble all the way at the right.

    // `(<Self as Stride>::BITS >> 1)`
    // The end of the bitmap (this bitmap is half the size of the pfx bitmap)

    // `nibble`
    // The bit position relative to the offset for the nibble length, this
    // index is only used at the last (relevant) stride, so the offset is
    // always 0.

    // get_pfx_index only needs nibble and len for fixed-layout bitarrays,
    // since the index can be deducted from them.
    fn get_pfx_index(nibble: u32, len: u8) -> usize;

    // Clear the bitmap to the right of the pointer and count the number of
    // ones. This number represents the index to the corresponding child node
    // in the ptr_vec.

    // Clearing is performed by shifting to the right until we have the
    // nibble all the way at the right.

    // For ptrbitarr the only index we want is the one for a full-length
    // nibble (stride length) at the last stride, so we don't need the length
    //  of the nibble.

    // `(<Self as Stride>::BITS >> 1)`
    // The end of the bitmap (this bitmap is half the size of the pfx bitmap)
    // AF::BITS is the size of the pfx bitmap.

    // `nibble`
    // The bit position relative to the offset for the nibble length, this
    // index is only used at the last (relevant) stride, so the offset is
    // always 0.
    fn get_ptr_index(
        bitmap: <<Self as Stride>::AtomicPtrSize as AtomicBitmap>::InnerType,
        nibble: u32,
    ) -> usize;

    #[allow(clippy::wrong_self_convention)]
    fn into_node_id<AF: AddressFamily>(
        addr_bits: AF,
        len: u8,
    ) -> super::node::StrideNodeId<AF>;

    // Convert a ptrbitarr into a pfxbitarr sized bitmap,
    // so we can do bitwise operations with a pfxbitarr sized
    // bitmap on them.
    // Since the last bit in the pfxbitarr isn't used, but the
    // full ptrbitarr *is* used, the prtbitarr should be shifted
    // one bit to the left.
    #[allow(clippy::wrong_self_convention)]
    fn into_stride_size(
        bitmap: <<Self as Stride>::AtomicPtrSize as AtomicBitmap>::InnerType,
    ) -> <<Self as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType;

    // Convert a pfxbitarr sized bitmap into a ptrbitarr sized
    // Note that bitwise operators align bits of unsigend types with
    // different sizes to the right, so we don't have to do anything to pad
    // the smaller sized type. We do have to shift one bit to the left, to
    // accomodate the unused pfxbitarr's last bit.
    #[allow(clippy::wrong_self_convention)]
    fn into_ptrbitarr_size(
        bitmap: <<Self as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType,
    ) -> <<Self as Stride>::AtomicPtrSize as AtomicBitmap>::InnerType;

    fn leading_zeros(self) -> u32;
}

impl_primitive_atomic_stride![3; 16; u16; AtomicStride3; u8; AtomicStride2];
impl_primitive_atomic_stride![4; 32; u32; AtomicStride4; u16; AtomicStride3];
impl_primitive_atomic_stride![5; 64; u64; AtomicStride5; u32; AtomicStride4];
// impl_primitive_stride![6; 128; u128; u64];

// impl Stride for Stride7 {
//     type PtrSize = u128;
//     const BITS: u8 = 255;
//     const STRIDE_LEN: u8 = 7;

//     fn get_bit_pos(nibble: u32, len: u8) -> Self {
//         match 256 - ((1 << len) - 1) as u16 - nibble as u16 - 1 {
//             n if n < 128 => U256(0, 1 << n),
//             n => U256(1 << (n as u16 - 128), 0),
//         }
//     }

//     fn get_pfx_index(bitmap: Self, nibble: u32, len: u8) -> usize {
//         let n = 256 - ((1 << len) - 1) as u16 - nibble as u16 - 1;
//         match n {
//             // if we move less than 128 bits to the right,
//             // all of bitmap.0 and a part of bitmap.1 will be used for counting zeros
//             // ex.
//             // ...1011_1010... >> 2 => ...0010_111010...
//             //    ____ ====                 -- --====
//             n if n < 128 => {
//                 bitmap.0.count_ones() as usize + (bitmap.1 >> n).count_ones() as usize - 1
//             }
//             // if we move more than 128 bits to the right,
//             // all of bitmap.1 wil be shifted out of sight,
//             // so we only have to count bitmap.0 zeroes than (after) shifting of course).
//             n => (bitmap.0 >> (n - 128)).count_ones() as usize - 1,
//         }
//     }

//     fn get_ptr_index(bitmap: Self::PtrSize, nibble: u32) -> usize {
//         (bitmap >> ((256 >> 1) - nibble as u16 - 1) as usize).count_ones() as usize - 1
//     }

//     fn into_stride_size(bitmap: Self::PtrSize) -> Self {
//         // One bit needs to move into the self.0 u128,
//         // since the last bit of the *whole* bitmap isn't used.
//         U256(bitmap >> 127, bitmap << 1)
//     }

//     fn into_ptrbitarr_size(bitmap: Self) -> Self::PtrSize {
//         // TODO expand:
//         // self.ptrbitarr =
//         // S::into_ptrbitarr_size(bit_pos | S::into_stride_size(self.ptrbitarr));
//         (bitmap.0 << 127 | bitmap.1 >> 1) as u128
//     }

//     #[inline]
//     fn leading_zeros(self) -> u32 {
//         let lz = self.0.leading_zeros();
//         if lz == 128 {
//             lz + self.1.leading_zeros()
//         } else {
//             lz
//         }
//     }
// }

// impl Stride for Stride8 {
//     type PtrSize = U256;
//     const BITS: u8 = 255; // bogus
//     const STRIDE_LEN: u8 = 8;

//     fn get_bit_pos(nibble: u32, len: u8) -> Self {
//         match 512 - ((1 << len) - 1) as u16 - nibble as u16 - 1 {
//             n if n < 128 => U512(0, 0, 0, 1 << n),
//             n if n < 256 => U512(0, 0, 1 << (n as u16 - 128), 0),
//             n if n < 384 => U512(0, 1 << (n as u16 - 256), 0, 0),
//             n => U512(1 << (n as u16 - 384), 0, 0, 0),
//         }
//     }

//     fn get_pfx_index(bitmap: Self, nibble: u32, len: u8) -> usize {
//         let n = 512 - ((1 << len) - 1) as u16 - nibble as u16 - 1;
//         match n {
//             // if we move less than 128 bits to the right, all of bitmap.2
//             // and a part of bitmap.3 will be used for counting zeros.
//             // ex.
//             // ...1011_1010... >> 2 => ...0010_111010...
//             //    ____ ====                 -- --====
//             n if n < 128 => {
//                 bitmap.0.count_ones() as usize
//                     + bitmap.1.count_ones() as usize
//                     + bitmap.2.count_ones() as usize
//                     + (bitmap.3 >> n).count_ones() as usize
//                     - 1
//             }

//             n if n < 256 => {
//                 bitmap.0.count_ones() as usize
//                     + bitmap.1.count_ones() as usize
//                     + (bitmap.2 >> (n - 128)).count_ones() as usize
//                     - 1
//             }

//             n if n < 384 => {
//                 bitmap.0.count_ones() as usize + (bitmap.1 >> (n - 256)).count_ones() as usize - 1
//             }

//             // if we move more than 384 bits to the right, all of bitmap.
//             // [1,2,3] will be shifted out of sight, so we only have to count
//             // bitmap.0 zeroes then (after shifting of course).
//             n => (bitmap.0 >> (n - 384)).count_ones() as usize - 1,
//         }
//     }

//     fn get_ptr_index(bitmap: Self::PtrSize, nibble: u32) -> usize {
//         let n = (512 >> 1) - nibble as u16 - 1;
//         match n {
//             // if we move less than 256 bits to the right, all of bitmap.0
//             // and a part of bitmap.1 will be used for counting zeros
//             // ex.
//             // ...1011_1010... >> 2 => ...0010_111010...
//             //    ____ ====                 -- --====
//             n if n < 128 => {
//                 bitmap.0.count_ones() as usize + (bitmap.1 >> n).count_ones() as usize - 1
//             }
//             // if we move more than 256 bits to the right, all of bitmap.1
//             // wil be shifted out of sight, so we only have to count bitmap.0
//             // zeroes than (after) shifting of course).
//             n => (bitmap.0 >> (n - 128)).count_ones() as usize - 1,
//         }
//     }

//     fn into_stride_size(bitmap: Self::PtrSize) -> Self {
//         // One bit needs to move into the self.0 u128,
//         // since the last bit of the *whole* bitmap isn't used.
//         U512(
//             0,
//             bitmap.0 >> 127,
//             (bitmap.0 << 1) | (bitmap.1 >> 127),
//             bitmap.1 << 1,
//         )
//     }

//     fn into_ptrbitarr_size(bitmap: Self) -> Self::PtrSize {
//         // TODO expand:
//         // self.ptrbitarr =
//         // S::into_ptrbitarr_size(bit_pos | S::into_stride_size(self.ptrbitarr));
//         U256(
//             (bitmap.1 << 127 | bitmap.2 >> 1) as u128,
//             (bitmap.2 << 127 | bitmap.3 >> 1) as u128,
//         )
//     }

//     #[inline]
//     fn leading_zeros(self) -> u32 {
//         let mut lz = self.0.leading_zeros();
//         if lz == 128 {
//             lz += self.1.leading_zeros();
//             if lz == 256 {
//                 lz += self.2.leading_zeros();
//                 if lz == 384 {
//                     lz += self.3.leading_zeros();
//                 }
//             }
//         }
//         lz
//     }
// }
