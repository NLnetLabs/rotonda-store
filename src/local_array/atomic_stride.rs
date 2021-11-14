use std::fmt::{Binary, Debug};
use std::sync::atomic::{
    AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering,
};

use num::{PrimInt, Zero};

use crate::impl_primitive_atomic_stride;
// use crate::synth_int::{U256, U512};

pub type Stride3 = u16;
pub type Stride4 = u32;
pub type Stride5 = u64;

// #[derive(Debug)]
// pub struct Stride3(u16);

// #[derive(Debug)]
// pub struct Stride4(u32);

// #[derive(Debug)]
// pub struct Stride5(u64);
// pub struct Stride6(u128);
// pub struct Stride7(U256);
// pub struct Stride8(U512);

// impl PartialEq for Stride3 {
//     fn eq(&self, other: &Self) -> bool {
//         self.0.load(Ordering::Relaxed) == other.0.load(Ordering::Relaxed)
//     }
// }
// impl Eq for Stride3 {}
// impl Ord for Stride3 {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.0.load(Ordering::Relaxed).cmp(&other.0.load(Ordering::Relaxed))
//     }
// }
// impl PartialOrd for Stride3 {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl PartialEq for Stride4 {
//     fn eq(&self, other: &Self) -> bool {
//         self.0.load(Ordering::Relaxed) == other.0.load(Ordering::Relaxed)
//     }
// }
// impl Eq for Stride4 {}
// impl Ord for Stride4 {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.0.load(Ordering::Relaxed).cmp(&other.0.load(Ordering::Relaxed))
//     }
// }
// impl PartialOrd for Stride4 {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl PartialEq for Stride5 {
//     fn eq(&self, other: &Self) -> bool {
//         self.0.load(Ordering::Relaxed) == other.0.load(Ordering::Relaxed)
//     }
// }
// impl Eq for Stride5 {}
// impl Ord for Stride5 {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.0.load(Ordering::Relaxed).cmp(&other.0.load(Ordering::Relaxed))
//     }
// }
// impl PartialOrd for Stride5 {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl PartialEq for Stride3 {
//     fn eq(&self, other: &Self) -> bool {
//         self.0 ==  other.0
//     }
// }
// impl Eq for Stride3 {}
// impl Ord for Stride3 {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.0.cmp(&other.0)
//     }
// }
// impl PartialOrd for Stride3 {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl PartialEq for Stride4 {
//     fn eq(&self, other: &Self) -> bool {
//         self.0 ==  other.0
//     }
// }
// impl Eq for Stride4 {}
// impl Ord for Stride4 {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.0.cmp(&other.0)
//     }
// }
// impl PartialOrd for Stride4 {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl PartialEq for Stride5 {
//     fn eq(&self, other: &Self) -> bool {
//         self.0 ==  other.0
//     }
// }
// impl Eq for Stride5 {}
// impl Ord for Stride5 {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.0.cmp(&other.0)
//     }
// }
// impl PartialOrd for Stride5 {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

pub struct AtomicStride2(pub AtomicU8);
pub struct AtomicStride3(pub AtomicU16);
pub struct AtomicStride4(pub AtomicU32);
pub struct AtomicStride5(pub AtomicU64);

pub trait AtomicBitmap {
    type InnerType: Binary + Debug + PrimInt + Zero;

    fn new() -> Self;
    fn inner(self) -> Self::InnerType;
    fn fetch_update<F: FnMut(Self::InnerType) -> Option<Self::InnerType>>(
        &self,
        f: F,
    ) -> Result<Self::InnerType, Self::InnerType>;
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> Result<Self::InnerType, Self::InnerType>;
    fn load(&self) -> Self::InnerType;
}

impl AtomicBitmap for AtomicStride2 {
    type InnerType = u8;

    fn new() -> Self {
        AtomicStride2(AtomicU8::new(0))
    }
    fn inner(self) -> Self::InnerType {
        self.0.into_inner()
    }
    fn fetch_update<F>(
        &self,
        f: F,
    ) -> Result<Self::InnerType, Self::InnerType>
    where
        F: FnMut(u8) -> Option<u8>,
    {
        self.0.fetch_update(Ordering::SeqCst, Ordering::SeqCst, f)
    }
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> Result<Self::InnerType, Self::InnerType> {
        self.0.compare_exchange(
            current,
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
    }
    fn load(&self) -> Self::InnerType {
        self.0.load(Ordering::SeqCst)
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
    fn fetch_update<F>(
        &self,
        f: F,
    ) -> Result<Self::InnerType, Self::InnerType>
    where
        F: FnMut(u16) -> Option<u16>,
    {
        self.0.fetch_update(Ordering::SeqCst, Ordering::SeqCst, f)
    }
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> Result<Self::InnerType, Self::InnerType> {
        self.0.compare_exchange(
            current,
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
    }
    fn load(&self) -> Self::InnerType {
        self.0.load(Ordering::SeqCst)
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
    fn fetch_update<F>(
        &self,
        f: F,
    ) -> Result<Self::InnerType, Self::InnerType>
    where
        F: FnMut(u32) -> Option<u32>,
    {
        self.0.fetch_update(Ordering::SeqCst, Ordering::SeqCst, f)
    }
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> Result<Self::InnerType, Self::InnerType> {
        self.0.compare_exchange(
            current,
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
    }
    fn load(&self) -> Self::InnerType {
        self.0.load(Ordering::SeqCst)
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
    fn fetch_update<F>(
        &self,
        f: F,
    ) -> Result<Self::InnerType, Self::InnerType>
    where
        F: FnMut(u64) -> Option<u64>,
    {
        self.0.fetch_update(Ordering::SeqCst, Ordering::SeqCst, f)
    }
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> Result<Self::InnerType, Self::InnerType> {
        self.0.compare_exchange(
            current,
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
    }
    fn load(&self) -> Self::InnerType {
        self.0.load(Ordering::SeqCst)
    }
}

// Sized + Debug + Binary + Eq + PartialOrd + PartialEq + Copy

pub trait Stride:
    Sized + Debug + Eq + Binary + PartialOrd + PartialEq + Zero
where
    Self::AtomicPtrSize: AtomicBitmap,
    Self::AtomicPfxSize: AtomicBitmap,
    Self::PtrSize: num::Zero,
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

    // Clear the bitmap to the right of the pointer and count the number of ones.
    // This numbder represents the index to the corresponding prefix in the pfx_vec.

    // Clearing is performed by shifting to the right until we have the nibble
    // all the way at the right.

    // `(<Self as Stride>::BITS >> 1)`
    // The end of the bitmap (this bitmap is half the size of the pfx bitmap)

    // `nibble`
    // The bit position relative to the offset for the nibble length, this index
    // is only used at the last (relevant) stride, so the offset is always 0.
    fn get_pfx_index(
        bitmap: <<Self as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType,
        nibble: u32,
        len: u8,
    ) -> usize;

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
    fn get_ptr_index(
        bitmap: <<Self as Stride>::AtomicPtrSize as AtomicBitmap>::InnerType,
        nibble: u32,
    ) -> usize;

    // Convert a ptrbitarr into a pfxbitarr sized bitmap,
    // so we can do bitwise operations with a pfxbitarr sized
    // bitmap on them.
    // Since the last bit in the pfxbitarr isn't used, but the
    // full ptrbitarr *is* used, the prtbitarr should be shifted
    // one bit to the left.
    fn into_stride_size(
        bitmap: <<Self as Stride>::AtomicPtrSize as AtomicBitmap>::InnerType,
    ) -> <<Self as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType;

    // Convert a pfxbitarr sized bitmap into a ptrbitarr sized
    // Note that bitwise operators align bits of unsigend types with different
    // sizes to the right, so we don't have to do anything to pad the smaller sized
    // type. We do have to shift one bit to the left, to accomodate the unused pfxbitarr's
    // last bit.
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
