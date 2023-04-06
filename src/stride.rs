use crate::impl_primitive_stride;
use crate::synth_int::{U256, U512};
use std::fmt::{Binary, Debug};

pub type Stride3 = u16;
pub type Stride4 = u32;
pub type Stride5 = u64;
pub type Stride6 = u128;
pub type Stride7 = U256;
pub type Stride8 = U512;

pub trait Stride:
    Sized + Debug + Binary + Eq + PartialOrd + PartialEq + Copy
{
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
    #[allow(clippy::wrong_self_convention)]
    fn into_stride_size(bitmap: Self::PtrSize) -> Self;

    // Convert a pfxbitarr sized bitmap into a ptrbitarr sized
    // Note that bitwise operators align bits of unsigend types with different
    // sizes to the right, so we don't have to do anything to pad the smaller sized
    // type. We do have to shift one bit to the left, to accomodate the unused pfxbitarr's
    // last bit.
    fn into_ptrbitarr_size(bitmap: Self) -> Self::PtrSize;

    fn leading_zeros(self) -> u32;
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
            n => U256(1 << (n - 128), 0),
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
                bitmap.0.count_ones() as usize
                    + (bitmap.1 >> n).count_ones() as usize
                    - 1
            }
            // if we move more than 128 bits to the right,
            // all of bitmap.1 wil be shifted out of sight,
            // so we only have to count bitmap.0 zeroes than (after shifting of course).
            n => (bitmap.0 >> (n - 128)).count_ones() as usize - 1,
        }
    }

    fn get_ptr_index(bitmap: Self::PtrSize, nibble: u32) -> usize {
        (bitmap >> ((256 >> 1) - nibble as u16 - 1) as usize).count_ones()
            as usize
            - 1
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
        bitmap.0 << 127 | bitmap.1 >> 1
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
            n if n < 256 => U512(0, 0, 1 << (n - 128), 0),
            n if n < 384 => U512(0, 1 << (n - 256), 0, 0),
            n => U512(1 << (n - 384), 0, 0, 0),
        }
    }

    fn get_pfx_index(bitmap: Self, nibble: u32, len: u8) -> usize {
        let n = 512 - ((1 << len) - 1) as u16 - nibble as u16 - 1;
        match n {
            // if we move less than 128 bits to the right, all of bitmap.2
            // and a part of bitmap.3 will be used for counting zeros.
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
                bitmap.0.count_ones() as usize
                    + (bitmap.1 >> (n - 256)).count_ones() as usize
                    - 1
            }

            // if we move more than 384 bits to the right, all of bitmap.
            // [1,2,3] will be shifted out of sight, so we only have to count
            // bitmap.0 zeroes then (after shifting of course).
            n => (bitmap.0 >> (n - 384)).count_ones() as usize - 1,
        }
    }

    fn get_ptr_index(bitmap: Self::PtrSize, nibble: u32) -> usize {
        let n = (512 >> 1) - nibble as u16 - 1;
        match n {
            // if we move less than 256 bits to the right, all of bitmap.0
            // and a part of bitmap.1 will be used for counting zeros
            // ex.
            // ...1011_1010... >> 2 => ...0010_111010...
            //    ____ ====                 -- --====
            n if n < 128 => {
                bitmap.0.count_ones() as usize
                    + (bitmap.1 >> n).count_ones() as usize
                    - 1
            }
            // if we move more than 256 bits to the right, all of bitmap.1
            // wil be shifted out of sight, so we only have to count bitmap.0
            // zeroes than (after) shifting of course).
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
            bitmap.1 << 127 | bitmap.2 >> 1,
            bitmap.2 << 127 | bitmap.3 >> 1,
        )
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
