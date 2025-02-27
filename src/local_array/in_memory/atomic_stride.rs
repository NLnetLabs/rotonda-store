use log::trace;
use parking_lot_core::SpinWait;
use std::fmt::{Binary, Debug};
use std::sync::atomic::{fence, AtomicU16, AtomicU32, Ordering};

use crate::af::Zero;
use crate::local_array;
use crate::local_array::bit_span::BitSpan;
use crate::local_array::rib::default_store::STRIDE_BITS;

pub type Stride4 = AtomicStride4;

pub struct AtomicStride3(pub AtomicU16);
pub struct AtomicStride4(pub AtomicU32);

pub struct CasResult<InnerType>(pub Result<InnerType, InnerType>);

impl<InnerType> CasResult<InnerType> {
    fn new(value: InnerType) -> Self {
        CasResult(Ok(value))
    }
}

pub(crate) trait AtomicBitmap
where
    Self: From<Self::InnerType>,
{
    type InnerType: Binary
        + Copy
        + Debug
        + Zero
        + PartialOrd
        + std::ops::BitAnd<Output = Self::InnerType>
        + std::ops::BitOr<Output = Self::InnerType>
        + std::ops::BitXor<Output = Self::InnerType>
        + num_traits::PrimInt;

    fn new() -> Self;
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> CasResult<Self::InnerType>;
    fn load(&self) -> Self::InnerType;
    fn merge_with(&self, node: Self::InnerType) {
        let mut spinwait = SpinWait::new();
        let current = self.load();

        fence(Ordering::Acquire);

        let mut new = current | node;
        loop {
            match self.compare_exchange(current, new) {
                CasResult(Ok(_)) => {
                    return;
                }
                CasResult(Err(current)) => {
                    new = current | node;
                }
            }
            spinwait.spin_no_yield();
        }
    }
}

// impl AtomicBitmap for AtomicStride2 {
//     type InnerType = u8;

//     fn new() -> Self {
//         AtomicStride2(AtomicU8::new(0))
//     }

//     fn compare_exchange(
//         &self,
//         current: Self::InnerType,
//         new: Self::InnerType,
//     ) -> CasResult<Self::InnerType> {
//         CasResult(self.0.compare_exchange(
//             current,
//             new,
//             Ordering::Acquire,
//             Ordering::Relaxed,
//         ))
//     }

//     fn load(&self) -> Self::InnerType {
//         self.0.load(Ordering::SeqCst)
//     }
// }

// impl Zero for AtomicStride2 {
//     fn zero() -> Self {
//         AtomicStride2(AtomicU8::new(0))
//     }

//     fn is_zero(&self) -> bool {
//         self.0.load(Ordering::SeqCst) == 0
//     }
// }

// impl From<u8> for AtomicStride2 {
//     fn from(value: u8) -> Self {
//         Self(AtomicU8::new(value))
//     }
// }

impl AtomicBitmap for AtomicStride3 {
    type InnerType = u16;

    fn new() -> Self {
        AtomicStride3(AtomicU16::new(0))
    }
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> CasResult<Self::InnerType> {
        CasResult(self.0.compare_exchange(
            current,
            new,
            Ordering::Acquire,
            Ordering::Relaxed,
        ))
    }

    fn load(&self) -> Self::InnerType {
        self.0.load(Ordering::Relaxed)
    }
}

impl From<u16> for AtomicStride3 {
    fn from(value: u16) -> Self {
        Self(AtomicU16::new(value))
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
    fn compare_exchange(
        &self,
        current: Self::InnerType,
        new: Self::InnerType,
    ) -> CasResult<Self::InnerType> {
        CasResult(self.0.compare_exchange(
            current,
            new,
            Ordering::Acquire,
            Ordering::Relaxed,
        ))
    }
    fn load(&self) -> Self::InnerType {
        self.0.load(Ordering::Relaxed)
    }
}

impl From<u32> for AtomicStride4 {
    fn from(value: u32) -> Self {
        Self(AtomicU32::new(value))
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

pub(crate) trait Stride {
    // const STRIDE_LEN: u8;

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
    fn get_bit_pos(bit_pos: BitSpan) -> u32;
    // ) -> <<Self as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType;

    fn bit_pos_from_index(i: u8) -> u32;

    fn ptr_bit_pos_from_index(i: u8) -> u16;

    fn cursor_from_bit_span(bs: BitSpan) -> u8;

    fn ptr_range(ptrbitarr: u16, range: BitSpan) -> (u16, u8);

    fn ms_pfx_mask(
        // pfxbitarr: <<Self as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType,
        pfxbitarr: u32,
        range: BitSpan,
    ) -> u32;

    // Clear the bitmap to the right of the pointer and count the number of
    // ones. This number represents the index to the corresponding prefix in
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

    // Convert a ptrbitarr into a pfxbitarr sized bitmap,
    // so we can do bitwise operations with a pfxbitarr sized
    // bitmap on them.
    // Since the last bit in the pfxbitarr isn't used, but the
    // full ptrbitarr *is* used, the prtbitarr should be shifted
    // one bit to the left.
    fn into_stride_size(
        bitmap: u16, // bitmap: <<Self as Stride>::AtomicPtrSize as AtomicBitmap>::InnerType,
                     // ) -> <<Self as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType;
    ) -> u32;

    // Convert a pfxbitarr sized bitmap into a ptrbitarr sized
    // Note that bitwise operators align bits of unsigned types with
    // different sizes to the right, so we don't have to do anything to pad
    // the smaller sized type. We do have to shift one bit to the left, to
    // accommodate the unused pfxbitarr's last bit.
    fn into_ptrbitarr_size(
        // bitmap: <<Self as Stride>::AtomicPfxSize as AtomicBitmap>::InnerType,
        bitmap: u32,
    ) -> u16;
    // ) -> <<Self as Stride>::AtomicPtrSize as AtomicBitmap>::InnerType;
}

// impl_primitive_atomic_stride![3; 16; u16; AtomicStride3; u8; AtomicStride2];
// impl_primitive_atomic_stride![4; 32; u32; AtomicStride4; u16; AtomicStride3];
// impl_primitive_atomic_stride![5; 64; u64; AtomicStride5; u32; AtomicStride4];
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
impl Stride for AtomicStride4 {
    // type AtomicPfxSize = $atomicpfxsize;
    // type AtomicPtrSize = $atomicptrsize;
    // type PtrSize = $ptrsize;
    // const BITS: u8 = $bits;
    // const STRIDE_LEN: u8 = 4;

    fn get_bit_pos(bs: BitSpan) -> u32 {
        // trace!("nibble {}, len {}, BITS {}", nibble, len, <Self as Stride>::BITS);
        1 << (STRIDE_BITS - ((1 << bs.len) - 1) as u8 - bs.bits as u8 - 1)
    }

    fn bit_pos_from_index(i: u8) -> u32 {
        <u32>::try_from(1).unwrap().rotate_right(1) >> i
    }

    fn ptr_bit_pos_from_index(i: u8) -> u16 {
        // trace!("pfx {} ptr {} strlen {}",
        // <$pfxsize>::BITS, <$ptrsize>::BITS, Self::STRIDE_LEN);
        <u16>::try_from(1).unwrap().rotate_right(1) >> (i + 1)
    }

    fn cursor_from_bit_span(bs: BitSpan) -> u8 {
        Self::get_bit_pos(bs).leading_zeros() as u8
    }

    fn ptr_range(ptrbitarr: u16, bs: BitSpan) -> (u16, u8) {
        let start: u8 = (bs.bits << (4 - bs.len)) as u8;
        let stop: u8 = start + (1 << (4 - bs.len));
        let mask: u16 = (((1_u32 << (stop as u32 - start as u32)) - 1)
            .rotate_right(stop as u32)
            >> 16)
            .try_into()
            .unwrap();
        trace!("- mask      {:032b}", mask);
        trace!("- ptrbitarr {:032b}", ptrbitarr);
        trace!("- shl bitar {:032b}", ptrbitarr & mask);

        // if ptrbitarr & mask == <$ptrsize>::zero() { panic!("stop"); }

        (ptrbitarr & mask, start)
    }

    fn ms_pfx_mask(pfxbitarr: u32, bs: BitSpan) -> u32 {
        local_array::in_memory::node::ms_prefix_mask_arr(bs) & pfxbitarr
    }

    fn into_stride_size(bitmap: u16) -> u32 {
        (bitmap as u32) << 1
    }

    fn into_ptrbitarr_size(bitmap: u32) -> u16 {
        (bitmap >> 1) as u16
    }
}
