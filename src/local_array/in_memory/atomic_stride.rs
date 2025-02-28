use log::{log_enabled, trace};
use parking_lot_core::SpinWait;
use std::fmt::{Binary, Debug};
use std::sync::atomic::{fence, AtomicU16, AtomicU32, Ordering};

use crate::local_array;
use crate::local_array::bit_span::BitSpan;

pub struct AtomicPtrBitArr(pub AtomicU16);
pub struct AtomicPfxBitArr(pub AtomicU32);

pub struct CasResult<InnerType>(pub Result<InnerType, InnerType>);

pub(crate) trait AtomicBitmap
where
    Self: From<Self::InnerType>,
{
    type InnerType: Binary
        + Copy
        + Debug
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

impl AtomicPtrBitArr {
    pub(crate) fn ptr_range(&self, bs: BitSpan) -> (u16, u8) {
        let ptrbitarr = self.load();
        let start: u8 = (bs.bits << (4 - bs.len)) as u8;
        let stop: u8 = start + (1 << (4 - bs.len));
        let mask: u16 = (((1_u32 << (stop as u32 - start as u32)) - 1)
            .rotate_right(stop as u32)
            >> 16)
            .try_into()
            .unwrap();
        if log_enabled!(log::Level::Trace) {
            trace!("- mask      {:032b}", mask);
            trace!("- ptrbitarr {:032b}", ptrbitarr);
            trace!("- shl bitar {:032b}", ptrbitarr & mask);
        }

        (ptrbitarr & mask, start)
    }

    pub(crate) fn as_stride_size(&self) -> u32 {
        (self.load() as u32) << 1
    }
}

impl AtomicBitmap for AtomicPtrBitArr {
    type InnerType = u16;

    fn new() -> Self {
        AtomicPtrBitArr(AtomicU16::new(0))
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

impl From<u16> for AtomicPtrBitArr {
    fn from(value: u16) -> Self {
        Self(AtomicU16::new(value))
    }
}

impl AtomicPfxBitArr {
    pub(crate) fn ms_pfx_mask(&self, bs: BitSpan) -> u32 {
        let pfxbitarr = self.load();
        local_array::in_memory::node::ms_prefix_mask_arr(bs) & pfxbitarr
    }
}

pub(crate) fn into_ptrbitarr(bitmap: u32) -> u16 {
    (bitmap >> 1) as u16
}

pub(crate) fn into_pfxbitarr(bitmap: u16) -> u32 {
    (bitmap as u32) << 1
}

pub(crate) fn bit_pos_from_index(i: u8) -> u32 {
    1_u32.rotate_right(1) >> i
}

pub(crate) fn ptr_bit_pos_from_index(i: u8) -> u16 {
    // trace!("pfx {} ptr {} strlen {}",
    // <$pfxsize>::BITS, <$ptrsize>::BITS, Self::STRIDE_LEN);
    1_u16.rotate_right(1) >> (i + 1)
}

impl AtomicBitmap for AtomicPfxBitArr {
    type InnerType = u32;

    fn new() -> Self {
        AtomicPfxBitArr(AtomicU32::new(0))
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

impl From<u32> for AtomicPfxBitArr {
    fn from(value: u32) -> Self {
        Self(AtomicU32::new(value))
    }
}

// pub(crate) trait Stride {
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
// fn get_bit_pos(bit_span: BitSpan) -> u32;

// fn bit_pos_from_index(i: u8) -> u32;

// fn ptr_bit_pos_from_index(i: u8) -> u16;

// fn cursor_from_bit_span(bs: BitSpan) -> u8;

// fn ptr_range(ptrbitarr: u16, range: BitSpan) -> (u16, u8);

// fn ms_pfx_mask(pfxbitarr: u32, range: BitSpan) -> u32;

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
// fn into_stride_size(bitmap: u16) -> u32;

// Convert a pfxbitarr sized bitmap into a ptrbitarr sized
// Note that bitwise operators align bits of unsigned types with
// different sizes to the right, so we don't have to do anything to pad
// the smaller sized type. We do have to shift one bit to the left, to
// accommodate the unused pfxbitarr's last bit.
// fn into_ptrbitarr_size(bitmap: u32) -> u16;
// }

// impl Stride for AtomicPfxBitArr {
// fn get_bit_pos(bs: BitSpan) -> u32 {
//     // trace!("nibble {}, len {}, BITS {}", nibble, len, <Self as Stride>::BITS);
//     1 << (STRIDE_BITS - ((1 << bs.len) - 1) as u8 - bs.bits as u8 - 1)
// }

// fn bit_pos_from_index(i: u8) -> u32 {
//     <u32>::try_from(1).unwrap().rotate_right(1) >> i
// }

// fn ptr_bit_pos_from_index(i: u8) -> u16 {
//     // trace!("pfx {} ptr {} strlen {}",
//     // <$pfxsize>::BITS, <$ptrsize>::BITS, Self::STRIDE_LEN);
//     <u16>::try_from(1).unwrap().rotate_right(1) >> (i + 1)
// }

// fn cursor_from_bit_span(bs: BitSpan) -> u8 {
//     Self::get_bit_pos(bs).leading_zeros() as u8
// }

// fn ptr_range(ptrbitarr: u16, bs: BitSpan) -> (u16, u8) {
//     let start: u8 = (bs.bits << (4 - bs.len)) as u8;
//     let stop: u8 = start + (1 << (4 - bs.len));
//     let mask: u16 = (((1_u32 << (stop as u32 - start as u32)) - 1)
//         .rotate_right(stop as u32)
//         >> 16)
//         .try_into()
//         .unwrap();
//     trace!("- mask      {:032b}", mask);
//     trace!("- ptrbitarr {:032b}", ptrbitarr);
//     trace!("- shl bitar {:032b}", ptrbitarr & mask);

//     // if ptrbitarr & mask == <$ptrsize>::zero() { panic!("stop"); }

//     (ptrbitarr & mask, start)
// }

// fn ms_pfx_mask(pfxbitarr: u32, bs: BitSpan) -> u32 {
//     local_array::in_memory::node::ms_prefix_mask_arr(bs) & pfxbitarr
// }

// fn into_stride_size(bitmap: u16) -> u32 {
//     (bitmap as u32) << 1
// }

// fn into_ptrbitarr_size(bitmap: u32) -> u16 {
//     (bitmap >> 1) as u16
// }
// }
