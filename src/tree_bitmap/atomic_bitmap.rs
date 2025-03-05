use parking_lot_core::SpinWait;
use std::fmt::{Binary, Debug};
use std::sync::atomic::{fence, AtomicU16, AtomicU32, Ordering};

use crate::types::BitSpan;

use super::tree_bitmap_node;

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
        tree_bitmap_node::ptr_range(ptrbitarr, bs)
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
        tree_bitmap_node::ms_prefix_mask_arr(bs) & pfxbitarr
    }
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
