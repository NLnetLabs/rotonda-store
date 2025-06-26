use std::fmt::{Debug, Display};
use std::ops::Bound;

use roaring::RoaringBitmap;
use zerocopy::{
    Immutable, IntoBytes, KnownLayout, NativeEndian, TryFromBytes, Unaligned,
    U32,
};

use crate::errors::FatalResult;
use crate::prefix_record::Meta;
use crate::types::prefix_record::Record;

use super::cht::MultiMapValue;

pub trait RecordKey:
    Copy
    + Debug
    + TryFromBytes
    + Immutable
    + Display
    + IntoBytes
    + Unaligned
    + KnownLayout
    + std::hash::Hash
    + PartialEq
    + Eq
{
    fn mui(&self) -> U32<NativeEndian>;
    fn path_id(&self) -> Option<[u8; 4]> {
        None
    }
    fn route_distuingisher(&self) -> Option<[u8; 8]> {
        None
    }
}

#[repr(C)]
#[derive(
    Copy,
    Clone,
    Debug,
    Immutable,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    TryFromBytes,
    IntoBytes,
    Unaligned,
    KnownLayout,
    Hash,
)]
pub struct MuiPathId(U32<NativeEndian>, [u8; 4], bool);

impl RecordKey for MuiPathId {
    fn mui(&self) -> U32<NativeEndian> {
        self.0
    }

    fn path_id(&self) -> Option<[u8; 4]> {
        if self.2 {
            Some(self.1)
        } else {
            None
        }
    }
}

impl MuiPathId {
    pub(crate) fn mui_range(
        mui: MuiPathId,
    ) -> (Bound<MuiPathId>, Bound<MuiPathId>) {
        (
            std::ops::Bound::Included(MuiPathId(mui.0, [0_u8; 4], false)),
            std::ops::Bound::Excluded(MuiPathId(mui.0 + 1, [0_u8; 4], false)),
        )
    }
}

impl Display for MuiPathId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, u32::from_be_bytes(self.1))
    }
}

impl From<(u32, [u8; 4])> for MuiPathId {
    fn from(value: (u32, [u8; 4])) -> Self {
        Self(U32::<NativeEndian>::from(value.0), value.1, true)
    }
}

#[repr(C)]
#[derive(
    Copy,
    Clone,
    Debug,
    Immutable,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    TryFromBytes,
    IntoBytes,
    Unaligned,
    KnownLayout,
    Hash,
)]
pub struct Mui(U32<NativeEndian>);

impl RecordKey for Mui {
    fn mui(&self) -> U32<NativeEndian> {
        self.0
    }
}

impl Display for Mui {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for Mui {
    fn from(value: u32) -> Self {
        Self(value.into())
    }
}

pub trait MapType<M: Meta>: Debug {
    type Key: RecordKey;
    fn new() -> Self;
    #[allow(clippy::type_complexity)]
    fn best_backup(
        &self,
        tbi: M::TBI,
    ) -> (Option<Self::Key>, Option<Self::Key>);
    fn mark_as_withdrawn_for_mui(&self, mui: Self::Key, ltime: u64);
    fn mark_as_active_for_mui(&self, mui: Self::Key, ltime: u64);
    #[allow(clippy::type_complexity)]
    fn upsert_record(
        &self,
        new_rec: Record<Self::Key, M>,
    ) -> FatalResult<(Option<(MultiMapValue<M>, usize)>, usize)>;
    fn get_filtered_records(
        &self,
        mui: Option<Self::Key>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Vec<Record<Self::Key, M>>>;
    fn get_records_for_mui(
        &self,
        mui: Self::Key,
        include_withdrawn: bool,
    ) -> Vec<Record<Self::Key, M>>;
    fn get_record_for_key(
        &self,
        key: Self::Key,
        include_withdrawn: bool,
    ) -> Option<Record<Self::Key, M>>;
}
