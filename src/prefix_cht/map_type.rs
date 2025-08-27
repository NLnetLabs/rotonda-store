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
use crate::types::RouteStatus;

use super::cht::MultiMapValue;

pub trait KeyExtensions:
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
    + Ord
{
    // const PREFIX_SIZE: usize;

    fn mui(&self) -> U32<NativeEndian>;
    fn path_id(&self) -> Option<[u8; 4]> {
        None
    }
    fn route_distuingisher(&self) -> Option<[u8; 8]> {
        None
    }
    fn blob(&self) -> Option<&[u8]> {
        None
    }
    fn as_mui_rd_path_id(&self) -> MuiRdPathId;
}

//------------ Mui -----------------------------------------------------------
//
// The key for the simplest MultiMap, that only uses the mui, an u32.

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

impl KeyExtensions for Mui {
    // const PREFIX_SIZE: usize = 4;

    fn mui(&self) -> U32<NativeEndian> {
        self.0
    }

    fn as_mui_rd_path_id(&self) -> MuiRdPathId {
        MuiRdPathId(self.0, [0; 8], [0; 4], false, false)
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

impl From<U32<NativeEndian>> for Mui {
    fn from(value: U32<NativeEndian>) -> Self {
        Self(value)
    }
}

pub trait MapType<M: Meta>: Debug {
    type Key: KeyExtensions;
    type Inner;
    fn new() -> Self;
    fn inner(&self) -> &Self::Inner;
    fn inner_mut(&mut self) -> &mut Self::Inner;
    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = (&'a Self::Key, &'a MultiMapValue<M>)>
    where
        <Self as MapType<M>>::Key: 'a,
        M: 'a;
    #[allow(clippy::type_complexity)]
    fn contains_key(&self, key: &Self::Key) -> bool;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn get<FK: KeyExtensions>(&self, key: FK) -> Option<&MultiMapValue<M>>
    where
        Self::Key: From<FK>;

    #[allow(clippy::type_complexity)]
    // Helper to filter out records that are not-active (Inactive or
    // Withdrawn), or whose mui appears in the global withdrawn index.
    fn get_filtered_records<FK: KeyExtensions>(
        &self,
        key: Option<FK>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Vec<Record<Self::Key, M>>>
    where
        Self::Key: From<FK>,
        // Record<Self::Key, M>: From<(FK, &'a MultiMapValue<M>)>,
        // M: 'a,
    {
        if let Some(mui) = key {
            Some(
                self.get_record_for_key(mui, include_withdrawn)
                    .and_then(|r| {
                        if !bmin.contains(r.multi_uniq_id.mui().into()) {
                            Some(r)
                        } else {
                            None
                        }
                    })
                    .into_iter()
                    .collect::<Vec<_>>(),
            )
        } else {
            match include_withdrawn {
                false => {
                    let recs = self.as_active_records_not_in_bmin(bmin);
                    if recs.is_empty() {
                        None
                    } else {
                        Some(recs)
                    }
                }
                true => {
                    let recs = self.as_records_with_rewritten_status(
                        bmin,
                        RouteStatus::Withdrawn,
                    );
                    if recs.is_empty() {
                        None
                    } else {
                        Some(recs)
                    }
                }
            }
        }
    }

    fn get_records_for_mui(
        &self,
        mui: Mui,
        include_withdrawn: bool,
    ) -> Vec<Record<Self::Key, M>>;

    fn get_record_for_key<FK: KeyExtensions>(
        &self,
        key: FK,
        include_withdrawn: bool,
    ) -> Option<Record<Self::Key, M>>
    where
        Self::Key: From<FK>,
    {
        self.get(key).and_then(|r| -> Option<Record<Self::Key, M>> {
            if include_withdrawn || r.route_status() == RouteStatus::Active {
                Some(Record::<Self::Key, M>::from((key.into(), r)))
            } else {
                None
            }
        })
    }

    fn get_records_for_mui_with_rewritten_status(
        &self,
        mui: Mui,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Vec<Record<Self::Key, M>>;

    fn best_backup(
        &self,
        tbi: M::TBI,
    ) -> (Option<Self::Key>, Option<Self::Key>) {
        // let record_map = self.acquire_read_guard();
        let ord_routes =
            self.iter().map(|r| (r.1.meta().as_orderable(tbi), *r.0));
        let (best, bckup) =
            routecore::bgp::path_selection::best_backup_generic(ord_routes);
        (best.map(|b| b.1), bckup.map(|b| b.1))
    }

    // Insert or replace the PublicRecord in the HashMap for the key of
    // record.multi_uniq_id. Returns the number of entries in the HashMap
    // after updating it, if it's more than 1. Returns None if this is the
    // first entry.
    #[allow(clippy::type_complexity)]
    fn upsert_record(
        &mut self,
        new_rec: Record<Self::Key, M>,
    ) -> FatalResult<(Option<(MultiMapValue<M>, usize)>, usize)> {
        let key = new_rec.multi_uniq_id;

        match self.contains_key(&key) {
            true => {
                let old_rec = self
                    .insert(key, MultiMapValue::from(new_rec))
                    .map(|r| (r, self.len()));
                Ok((old_rec, 0))
            }
            false => {
                let new_rec = MultiMapValue::from(new_rec);
                let old_rec = self.insert(key, new_rec);
                assert!(old_rec.is_none());
                Ok((None, 0))
            }
        }
    }

    fn insert(
        &mut self,
        key: Self::Key,
        value: MultiMapValue<M>,
    ) -> Option<MultiMapValue<M>>;
    fn mark_as_withdrawn_for_mui(&mut self, mui: Mui, ltime: u64);
    fn mark_as_active_for_mui(&mut self, mui: Mui, ltime: u64);

    // return all records regardless of their local status, or any globally
    // set status for the mui of the record. However, the local status for a
    // record whose mui appears in the specified bitmap index, will be
    // rewritten with the specified RouteStatus.
    fn as_records_with_rewritten_status(
        &self,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Vec<Record<Self::Key, M>> {
        self.iter()
            .map(move |r| {
                let mut rec = r.1.clone();
                if bmin.contains(r.0.mui().into()) {
                    rec.set_route_status(rewrite_status);
                }
                Record::<Self::Key, M>::from((*r.0, &rec))
            })
            .collect::<Vec<_>>()
    }

    fn as_records(&self) -> Vec<Record<Self::Key, M>> {
        self.iter()
            .map(|r| Record::<Self::Key, M>::from((*r.0, r.1)))
            .collect::<Vec<_>>()
    }

    // Returns a vec of records whose keys are not in the supplied bitmap
    // index, and whose local Status is set to Active. Used to filter out
    // withdrawn routes.
    fn as_active_records_not_in_bmin(
        &self,
        bmin: &RoaringBitmap,
    ) -> Vec<Record<Self::Key, M>> {
        self.iter()
            .filter_map(|r| {
                if r.1.route_status() == RouteStatus::Active
                    && !bmin.contains(r.0.mui().into())
                {
                    Some(Record::<Self::Key, M>::from((*r.0, r.1)))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    fn get_filtered_records_for_mui(
        &self,
        mui: Mui,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Vec<Record<Self::Key, M>> {
        match include_withdrawn {
            false => self.get_records_for_mui(mui, include_withdrawn),
            true => self.get_records_for_mui_with_rewritten_status(
                mui,
                bmin,
                RouteStatus::Withdrawn,
            ),
        }
    }
}

//------------ MuiRdPathId ---------------------------------------------------
//
// Used by the MuiRdPathIdStarCastrib to store a (nui, rd, path_id) tuple as
// the key.

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
pub struct MuiRdPathId(U32<NativeEndian>, [u8; 8], [u8; 4], bool, bool);

impl KeyExtensions for MuiRdPathId {
    // mui (4) + rd (8 + 1) + path_id (4 + 1)
    // const PREFIX_SIZE: usize = 18;
    fn mui(&self) -> U32<NativeEndian> {
        self.0
    }

    fn path_id(&self) -> Option<[u8; 4]> {
        if self.4 {
            Some(self.2)
        } else {
            None
        }
    }

    fn route_distuingisher(&self) -> Option<[u8; 8]> {
        if self.3 {
            Some(self.1)
        } else {
            None
        }
    }

    fn as_mui_rd_path_id(&self) -> MuiRdPathId {
        *self
    }
}

impl MuiRdPathId {
    pub(crate) fn mui_range(
        mui: Mui,
    ) -> (Bound<MuiRdPathId>, Bound<MuiRdPathId>) {
        (
            std::ops::Bound::Included(MuiRdPathId(
                mui.0, [0_u8; 8], [0_u8; 4], false, false,
            )),
            std::ops::Bound::Excluded(MuiRdPathId(
                mui.0 + 1,
                [0_u8; 8],
                [0_u8; 4],
                false,
                false,
            )),
        )
    }
}

impl Display for MuiRdPathId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.0,
            u64::from_be_bytes(self.1),
            u32::from_be_bytes(self.2)
        )
    }
}

impl From<(u32, [u8; 8], [u8; 4])> for MuiRdPathId {
    fn from(value: (u32, [u8; 8], [u8; 4])) -> Self {
        Self(
            U32::<NativeEndian>::from(value.0),
            value.1,
            value.2,
            true,
            true,
        )
    }
}

impl From<Mui> for MuiRdPathId {
    fn from(value: Mui) -> Self {
        Self(value.0, [0; 8], [0; 4], false, false)
    }
}

// impl<const BLOB_SIZE: usize> From<&MuiRdPathIdBlob<BLOB_SIZE>>
//     for MuiRdPathId
// {
//     fn from(value: &MuiRdPathIdBlob<BLOB_SIZE>) -> Self {
//         #[allow(clippy::unwrap_used)]
//         value.as_mui_rd_path_id()
//     }
// }

impl<K: KeyExtensions> From<&K> for MuiRdPathId {
    #[allow(clippy::unwrap_used)]
    fn from(value: &K) -> Self {
        Self(
            value.mui(),
            value.route_distuingisher().unwrap_or([0; 8]),
            value.path_id().unwrap_or([0; 4]),
            value.route_distuingisher().is_some(),
            value.path_id().is_some(),
        )
    }
}
//------------ MuiPathId -----------------------------------------------------
//
// Used by the MuiPathIdStarCastRib to store a (mui, path_id) tuple as the key
// for the multimap.

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

impl KeyExtensions for MuiPathId {
    // const PREFIX_SIZE: usize = 9;
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

    fn as_mui_rd_path_id(&self) -> MuiRdPathId {
        MuiRdPathId(self.0, [0; 8], self.1, self.2, false)
    }
}

impl MuiPathId {
    pub(crate) fn mui_range(
        mui: Mui,
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

impl From<Mui> for MuiPathId {
    fn from(value: Mui) -> Self {
        Self(value.mui(), [0; 4], false)
    }
}

//------------ MuiRdPathIdBlob -----------------------------------------------
//
// Used by the BlobRib to store a (nui, rd, path_id) tuple as
// the key.

// #[repr(C)]
// #[derive(
//     Copy,
//     Clone,
//     Debug,
//     Immutable,
//     PartialEq,
//     Eq,
//     PartialOrd,
//     Ord,
//     TryFromBytes,
//     IntoBytes,
//     Unaligned,
//     KnownLayout,
//     Hash,
// )]
// pub struct MuiRdPathIdBlob<const BLOB_SIZE: usize>(
//     U32<NativeEndian>, // 0 mui (0 ..= 3)
//     [u8; 8],           // 1 rd (4 ..= 11)
//     [u8; 4],           // 2 path_id (12 ..= 15)
//     bool,              // 3 optional rd 16)
//     bool,              // 4 optional path_id (16)
//     bool,              // 5 optional blob (16)
//     [u8; BLOB_SIZE],   // 6 nlri blob (17 ..= 17 + BLOB_SIZE)
// );

// impl<const BLOB_SIZE: usize> KeyExtensions for MuiRdPathIdBlob<BLOB_SIZE> {
//     // mui (4) + rd (8 + 1) + path_id (4 + 1)
//     // const PREFIX_SIZE: usize = 18 + BLOB_SIZE;
//     fn mui(&self) -> U32<NativeEndian> {
//         self.0
//     }

//     fn route_distuingisher(&self) -> Option<[u8; 8]> {
//         if self.3 {
//             Some(self.1)
//         } else {
//             None
//         }
//     }

//     fn path_id(&self) -> Option<[u8; 4]> {
//         if self.4 {
//             Some(self.2)
//         } else {
//             None
//         }
//     }

//     fn blob(&self) -> Option<&[u8]> {
//         if self.5 {
//             Some(&self.6)
//         } else {
//             None
//         }
//     }

//     fn as_mui_rd_path_id(&self) -> MuiRdPathId {
//         MuiRdPathId(self.0, self.1, self.2, self.3, self.4)
//     }
// }

// impl<const BLOB_SIZE: usize> MuiRdPathIdBlob<BLOB_SIZE> {
//     pub(crate) fn mui_range(
//         mui: Mui,
//     ) -> (
//         Bound<MuiRdPathIdBlob<BLOB_SIZE>>,
//         Bound<MuiRdPathIdBlob<BLOB_SIZE>>,
//     ) {
//         (
//             std::ops::Bound::Included(MuiRdPathIdBlob(
//                 mui.0,
//                 [0_u8; 8],
//                 [0_u8; 4],
//                 false,
//                 false,
//                 false,
//                 [0; BLOB_SIZE],
//             )),
//             std::ops::Bound::Excluded(MuiRdPathIdBlob(
//                 mui.0 + 1,
//                 [0_u8; 8],
//                 [0_u8; 4],
//                 false,
//                 false,
//                 false,
//                 [0; BLOB_SIZE],
//             )),
//         )
//     }
// }

// impl<const BLOB_SIZE: usize> Display for MuiRdPathIdBlob<BLOB_SIZE> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "{}:{}:{}",
//             self.0,
//             u64::from_be_bytes(self.1),
//             u32::from_be_bytes(self.2)
//         )
//     }
// }

// impl<const BLOB_SIZE: usize> From<(u32, [u8; 8], [u8; 4], [u8; BLOB_SIZE])>
//     for MuiRdPathIdBlob<BLOB_SIZE>
// {
//     fn from(value: (u32, [u8; 8], [u8; 4], [u8; BLOB_SIZE])) -> Self {
//         Self(
//             U32::<NativeEndian>::from(value.0),
//             value.1,
//             value.2,
//             true,
//             true,
//             true,
//             value.3,
//         )
//     }
// }

// impl<const BLOB_SIZE: usize> From<Mui> for MuiRdPathIdBlob<BLOB_SIZE> {
//     fn from(value: Mui) -> Self {
//         Self(value.0, [0; 8], [0; 4], false, false, false, [0; BLOB_SIZE])
//     }
// }

// impl<const BLOB_SIZE: usize, K: KeyExtensions> From<(K, &[u8])>
//     for MuiRdPathIdBlob<BLOB_SIZE>
// {
//     #[allow(clippy::unwrap_used)]
//     fn from(value: (K, &[u8])) -> Self {
//         Self(
//             value.0.mui(),
//             value.0.route_distuingisher().unwrap_or([0; 8]),
//             value.0.path_id().unwrap_or([0; 4]),
//             value.0.route_distuingisher().is_some(),
//             value.0.path_id().is_some(),
//             true,
//             *value.1.first_chunk::<BLOB_SIZE>().unwrap(),
//         )
//     }
// }
