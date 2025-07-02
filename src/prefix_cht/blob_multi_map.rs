//------------ BlobMultiMap --------------------------------------------------

use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    ops::Bound,
};

use zerocopy::{
    Immutable, IntoBytes, KnownLayout, NativeEndian, TryFromBytes, Unaligned,
    U32, U64,
};

use crate::{
    prefix_record::Meta,
    types::{Record, RouteStatus},
};

use super::{
    cht::{MultiMapValue, NlriBlobMultiMapValue},
    map_type::{MapType, Mui, RecordKey},
};

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
pub struct HashedBlobKey(U32<NativeEndian>, U64<NativeEndian>);

pub struct BlobKey(Mui, Vec<u8>);

impl BlobKey {
    pub fn blob(&self) -> &[u8] {
        &self.1
    }
}

impl std::fmt::Display for HashedBlobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{:?}]", self.0, self.1)
    }
}

#[derive(Debug)]
pub struct BlobMultiMap<M: Meta>(
    BTreeMap<HashedBlobKey, (Vec<u8>, MultiMapValue<M>)>,
);

impl HashedBlobKey {
    fn mui_range(
        mui: Mui,
    ) -> (Bound<HashedBlobKey>, std::ops::Bound<HashedBlobKey>) {
        (
            Bound::Included(HashedBlobKey(mui.mui(), 0.into())),
            Bound::Excluded(HashedBlobKey(mui.mui(), 0.into())),
        )
    }
    fn hash_from_vec(value: &Vec<u8>) -> U64<NativeEndian> {
        let mut s = DefaultHasher::new();
        value.hash(&mut s);
        s.finish().into()
    }
}

impl RecordKey for HashedBlobKey {
    fn mui(&self) -> U32<NativeEndian> {
        self.0
    }
}

impl<M: Meta> From<Record<HashedBlobKey, M>> for NlriBlobMultiMapValue<M> {
    fn from(value: Record<HashedBlobKey, M>) -> Self {
        NlriBlobMultiMapValue {
            meta: value.meta,
            ltime: value.ltime,
            route_status: value.route_status,
            nlri: vec![],
        }
    }
}

impl<M: Meta> MapType<M> for BlobMultiMap<M> {
    type Key = HashedBlobKey;
    type SingleValue = NlriBlobMultiMapValue<M>;
    type MultiValue = BTreeMap<HashedBlobKey, NlriBlobMultiMapValue<M>>;

    fn new() -> Self {
        Self(BTreeMap::new())
    }

    fn inner(&self) -> &Self::MultiValue {
        &self.0
    }

    fn inner_mut(&mut self) -> &mut Self::MultiValue {
        &mut self.0
    }

    fn contains_key(&self, key: &Self::Key) -> bool {
        self.0.contains_key(key)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn get(&self, key: &Self::Key) -> Option<&MultiMapValue<M>> {
        self.0.get(key).map(|v| &v.1)
    }

    fn get_records_for_mui(
        &self,
        mui: super::map_type::Mui,
        include_withdrawn: bool,
    ) -> Vec<crate::types::Record<Self::Key, M>> {
        let mut res = vec![];

        let range = Self::Key::mui_range(mui);

        for r in self.0.range(range) {
            if include_withdrawn
                || r.1 .1.route_status() == RouteStatus::Active
            {
                res.push(Record::<Self::Key, M>::from((*r.0, &r.1 .1)))
            }
        }

        res
    }

    fn get_records_for_mui_with_rewritten_status(
        &self,
        mui: super::map_type::Mui,
        bmin: &roaring::RoaringBitmap,
        rewrite_status: crate::types::RouteStatus,
    ) -> Vec<crate::types::Record<Self::Key, M>> {
        todo!()
    }

    fn insert<'a>(
        &mut self,
        key: Self::Key,
        value: (Vec<u8>, MultiMapValue<M>),
    ) -> Option<(Vec<u8>, MultiMapValue<M>)> {
        self.0.insert(key, value)
    }

    fn mark_as_withdrawn_for_mui(
        &mut self,
        mui: super::map_type::Mui,
        ltime: u64,
    ) {
        let range = Self::Key::mui_range(mui);
        for rec in self.inner_mut().range_mut(range) {
            rec.1 .1.set_route_status(RouteStatus::Withdrawn);
            rec.1 .1.set_logical_time(ltime);
        }
    }

    fn mark_as_active_for_mui(
        &mut self,
        mui: super::map_type::Mui,
        ltime: u64,
    ) {
        todo!()
    }

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = (&'a Self::Key, &'a MultiMapValue<M>)>
    where
        <Self as MapType<M>>::Key: 'a,
        M: 'a,
    {
        self.0.iter().map(|r| (r.0, &r.1 .1))
    }
}
