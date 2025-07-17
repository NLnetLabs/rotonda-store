use std::fmt;
use std::fmt::Debug;

use crate::{
    errors::FatalError,
    lsm_tree::PrimKey,
    prefix_cht::{
        cht::MultiMapValue,
        map_type::{MuiRdPathId, MuiRdPathIdBlob, SecKey},
    },
    types::AddressFamily,
};
use inetnum::addr::Prefix;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use super::PrefixId;

pub use super::route_status::RouteStatus;

//------------ Meta ----------------------------------------------------------

/// Trait for types that can be used as metadata of a record
pub trait Meta
where
    Self: fmt::Debug
        + fmt::Display
        + Clone
        + Sized
        + Send
        + Sync
        + AsRef<[u8]>
        + From<Vec<u8>>,
{
    type Orderable<'a>: Ord
    where
        Self: 'a;
    type TBI: Copy;

    fn as_orderable(&self, tbi: Self::TBI) -> Self::Orderable<'_>;
}

//------------ PublicRecord --------------------------------------------------

#[derive(Clone, Debug)]
pub struct Record<K, M> {
    pub multi_uniq_id: K,
    pub ltime: u64,
    pub status: RouteStatus,
    pub meta: M,
}

impl<K: Copy + Clone + Debug + PartialEq + Eq, M> Record<K, M> {
    pub fn new(
        multi_uniq_id: K,
        ltime: u64,
        status: RouteStatus,
        meta: M,
    ) -> Self {
        Self {
            meta,
            multi_uniq_id,
            ltime,
            status,
        }
    }
}

impl<K: Copy + Clone + Debug + Eq, M: Meta> From<(K, &MultiMapValue<M>)>
    for Record<K, M>
{
    fn from(value: (K, &MultiMapValue<M>)) -> Self {
        Self {
            multi_uniq_id: value.0,
            ltime: value.1.logical_time(),
            status: value.1.route_status(),
            meta: value.1.meta().clone(),
        }
    }
}
// impl<K: Copy + Clone + Debug + Eq, L: KeyExtensions, M: Meta>
//     From<(K, &Record<L, M>)> for Record<K, M>
// {
//     fn from(value: (K, &Record<L, M>)) -> Self {
//         Self {
//             multi_uniq_id: value.0,
//             ltime: value.1.ltime,
//             status: value.1.status,
//             meta: value.1.meta.clone(),
//         }
//     }
// }
impl<K: SecKey, L: SecKey, M: Meta> From<(K, &Record<L, M>)>
    for Record<K, M>
{
    fn from(value: (K, &Record<L, M>)) -> Self {
        Self {
            multi_uniq_id: value.0,
            ltime: value.1.ltime,
            status: value.1.status,
            meta: value.1.meta.clone(),
        }
    }
}

impl<const BLOB_SIZE: usize, M: Meta>
    From<Record<MuiRdPathIdBlob<BLOB_SIZE>, M>> for Record<MuiRdPathId, M>
{
    fn from(value: Record<MuiRdPathIdBlob<BLOB_SIZE>, M>) -> Self {
        let multi_uniq_id = <MuiRdPathId>::from(&value.multi_uniq_id);

        Self {
            multi_uniq_id,
            ltime: value.ltime,
            status: value.status,
            meta: value.meta,
        }
    }
}

impl<K: Copy + std::fmt::Display, M: std::fmt::Display> std::fmt::Display
    for Record<K, M>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ mui: {}, ltime: {}, status: {}, meta: {} }}",
            self.multi_uniq_id, self.ltime, self.status, self.meta
        )
    }
}

#[derive(KnownLayout, Immutable, Unaligned, IntoBytes, TryFromBytes)]
#[repr(C, packed)]
pub(crate) struct ZeroCopyRecord<PK: PrimKey, K: SecKey> {
    pub prefix: PK,
    pub multi_uniq_id: K,
    pub ltime: u64,
    pub status: RouteStatus,
    pub meta: [u8],
}

impl<PK: PrimKey, K: SecKey> ZeroCopyRecord<PK, K> {
    pub(crate) fn from_bytes(b: &[u8]) -> Result<&Self, FatalError> {
        Self::try_ref_from_bytes(b).map_err(|_| FatalError)
    }
}

impl<PK: PrimKey + std::fmt::Display, K: SecKey> std::fmt::Display
    for ZeroCopyRecord<PK, K>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mui = self.multi_uniq_id;
        let ltime = self.ltime;
        write!(
            f,
            "{{ mui: {}, ltime: {}, status: {}, meta: {:?} }}",
            mui, ltime, self.status, &self.meta
        )
    }
}

#[derive(KnownLayout, Immutable, Unaligned, IntoBytes, TryFromBytes)]
#[repr(C, packed)]
pub(crate) struct ValueHeader {
    pub ltime: u64,
    pub status: RouteStatus,
}

impl std::fmt::Display for ValueHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ltime = self.ltime;
        write!(f, "{{ ltime: {}, status: {} }}", ltime, self.status,)
    }
}

//------------ PublicPrefixRecord --------------------------------------------

#[derive(Clone, Debug)]
pub struct PrefixRecord<K, M: Meta> {
    pub prefix: Prefix,
    pub meta: Vec<Record<K, M>>,
}

impl<K: Copy + Debug + PartialEq + Eq, M: Meta> PrefixRecord<K, M> {
    pub fn new(prefix: Prefix, meta: Vec<Record<K, M>>) -> Self {
        Self { prefix, meta }
    }

    pub fn get_record_for_mui(&self, mui: K) -> Option<&Record<K, M>> {
        self.meta.iter().find(|r| r.multi_uniq_id == mui)
    }
}

impl<AF, K, M> From<(PrefixId<AF>, Vec<Record<K, M>>)> for PrefixRecord<K, M>
where
    AF: AddressFamily,
    M: Meta,
{
    fn from(record: (PrefixId<AF>, Vec<Record<K, M>>)) -> Self {
        Self {
            prefix: record.0.into(),
            meta: record.1,
        }
    }
}

impl<K: Copy + fmt::Display, M: Meta + std::fmt::Display> fmt::Display
    for PrefixRecord<K, M>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: [", self.prefix)?;
        for rec in &self.meta {
            write!(f, "{},", rec)?;
        }
        write!(f, "]")
    }
}

impl<K, M: Meta> From<(Prefix, Vec<Record<K, M>>)> for PrefixRecord<K, M> {
    fn from((prefix, meta): (Prefix, Vec<Record<K, M>>)) -> Self {
        Self { prefix, meta }
    }
}

//------------ RecordSet -----------------------------------------------------

#[derive(Clone, Debug)]
pub struct RecordSet<K, M: Meta> {
    pub v4: Vec<PrefixRecord<K, M>>,
    pub v6: Vec<PrefixRecord<K, M>>,
}

impl<K: Copy + Debug + Eq, M: Meta> RecordSet<K, M> {
    pub fn new() -> Self {
        Self {
            v4: Default::default(),
            v6: Default::default(),
        }
    }

    pub fn push(&mut self, prefix: Prefix, meta: Vec<Record<K, M>>) {
        match prefix.addr() {
            std::net::IpAddr::V4(_) => &mut self.v4,
            std::net::IpAddr::V6(_) => &mut self.v6,
        }
        .push(PrefixRecord::new(prefix, meta));
    }

    pub fn is_empty(&self) -> bool {
        self.v4.is_empty() && self.v6.is_empty()
    }

    pub fn iter(&self) -> RecordSetIter<K, M> {
        RecordSetIter {
            v4: if self.v4.is_empty() {
                None
            } else {
                Some(self.v4.iter())
            },
            v6: self.v6.iter(),
        }
    }

    #[must_use]
    pub fn reverse(mut self) -> RecordSet<K, M> {
        self.v4.reverse();
        self.v6.reverse();
        self
    }

    pub fn len(&self) -> usize {
        self.v4.len() + self.v6.len()
    }
}

impl<K: Copy + Debug + Eq, M: Meta> Default for RecordSet<K, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Copy + fmt::Display, M: Meta> fmt::Display for RecordSet<K, M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let arr_str_v4 =
            self.v4.iter().fold("".to_string(), |pfx_arr, pfx| {
                format!("{} {}", pfx_arr, *pfx)
            });
        let arr_str_v6 =
            self.v6.iter().fold("".to_string(), |pfx_arr, pfx| {
                format!("{} {}", pfx_arr, *pfx)
            });

        write!(f, "V4: [{}], V6: [{}]", arr_str_v4, arr_str_v6)
    }
}

impl<K, M: Meta> From<(Vec<PrefixRecord<K, M>>, Vec<PrefixRecord<K, M>>)>
    for RecordSet<K, M>
{
    fn from(
        (v4, v6): (Vec<PrefixRecord<K, M>>, Vec<PrefixRecord<K, M>>),
    ) -> Self {
        Self { v4, v6 }
    }
}

impl<K: Copy + Debug + Eq, M: Meta>
    std::iter::FromIterator<PrefixRecord<K, M>> for RecordSet<K, M>
{
    fn from_iter<I: IntoIterator<Item = PrefixRecord<K, M>>>(
        iter: I,
    ) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = pfx.prefix;
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(PrefixRecord::new(u_pfx, pfx.meta));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PrefixRecord::new(u_pfx, pfx.meta));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<AF: AddressFamily, K: Copy + Debug + Eq, M: Meta>
    std::iter::FromIterator<(PrefixId<AF>, Vec<Record<K, M>>)>
    for RecordSet<K, M>
{
    fn from_iter<
        I: IntoIterator<Item = (PrefixId<AF>, Vec<Record<K, M>>)>,
    >(
        iter: I,
    ) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = Prefix::from(pfx.0);
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(PrefixRecord::new(u_pfx, pfx.1));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PrefixRecord::new(u_pfx, pfx.1));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<'a, K: Copy + Debug + Eq, M: Meta + 'a>
    std::iter::FromIterator<&'a PrefixRecord<K, M>> for RecordSet<K, M>
{
    fn from_iter<I: IntoIterator<Item = &'a PrefixRecord<K, M>>>(
        iter: I,
    ) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = pfx.prefix;
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(PrefixRecord::new(u_pfx, pfx.meta.clone()));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PrefixRecord::new(u_pfx, pfx.meta.clone()));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<K, M: Meta> std::ops::Index<usize> for RecordSet<K, M> {
    type Output = PrefixRecord<K, M>;

    // This does not change the behaviour of the Index trait
    #[allow(clippy::indexing_slicing)]
    fn index(&self, index: usize) -> &Self::Output {
        if index < self.v4.len() {
            &self.v4[index]
        } else {
            &self.v6[index - self.v4.len()]
        }
    }
}

//------------ RecordSetIter -------------------------------------------------

#[derive(Clone, Debug)]
pub struct RecordSetIter<'a, K, M: Meta> {
    v4: Option<std::slice::Iter<'a, PrefixRecord<K, M>>>,
    v6: std::slice::Iter<'a, PrefixRecord<K, M>>,
}

impl<K: Copy, M: Meta> Iterator for RecordSetIter<'_, K, M> {
    type Item = PrefixRecord<K, M>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.v4.is_none() {
            return self.v6.next().map(|res| res.to_owned());
        }

        if let Some(res) = self.v4.as_mut().and_then(|v4| v4.next()) {
            return Some(res.to_owned());
        }
        self.v4 = None;
        self.next()
    }
}
