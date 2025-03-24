use std::fmt;
use std::fmt::Debug;

use crate::{errors::FatalError, types::AddressFamily};
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
pub struct Record<M> {
    pub multi_uniq_id: u32,
    pub ltime: u64,
    pub status: RouteStatus,
    pub meta: M,
}

impl<M> Record<M> {
    pub fn new(
        multi_uniq_id: u32,
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

impl<M: std::fmt::Display> std::fmt::Display for Record<M> {
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
pub(crate) struct ZeroCopyRecord<AF: AddressFamily> {
    pub prefix: PrefixId<AF>,
    pub multi_uniq_id: u32,
    pub ltime: u64,
    pub status: RouteStatus,
    pub meta: [u8],
}

impl<AF: AddressFamily> ZeroCopyRecord<AF> {
    pub(crate) fn from_bytes(b: &[u8]) -> Result<&Self, FatalError> {
        Self::try_ref_from_bytes(b).or_else(|_| Err(FatalError))
    }
}

impl<AF: AddressFamily + std::fmt::Display> std::fmt::Display
    for ZeroCopyRecord<AF>
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
pub struct PrefixRecord<M: Meta> {
    pub prefix: Prefix,
    pub meta: Vec<Record<M>>,
}

impl<M: Meta> PrefixRecord<M> {
    pub fn new(prefix: Prefix, meta: Vec<Record<M>>) -> Self {
        Self { prefix, meta }
    }

    pub fn get_record_for_mui(&self, mui: u32) -> Option<&Record<M>> {
        self.meta.iter().find(|r| r.multi_uniq_id == mui)
    }
}

impl<AF, M> From<(PrefixId<AF>, Vec<Record<M>>)> for PrefixRecord<M>
where
    AF: AddressFamily,
    M: Meta,
{
    fn from(record: (PrefixId<AF>, Vec<Record<M>>)) -> Self {
        Self {
            prefix: record.0.into(),
            meta: record.1,
        }
    }
}

impl<M: Meta + std::fmt::Display> std::fmt::Display for PrefixRecord<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: [", self.prefix)?;
        for rec in &self.meta {
            write!(f, "{},", rec)?;
        }
        write!(f, "]")
    }
}

impl<M: Meta> From<(Prefix, Vec<Record<M>>)> for PrefixRecord<M> {
    fn from((prefix, meta): (Prefix, Vec<Record<M>>)) -> Self {
        Self { prefix, meta }
    }
}

//------------ RecordSet -----------------------------------------------------

#[derive(Clone, Debug)]
pub struct RecordSet<M: Meta> {
    pub v4: Vec<PrefixRecord<M>>,
    pub v6: Vec<PrefixRecord<M>>,
}

impl<M: Meta> RecordSet<M> {
    pub fn new() -> Self {
        Self {
            v4: Default::default(),
            v6: Default::default(),
        }
    }

    pub fn push(&mut self, prefix: Prefix, meta: Vec<Record<M>>) {
        match prefix.addr() {
            std::net::IpAddr::V4(_) => &mut self.v4,
            std::net::IpAddr::V6(_) => &mut self.v6,
        }
        .push(PrefixRecord::new(prefix, meta));
    }

    pub fn is_empty(&self) -> bool {
        self.v4.is_empty() && self.v6.is_empty()
    }

    pub fn iter(&self) -> RecordSetIter<M> {
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
    pub fn reverse(mut self) -> RecordSet<M> {
        self.v4.reverse();
        self.v6.reverse();
        self
    }

    pub fn len(&self) -> usize {
        self.v4.len() + self.v6.len()
    }
}

impl<M: Meta> Default for RecordSet<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Meta> fmt::Display for RecordSet<M> {
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

impl<M: Meta> From<(Vec<PrefixRecord<M>>, Vec<PrefixRecord<M>>)>
    for RecordSet<M>
{
    fn from((v4, v6): (Vec<PrefixRecord<M>>, Vec<PrefixRecord<M>>)) -> Self {
        Self { v4, v6 }
    }
}

impl<M: Meta> std::iter::FromIterator<PrefixRecord<M>> for RecordSet<M> {
    fn from_iter<I: IntoIterator<Item = PrefixRecord<M>>>(iter: I) -> Self {
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

impl<AF: AddressFamily, M: Meta>
    std::iter::FromIterator<(PrefixId<AF>, Vec<Record<M>>)> for RecordSet<M>
{
    fn from_iter<I: IntoIterator<Item = (PrefixId<AF>, Vec<Record<M>>)>>(
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

impl<'a, M: Meta + 'a> std::iter::FromIterator<&'a PrefixRecord<M>>
    for RecordSet<M>
{
    fn from_iter<I: IntoIterator<Item = &'a PrefixRecord<M>>>(
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

impl<M: Meta> std::ops::Index<usize> for RecordSet<M> {
    type Output = PrefixRecord<M>;

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
pub struct RecordSetIter<'a, M: Meta> {
    v4: Option<std::slice::Iter<'a, PrefixRecord<M>>>,
    v6: std::slice::Iter<'a, PrefixRecord<M>>,
}

impl<M: Meta> Iterator for RecordSetIter<'_, M> {
    type Item = PrefixRecord<M>;

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
