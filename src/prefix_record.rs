use std::fmt;
use std::fmt::Debug;
use std::{cmp::Ordering, sync::Arc};

use crate::af::AddressFamily;
use crate::local_array::types::RouteStatus;
use crate::prelude::multi::PrefixId;
use inetnum::addr::Prefix;
use zerocopy::{
    Immutable, IntoBytes, KnownLayout, NetworkEndian, TryFromBytes,
    Unaligned, U128, U32,
};

//------------ InternalPrefixRecord -----------------------------------------

// This struct is used for the SingleThreadedStore only.
#[derive(Clone, Copy)]
pub struct InternalPrefixRecord<AF, M>
where
    M: Meta,
    AF: AddressFamily,
{
    pub net: AF,
    pub len: u8,
    pub meta: M,
}

impl<M, AF> InternalPrefixRecord<AF, M>
where
    M: Meta,
    AF: AddressFamily,
{
    pub fn new_with_meta(
        net: AF,
        len: u8,
        meta: M,
    ) -> InternalPrefixRecord<AF, M> {
        Self { net, len, meta }
    }

    // This should never fail, since there shouldn't be a invalid prefix in
    // this record in the first place.
    pub fn prefix_into_pub(&self) -> Prefix {
        Prefix::new(self.net.into_ipaddr(), self.len)
            .unwrap_or_else(|p| panic!("can't convert {:?} into prefix.", p))
    }

    pub fn get_prefix_id(&self) -> PrefixId<AF> {
        PrefixId::new(self.net, self.len)
    }

    pub fn get_meta(&self) -> &M {
        &self.meta
    }
}

impl<M, AF> std::fmt::Display for InternalPrefixRecord<AF, M>
where
    M: Meta,
    AF: AddressFamily,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{} {}",
            AddressFamily::fmt_net(self.net),
            self.len,
            self.meta
        )
    }
}

impl<AF, M> Ord for InternalPrefixRecord<AF, M>
where
    M: Meta,
    AF: AddressFamily,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (self.net >> AF::from_u8(AF::BITS - self.len))
            .cmp(&(other.net >> AF::from_u8((AF::BITS - other.len) % 32)))
    }
}

impl<AF, M> PartialEq for InternalPrefixRecord<AF, M>
where
    M: Meta,
    AF: AddressFamily,
{
    fn eq(&self, other: &Self) -> bool {
        self.net >> AF::from_u8(AF::BITS - self.len)
            == other.net >> AF::from_u8((AF::BITS - other.len) % 32)
    }
}

impl<AF, M> PartialOrd for InternalPrefixRecord<AF, M>
where
    M: Meta,
    AF: AddressFamily,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            (self.net >> AF::from_u8(AF::BITS - self.len)).cmp(
                &(other.net >> AF::from_u8((AF::BITS - other.len) % 32)),
            ),
        )
    }
}

impl<AF, M> Eq for InternalPrefixRecord<AF, M>
where
    M: Meta,
    AF: AddressFamily,
{
}

impl<T, AF> Debug for InternalPrefixRecord<AF, T>
where
    AF: AddressFamily,
    T: Meta,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "{}/{} with {:?}",
            AddressFamily::fmt_net(self.net),
            self.len,
            self.meta
        ))
    }
}

// impl<AF, T> std::hash::Hash for InternalPrefixRecord<AF, T>
// where
//     AF: AddressFamily + PrimInt + Debug,
//     T: Meta,
// {
//     fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
//         self.net.hash(state);
//         self.len.hash(state);
//     }
// }

impl<AF, M> From<InternalPrefixRecord<AF, M>> for PrefixId<AF>
where
    AF: AddressFamily,
    M: Meta,
{
    fn from(record: InternalPrefixRecord<AF, M>) -> Self {
        Self::new(record.net, record.len)
    }
}

impl<AF, T> From<&InternalPrefixRecord<AF, T>> for PrefixId<AF>
where
    AF: AddressFamily,
    T: Meta,
{
    fn from(record: &InternalPrefixRecord<AF, T>) -> Self {
        Self::new(record.net, record.len)
    }
}

impl<M: Meta> From<PublicPrefixSingleRecord<M>>
    for InternalPrefixRecord<crate::IPv4, M>
{
    fn from(record: PublicPrefixSingleRecord<M>) -> Self {
        Self {
            net: if let std::net::IpAddr::V4(ip) = record.prefix.addr() {
                U32::<NetworkEndian>::from(ip.octets())
            } else {
                0.into()
            },
            len: record.prefix.len(),
            meta: record.meta,
        }
    }
}

impl<M: Meta> From<PublicPrefixSingleRecord<M>>
    for InternalPrefixRecord<crate::IPv6, M>
{
    fn from(record: PublicPrefixSingleRecord<M>) -> Self {
        Self {
            net: if let std::net::IpAddr::V6(ip) = record.prefix.addr() {
                U128::<NetworkEndian>::from(ip.octets())
            } else {
                0.into()
            },
            len: record.prefix.len(),
            meta: record.meta,
        }
    }
}

//------------ PublicPrefixSingleRecord --------------------------------------

#[derive(Clone, Debug)]
pub struct PublicPrefixSingleRecord<M: Meta> {
    pub prefix: Prefix,
    pub meta: M,
}

impl<M: Meta> PublicPrefixSingleRecord<M> {
    pub fn new(prefix: Prefix, meta: M) -> Self {
        Self { prefix, meta }
    }

    pub fn new_from_record<AF: AddressFamily>(
        record: InternalPrefixRecord<AF, M>,
    ) -> Self {
        Self {
            prefix: record.prefix_into_pub(),
            meta: record.meta,
        }
    }
}

impl<AF, M> From<(PrefixId<AF>, Arc<M>)> for PublicPrefixSingleRecord<M>
where
    AF: AddressFamily,
    M: Meta,
{
    fn from(record: (PrefixId<AF>, Arc<M>)) -> Self {
        Self {
            prefix: record.0.into(),
            meta: (*record.1).clone(),
        }
    }
}

impl<M: Meta> std::fmt::Display for PublicPrefixSingleRecord<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} :{:?}", self.prefix, self.meta)
    }
}

impl<M: Meta> From<(Prefix, M)> for PublicPrefixSingleRecord<M> {
    fn from((prefix, meta): (Prefix, M)) -> Self {
        Self { prefix, meta }
    }
}

//------------ PublicRecord --------------------------------------------------

#[derive(Clone, Debug)]
pub struct PublicRecord<M> {
    pub multi_uniq_id: u32,
    pub ltime: u64,
    pub status: RouteStatus,
    pub meta: M,
}

impl<M> PublicRecord<M> {
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

impl<M: std::fmt::Display> std::fmt::Display for PublicRecord<M> {
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
pub struct ZeroCopyRecord<AF: AddressFamily> {
    pub prefix: PrefixId<AF>,
    pub multi_uniq_id: u32,
    pub ltime: u64,
    pub status: RouteStatus,
    pub meta: [u8],
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
pub struct ValueHeader {
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
pub struct PublicPrefixRecord<M: Meta> {
    pub prefix: Prefix,
    pub meta: Vec<PublicRecord<M>>,
}

impl<M: Meta> PublicPrefixRecord<M> {
    pub fn new(prefix: Prefix, meta: Vec<PublicRecord<M>>) -> Self {
        Self { prefix, meta }
    }

    pub fn get_record_for_mui(&self, mui: u32) -> Option<&PublicRecord<M>> {
        self.meta.iter().find(|r| r.multi_uniq_id == mui)
    }
}

impl<AF, M> From<(PrefixId<AF>, Vec<PublicRecord<M>>)>
    for PublicPrefixRecord<M>
where
    AF: AddressFamily,
    M: Meta,
{
    fn from(record: (PrefixId<AF>, Vec<PublicRecord<M>>)) -> Self {
        Self {
            prefix: record.0.into(),
            meta: record.1,
        }
    }
}

impl<M: Meta + std::fmt::Display> std::fmt::Display
    for PublicPrefixRecord<M>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: [", self.prefix)?;
        for rec in &self.meta {
            write!(f, "{},", rec)?;
        }
        write!(f, "]")
    }
}

impl<M: Meta> From<(Prefix, Vec<PublicRecord<M>>)> for PublicPrefixRecord<M> {
    fn from((prefix, meta): (Prefix, Vec<PublicRecord<M>>)) -> Self {
        Self { prefix, meta }
    }
}

//------------ RecordSingleSet -----------------------------------------------

#[derive(Clone, Debug)]
pub struct RecordSingleSet<M: Meta> {
    pub v4: Vec<PublicPrefixSingleRecord<M>>,
    pub v6: Vec<PublicPrefixSingleRecord<M>>,
}

impl<M: Meta> RecordSingleSet<M> {
    pub fn new() -> Self {
        Self {
            v4: Default::default(),
            v6: Default::default(),
        }
    }

    pub fn push(&mut self, prefix: Prefix, meta: M) {
        match prefix.addr() {
            std::net::IpAddr::V4(_) => &mut self.v4,
            std::net::IpAddr::V6(_) => &mut self.v6,
        }
        .push(PublicPrefixSingleRecord::new(prefix, meta));
    }

    pub fn is_empty(&self) -> bool {
        self.v4.is_empty() && self.v6.is_empty()
    }

    pub fn iter(&self) -> RecordSetSingleIter<M> {
        RecordSetSingleIter {
            v4: if self.v4.is_empty() {
                None
            } else {
                Some(self.v4.iter())
            },
            v6: self.v6.iter(),
        }
    }

    #[must_use]
    pub fn reverse(mut self) -> RecordSingleSet<M> {
        self.v4.reverse();
        self.v6.reverse();
        self
    }

    pub fn len(&self) -> usize {
        self.v4.len() + self.v6.len()
    }
}

impl<M: Meta> Default for RecordSingleSet<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Meta> fmt::Display for RecordSingleSet<M> {
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

impl<M: Meta>
    From<(
        Vec<PublicPrefixSingleRecord<M>>,
        Vec<PublicPrefixSingleRecord<M>>,
    )> for RecordSingleSet<M>
{
    fn from(
        (v4, v6): (
            Vec<PublicPrefixSingleRecord<M>>,
            Vec<PublicPrefixSingleRecord<M>>,
        ),
    ) -> Self {
        Self { v4, v6 }
    }
}

impl<M: Meta> std::iter::FromIterator<Arc<PublicPrefixSingleRecord<M>>>
    for RecordSingleSet<M>
{
    fn from_iter<I: IntoIterator<Item = Arc<PublicPrefixSingleRecord<M>>>>(
        iter: I,
    ) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = pfx.prefix;
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(PublicPrefixSingleRecord::new(
                        u_pfx,
                        pfx.meta.clone(),
                    ));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PublicPrefixSingleRecord::new(
                        u_pfx,
                        pfx.meta.clone(),
                    ));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<AF: AddressFamily, M: Meta>
    std::iter::FromIterator<(PrefixId<AF>, Arc<M>)> for RecordSingleSet<M>
{
    fn from_iter<I: IntoIterator<Item = (PrefixId<AF>, Arc<M>)>>(
        iter: I,
    ) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = Prefix::from(pfx.0);
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(PublicPrefixSingleRecord::new(
                        u_pfx,
                        (*pfx.1).clone(),
                    ));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PublicPrefixSingleRecord::new(
                        u_pfx,
                        (*pfx.1).clone(),
                    ));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<'a, AF: AddressFamily, M: Meta + 'a>
    std::iter::FromIterator<&'a InternalPrefixRecord<AF, M>>
    for RecordSingleSet<M>
{
    fn from_iter<I: IntoIterator<Item = &'a InternalPrefixRecord<AF, M>>>(
        iter: I,
    ) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = (*pfx).prefix_into_pub();
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(PublicPrefixSingleRecord::new(
                        u_pfx,
                        pfx.meta.clone(),
                    ));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PublicPrefixSingleRecord::new(
                        u_pfx,
                        pfx.meta.clone(),
                    ));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<M: Meta> std::ops::Index<usize> for RecordSingleSet<M> {
    type Output = PublicPrefixSingleRecord<M>;

    fn index(&self, index: usize) -> &Self::Output {
        if index < self.v4.len() {
            &self.v4[index]
        } else {
            &self.v6[index - self.v4.len()]
        }
    }
}

//------------ RecordSet -----------------------------------------------------

#[derive(Clone, Debug)]
pub struct RecordSet<M: Meta> {
    pub v4: Vec<PublicPrefixRecord<M>>,
    pub v6: Vec<PublicPrefixRecord<M>>,
}

impl<M: Meta> RecordSet<M> {
    pub fn new() -> Self {
        Self {
            v4: Default::default(),
            v6: Default::default(),
        }
    }

    pub fn push(&mut self, prefix: Prefix, meta: Vec<PublicRecord<M>>) {
        match prefix.addr() {
            std::net::IpAddr::V4(_) => &mut self.v4,
            std::net::IpAddr::V6(_) => &mut self.v6,
        }
        .push(PublicPrefixRecord::new(prefix, meta));
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

impl<M: Meta> From<(Vec<PublicPrefixRecord<M>>, Vec<PublicPrefixRecord<M>>)>
    for RecordSet<M>
{
    fn from(
        (v4, v6): (Vec<PublicPrefixRecord<M>>, Vec<PublicPrefixRecord<M>>),
    ) -> Self {
        Self { v4, v6 }
    }
}

impl<M: Meta> std::iter::FromIterator<PublicPrefixRecord<M>>
    for RecordSet<M>
{
    fn from_iter<I: IntoIterator<Item = PublicPrefixRecord<M>>>(
        iter: I,
    ) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = pfx.prefix;
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(PublicPrefixRecord::new(u_pfx, pfx.meta));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PublicPrefixRecord::new(u_pfx, pfx.meta));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<AF: AddressFamily, M: Meta>
    std::iter::FromIterator<(PrefixId<AF>, Vec<PublicRecord<M>>)>
    for RecordSet<M>
{
    fn from_iter<
        I: IntoIterator<Item = (PrefixId<AF>, Vec<PublicRecord<M>>)>,
    >(
        iter: I,
    ) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = Prefix::from(pfx.0);
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(PublicPrefixRecord::new(u_pfx, pfx.1));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PublicPrefixRecord::new(u_pfx, pfx.1));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<'a, M: Meta + 'a> std::iter::FromIterator<&'a PublicPrefixRecord<M>>
    for RecordSet<M>
{
    fn from_iter<I: IntoIterator<Item = &'a PublicPrefixRecord<M>>>(
        iter: I,
    ) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = pfx.prefix;
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(PublicPrefixRecord::new(u_pfx, pfx.meta.clone()));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PublicPrefixRecord::new(u_pfx, pfx.meta.clone()));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<M: Meta> std::ops::Index<usize> for RecordSet<M> {
    type Output = PublicPrefixRecord<M>;

    fn index(&self, index: usize) -> &Self::Output {
        if index < self.v4.len() {
            &self.v4[index]
        } else {
            &self.v6[index - self.v4.len()]
        }
    }
}

//------------ RecordSetSingleIter -------------------------------------------

#[derive(Clone, Debug)]
pub struct RecordSetSingleIter<'a, M: Meta> {
    v4: Option<std::slice::Iter<'a, PublicPrefixSingleRecord<M>>>,
    v6: std::slice::Iter<'a, PublicPrefixSingleRecord<M>>,
}

impl<M: Meta> Iterator for RecordSetSingleIter<'_, M> {
    type Item = PublicPrefixSingleRecord<M>;

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

//------------ RecordSetIter -------------------------------------------------

#[derive(Clone, Debug)]
pub struct RecordSetIter<'a, M: Meta> {
    v4: Option<std::slice::Iter<'a, PublicPrefixRecord<M>>>,
    v6: std::slice::Iter<'a, PublicPrefixRecord<M>>,
}

impl<M: Meta> Iterator for RecordSetIter<'_, M> {
    type Item = PublicPrefixRecord<M>;

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

//----------------------- meta-data traits/types-----------------------------

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
