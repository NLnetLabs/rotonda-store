use crate::local_array::storage_backend::{InMemStorage, StorageBackend};
use crate::local_array::tree::TreeBitMap;
use crate::stats::StrideStats;
use crate::{AddressFamily, IPv4, IPv6, MatchOptions, MatchType, PrefixInfoUnit};
use routecore::prefix::Prefix;
use routecore::record::{MergeUpdate, NoMeta, Record, SinglePrefixRoute};

use std::{fmt, slice};

pub struct Store<Meta: routecore::record::Meta + MergeUpdate> {
    pub v4: TreeBitMap<InMemStorage<IPv4, Meta>>,
    pub v6: TreeBitMap<InMemStorage<IPv6, Meta>>,
}

impl<Meta: routecore::record::Meta + MergeUpdate> Default for Store<Meta> {
    fn default() -> Self {
        Self::new(vec![3, 3, 3, 3, 3, 3, 3, 3, 4, 4], vec![8])
    }
}

impl<Meta: routecore::record::Meta + MergeUpdate> Store<Meta> {
    pub fn new(v4_strides: Vec<u8>, v6_strides: Vec<u8>) -> Self {
        Store {
            v4: TreeBitMap::new(v4_strides),
            v6: TreeBitMap::new(v6_strides),
        }
    }
}

impl<'a, Meta: routecore::record::Meta + MergeUpdate + Copy> Store<Meta> {
    pub fn match_prefix(
        &'a self,
        search_pfx: &Prefix,
        options: &MatchOptions,
    ) -> QueryResult<'a, Meta> {
        match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => self.v4.match_prefix(
                &PrefixInfoUnit::<IPv4, NoMeta>::new(addr.into(), search_pfx.len()),
                options,
            ),
            std::net::IpAddr::V6(addr) => self.v6.match_prefix(
                &PrefixInfoUnit::<IPv6, NoMeta>::new(addr.into(), search_pfx.len()),
                options,
            ),
        }
    }

    pub fn insert(
        &mut self,
        prefix: &Prefix,
        meta: Meta,
    ) -> Result<(), std::boxed::Box<dyn std::error::Error>> {
        match prefix.addr() {
            std::net::IpAddr::V4(addr) => self.v4.insert(PrefixInfoUnit::new_with_meta(
                addr.into(),
                prefix.len(),
                meta,
            )),
            std::net::IpAddr::V6(addr) => self.v6.insert(PrefixInfoUnit::new_with_meta(
                addr.into(),
                prefix.len(),
                meta,
            )),
        }
    }

    pub fn prefixes(&'a self) -> RecordSet<'a, Meta> {
        let rs4 = self
            .v4
            .store
            .prefixes
            .iter()
            .collect::<RecordSet<'a, Meta>>();
        let rs6 = self
            .v6
            .store
            .prefixes
            .iter()
            .collect::<RecordSet<'a, Meta>>();

        RecordSet::<'a, Meta> {
            v4: rs4.v4,
            v6: rs6.v6,
        }
    }

    pub fn prefixes_len(&self) -> usize {
        self.v4.store.prefixes.len() + self.v6.store.prefixes.len()
    }

    pub fn nodes_len(&self) -> usize {
        self.v4.store.get_nodes_len() + self.v6.store.get_nodes_len()
    }

    pub fn nodes_per_stride(&'a self) -> (Vec<u32>, Vec<u32>) {
        let mut stride_levels_v4 = Vec::from([]);
        let mut stride_levels_v6 = Vec::from([]);
        for stride in self.strides().v4.iter().enumerate() {
            stride_levels_v4.push(
                self.v4
                    .stats
                    .iter()
                    .find(|s| s.stride_len == *stride.1)
                    .unwrap()
                    .created_nodes[stride.0]
                    .count as u32,
            );
        }
        for stride in self.strides().v6.iter().enumerate() {
            stride_levels_v6.push(
                self.v6
                    .stats
                    .iter()
                    .find(|s| s.stride_len == *stride.1)
                    .unwrap()
                    .created_nodes[stride.0]
                    .count as u32,
            );
        }
        (stride_levels_v4, stride_levels_v6)
    }

    pub fn stats(&self) -> Stats {
        Stats {
            v4: &self.v4.stats,
            v6: &self.v6.stats,
        }
    }

    pub fn strides(&'a self) -> Strides {
        Strides {
            v4: &self.v4.strides,
            v6: &self.v6.strides,
        }
    }
}

pub(crate) type AfStrideStats = Vec<StrideStats>;

pub struct Stats<'a> {
    pub v4: &'a AfStrideStats,
    pub v6: &'a AfStrideStats,
}

pub struct Strides<'a> {
    pub v4: &'a Vec<u8>,
    pub v6: &'a Vec<u8>,
}

impl<'a> std::fmt::Debug for Strides<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v4 ")?;
        for s in self.v4.iter() {
            write!(f, "{} ", s)?;
        }
        writeln!(f, "v5 ")?;
        for s in self.v6.iter() {
            write!(f, "{} ", s)?;
        }
        Ok(())
    }
}

//------------ RecordSet -----------------------------------------------------

#[derive(Clone, Debug)]
pub struct RecordSet<'a, Meta: routecore::record::Meta + Copy> {
    v4: Vec<SinglePrefixRoute<'a, Meta>>,
    v6: Vec<SinglePrefixRoute<'a, Meta>>,
}

impl<'a, Meta: routecore::record::Meta + Copy> RecordSet<'a, Meta> {
    pub fn is_empty(&self) -> bool {
        self.v4.is_empty() && self.v6.is_empty()
    }

    pub fn iter(&self) -> RecordSetIter<Meta> {
        RecordSetIter {
            v4: if self.v4.is_empty() {
                None
            } else {
                Some(self.v4.iter())
            },
            v6: self.v6.iter(),
        }
    }

    pub fn reverse(mut self) -> RecordSet<'a, Meta> {
        self.v4.reverse();
        self.v6.reverse();
        self
    }

    pub fn len(&self) -> usize {
        self.v4.len() + self.v6.len()
    }
}

impl<'a, Meta: routecore::record::Meta + Copy> fmt::Display for RecordSet<'a, Meta> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let arr_str_v4 = self.v4.iter().fold("".to_string(), |pfx_arr, pfx| {
            format!("{} {}", pfx_arr, *pfx)
        });
        let arr_str_v6 = self.v6.iter().fold("".to_string(), |pfx_arr, pfx| {
            format!("{} {}", pfx_arr, *pfx)
        });

        write!(f, "V4: [{}], V6: [{}]", arr_str_v4, arr_str_v6)
    }
}

impl<'a, AF: 'a + AddressFamily, Meta: routecore::record::Meta + Copy>
    std::iter::FromIterator<&'a PrefixInfoUnit<AF, Meta>> for RecordSet<'a, Meta>
{
    fn from_iter<I: IntoIterator<Item = &'a PrefixInfoUnit<AF, Meta>>>(iter: I) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = Prefix::new(pfx.net.into_ipaddr(), pfx.len).unwrap();
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(SinglePrefixRoute::new(u_pfx, pfx.meta.as_ref().unwrap()));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(SinglePrefixRoute::new(u_pfx, pfx.meta.as_ref().unwrap()));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<'a, Meta: routecore::record::Meta + Copy>
    std::iter::FromIterator<&'a SinglePrefixRoute<'a, Meta>> for RecordSet<'a, Meta>
{
    fn from_iter<I: IntoIterator<Item = &'a SinglePrefixRoute<'a, Meta>>>(iter: I) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = pfx.prefix;
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(SinglePrefixRoute::new(u_pfx, pfx.meta.as_ref()));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(SinglePrefixRoute::new(u_pfx, pfx.meta.as_ref()));
                }
            }
        }
        Self { v4, v6 }
    }
}

impl<'a, Meta: routecore::record::Meta + Copy> std::ops::Index<usize> for RecordSet<'a, Meta> {
    type Output = SinglePrefixRoute<'a, Meta>;

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
pub struct RecordSetIter<'a, Meta: routecore::record::Meta> {
    v4: Option<slice::Iter<'a, SinglePrefixRoute<'a, Meta>>>,
    v6: slice::Iter<'a, SinglePrefixRoute<'a, Meta>>,
}

impl<'a, Meta: routecore::record::Meta> Iterator for RecordSetIter<'a, Meta> {
    type Item = SinglePrefixRoute<'a, Meta>;

    fn next(&mut self) -> Option<Self::Item> {
        // V4 is already done.
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

//------------- QueryResult ---------------------------------------------------

#[derive(Clone, Debug)]
pub struct QueryResult<'a, Meta: routecore::record::Meta + Copy> {
    pub match_type: MatchType,
    pub prefix: Option<Prefix>,
    pub prefix_meta: Option<&'a Meta>,
    pub less_specifics: Option<RecordSet<'a, Meta>>,
    pub more_specifics: Option<RecordSet<'a, Meta>>,
}

impl<Meta: routecore::record::Meta + MergeUpdate> fmt::Display for InMemStorage<IPv4, Meta> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InMemStorage<u32, {}>", std::any::type_name::<Meta>())
    }
}

impl<Meta: routecore::record::Meta + MergeUpdate> fmt::Display for InMemStorage<IPv6, Meta> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InMemStorage<u128, {}>", std::any::type_name::<Meta>())
    }
}

impl<'a, Meta: routecore::record::Meta + Copy> fmt::Display for QueryResult<'a, Meta> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let pfx_str = match self.prefix {
            Some(pfx) => format!("{}", pfx),
            None => "".to_string(),
        };
        let pfx_meta_str = match self.prefix_meta {
            Some(pfx_meta) => format!("{}", pfx_meta),
            None => "".to_string(),
        };
        write!(
            f,
            "match_type: {}\nprefix: {}\nmetadata: {}\nless_specifics: {}\nmore_specifics: {}",
            self.match_type,
            pfx_str,
            pfx_meta_str,
            if let Some(ls) = self.less_specifics.as_ref() {
                format!("{}", ls)
            } else {
                "".to_string()
            },
            if let Some(ms) = self.more_specifics.as_ref() {
                format!("{}", ms)
            } else {
                "".to_string()
            },
        )
    }
}
