use std::{fmt, slice};

use crate::{MatchType, PrefixInfoUnit, stats::StrideStats, AddressFamily};
use routecore::{prefix::Prefix, record::{Record, SinglePrefixRoute}};

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
pub struct RecordSet<'a, Meta: routecore::record::Meta> {
    pub v4: Vec<SinglePrefixRoute<'a, Meta>>,
    pub v6: Vec<SinglePrefixRoute<'a, Meta>>,
}

impl<'a, Meta: routecore::record::Meta> RecordSet<'a, Meta> {
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

impl<'a, Meta: routecore::record::Meta> fmt::Display for RecordSet<'a, Meta> {
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

impl<'a, Meta: routecore::record::Meta> std::iter::FromIterator<&'a SinglePrefixRoute<'a, Meta>>
    for RecordSet<'a, Meta>
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

impl<'a, Meta: routecore::record::Meta> std::ops::Index<usize> for RecordSet<'a, Meta> {
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
pub struct QueryResult<'a, Meta: routecore::record::Meta> {
    pub match_type: MatchType,
    pub prefix: Option<Prefix>,
    pub prefix_meta: Option<&'a Meta>,
    pub less_specifics: Option<RecordSet<'a, Meta>>,
    pub more_specifics: Option<RecordSet<'a, Meta>>,
}

impl<'a, Meta: routecore::record::Meta> fmt::Display for QueryResult<'a, Meta> {
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