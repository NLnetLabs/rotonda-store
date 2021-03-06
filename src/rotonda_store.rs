use std::collections::HashMap;
use std::{fmt, slice};

use crate::{prefix_record::InternalPrefixRecord, stats::StrideStats};

use routecore::bgp::MetaDataSet;
use routecore::{
    addr::Prefix,
    bgp::{PrefixRecord, RecordSet},
    record::{MergeUpdate, Record},
};

pub use crate::af::{AddressFamily, IPv4, IPv6};

pub use crate::local_array::store::custom_alloc;

pub const RECORDS_MAX_NUM: usize = 3;

//------------ The publicly available Rotonda Stores ------------------------

pub use crate::local_array::store::DefaultStore as MultiThreadedStore;
pub use crate::local_vec::store::Store as SingleThreadedStore;

//------------ Types for strides displaying/monitoring ----------------------

type AfStrideStats = Vec<StrideStats>;

pub struct Stats<'a> {
    pub v4: &'a AfStrideStats,
    pub v6: &'a AfStrideStats,
}

impl<'a> std::fmt::Display for Stats<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "v4 ")?;
        for s in self.v4.iter() {
            writeln!(f, "{} ", s)?;
        }
        writeln!(f, "v6 ")?;
        for s in self.v6.iter() {
            writeln!(f, "{} ", s)?;
        }
        Ok(())
    }
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

//------------ MatchOptions / MatchType -------------------------------------

pub struct MatchOptions {
    pub match_type: MatchType,
    pub include_all_records: bool,
    pub include_less_specifics: bool,
    pub include_more_specifics: bool,
}

#[derive(Debug, Clone)]
pub enum MatchType {
    ExactMatch,
    LongestMatch,
    EmptyMatch,
}

impl MatchType {
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::EmptyMatch)
    }
}

impl std::fmt::Display for MatchType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MatchType::ExactMatch => write!(f, "exact-match"),
            MatchType::LongestMatch => write!(f, "longest-match"),
            MatchType::EmptyMatch => write!(f, "empty-match"),
        }
    }
}

//------------ Metadata Types -----------------------------------------------

#[derive(Debug, Copy, Clone)]
pub struct PrefixAs(pub u32);

impl MergeUpdate for PrefixAs {
    fn merge_update(
        &mut self,
        update_record: PrefixAs,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.0 = update_record.0;
        Ok(())
    }

    fn clone_merge_update(
        &self,
        update_meta: &Self,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: std::marker::Sized,
    {
        Ok(PrefixAs(update_meta.0))
    }
}

impl fmt::Display for PrefixAs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AS{}", self.0)
    }
}

impl<'a, AF: 'a + AddressFamily, Meta: routecore::record::Meta>
    std::iter::FromIterator<&'a InternalPrefixRecord<AF, Meta>>
    for RecordSet<'a, Meta>
{
    fn from_iter<
        I: IntoIterator<Item = &'a InternalPrefixRecord<AF, Meta>>,
    >(
        iter: I,
    ) -> Self {
        let mut v4 = vec![];
        let mut v6 = vec![];
        for pfx in iter {
            let u_pfx = Prefix::new(pfx.net.into_ipaddr(), pfx.len).unwrap();
            match u_pfx.addr() {
                std::net::IpAddr::V4(_) => {
                    v4.push(PrefixRecord::new(u_pfx, &pfx.meta));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PrefixRecord::new(u_pfx, &pfx.meta));
                }
            }
        }
        Self { v4, v6 }
    }
}

// Hash implementation that always returns the same hash, so that all
// records get thrown on one big heap.
impl std::hash::Hash for PrefixAs {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        0.hash(state);
    }
}

//------------ PrefixRecordIter ---------------------------------------------

// Converts from the InternalPrefixRecord to the (public) PrefixRecord
// while iterating.
#[derive(Clone, Debug)]
pub struct PrefixRecordIter<'a, Meta: routecore::record::Meta> {
    pub(crate) v4: Option<slice::Iter<'a, InternalPrefixRecord<IPv4, Meta>>>,
    pub(crate) v6: slice::Iter<'a, InternalPrefixRecord<IPv6, Meta>>,
}

impl<'a, Meta: routecore::record::Meta> Iterator
    for PrefixRecordIter<'a, Meta>
{
    type Item = PrefixRecord<'a, Meta>;

    fn next(&mut self) -> Option<Self::Item> {
        // V4 is already done.
        if self.v4.is_none() {
            return self.v6.next().map(|res| {
                PrefixRecord::new(
                    Prefix::new(res.net.into_ipaddr(), res.len).unwrap(),
                    &res.meta,
                )
            });
        }

        if let Some(res) = self.v4.as_mut().and_then(|v4| v4.next()) {
            return Some(PrefixRecord::new(
                Prefix::new(res.net.into_ipaddr(), res.len).unwrap(),
                &res.meta,
            ));
        }
        self.v4 = None;
        self.next()
    }
}

//------------- PrefixRecordMap ---------------------------------------------

// A HashMap that's keyed on prefix and contains multiple meta-data instances

#[derive(Debug, Clone)]
pub struct PrefixRecordMap<'a, M: routecore::record::Meta>(
    HashMap<Prefix, MetaDataSet<'a, M>>,
);

impl<'a, M: routecore::record::Meta> PrefixRecordMap<'a, M> {
    pub fn new(prefix: Prefix, meta_data_set: MetaDataSet<'a, M>) -> Self {
        let mut map = HashMap::new();
        map.insert(prefix, meta_data_set);
        Self(map)
    }

    pub fn empty() -> Self {
        Self(HashMap::new())
    }

    // pub fn into_prefix_record_map<AF: AddressFamily + 'a>(
    //     iter: impl Iterator<Item = &'a StoredPrefix<AF, M>>,
    //     guard: &'a Guard,
    // ) -> Self {
    //     let mut map = HashMap::new();
    //     for rec in iter {
    //         map.entry(rec.prefix.into_pub()).or_insert_with(|| {
    //             rec.iter_latest_unique_meta_data(guard)
    //                 .collect::<MetaDataSet<'a, M>>()
    //         });
    //     }
    //     Self(map)
    // }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&Prefix, &MetaDataSet<'a, M>)> {
        self.0.iter()
    }
}

impl<'a, M: routecore::record::Meta>
    std::iter::FromIterator<(Prefix, MetaDataSet<'a, M>)>
    for PrefixRecordMap<'a, M>
{
    fn from_iter<I: IntoIterator<Item = (Prefix, MetaDataSet<'a, M>)>>(
        iter: I,
    ) -> Self {
        let mut map = PrefixRecordMap::empty();
        for (prefix, meta_data_set) in iter {
            map.0.entry(prefix).or_insert(meta_data_set);
        }
        map
    }
}

impl<'a, M: routecore::record::Meta> std::fmt::Display
    for PrefixRecordMap<'a, M>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (pfx, meta) in self.0.iter() {
            writeln!(f, "{} {}", pfx, meta)?;
        }
        Ok(())
    }
}

//------------- QueryResult -------------------------------------------------

#[derive(Clone, Debug)]
pub struct QueryResult<'a, M: routecore::record::Meta> {
    pub match_type: MatchType,
    pub prefix: Option<Prefix>,
    pub prefix_meta: Option<&'a M>,
    pub less_specifics: Option<RecordSet<'a, M>>,
    pub more_specifics: Option<RecordSet<'a, M>>,
}

impl<'a, M: routecore::record::Meta> fmt::Display for QueryResult<'a, M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let pfx_str = match self.prefix {
            Some(pfx) => format!("{}", pfx),
            None => "".to_string(),
        };
        let pfx_meta_str = match &self.prefix_meta {
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
