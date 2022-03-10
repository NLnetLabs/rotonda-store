use std::{fmt, slice};

use crate::{
    prefix_record::InternalPrefixRecord,
    stats::StrideStats,
};

use routecore::{
    addr::Prefix,
    bgp::{PrefixRecord, RecordSet},
    record::{MergeUpdate, Record},
};

pub use crate::af::{AddressFamily, IPv4, IPv6};

pub use crate::local_array::store::custom_alloc;

//------------ The publicly available Rotonda Stores ------------------------

pub use crate::local_array::store::Store as MultiThreadedStore;
pub use crate::local_vec::store::Store as SingleThreadedStore;

use self::custom_alloc::{PrefixBuckets, PrefixesLengthsIter};

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
    pub include_less_specifics: bool,
    pub include_more_specifics: bool,
}

#[derive(Debug, Clone)]
pub enum MatchType {
    ExactMatch,
    LongestMatch,
    EmptyMatch,
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
        Ok(PrefixAs(self.0.max(update_meta.0)))
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
                    v4.push(PrefixRecord::new(
                        u_pfx,
                        pfx.meta.as_ref().unwrap(),
                    ));
                }
                std::net::IpAddr::V6(_) => {
                    v6.push(PrefixRecord::new(
                        u_pfx,
                        pfx.meta.as_ref().unwrap(),
                    ));
                }
            }
        }
        Self { v4, v6 }
    }
}

//------------ HashMapPrefixRecordIterator ----------------------------------
pub struct CustomAllocPrefixRecordIterator<
    'a,
    Meta: routecore::record::Meta,
    PB4: PrefixBuckets<IPv4, Meta> + Sized,
    PB6: PrefixBuckets<IPv6, Meta> + Sized
> {
    pub v4: Option<PrefixesLengthsIter<'a, IPv4, Meta, PB4>>,
    pub v6: PrefixesLengthsIter<'a, IPv6, Meta, PB6>,
}

impl<
        'a,
        Meta: routecore::record::Meta + 'a,
        PB4: PrefixBuckets<IPv4, Meta>,
        PB6: PrefixBuckets<IPv6, Meta>,
    > Iterator for CustomAllocPrefixRecordIterator<'a, Meta, PB4, PB6>
{
    type Item = PrefixRecord<'a, Meta>;

    fn next(&mut self) -> Option<Self::Item> {
        // V4 is already done.
        if self.v4.is_none() {
            return self.v6.next().map(|res| {
                PrefixRecord::new_with_local_meta(
                    Prefix::new(res.net.into_ipaddr(), res.len).unwrap(),
                    res.meta.clone().unwrap(),
                )
            });
        }

        if let Some(res) = self.v4.as_mut().and_then(|v4| v4.next()) {
            return Some(PrefixRecord::new_with_local_meta(
                Prefix::new(res.net.into_ipaddr(), res.len).unwrap(),
                res.meta.clone().unwrap(),
            ));
        }
        self.v4 = None;
        self.next()
    }
}

// impl<'a, Meta: routecore::record::Meta + 'a> std::fmt::Display
//     for HashMapPrefixRecordIterator<'a, Meta>
// {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         write!(f, "some kind of prefixes iterator")
//     }
// }

// impl<'a, Meta: routecore::record::Meta + 'a> std::iter::FromIterator<routecore::bgp::PrefixRecord<'a, Meta>> for HashMapPrefixRecordIterator<'a, Meta> {
//     fn from_iter<
//         I: IntoIterator<Item = routecore::bgp::PrefixRecord<'a, Meta>>,
//     >(
//         iter: I,
//     ) -> Self {

//     }
// }

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
                    res.meta.as_ref().unwrap(),
                )
            });
        }

        if let Some(res) = self.v4.as_mut().and_then(|v4| v4.next()) {
            return Some(PrefixRecord::new(
                Prefix::new(res.net.into_ipaddr(), res.len).unwrap(),
                res.meta.as_ref().unwrap(),
            ));
        }
        self.v4 = None;
        self.next()
    }
}

// impl<'a, Meta: routecore::record::Meta> DoubleEndedIterator
//     for PrefixRecordIter<'a, Meta>
// {
//     fn next_back(&mut self) -> Option<Self::Item> {
//         // V4 is already done.
//         if self.v4.is_none() {
//             return self.v6.next_back().map(|res| {
//                 PrefixRecord::new(
//                     Prefix::new(res.net.into_ipaddr(), res.len).unwrap(),
//                     res.meta.as_ref().unwrap(),
//                 )
//             });
//         }

//         if let Some(res) = self.v4.as_mut().and_then(|v4| v4.next_back()) {
//             return Some(PrefixRecord::new(
//                 Prefix::new(res.net.into_ipaddr(), res.len).unwrap(),
//                 res.meta.as_ref().unwrap(),
//             ));
//         }
//         self.v4 = None;
//         self.next_back()
//     }
// }

//------------- QueryResult -------------------------------------------------

#[derive(Clone, Debug)]
pub struct QueryResult<'a, Meta: routecore::record::Meta> {
    pub match_type: MatchType,
    pub prefix: Option<Prefix>,
    pub prefix_meta: Option<&'a Meta>,
    pub less_specifics: Option<RecordSet<'a, Meta>>,
    pub more_specifics: Option<RecordSet<'a, Meta>>,
}

impl<'a, Meta: routecore::record::Meta> fmt::Display
    for QueryResult<'a, Meta>
{
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
