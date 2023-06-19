use std::{fmt, slice};

use crate::prefix_record::{PublicPrefixRecord, Meta, RecordSet};
use crate::{prefix_record::InternalPrefixRecord, stats::StrideStats};

use routecore::addr::Prefix;

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

/// Options for the `match_prefix` method
/// 
/// The `MatchOptions` struct is used to specify the options for the 
/// `match_prefix` method on the store.
/// 
/// Note that the `match_type` field may be different from the actual
/// `MatchType` returned from the result. 
/// 
/// See [MultiThreadedStore::match_prefix] for more details.
#[derive(Debug, Clone)]
pub struct MatchOptions {
    /// The requested [MatchType]
    pub match_type: MatchType,
    /// Unused
    pub include_all_records: bool,
    /// Whether to include all less-specific records in the query result
    pub include_less_specifics: bool,
    // Whether to include all more-specific records in the query result
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


//------------ PrefixRecordIter ---------------------------------------------

// Converts from the InternalPrefixRecord to the (public) PrefixRecord
// while iterating.
#[derive(Clone, Debug)]
pub struct PrefixRecordIter<'a, M: Meta> {
    pub(crate) v4: Option<slice::Iter<'a, InternalPrefixRecord<IPv4, M>>>,
    pub(crate) v6: slice::Iter<'a, InternalPrefixRecord<IPv6, M>>,
}

impl<'a, M: Meta> Iterator
    for PrefixRecordIter<'a, M>
{
    type Item = PublicPrefixRecord<M>;

    fn next(&mut self) -> Option<Self::Item> {
        // V4 is already done.
        if self.v4.is_none() {
            return self.v6.next().map(|res| {
                PublicPrefixRecord::new(
                    Prefix::new(res.net.into_ipaddr(), res.len).unwrap(),
                    res.meta.clone(),
                )
            });
        }

        if let Some(res) = self.v4.as_mut().and_then(|v4| v4.next()) {
            return Some(PublicPrefixRecord::new(
                Prefix::new(res.net.into_ipaddr(), res.len).unwrap(),
                res.meta.clone(),
            ));
        }
        self.v4 = None;
        self.next()
    }
}


//------------- QueryResult -------------------------------------------------

/// The type that is returned by a query.
/// 
/// This is the result type of a query. It contains the prefix record that was
/// found in the store, as well as less- or more-specifics as requested.
/// 
/// See [MultiThreadedStore::match_prefix] for more details.


#[derive(Clone, Debug)]
pub struct QueryResult<M: crate::prefix_record::Meta> {
    /// The match type of the resulting prefix
    pub match_type: MatchType,
    /// The resulting prefix record
    pub prefix: Option<Prefix>,
    /// The meta data associated with the resulting prefix record
    pub prefix_meta: Option<M>,
    /// The less-specifics of the resulting prefix together with their meta data
    pub less_specifics: Option<RecordSet<M>>,
    /// The more-specifics of the resulting prefix together with their meta data
    pub more_specifics: Option<RecordSet<M>>,
}

impl<M: Meta> fmt::Display for QueryResult<M> {
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
