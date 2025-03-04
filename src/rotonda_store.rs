use std::{fmt, slice};

use crate::prefix_record::InternalPrefixRecord;
pub use crate::prefix_record::{
    Meta, PublicPrefixSingleRecord, RecordSingleSet,
};
use crate::prefix_record::{PublicRecord, RecordSet};

use inetnum::addr::Prefix;

pub use crate::af::{AddressFamily, IPv4, IPv6};

pub use crate::local_array::rib::rib;

pub const RECORDS_MAX_NUM: usize = 3;

//------------ The publicly available Rotonda Stores ------------------------

pub use crate::local_array::rib::DefaultStore as MultiThreadedStore;
// pub use crate::local_vec::store::Store as SingleThreadedStore;

//------------ Types for strides displaying/monitoring ----------------------

// type AfStrideStats<AF> = Vec<StrideStats<AF>>;

// pub struct Stats<'a> {
//     pub v4: &'a AfStrideStats<IPv4>,
//     pub v6: &'a AfStrideStats<IPv6>,
// }

// impl std::fmt::Display for Stats<'_> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         writeln!(f, "v4 ")?;
//         for s in self.v4.iter() {
//             writeln!(f, "{} ", s)?;
//         }
//         writeln!(f, "v6 ")?;
//         for s in self.v6.iter() {
//             writeln!(f, "{} ", s)?;
//         }
//         Ok(())
//     }
// }

// pub struct Strides<'a> {
//     pub v4: &'a Vec<u8>,
//     pub v6: &'a Vec<u8>,
// }

// impl std::fmt::Debug for Strides<'_> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "v4 ")?;
//         for s in self.v4.iter() {
//             write!(f, "{} ", s)?;
//         }
//         writeln!(f, "v5 ")?;
//         for s in self.v6.iter() {
//             write!(f, "{} ", s)?;
//         }
//         Ok(())
//     }
// }

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
    pub include_withdrawn: bool,
    /// Whether to include all less-specific records in the query result
    pub include_less_specifics: bool,
    // Whether to include all more-specific records in the query result
    pub include_more_specifics: bool,
    /// Whether to return records for a specific multi_uniq_id, None indicates
    /// all records.
    pub mui: Option<u32>,
    /// Whether to include historical records, i.e. records that have been
    /// superceded by updates. `SearchPrefix` means only historical records
    /// for the search prefix will be included (if present), `All` means
    /// all retrieved prefixes, i.e. next to the search prefix, also the
    /// historical records for less and more specific prefixes will be
    /// included.
    pub include_history: IncludeHistory,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IncludeHistory {
    None,
    SearchPrefix,
    All,
}

//------------ PrefixRecordIter ---------------------------------------------

// Converts from the InternalPrefixRecord to the (public) PrefixRecord
// while iterating.
#[derive(Clone, Debug)]
pub struct PrefixSingleRecordIter<'a, M: Meta> {
    pub(crate) v4: Option<slice::Iter<'a, InternalPrefixRecord<IPv4, M>>>,
    pub(crate) v6: slice::Iter<'a, InternalPrefixRecord<IPv6, M>>,
}

impl<M: Meta> Iterator for PrefixSingleRecordIter<'_, M> {
    type Item = PublicPrefixSingleRecord<M>;

    fn next(&mut self) -> Option<Self::Item> {
        // V4 is already done.
        if self.v4.is_none() {
            return self.v6.next().map(|res| {
                PublicPrefixSingleRecord::new(
                    Prefix::new(res.net.into_ipaddr(), res.len).unwrap(),
                    res.meta.clone(),
                )
            });
        }

        if let Some(res) = self.v4.as_mut().and_then(|v4| v4.next()) {
            return Some(PublicPrefixSingleRecord::new(
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
pub struct QueryResult<M: Meta> {
    /// The match type of the resulting prefix
    pub match_type: MatchType,
    /// The resulting prefix record
    pub prefix: Option<Prefix>,
    /// The meta data associated with the resulting prefix record
    pub prefix_meta: Vec<PublicRecord<M>>,
    /// The less-specifics of the resulting prefix together with their meta
    /// data
    pub less_specifics: Option<RecordSet<M>>,
    /// The more-specifics of the resulting prefix together with their meta
    //// data
    pub more_specifics: Option<RecordSet<M>>,
}

impl<M: Meta> QueryResult<M> {
    pub fn empty() -> Self {
        QueryResult {
            match_type: MatchType::EmptyMatch,
            prefix: None,
            prefix_meta: vec![],
            less_specifics: None,
            more_specifics: None,
        }
    }
}

impl<M: Meta> fmt::Display for QueryResult<M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let pfx_str = match self.prefix {
            Some(pfx) => format!("{}", pfx),
            None => "".to_string(),
        };
        // let pfx_meta_str = match &self.prefix_meta {
        //     Some(pfx_meta) => format!("{}", pfx_meta),
        //     None => "".to_string(),
        // };
        writeln!(f, "match_type: {}", self.match_type)?;
        writeln!(f, "prefix: {}", pfx_str)?;
        write!(f, "meta: [ ")?;
        for rec in &self.prefix_meta {
            write!(f, "{},", rec)?;
        }
        writeln!(f, " ]")?;
        writeln!(
            f,
            "less_specifics: {{ {} }}",
            if let Some(ls) = self.less_specifics.as_ref() {
                format!("{}", ls)
            } else {
                "".to_string()
            }
        )?;
        writeln!(
            f,
            "more_specifics: {{ {} }}",
            if let Some(ms) = self.more_specifics.as_ref() {
                format!("{}", ms)
            } else {
                "".to_string()
            }
        )
    }
}
