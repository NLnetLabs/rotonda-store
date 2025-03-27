use crate::types::{prefix_record::RecordSet, Record};
use std::fmt;

use inetnum::addr::Prefix;

use super::prefix_record::Meta;

//------------ MatchOptions / MatchType -------------------------------------

/// Options for the `match_prefix` method
///
/// The `MatchOptions` struct is used to specify the options for the
/// `match_prefix` method on the store.
///
/// Note that the `match_type` field may be different from the actual
/// `MatchType` returned from the result.
///
/// See [crate::rib::StarCastRib::match_prefix] for more details.
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

/// Option to set the match type for a prefix match. Type can be Exact,
/// Longest, or Empty. The match type only applies to the `prefix` and
/// `records` fields in the [QueryResult] that is returned by a
/// [StarCastRib::match_prefix()] query.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MatchType {
    /// Only return the requested prefix, and the associated records, if the
    /// requested prefix exactly matches the found prefix(es) (if any).
    ExactMatch,
    /// Return the longest matching prefix for the requested prefix (if
    /// any). May match the prefix exactly.
    LongestMatch,
    /// Return the longest matching prefix, or none at all.
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

/// Match option to indicate that the result should return historical records,
/// for the requested prefixes and the more- and less-specific prefixes. This
/// option is ignored if the persist strategy config option is anythin other
/// than `PersistHistory` or WriteAhead`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IncludeHistory {
    /// Do not return any historical records.
    None,
    /// Return historical records for the requested prefix only.
    SearchPrefix,
    /// Return historical records for all prefixes in the result.
    All,
}

//------------- QueryResult -------------------------------------------------

/// The type that is returned by a query.
///
/// This is the result type of a query. It contains the prefix record that was
/// found in the store, as well as less- or more-specifics as requested.
///
/// See [crate::rib::StarCastRib::match_prefix] for more details.

#[derive(Clone, Debug)]
pub struct QueryResult<M: Meta> {
    /// The match type of the resulting prefix
    pub match_type: MatchType,
    /// The resulting prefix record
    pub prefix: Option<Prefix>,
    /// The meta data associated with the resulting prefix record
    pub records: Vec<Record<M>>,
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
            records: vec![],
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
        for rec in &self.records {
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
