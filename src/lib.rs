//! A treebitmap based IP Prefix Store

//! IP prefixes storage and retrieval data structures for IPv4 and IPv6 prefixes.
//! This crate contains structures for both single and multi-threaded contexts, as
//! well as async contexts.
//!
//! The underlying tree structure is based on the tree bitmap as outlined in
//! [this paper](https://www.cs.cornell.edu/courses/cs419/2005sp/tree-bitmap.pdf).
//!
//! Part of the Rotonda modular BGP engine.

//! Read more about the data-structure in this [blog post](https://blog.nlnetlabs.nl/donkeys-mules-horses/).

#[macro_use]
mod macros;

pub(crate) mod types;

// Public Interfaces

/// Some simple metadata implementations
pub mod meta_examples;

pub(crate) mod cht;
pub(crate) mod in_memory;
pub(crate) mod persist;
pub(crate) mod prefix_cht;
mod tests;

// re-exports
pub use crossbeam_epoch::{self as epoch, Guard};
pub use inetnum::addr;

pub mod rib;

pub use rib::starcast::StarCastRib;
pub use rib::starcast_af::{
    MemoryOnlyConfig, PersistHistoryConfig, PersistOnlyConfig,
    WriteAheadConfig,
};
pub use types::af::IPv4;
pub use types::af::IPv6;
pub use types::af::IntoIpAddr;
pub use types::errors;
pub use types::match_options::{
    IncludeHistory, MatchOptions, MatchType, QueryResult,
};
pub use types::prefix_id::RouteStatus;
pub use types::prefix_record::PublicPrefixRecord as PrefixRecord;
pub use types::prefix_record::PublicRecord as Record;
pub use types::stats;
pub use types::test_types;
pub use types::AddressFamily;
pub use types::Meta;
