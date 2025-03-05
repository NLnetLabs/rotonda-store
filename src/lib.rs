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
mod cht;
mod lsm_tree;
mod prefix_cht;
mod rib;
mod tree_bitmap;
mod types;

#[macro_use]
mod macros;

pub(crate) use lsm_tree::LsmTree;
pub(crate) use tree_bitmap::TreeBitMap;

// re-exports
pub use crossbeam_epoch::{self as epoch, Guard};
pub use inetnum::addr;

// Public Interfaces on the root of the crate

pub use rib::starcast::StarCastRib;
pub use rib::starcast_af::{
    Config, MemoryOnlyConfig, PersistHistoryConfig, PersistOnlyConfig,
    PersistStrategy, UpsertReport, WriteAheadConfig,
};
pub use types::af::AddressFamily;
pub use types::af::IPv4;
pub use types::af::IPv6;
pub use types::af::IntoIpAddr;
pub use types::errors;
pub use types::match_options::{
    IncludeHistory, MatchOptions, MatchType, QueryResult,
};
pub use types::meta_examples;
pub use types::prefix_record::Meta;
pub use types::prefix_record::PublicPrefixRecord as PrefixRecord;
pub use types::prefix_record::PublicRecord as Record;
pub use types::route_status::RouteStatus;
pub use types::stats;
pub use types::test_types;
