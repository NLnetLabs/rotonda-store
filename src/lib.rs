//! A library that provides abstractions for a BGP Routing Information Base (RIB) for different AFI/SAFI types, as a database.
//!
//! The data structures provides by this crate can be used to store and query routes (or other metadata keyed on IP prefixes, or a comparable bitarray) in memory and on-disk, for both current and historical data.
//!
//! [^1]:[Paper](https://www.cs.cornell.edu/courses/cs419/2005sp/tree-bitmap.pdf).
//! [^2]: Read more about the data-structure in this [blogpost](https://blog.nlnetlabs.nl/donkeys-mules-horses/).

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

#[doc(hidden)]
pub use types::meta_examples;
#[doc(hidden)]
pub use types::test_types;

pub use types::prefix_record::Meta;
pub use types::prefix_record::PublicPrefixRecord as PrefixRecord;
pub use types::prefix_record::PublicRecord as Record;
pub use types::route_status::RouteStatus;
pub use types::stats;
