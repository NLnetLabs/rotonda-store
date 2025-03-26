#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![allow(clippy::multiple_crate_versions)]
//! A library that provides abstractions for a BGP Routing Information Base
//! (RIB) for different AFI/SAFI types, as a database.
//!
//! The data structures provided by this crate can be used to store and query
//! routes (and route-like data) in memory and on-disk, for both current and
//! historical data.
//!
//! The main data structures that this crate implements are firstly a tree
//! bitmap, largely as described in this paper[^1] - but with a twist.
//! There's also a blog post[^2] about the tree bitmap, and similar data
//! structures. Secondly, this repo implements a concurrent chained hash
//! table (called `cht` throughout this repo), geared towards keys that are
//! "prefix-like", i.e. variable-length bitfields, that fit within a
//! primitive integer type.
//!
//! The log-structured merge tree ("lsm_tree") used in this library is
//! provided by the `lsm_tree` crate - the crate that powers `fjall`.
//!
//! [^1]: <https://www.cs.cornell.edu/courses/cs419/2005sp/tree-bitmap.pdf>
//! [^2]: <https://blog.nlnetlabs.nl/donkeys-mules-horses/>

//                                ┌───────────────────┐
//                                │    StarCastRib    │
//                                └────────┬┬─────────┘
//                        ┌────────────────┘└─────────────────┐
//                     ┌──▼──┐                             ┌──▼──┐
//                     │ v4  │                             │ v4  │
//                     └──┬──┘                             └──┬──┘
//            ┌───────────┼──────────┐            ┌───────────┼──────────┐
//      ┌─────▼────┐┌─────▼────┐┌───▼─────┐┌─────▼────┐┌─────▼────┐┌────▼───┐
//      │treebitmap││prefix_cht││lsm_tree ││treebitmap││prefix_cht││lsm_tree│
//      └─────┬────┘└──────────┘└─────────┘└─────┬────┘└──────────┘└────────┘
//     ┌──────┴─────┐                      ┌──────┴─────┐
// ┌───▼────┐┌──────▼────┐             ┌───▼────┐┌──────▼────┐
// │node_cht││muis_bitmap│             │node_cht││muis_bitmap│
// └────────┘└───────────┘             └────────┘└───────────┘

// Rotonda-store is a fairly layered repo, it uses three different
// types of trees, that are all hidden behind one public interface.

// `rib::starcast::StarCastRib`, holds that public API. This is the RIB
// that stores (route-like) data for IPv4/IPv6 unicast and multicast (hence
// *cast). This is a good starting point to dive into this repo.

// `rib::starcast_af::StarCastAfRib` holds the three trees for a store, per
// Address Family. From there `tree_bitmap` (the mod.rs file), holds the tree
// bitmap, `tree_bit_map::node_cht` holds the CHT that stores the nodes for
// the tree bitmap. Next to the tree, it also holds a bitmap that indexes all
// muis that are withdrawn for the whole tree. The tree bitmap is used for
// all strategies.

// `prefix_cht::cht` holds the CHT that stores all the route-like data for the
// in-memory strategies. This CHT is the same data-structure that is used for
// the nodes, but it stores `MultiMap` typed values in its nodes (described in
// the same file).

// `lsm_tree` (again, in the mod.rs file) holds the log-structured merge tree
// used for persistent storage on disk.

//------------ Modules -------------------------------------------------------

// the Chained Hash Table that is used by the treebitmap, and the CHT for the
// prefixes (for in-menory storage of prefixes).
mod cht;

// The log-structured merge tree, used as persistent storage (on disk).
mod lsm_tree;

// The Chained Hash Table that stores the records for the prefixers in memory
mod prefix_cht;

// The Treebitmap, that stores the existence of all prefixes, and that is used
//for all strategies.
mod tree_bitmap;

// Types, both public and private, that are used throughout the store.
mod types;

#[macro_use]
mod macros;

pub(crate) use lsm_tree::LsmTree;
pub(crate) use tree_bitmap::TreeBitMap;

// re-exports
pub use crossbeam_epoch::{self as epoch, Guard};
pub use inetnum::addr;

// Public Interfaces on the root of the crate

/// RIBs for various AFI/SAFI types
pub mod rib;

/// Types used to create match queries on a RIB
pub use types::match_options;

/// Record, Record Iterator and related types/traits
pub use types::prefix_record;

/// Error types returned by a RIB
pub use types::errors;

/// Trait that defines the AFIs 1 (IPv4) and 2 (IPv6)
pub use types::af::AddressFamily;

/// The underlying value (u32) and trait impl for AFI 1.
pub use types::af::IPv4;
/// The underlying value (u128) and trait impl for AFI 2.
pub use types::af::IPv6;

/// Trait that describes the conversion of a u32 or u128 in to a IPv4, or IPV6
/// respectively.
pub use types::af::IntoIpAddr;

/// Statistics and metrics types returned by methods on a RIB
pub use types::stats;

// Used in tests
#[doc(hidden)]
pub use types::test_types;
