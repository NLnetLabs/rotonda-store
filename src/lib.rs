#![allow(clippy::type_complexity)]

pub use tree::TreeBitMap;
pub use store::{InMemStorage, StorageBackend};
pub use common::*;
pub use query::{MatchOptions, MatchType, QueryResult};

pub mod common;
pub mod query;
pub mod store;
pub mod synth_int;
pub mod tree;
pub mod stride;

#[macro_use]
mod macros;
