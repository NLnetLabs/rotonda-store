#![allow(clippy::type_complexity)]

pub use tree::{
    CacheGuard, InMemNodeId, InMemStrideNodeId, InMemStorage, PrefixCacheGuard, SizedStrideNode, SortableNodeId,
    StorageBackend, Stride, TreeBitMap, TreeBitMapNode,
};

pub use query::{ MatchOptions, MatchType, QueryResult};

pub mod common;
pub mod query;
pub mod synth_int;
pub mod tree;

#[macro_use]
mod macros;
