#![allow(clippy::type_complexity)]

pub use tree::{
    CacheGuard, InMemNodeId, InMemStorage, PrefixCacheGuard, SizedStrideNode, SortableNodeId,
    StorageBackend, Stride, TreeBitMap, TreeBitMapNode,
};

pub mod common;
mod query;
pub mod synth_int;
pub mod tree;

#[macro_use]
mod macros;
