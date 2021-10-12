pub(crate) mod node;
pub(crate) mod query;
pub(crate) mod tree;
pub(crate) mod store;

pub use tree::TreeBitMap;
pub use store::{StorageBackend, InMemStorage};
pub use query::{MatchOptions, MatchType};