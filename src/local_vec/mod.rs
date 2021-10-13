pub mod tree;
pub mod query;
pub mod node;
pub mod store;

pub use tree::TreeBitMap;
pub use store::{StorageBackend, InMemStorage};
pub use query::{MatchOptions, MatchType};

#[macro_use]
mod macros;