pub mod common;
pub mod store;

pub(crate) mod synth_int;
mod stride;
pub(crate) mod node_id;

pub(crate) mod local_array;
pub(crate) mod local_vec;
pub mod stats;

pub use common::*;
pub use store::*;
pub use local_array::store::Store as MultiThreadedStore;
pub use local_vec::store::Store as SingleThreadedStore;
pub use local_array::storage_backend::StorageBackend as MultiThreadedStorageBackend;
pub use local_vec::storage_backend::StorageBackend as SingleThreadedStorageBackend;

// routecore re-exports
pub use routecore::record::*;
pub use routecore::addr::*;
pub use routecore::bgp::*;

#[macro_use]
mod macros;