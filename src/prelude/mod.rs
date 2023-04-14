
pub use routecore::addr::Prefix;

pub use crate::{AddressFamily, IPv4, IPv6};

pub use crate::prefix_record::{
    PublicPrefixRecord as PrefixRecord,
    Meta,
    MergeUpdate
};
pub use crate::{MatchOptions, MatchType, QueryResult};
pub use crate::stride::{Stride3, Stride4, Stride5};

pub mod multi {
    pub use crate::MultiThreadedStore;

    pub use rotonda_macros::create_store;
    pub use rotonda_macros::stride_sizes;

    pub use crossbeam_epoch::{self as epoch, Guard};

    pub use crate::local_array::store::atomic_types::{
        NodeBuckets, NodeSet, PrefixBuckets, PrefixSet,
    };
    pub use crate::local_array::tree::{PrefixId, StrideNodeId, TreeBitMap};
    pub use crate::local_array::store::errors::PrefixStoreError;

    pub use crate::custom_alloc::{Upsert, Counters, StoreStats};
    pub use crate::custom_alloc::CustomAllocStorage;
}