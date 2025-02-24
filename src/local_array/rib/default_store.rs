use crate::local_array::in_memory::atomic_types::NodeSet;
use crate::local_array::persist::lsm_tree::LongKey;
use crate::prelude::multi::*;
use crate::prelude::*;
use rand::prelude::*;

// The default stride sizes for IPv4, IPv6, resp.
#[create_store((
    ([4, 4, 4, 4, 4, 4, 4, 4], 5, 18, LongKey),
    ([4, 4, 4, 4, 4, 4, 4, 4,
      4, 4, 4, 4, 4, 4, 4, 4,
      4, 4, 4, 4, 4, 4, 4, 4,
      4, 4, 4, 4, 4, 4, 4, 4], 17, 30, LongKey)
))]
struct DefaultStore;

/// Try some
impl<M: Meta, C: Config> DefaultStore<M, C> {
    pub fn try_default() -> Result<Self, PrefixStoreError> {
        let config = C::default();
        Self::new_with_config(config)
            .map_err(|_| PrefixStoreError::StoreNotReadyError)
    }
}

// impl Default for StoreConfig {
//     fn default() -> Self {
//         Self {
//             persist_strategy: PersistStrategy::MemoryOnly,
//             persist_path: "/tmp/rotonda/".to_string(),
//         }
//     }
// }

// impl StoreConfig {
//     fn persist_default() -> Self {
//         Self {
//             persist_strategy: PersistStrategy::PersistOnly,
//             persist_path: "/tmp/rotonda/".to_string(),
//         }
//     }
// }

// pub mod persist_only_store {
//     use crate::local_array::in_memory::atomic_types::NodeSet;
//     use crate::local_array::persist::lsm_tree::ShortKey;
//     use crate::prelude::multi::*;
//     use crate::prelude::*;
//     use rand::prelude::*;
//     #[create_store((
//         ([4], 5, 18, PersistOnlyConfig, ShortKey),
//         ([4], 17, 30, PersistOnlyconfig, ShortKey)
//     ))]
//     struct PersistOnlyStore;

//     /// Try some
//     impl<M: Meta, C: Config> PersistOnlyStore<M, C> {
//         pub fn try_default() -> Result<Self, PrefixStoreError> {
//             let config = C::default();
//             Self::new_with_config(config)
//                 .map_err(|_| PrefixStoreError::StoreNotReadyError)
//         }
//     }
// }
