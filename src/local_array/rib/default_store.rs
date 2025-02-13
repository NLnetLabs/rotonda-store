use crate::local_array::in_memory::atomic_types::NodeSet;
use crate::prelude::multi::*;
use crate::prelude::*;
use rand::prelude::*;

// The default stride sizes for IPv4, IPv6, resp.
#[create_store((
    // ([5, 5, 4, 3, 3, 3, 3, 3, 3, 3], 5, 18),
    ([4, 4, 4, 4, 4, 4, 4, 4], 5, 18),
    ([4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 4], 17, 30)
))]
struct DefaultStore;

/// Try some
impl<M: Meta> DefaultStore<M> {
    pub fn try_default() -> Result<Self, PrefixStoreError> {
        let config = StoreConfig::default();
        Self::new_with_config(config)
            .map_err(|_| PrefixStoreError::StoreNotReadyError)
    }
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            persist_strategy: PersistStrategy::MemoryOnly,
            persist_path: "/tmp/rotonda/".to_string(),
        }
    }
}
