use crate::local_array::in_memory::atomic_types::NodeSet;
use crate::local_array::persist::lsm_tree::LongKey;
use crate::prelude::multi::*;
use crate::prelude::*;
use rand::prelude::*;

pub const STRIDE_SIZE: u8 = 4;

// The default stride sizes for IPv4, IPv6, resp.
#[create_store((
    ([4, 4, 4, 4, 4, 4, 4, 4], 5, 18, LongKey),
    ([4, 4, 4, 4, 4, 4, 4, 4,
      4, 4, 4, 4, 4, 4, 4, 4,
      4, 4, 4, 4, 4, 4, 4, 4,
      4, 4, 4, 4, 4, 4, 4, 4], 17, 30, LongKey)
))]
struct DefaultStore;

impl<M: Meta, C: Config> DefaultStore<M, C> {
    pub fn try_default() -> Result<Self, PrefixStoreError> {
        let config = C::default();
        Self::new_with_config(config)
            .map_err(|_| PrefixStoreError::StoreNotReadyError)
    }
}
