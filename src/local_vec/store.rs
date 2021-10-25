use crate::common::{IPv4, IPv6};
use crate::local_vec::storage_backend::InMemStorage;
use crate::local_vec::TreeBitMap;

use routecore::record::MergeUpdate;

pub struct Store<Meta: routecore::record::Meta>
where
    Meta: MergeUpdate,
{
    pub v4: TreeBitMap<InMemStorage<IPv4, Meta>>,
    pub v6: TreeBitMap<InMemStorage<IPv6, Meta>>,
}

impl<Meta: routecore::record::Meta + MergeUpdate> Store<Meta> {
    pub fn new(v4_strides: Vec<u8>, v6_strides: Vec<u8>) -> Self {
        Store {
            v4: TreeBitMap::new(v4_strides),
            v6: TreeBitMap::new(v6_strides),
        }
    }
}
