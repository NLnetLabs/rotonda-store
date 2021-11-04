use crate::local_array::storage_backend::{InMemStorage, StorageBackend};
use crate::local_array::tree::TreeBitMap;
use crate::node_id::SortableNodeId;
use crate::{InternalPrefixRecord, MatchOptions};
use crate::{QueryResult, Stats, Strides};
use routecore::addr::{AddressFamily, Prefix};
use routecore::addr::{IPv4, IPv6};
use routecore::record::{MergeUpdate, NoMeta};

use std::fmt;

use super::node::{InMemStrideNodeId, SizedStrideRef};

pub struct Store<Meta: routecore::record::Meta + MergeUpdate> {
    pub(crate) v4: TreeBitMap<InMemStorage<IPv4, Meta>>,
    pub(crate) v6: TreeBitMap<InMemStorage<IPv6, Meta>>,
}

impl<Meta: routecore::record::Meta + MergeUpdate> Default for Store<Meta> {
    fn default() -> Self {
        Self::new(vec![3, 3, 3, 3, 3, 3, 3, 3, 4, 4], vec![8])
    }
}

impl<Meta: routecore::record::Meta + MergeUpdate> Store<Meta> {
    pub fn new(v4_strides: Vec<u8>, v6_strides: Vec<u8>) -> Self {
        Store {
            v4: TreeBitMap::new(v4_strides),
            v6: TreeBitMap::new(v6_strides),
        }
    }
}

impl<'a, Meta: routecore::record::Meta + MergeUpdate> Store<Meta> {
    pub fn match_prefix(
        &'a self,
        search_pfx: &Prefix,
        options: &MatchOptions,
    ) -> QueryResult<'a, Meta> {
        match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => self.v4.match_prefix(
                &InternalPrefixRecord::<IPv4, NoMeta>::new(addr.into(), search_pfx.len()),
                options,
            ),
            std::net::IpAddr::V6(addr) => self.v6.match_prefix(
                &InternalPrefixRecord::<IPv6, NoMeta>::new(addr.into(), search_pfx.len()),
                options,
            ),
        }
    }

    pub fn insert(
        &mut self,
        prefix: &Prefix,
        meta: Meta,
    ) -> Result<(), std::boxed::Box<dyn std::error::Error>> {
        match prefix.addr() {
            std::net::IpAddr::V4(addr) => self.v4.insert(InternalPrefixRecord::new_with_meta(
                addr.into(),
                prefix.len(),
                meta,
            )),
            std::net::IpAddr::V6(addr) => self.v6.insert(InternalPrefixRecord::new_with_meta(
                addr.into(),
                prefix.len(),
                meta,
            )),
        }
    }

    pub fn prefixes_iter(&self) -> crate::PrefixRecordIter<Meta> {
        let rs4: std::slice::Iter<InternalPrefixRecord<IPv4, Meta>> =
            self.v4.store.prefixes[..].iter();
        let rs6 = self.v6.store.prefixes[..].iter();

        crate::PrefixRecordIter::<Meta> {
            v4: Some(rs4),
            v6: rs6,
        }
    }

    pub fn bla() -> impl Iterator<Item = u32> {
        vec![1, 2, 3].into_iter()
    }

    pub fn nodes_v4_iter(
        &'a self,
    ) -> impl Iterator<Item = SizedStrideRef<'a, IPv4, InMemStrideNodeId>> + 'a {
        self.v4
            .store
            .nodes3
            .iter().map(|n| SizedStrideRef::Stride3(n))
            .chain(self.v4.store.nodes4.iter().map(|n| SizedStrideRef::Stride4(n)))
            .chain(self.v4.store.nodes5.iter().map(|n| SizedStrideRef::Stride5(n)))
            .chain(self.v4.store.nodes6.iter().map(|n| SizedStrideRef::Stride6(n)))
            .chain(self.v4.store.nodes7.iter().map(|n| SizedStrideRef::Stride7(n)))
            .chain(self.v4.store.nodes8.iter().map(|n| SizedStrideRef::Stride8(n)))
    }

    pub fn nodes_v6_iter(
        &'a self,
    ) -> impl Iterator<Item = SizedStrideRef<'a, IPv6, InMemStrideNodeId>> + 'a {
        self.v6
            .store
            .nodes3
            .iter().map(|n| SizedStrideRef::Stride3(n))
            .chain(self.v6.store.nodes4.iter().map(|n| SizedStrideRef::Stride4(n)))
            .chain(self.v6.store.nodes5.iter().map(|n| SizedStrideRef::Stride5(n)))
            .chain(self.v6.store.nodes6.iter().map(|n| SizedStrideRef::Stride6(n)))
            .chain(self.v6.store.nodes7.iter().map(|n| SizedStrideRef::Stride7(n)))
            .chain(self.v6.store.nodes8.iter().map(|n| SizedStrideRef::Stride8(n)))
    }

    pub fn prefixes_len(&self) -> usize {
        self.v4.store.prefixes.len() + self.v6.store.prefixes.len()
    }

    pub fn prefixes_v4_len(&self) -> usize {
        self.v4.store.prefixes.len()
    }

    pub fn prefixes_v6_len(&self) -> usize {
        self.v6.store.prefixes.len()
    }

    pub fn nodes_len(&self) -> usize {
        self.v4.store.get_nodes_len() + self.v6.store.get_nodes_len()
    }

    pub fn nodes_v4_len(&self) -> usize {
        self.v4.store.get_nodes_len()
    }

    pub fn nodes_v6_len(&self) -> usize {
        self.v6.store.get_nodes_len()
    }

    pub fn nodes_per_stride(&'a self) -> (Vec<u32>, Vec<u32>) {
        let mut stride_levels_v4 = Vec::from([]);
        let mut stride_levels_v6 = Vec::from([]);
        for stride in self.strides().v4.iter().enumerate() {
            stride_levels_v4.push(
                self.v4
                    .stats
                    .iter()
                    .find(|s| s.stride_len == *stride.1)
                    .unwrap()
                    .created_nodes[stride.0]
                    .count as u32,
            );
        }
        for stride in self.strides().v6.iter().enumerate() {
            stride_levels_v6.push(
                self.v6
                    .stats
                    .iter()
                    .find(|s| s.stride_len == *stride.1)
                    .unwrap()
                    .created_nodes[stride.0]
                    .count as u32,
            );
        }
        (stride_levels_v4, stride_levels_v6)
    }

    pub fn stats(&self) -> Stats {
        Stats {
            v4: &self.v4.stats,
            v6: &self.v6.stats,
        }
    }

    pub fn strides(&'a self) -> Strides {
        Strides {
            v4: &self.v4.strides,
            v6: &self.v6.strides,
        }
    }
}

impl<Meta: routecore::record::Meta + MergeUpdate> fmt::Display for InMemStorage<IPv4, Meta> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InMemStorage<u32, {}>", std::any::type_name::<Meta>())
    }
}

impl<Meta: routecore::record::Meta + MergeUpdate> fmt::Display for InMemStorage<IPv6, Meta> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InMemStorage<u128, {}>", std::any::type_name::<Meta>())
    }
}

impl<Meta: routecore::record::Meta + MergeUpdate> std::fmt::Debug for Store<Meta> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Store")?;
        write!(f, "IPv4 Tree")?;
        write!(f, "{:?}", self.v4.store)?;
        write!(f, "IPv6 Tree")?;
        write!(f, "{:?}", self.v6.store)?;
        Ok(())
    }
}
