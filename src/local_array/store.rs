use crate::local_array::storage_backend::{InMemStorage, StorageBackend};
use crate::local_array::tree::TreeBitMap;
use crate::MatchOptions;
use crate::{QueryResult, Stats, Strides};
use crate::prefix_record::InternalPrefixRecord;
use routecore::addr::Prefix;
use routecore::addr::{IPv4, IPv6};
use routecore::record::{Meta, MergeUpdate, NoMeta};

use std::fmt;

use super::node::{InMemStrideNodeId, SizedStrideRef};

/// A concurrently read/writable, lock-free Prefix Store, for use in a multi-threaded context.
pub struct Store<Meta: routecore::record::Meta + MergeUpdate> {
    v4: TreeBitMap<InMemStorage<IPv4, Meta>>,
    v6: TreeBitMap<InMemStorage<IPv6, Meta>>,
}

impl<Meta: routecore::record::Meta + MergeUpdate> Default for Store<Meta> {
    fn default() -> Self {
        Self::new(vec![3, 3, 3, 3, 3, 3, 3, 3, 4, 4], vec![8])
    }
}

impl<Meta: routecore::record::Meta + MergeUpdate> Store<Meta> {
    /// Creates a new empty store with a tree for IPv4 and on for IPv6.
    /// 
    /// You'll have to provide the stride sizes per address family and the
    /// meta-data type. Some meta-data type are included with this crate.
    /// 
    /// The stride-sizes can be any of [3,4,5,6,7,8], and they should add up
    /// to the total number of bits in the address family (32 for IPv4 and
    /// 128 for IPv6).
    /// 
    /// # Example
    /// ```
    /// use rotonda_store::MultiThreadedStore;
    /// use routecore::bgp::PrefixAs;
    /// 
    /// let store = MultiThreadedStore::<PrefixAs>::new(
    ///     vec![3, 3, 3, 3, 3, 3, 3, 3, 4, 4], vec![8]
    /// );
    /// ```
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

    pub fn nodes_v4_iter(
        &'a self,
    ) -> impl Iterator<Item = SizedStrideRef<'a, IPv4, InMemStrideNodeId>> + 'a {
        self.v4
            .store
            .nodes3
            .iter()
            .map(|n| SizedStrideRef::Stride3(n))
            .chain(
                self.v4
                    .store
                    .nodes4
                    .iter()
                    .map(|n| SizedStrideRef::Stride4(n)),
            )
            .chain(
                self.v4
                    .store
                    .nodes5
                    .iter()
                    .map(|n| SizedStrideRef::Stride5(n)),
            )
            .chain(
                self.v4
                    .store
                    .nodes6
                    .iter()
                    .map(|n| SizedStrideRef::Stride6(n)),
            )
            .chain(
                self.v4
                    .store
                    .nodes7
                    .iter()
                    .map(|n| SizedStrideRef::Stride7(n)),
            )
            .chain(
                self.v4
                    .store
                    .nodes8
                    .iter()
                    .map(|n| SizedStrideRef::Stride8(n)),
            )
    }

    pub fn nodes_v6_iter(
        &'a self,
    ) -> impl Iterator<Item = SizedStrideRef<'a, IPv6, InMemStrideNodeId>> + 'a {
        self.v6
            .store
            .nodes3
            .iter()
            .map(|n| SizedStrideRef::Stride3(n))
            .chain(
                self.v6
                    .store
                    .nodes4
                    .iter()
                    .map(|n| SizedStrideRef::Stride4(n)),
            )
            .chain(
                self.v6
                    .store
                    .nodes5
                    .iter()
                    .map(|n| SizedStrideRef::Stride5(n)),
            )
            .chain(
                self.v6
                    .store
                    .nodes6
                    .iter()
                    .map(|n| SizedStrideRef::Stride6(n)),
            )
            .chain(
                self.v6
                    .store
                    .nodes7
                    .iter()
                    .map(|n| SizedStrideRef::Stride7(n)),
            )
            .chain(
                self.v6
                    .store
                    .nodes8
                    .iter()
                    .map(|n| SizedStrideRef::Stride8(n)),
            )
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

    #[cfg(feature = "cli")]
    pub fn print_funky_stats(&self) {
        println!("{}", self.v4);
        println!("{}", self.v6);
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
