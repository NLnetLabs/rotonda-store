use crate::local_vec::storage_backend::{InMemStorage, StorageBackend};
use crate::local_vec::TreeBitMap;
use crate::node_id::InMemNodeId;
use crate::prefix_record::InternalPrefixRecord;
use super::query::QueryResult;
use crate::{MatchOptions, Stats, Strides};

use crate::af::{IPv4, IPv6};
use routecore::addr::Prefix;
use routecore::record::MergeUpdate;

use super::query::PrefixId;
use super::tree::SizedStrideNode;
/// A fast, memory-efficient Prefix Store, for use in single-threaded contexts.
///
/// Can be used in multi-threaded contexts by wrapping it in a `Arc<Mutex<_>>`.
/// Be aware that this is undesirable in cases with high contention.
/// Use cases with high contention are best served by the [`crate::MultiThreadedStore`].
pub struct Store<Meta: routecore::record::Meta>
where
    Meta: MergeUpdate,
{
    v4: TreeBitMap<InMemStorage<IPv4, Meta>>,
    v6: TreeBitMap<InMemStorage<IPv6, Meta>>,
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
    ) -> QueryResult<Meta> {
        match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => self.v4.match_prefix(
                PrefixId::<IPv4>::new(addr.into(), search_pfx.len()),
                options,
            ),
            std::net::IpAddr::V6(addr) => self.v6.match_prefix(
                PrefixId::<IPv6>::new(addr.into(), search_pfx.len()),
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
            std::net::IpAddr::V4(addr) => {
                self.v4.insert(InternalPrefixRecord::new_with_meta(
                    addr.into(),
                    prefix.len(),
                    meta,
                ))
            }
            std::net::IpAddr::V6(addr) => {
                self.v6.insert(InternalPrefixRecord::new_with_meta(
                    addr.into(),
                    prefix.len(),
                    meta,
                ))
            }
        }
    }

    // pub fn prefixes_iter(&'a self) -> crate::PrefixRecordIter<'a, Meta> {
    //     let rs4: std::slice::Iter<InternalPrefixRecord<IPv4, Meta>> =
    //         self.v4.store.prefixes[..].iter();
    //     let rs6 = self.v6.store.prefixes[..].iter();

    //     crate::PrefixRecordIter::<'a, Meta> {
    //         v4: Some(rs4),
    //         v6: rs6,
    //     }
    // }

    pub fn nodes_v4_iter(
        &'a self,
    ) -> impl Iterator<Item = &'a SizedStrideNode<IPv4, InMemNodeId>> + 'a
    {
        self.v4.store.nodes.iter()
    }

    pub fn nodes_v6_iter(
        &'a self,
    ) -> impl Iterator<Item = &'a SizedStrideNode<IPv6, InMemNodeId>> + 'a
    {
        self.v6.store.nodes.iter()
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
