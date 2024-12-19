//------------ PersistTree ---------------------------------------------------

use std::marker::PhantomData;
use std::path::Path;

use lsm_tree::AbstractTree;

use crate::local_array::types::{PrefixId, RouteStatus};
use crate::rib::Counters;
use crate::{AddressFamily, MatchType, Meta, PublicRecord, QueryResult};

pub struct PersistTree<
    AF: AddressFamily,
    // The size in bytes of the prefix in the peristed storage (disk), this
    // amounnts to the bytes for the addres (4 for IPv4, 16 for IPv6) and 1
    // bytefor the prefix length.
    const PREFIX_SIZE: usize,
    // The size in bytes of the complete key in the persisted storage, this
    // is PREFIX_SIZE bytes (4; 16) + mui size (4) + ltime (8)
    const KEY_SIZE: usize,
> {
    tree: lsm_tree::Tree,
    counters: Counters,
    _af: PhantomData<AF>,
}

impl<AF: AddressFamily, const PREFIX_SIZE: usize, const KEY_SIZE: usize>
    PersistTree<AF, PREFIX_SIZE, KEY_SIZE>
{
    pub fn new(
        persist_path: &Path,
    ) -> PersistTree<AF, PREFIX_SIZE, KEY_SIZE> {
        PersistTree::<AF, PREFIX_SIZE, KEY_SIZE> {
            tree: lsm_tree::Config::new(persist_path).open().unwrap(),
            counters: Counters::default(),
            _af: PhantomData,
        }
    }

    pub fn insert(&self, key: [u8; KEY_SIZE], value: &[u8]) -> (u32, u32) {
        self.tree.insert::<[u8; KEY_SIZE], &[u8]>(key, value, 0)
    }

    pub fn get_records_for_prefix<M: Meta>(
        &self,
        prefix: PrefixId<AF>,
    ) -> Vec<PublicRecord<M>> {
        let prefix_b = &prefix.as_bytes::<PREFIX_SIZE>();
        (*self.tree.prefix(prefix_b))
            .into_iter()
            .map(|kv| {
                let kv = kv.unwrap();
                let (_, mui, ltime, status) = Self::parse_key(kv.0.as_ref());
                PublicRecord::new(
                    mui,
                    ltime,
                    status.try_into().unwrap(),
                    kv.1.as_ref().to_vec().into(),
                )
            })
            .collect::<Vec<_>>()
    }

    pub fn get_records_for_key<M: Meta + From<Vec<u8>>>(
        &self,
        key: &[u8],
    ) -> Vec<(inetnum::addr::Prefix, PublicRecord<M>)> {
        (*self.tree.prefix(key))
            .into_iter()
            .map(|kv| {
                let kv = kv.unwrap();
                let (pfx, mui, ltime, status) =
                    Self::parse_key(kv.0.as_ref());

                (
                    PrefixId::<AF>::from(pfx).into_pub(),
                    PublicRecord::new(
                        mui,
                        ltime,
                        status.try_into().unwrap(),
                        kv.1.as_ref().to_vec().into(),
                    ),
                )
            })
            .collect::<Vec<_>>()
    }

    fn match_prefix_in_persisted_store<M: Meta>(
        &self,
        search_pfx: PrefixId<AF>,
        mui: Option<u32>,
    ) -> QueryResult<M> {
        let key: Vec<u8> = if let Some(mui) = mui {
            PersistTree::<AF,
        PREFIX_SIZE, KEY_SIZE>::prefix_mui_persistence_key(search_pfx, mui)
        } else {
            search_pfx.as_bytes::<PREFIX_SIZE>().to_vec()
        };

        QueryResult {
            prefix: Some(search_pfx.into_pub()),
            match_type: MatchType::ExactMatch,
            prefix_meta: self
                .get_records_for_key(&key)
                .into_iter()
                .map(|(_, rec)| rec)
                .collect::<Vec<_>>(),
            less_specifics: None,
            more_specifics: None,
        }
    }

    pub fn flush_to_disk(&self) -> Result<(), lsm_tree::Error> {
        let segment = self.tree.flush_active_memtable(0);

        if let Ok(Some(segment)) = segment {
            self.tree.register_segments(&[segment])?;
            self.tree.compact(
                std::sync::Arc::new(lsm_tree::compaction::Leveled::default()),
                0,
            )?;
        };

        Ok(())
    }

    pub fn approximate_len(&self) -> usize {
        self.tree.approximate_len()
    }

    pub fn disk_space(&self) -> u64 {
        self.tree.disk_space()
    }

    pub fn get_prefixes_count(&self) -> usize {
        self.counters.get_prefixes_count().iter().sum()
    }

    pub fn get_prefixes_count_for_len(&self, len: u8) -> usize {
        self.counters.get_prefixes_count()[len as usize]
    }

    #[cfg(feature = "persist")]
    pub fn persistence_key(
        // PREFIX_SIZE bytes
        prefix_id: PrefixId<AF>,
        // 4 bytes
        mui: u32,
        // 8 bytes
        ltime: u64,
        // 1 byte
        status: RouteStatus,
    ) -> [u8; KEY_SIZE] {
        assert!(KEY_SIZE > PREFIX_SIZE);
        let key = &mut [0_u8; KEY_SIZE];

        // prefix 5 or 17 bytes
        *key.first_chunk_mut::<PREFIX_SIZE>().unwrap() = prefix_id.as_bytes();

        // mui 4 bytes
        *key[PREFIX_SIZE..PREFIX_SIZE + 4]
            .first_chunk_mut::<4>()
            .unwrap() = mui.to_le_bytes();

        // ltime 8 bytes
        *key[PREFIX_SIZE + 4..PREFIX_SIZE + 12]
            .first_chunk_mut::<8>()
            .unwrap() = ltime.to_le_bytes();

        // status 1 byte
        key[PREFIX_SIZE + 12] = status.into();

        *key
    }

    #[cfg(feature = "persist")]
    pub fn prefix_mui_persistence_key(
        prefix_id: PrefixId<AF>,
        mui: u32,
    ) -> Vec<u8> {
        let mut key = vec![];
        // prefix 5 or 17 bytes
        *key.first_chunk_mut::<PREFIX_SIZE>().unwrap() = prefix_id.as_bytes();

        // mui 4 bytes
        *key[PREFIX_SIZE..PREFIX_SIZE + 4]
            .first_chunk_mut::<4>()
            .unwrap() = mui.to_le_bytes();

        key
    }

    #[cfg(feature = "persist")]
    pub fn parse_key(bytes: &[u8]) -> ([u8; PREFIX_SIZE], u32, u64, u8) {
        (
            // prefix 5 or 17 bytes
            *bytes.first_chunk::<PREFIX_SIZE>().unwrap(),
            // mui 4 bytes
            u32::from_le_bytes(
                *bytes[PREFIX_SIZE..PREFIX_SIZE + 4]
                    .first_chunk::<4>()
                    .unwrap(),
            ),
            // ltime 8 bytes
            u64::from_le_bytes(
                *bytes[PREFIX_SIZE + 4..PREFIX_SIZE + 12]
                    .first_chunk::<8>()
                    .unwrap(),
            ),
            // status 1 byte
            bytes[PREFIX_SIZE + 12],
        )
    }

    #[cfg(feature = "persist")]
    pub fn parse_prefix(bytes: &[u8]) -> [u8; PREFIX_SIZE] {
        *bytes.first_chunk::<PREFIX_SIZE>().unwrap()
    }

    #[cfg(feature = "persist")]
    pub(crate) fn persist_record<M: Meta>(
        &self,
        prefix: PrefixId<AF>,
        // mui: u32,
        record: &PublicRecord<M>,
    ) {
        self.insert(
            PersistTree::<AF, PREFIX_SIZE, KEY_SIZE>::persistence_key(
                prefix,
                record.multi_uniq_id,
                record.ltime,
                record.status,
            ),
            record.meta.as_ref(),
        );
    }
}

impl<AF: AddressFamily, const PREFIX_SIZE: usize, const KEY_SIZE: usize>
    std::fmt::Debug for PersistTree<AF, PREFIX_SIZE, KEY_SIZE>
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
