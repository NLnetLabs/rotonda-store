//------------ PersistTree ---------------------------------------------------

use std::marker::PhantomData;
use std::path::Path;

use inetnum::addr::Prefix;
use log::trace;
use lsm_tree::{AbstractTree, KvPair};
use roaring::RoaringBitmap;

use crate::local_array::types::{PrefixId, RouteStatus};
use crate::rib::query::{FamilyQueryResult, FamilyRecord, TreeQueryResult};
use crate::rib::Counters;
use crate::{
    AddressFamily, IncludeHistory, MatchOptions, Meta, PublicRecord,
};

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

    pub(crate) fn insert(
        &self,
        key: [u8; KEY_SIZE],
        value: &[u8],
    ) -> (u32, u32) {
        self.tree.insert::<[u8; KEY_SIZE], &[u8]>(key, value, 0)
    }

    pub fn remove(&self, key: [u8; KEY_SIZE]) {
        self.tree.remove(key, 0);
        // the last byte of the prefix holds the length of the prefix.
        self.counters.dec_prefixes_count(key[PREFIX_SIZE]);
    }

    pub fn get_records_for_prefix<M: Meta>(
        &self,
        prefix: PrefixId<AF>,
        mui: Option<u32>,
    ) -> Vec<PublicRecord<M>> {
        if let Some(mui) = mui {
            let prefix_b = Self::prefix_mui_persistence_key(prefix, mui);

            (*self.tree.prefix(prefix_b))
                .into_iter()
                .map(|kv| {
                    let kv = kv.unwrap();
                    let (_, mui, ltime, status) =
                        Self::parse_key(kv.0.as_ref());
                    PublicRecord::new(
                        mui,
                        ltime,
                        status.try_into().unwrap(),
                        kv.1.as_ref().to_vec().into(),
                    )
                })
                .collect::<Vec<_>>()
        } else {
            let prefix_b = &prefix.as_bytes::<PREFIX_SIZE>();

            (*self.tree.prefix(prefix_b))
                .into_iter()
                .map(|kv| {
                    let kv = kv.unwrap();
                    let (_, mui, ltime, status) =
                        Self::parse_key(kv.0.as_ref());
                    PublicRecord::new(
                        mui,
                        ltime,
                        status.try_into().unwrap(),
                        kv.1.as_ref().to_vec().into(),
                    )
                })
                .collect::<Vec<_>>()
        }
    }

    pub fn get_most_recent_record_for_prefix_mui<M: Meta>(
        &self,
        prefix: PrefixId<AF>,
        mui: u32,
    ) -> Option<PublicRecord<M>> {
        let key_b = Self::prefix_mui_persistence_key(prefix, mui);

        (*self.tree.prefix(key_b))
            .into_iter()
            .map(|kv| {
                let kv = kv.unwrap();
                (Self::parse_key(kv.0.as_ref()), kv.1)
            })
            .max_by(|(a, _), (b, _)| a.2.cmp(&b.2))
            .map(|((_pfx, mui, ltime, status), m)| {
                PublicRecord::new(
                    mui,
                    ltime,
                    status.try_into().unwrap(),
                    m.as_ref().to_vec().into(),
                )
            })
    }

    pub(crate) fn get_records_with_keys_for_prefix_mui<M: Meta>(
        &self,
        prefix: PrefixId<AF>,
        mui: u32,
    ) -> Vec<(Vec<u8>, PublicRecord<M>)> {
        let key_b = Self::prefix_mui_persistence_key(prefix, mui);

        (*self.tree.prefix(key_b))
            .into_iter()
            .map(|kv| {
                let kv = kv.unwrap();
                let (_, mui, ltime, status) = Self::parse_key(kv.0.as_ref());
                (
                    kv.0.to_vec(),
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

    pub fn _get_records_for_key<M: Meta + From<Vec<u8>>>(
        &self,
        key: &[u8],
    ) -> Vec<(PrefixId<AF>, PublicRecord<M>)> {
        (*self.tree.prefix(key))
            .into_iter()
            .map(|kv| {
                let kv = kv.unwrap();
                let (pfx, mui, ltime, status) =
                    Self::parse_key(kv.0.as_ref());

                (
                    PrefixId::<AF>::from(pfx),
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

    fn enrich_prefixes_most_recent<M: Meta>(
        &self,
        prefixes: Option<Vec<PrefixId<AF>>>,
        mui: Option<u32>,
    ) -> Option<FamilyRecord<AF, M>> {
        prefixes.map(|pfxs| {
            pfxs.iter()
                .map(|pfx| {
                    let prefix_b = if let Some(mui) = mui {
                        &Self::prefix_mui_persistence_key(*pfx, mui)
                    } else {
                        &pfx.as_bytes::<PREFIX_SIZE>().to_vec()
                    };
                    (
                        *pfx,
                        (*self.tree.prefix(prefix_b))
                            .into_iter()
                            .last()
                            .map(|kv| {
                                let kv = kv.unwrap();
                                let (_, mui, ltime, status) =
                                    Self::parse_key(kv.0.as_ref());
                                vec![PublicRecord::new(
                                    mui,
                                    ltime,
                                    status.try_into().unwrap(),
                                    kv.1.as_ref().to_vec().into(),
                                )]
                            })
                            .unwrap_or_default(),
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    fn enrich_prefix<M: Meta>(
        &self,
        prefix: Option<PrefixId<AF>>,
        mui: Option<u32>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> (Option<PrefixId<AF>>, Vec<PublicRecord<M>>) {
        match prefix {
            Some(pfx) => (
                Some(pfx),
                self.get_records_for_prefix(pfx, mui)
                    .into_iter()
                    .filter_map(|mut r| {
                        if bmin.contains(r.multi_uniq_id) {
                            if !include_withdrawn {
                                return None;
                            }
                            r.status = RouteStatus::Withdrawn;
                        }
                        Some(r)
                    })
                    .collect(),
            ),
            None => (None, vec![]),
        }
    }

    fn enrich_prefixes<M: Meta>(
        &self,
        prefixes: Option<Vec<PrefixId<AF>>>,
        mui: Option<u32>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<FamilyRecord<AF, M>> {
        prefixes.map(|recs| {
            recs.iter()
                .flat_map(move |pfx| {
                    Some((
                        *pfx,
                        self.get_records_for_prefix(*pfx, mui)
                            .into_iter()
                            .filter_map(|mut r| {
                                if bmin.contains(r.multi_uniq_id) {
                                    if !include_withdrawn {
                                        return None;
                                    }
                                    r.status = RouteStatus::Withdrawn;
                                }
                                Some(r)
                            })
                            .collect(),
                    ))
                })
                .collect()
        })
    }

    fn sparse_record_set<M: Meta>(
        &self,
        prefixes: Option<Vec<PrefixId<AF>>>,
    ) -> Option<FamilyRecord<AF, M>> {
        prefixes.map(|recs| {
            recs.iter().flat_map(|pfx| Some((*pfx, vec![]))).collect()
        })
    }

    pub(crate) fn match_prefix<M: Meta>(
        &self,
        search_pfxs: TreeQueryResult<AF>,
        options: &MatchOptions,
        bmin: &RoaringBitmap,
    ) -> FamilyQueryResult<AF, M> {
        match options.include_history {
            // All the records for all the prefixes
            IncludeHistory::All => {
                let (prefix, prefix_meta) = self.enrich_prefix(
                    search_pfxs.prefix,
                    options.mui,
                    options.include_withdrawn,
                    bmin,
                );

                FamilyQueryResult {
                    prefix,
                    prefix_meta,
                    match_type: search_pfxs.match_type,
                    less_specifics: self.enrich_prefixes(
                        search_pfxs.less_specifics,
                        options.mui,
                        options.include_withdrawn,
                        bmin,
                    ),
                    more_specifics: self.enrich_prefixes(
                        search_pfxs.more_specifics,
                        options.mui,
                        options.include_withdrawn,
                        bmin,
                    ),
                }
            }
            // Only the search prefix itself has historical records attacched
            // to it, other prefixes (less|more specifics), have no records
            // attached. Not useful with the MemoryOnly strategy (historical
            // records are neve kept in memory).
            IncludeHistory::SearchPrefix => {
                let (prefix, prefix_meta) = self.enrich_prefix(
                    search_pfxs.prefix,
                    options.mui,
                    options.include_withdrawn,
                    bmin,
                );

                FamilyQueryResult {
                    prefix,
                    prefix_meta,
                    match_type: search_pfxs.match_type,
                    less_specifics: self
                        .sparse_record_set(search_pfxs.less_specifics),
                    more_specifics: self
                        .sparse_record_set(search_pfxs.more_specifics),
                }
            }
            // Only the most recent record of the search prefix is returned
            // with the prefixes. This is used for the PersistOnly strategy.
            IncludeHistory::None => {
                let (prefix, prefix_meta) = self.enrich_prefix(
                    search_pfxs.prefix,
                    options.mui,
                    options.include_withdrawn,
                    bmin,
                );

                FamilyQueryResult {
                    prefix,
                    prefix_meta,
                    match_type: search_pfxs.match_type,
                    less_specifics: self.enrich_prefixes_most_recent(
                        search_pfxs.less_specifics,
                        options.mui,
                    ),
                    more_specifics: self.enrich_prefixes_most_recent(
                        search_pfxs.more_specifics,
                        options.mui,
                    ),
                }
            }
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
        let mut key = vec![0; PREFIX_SIZE + 4];
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

    // #[cfg(feature = "persist")]
    // pub fn parse_prefix(bytes: &[u8]) -> [u8; PREFIX_SIZE] {
    //     *bytes.first_chunk::<PREFIX_SIZE>().unwrap()
    // }

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

    pub(crate) fn prefixes_iter<'a, M: Meta + 'a>(
        &'a self,
    ) -> impl Iterator<Item = (Prefix, Vec<PublicRecord<M>>)> + 'a {
        PersistedPrefixIter::<AF, M, PREFIX_SIZE, KEY_SIZE> {
            tree_iter: self.tree.iter(),
            cur_rec: None,
            _af: PhantomData,
            _m: PhantomData,
        }
    }
}

impl<AF: AddressFamily, const PREFIX_SIZE: usize, const KEY_SIZE: usize>
    std::fmt::Debug for PersistTree<AF, PREFIX_SIZE, KEY_SIZE>
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub(crate) struct PersistedPrefixIter<
    AF: AddressFamily,
    M: Meta,
    const PREFIX_SIZE: usize,
    const KEY_SIZE: usize,
> {
    cur_rec: Option<([u8; PREFIX_SIZE], Vec<PublicRecord<M>>)>,
    tree_iter:
        Box<dyn DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>>,
    _af: PhantomData<AF>,
    _m: PhantomData<M>,
}

impl<
        AF: AddressFamily,
        M: Meta,
        const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > Iterator for PersistedPrefixIter<AF, M, PREFIX_SIZE, KEY_SIZE>
{
    type Item = (Prefix, Vec<PublicRecord<M>>);
    fn next(&mut self) -> Option<Self::Item> {
        let rec;

        // Do we already have a record in our iter struct?
        if let Some(_cur_rec) = &mut self.cur_rec {
            rec = std::mem::take(&mut self.cur_rec);
        } else {
            // No, advance to the next record in the persist tree.
            let next_rec = self.tree_iter.next();

            match next_rec {
                // The persist tree is completely done, iterator's done.
                None => {
                    return None;
                }
                Some(Ok((k, v))) => {
                    let p_k =
                        PersistTree::<AF, PREFIX_SIZE, KEY_SIZE>::parse_key(
                            k.as_ref(),
                        );
                    rec = Some((
                        p_k.0,
                        vec![PublicRecord::<M> {
                            multi_uniq_id: p_k.1,
                            ltime: p_k.2,
                            status: p_k.3.try_into().unwrap(),
                            meta: v.to_vec().into(),
                        }],
                    ));
                }
                Some(Err(_)) => {
                    // This is NOT GOOD. Both that it happens, and that we are
                    // silently ignoring it.
                    self.cur_rec = None;
                    return None;
                }
            }
        };

        if let Some(mut r_rec) = rec {
            for (k, v) in self.tree_iter.by_ref().flatten() {
                let (pfx, mui, ltime, status) =
                    PersistTree::<AF, PREFIX_SIZE, KEY_SIZE>::parse_key(
                        k.as_ref(),
                    );

                if pfx == r_rec.0 {
                    r_rec.1.push(PublicRecord {
                        meta: v.to_vec().into(),
                        multi_uniq_id: mui,
                        ltime,
                        status: status.try_into().unwrap(),
                    });
                } else {
                    self.cur_rec = Some((
                        pfx,
                        vec![PublicRecord {
                            meta: v.to_vec().into(),
                            multi_uniq_id: mui,
                            ltime,
                            status: status.try_into().unwrap(),
                        }],
                    ));
                    break;
                }
            }

            Some((
                Prefix::from(PrefixId::<AF>::from(
                    *r_rec.0.first_chunk::<PREFIX_SIZE>().unwrap(),
                )),
                r_rec.1,
            ))
        } else {
            None
        }
    }
}
