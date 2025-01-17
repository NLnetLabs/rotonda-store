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
    AddressFamily, IncludeHistory, MatchOptions, MatchType, Meta,
    PublicRecord,
};

pub struct PersistTree<
    AF: AddressFamily,
    // The size in bytes of the prefix in the persisted storage (disk), this
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
        self.tree.remove_weak(key, 0);
        // the last byte of the prefix holds the length of the prefix.
        self.counters.dec_prefixes_count(key[PREFIX_SIZE]);
    }

    pub fn get_records_for_prefix<M: Meta>(
        &self,
        prefix: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        withdrawn_muis_bmin: &RoaringBitmap,
    ) -> Vec<PublicRecord<M>> {
        match (mui, include_withdrawn) {
            // Specific mui, include withdrawn routes
            (Some(mui), true) => {
                // get the records from the persist store for the (prefix,
                // mui) tuple only.
                let prefix_b = Self::prefix_mui_persistence_key(prefix, mui);
                (*self.tree.prefix(prefix_b))
                    .into_iter()
                    .map(|kv| {
                        let kv = kv.unwrap();
                        let (_, r_mui, ltime, mut status) =
                            Self::parse_key(kv.0.as_ref());

                        // If mui is in the global withdrawn muis table, then
                        // rewrite the routestatus of the record to withdrawn.
                        if withdrawn_muis_bmin.contains(r_mui) {
                            status = RouteStatus::Withdrawn;
                        }
                        PublicRecord::new(
                            mui,
                            ltime,
                            status,
                            kv.1.as_ref().to_vec().into(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
            // Al muis, include withdrawn routes
            (None, true) => {
                // get all records for this prefix
                let prefix_b = &prefix.to_len_first_bytes::<PREFIX_SIZE>();
                (*self.tree.prefix(prefix_b))
                    .into_iter()
                    .map(|kv| {
                        let kv = kv.unwrap();
                        let (_, r_mui, ltime, mut status) =
                            Self::parse_key(kv.0.as_ref());

                        // If mui is in the global withdrawn muis table, then
                        // rewrite the routestatus of the record to withdrawn.
                        if withdrawn_muis_bmin.contains(r_mui) {
                            status = RouteStatus::Withdrawn;
                        }
                        PublicRecord::new(
                            r_mui,
                            ltime,
                            status,
                            kv.1.as_ref().to_vec().into(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
            // All muis, exclude withdrawn routes
            (None, false) => {
                // get all records for this prefix
                let prefix_b = &prefix.to_len_first_bytes::<PREFIX_SIZE>();
                (*self.tree.prefix(prefix_b))
                    .into_iter()
                    .filter_map(|kv| {
                        let kv = kv.unwrap();
                        let (_, r_mui, ltime, status) =
                            Self::parse_key(kv.0.as_ref());

                        // If mui is in the global withdrawn muis table, then
                        // skip this record
                        if status == RouteStatus::Withdrawn
                            || withdrawn_muis_bmin.contains(r_mui)
                        {
                            return None;
                        }
                        Some(PublicRecord::new(
                            r_mui,
                            ltime,
                            status,
                            kv.1.as_ref().to_vec().into(),
                        ))
                    })
                    .collect::<Vec<_>>()
            }
            // Specific mui, exclude withdrawn routes
            (Some(mui), false) => {
                // get the records from the persist store for the (prefix,
                // mui) tuple only.
                let prefix_b = Self::prefix_mui_persistence_key(prefix, mui);
                (*self.tree.prefix(prefix_b))
                    .into_iter()
                    .filter_map(|kv| {
                        let kv = kv.unwrap();
                        let (_, r_mui, ltime, status) =
                            Self::parse_key(kv.0.as_ref());

                        // If mui is in the global withdrawn muis table, then
                        // skip this record
                        if status == RouteStatus::Withdrawn
                            || withdrawn_muis_bmin.contains(r_mui)
                        {
                            return None;
                        }
                        Some(PublicRecord::new(
                            mui,
                            ltime,
                            status,
                            kv.1.as_ref().to_vec().into(),
                        ))
                    })
                    .collect::<Vec<_>>()
            }
        }

        // if let Some(mui) = mui {
        //     let prefix_b = Self::prefix_mui_persistence_key(prefix, mui);

        //     (*self.tree.prefix(prefix_b))
        //         .into_iter()
        //         .filter_map(|kv| {
        //             let kv = kv.unwrap();
        //             let (_, mui, ltime, mut status) =
        //                 Self::parse_key(kv.0.as_ref());
        //             if include_withdrawn {
        //                 // If mui is in the global withdrawn muis table, then
        //                 // rewrite the routestatus of the record to withdrawn.
        //                 if withdrawn_muis_bmin.contains(mui) {
        //                     status = RouteStatus::Withdrawn;
        //                 }
        //             // If the use does not want withdrawn routes then filter
        //             // them out here.
        //             } else if status == RouteStatus::Withdrawn {
        //                 return None;
        //             }
        //             Some(PublicRecord::new(
        //                 mui,
        //                 ltime,
        //                 status,
        //                 kv.1.as_ref().to_vec().into(),
        //             ))
        //         })
        //         .collect::<Vec<_>>()
        // } else {
        //     let prefix_b = &prefix.to_len_first_bytes::<PREFIX_SIZE>();

        //     (*self.tree.prefix(prefix_b))
        //         .into_iter()
        //         .map(|kv| {
        //             let kv = kv.unwrap();
        //             let (_, mui, ltime, status) =
        //                 Self::parse_key(kv.0.as_ref());
        //             if include_withdrawn || status != RouteStatus::Withdrawn {
        //                 Some(PublicRecord::new(
        //                     mui,
        //                     ltime,
        //                     status,
        //                     kv.1.as_ref().to_vec().into(),
        //                 ))
        //             } else {
        //                 None
        //             }
        //         })
        //         .collect::<Vec<_>>()
        // }
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
                    status,
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
                        status,
                        kv.1.as_ref().to_vec().into(),
                    ),
                )
            })
            .collect::<Vec<_>>()
    }

    pub fn get_records_for_more_specific_prefix_in_len(
        &self,
        prefix: PrefixId<AF>,
        len: u8,
    ) -> Box<
        dyn DoubleEndedIterator<
            Item = Result<
                (lsm_tree::Slice, lsm_tree::Slice),
                lsm_tree::Error,
            >,
        >,
    > {
        let start = PrefixId::new(prefix.get_net(), len);
        let end: [u8; PREFIX_SIZE] = start.inc_len().to_len_first_bytes();

        self.tree.range(start.to_len_first_bytes()..end)
    }

    // fn enrich_prefix<M: Meta>(
    //     &self,
    //     prefix: PrefixId<AF>,
    //     mui: Option<u32>,
    //     include_withdrawn: bool,
    //     bmin: &RoaringBitmap,
    // ) -> Vec<PublicRecord<M>> {
    //     self.get_records_for_prefix(prefix, mui, include_withdrawn, bmin)
    // .into_iter()
    // .filter_map(|mut r| {
    //     if !include_withdrawn && r.status == RouteStatus::Withdrawn {
    //         return None;
    //     }
    //     if bmin.contains(r.multi_uniq_id) {
    //         if !include_withdrawn {
    //             return None;
    //         }
    //         r.status = RouteStatus::Withdrawn;
    //     }
    //     Some(r)
    // })
    // .collect()
    // }

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
                        self.get_records_for_prefix(
                            *pfx,
                            mui,
                            include_withdrawn,
                            bmin,
                        )
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
        let (prefix, prefix_meta) = if let Some(prefix) = search_pfxs.prefix {
            (
                prefix,
                self.get_records_for_prefix(
                    prefix,
                    options.mui,
                    options.include_withdrawn,
                    bmin,
                ),
            )
        } else {
            return FamilyQueryResult {
                match_type: MatchType::EmptyMatch,
                prefix: None,
                prefix_meta: vec![],
                less_specifics: if options.include_less_specifics {
                    search_pfxs.less_specifics.map(|v| {
                        v.into_iter().map(|p| (p, vec![])).collect::<Vec<_>>()
                    })
                } else {
                    None
                },
                more_specifics: if options.include_more_specifics {
                    search_pfxs.more_specifics.map(|v| {
                        v.into_iter().map(|p| (p, vec![])).collect::<Vec<_>>()
                    })
                } else {
                    None
                },
            };
        };

        let mut res = match options.include_history {
            // All the records for all the prefixes
            IncludeHistory::All => FamilyQueryResult {
                prefix: Some(prefix),
                prefix_meta,
                match_type: search_pfxs.match_type,
                less_specifics: self.enrich_prefixes(
                    search_pfxs.less_specifics,
                    options.mui,
                    options.include_withdrawn,
                    bmin,
                ),
                more_specifics: search_pfxs.more_specifics.map(|ms| {
                    self.more_specific_prefix_iter_from(
                        prefix,
                        ms.iter().map(|p| p.get_len()).collect::<Vec<_>>(),
                        options.mui,
                        bmin,
                        options.include_withdrawn,
                    )
                    .collect::<Vec<_>>()
                }),
            },
            // Only the search prefix itself has historical records attached
            // to it, other prefixes (less|more specifics), have no records
            // attached. Not useful with the MemoryOnly strategy (historical
            // records are neve kept in memory).
            IncludeHistory::SearchPrefix => FamilyQueryResult {
                prefix: Some(prefix),
                prefix_meta,
                match_type: search_pfxs.match_type,
                less_specifics: self
                    .sparse_record_set(search_pfxs.less_specifics),
                more_specifics: self
                    .sparse_record_set(search_pfxs.more_specifics),
            },
            // Only the most recent record of the search prefix is returned
            // with the prefixes. This is used for the PersistOnly strategy.
            IncludeHistory::None => {
                println!("Include history: None");
                FamilyQueryResult {
                    prefix: Some(prefix),
                    prefix_meta,
                    match_type: search_pfxs.match_type,
                    less_specifics: search_pfxs.less_specifics.map(|ls| {
                        self.less_specific_prefix_iter_from(
                            ls,
                            options.mui,
                            bmin,
                            options.include_withdrawn,
                        )
                        .collect::<Vec<_>>()
                    }),
                    more_specifics: search_pfxs.more_specifics.map(|ms| {
                        self.more_specific_prefix_iter_from(
                            prefix,
                            ms.iter()
                                .map(|p| p.get_len())
                                .collect::<Vec<_>>(),
                            options.mui,
                            bmin,
                            options.include_withdrawn,
                        )
                        .collect::<Vec<_>>()
                    }),
                }
            }
        };

        res.match_type = match (options.match_type, &res) {
            (_, res) if !res.prefix_meta.is_empty() => MatchType::ExactMatch,
            (MatchType::LongestMatch | MatchType::EmptyMatch, _) => {
                if res
                    .less_specifics
                    .as_ref()
                    .is_some_and(|lp| !lp.is_empty())
                {
                    MatchType::LongestMatch
                } else {
                    MatchType::EmptyMatch
                }
            }
            (MatchType::ExactMatch, _) => MatchType::EmptyMatch,
        };

        res
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
        *key.first_chunk_mut::<PREFIX_SIZE>().unwrap() =
            prefix_id.to_len_first_bytes();

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
        *key.first_chunk_mut::<PREFIX_SIZE>().unwrap() =
            prefix_id.to_len_first_bytes();

        // mui 4 bytes
        *key[PREFIX_SIZE..PREFIX_SIZE + 4]
            .first_chunk_mut::<4>()
            .unwrap() = mui.to_le_bytes();

        key
    }

    #[cfg(feature = "persist")]
    pub fn parse_key(bytes: &[u8]) -> (PrefixId<AF>, u32, u64, RouteStatus) {
        (
            // prefix 5 or 17 bytes
            PrefixId::from(*bytes.first_chunk::<PREFIX_SIZE>().unwrap()),
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
            RouteStatus::try_from(bytes[PREFIX_SIZE + 12]).unwrap(),
        )
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

    pub(crate) fn more_specific_prefix_iter_from<'a, M: Meta + 'a>(
        &'a self,
        search_prefix: PrefixId<AF>,
        mut search_lengths: Vec<u8>,
        mui: Option<u32>,
        global_withdrawn_bmin: &'a RoaringBitmap,
        include_withdrawn: bool,
    ) -> impl Iterator<Item = (PrefixId<AF>, Vec<PublicRecord<M>>)> + 'a {
        trace!("search more specifics in the persist store.");
        if search_lengths.is_empty() {
            for l in search_prefix.get_len() + 1..=AF::BITS {
                search_lengths.push(l);
            }
        }
        println!("more specific prefix lengths {:?}", search_lengths);

        let len = search_lengths.pop().unwrap();
        let cur_range = self
            .get_records_for_more_specific_prefix_in_len(search_prefix, len);

        MoreSpecificPrefixIter {
            store: self,
            search_prefix,
            search_lengths,
            mui,
            global_withdrawn_bmin,
            include_withdrawn,
            cur_range,
            next_rec: None,
        }
    }

    pub(crate) fn less_specific_prefix_iter_from<'a, M: Meta + 'a>(
        &'a self,
        search_lengths: Vec<PrefixId<AF>>,
        mui: Option<u32>,
        global_withdrawn_bmin: &'a RoaringBitmap,
        include_withdrawn: bool,
    ) -> impl Iterator<Item = (PrefixId<AF>, Vec<PublicRecord<M>>)> + 'a {
        LessSpecificPrefixIter {
            store: self,
            search_lengths,
            mui,
            global_withdrawn_bmin,
            include_withdrawn,
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

// Iterator for all items in a lsm tree partition. The iterator used for
// this will scann through the entire tree, and there's no way to start at a
// specified offset.
pub(crate) struct PersistedPrefixIter<
    AF: AddressFamily,
    M: Meta,
    const PREFIX_SIZE: usize,
    const KEY_SIZE: usize,
> {
    cur_rec: Option<(PrefixId<AF>, Vec<PublicRecord<M>>)>,
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
            // yes, use it.
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
                            status: p_k.3,
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
                        status,
                    });
                } else {
                    self.cur_rec = Some((
                        pfx,
                        vec![PublicRecord {
                            meta: v.to_vec().into(),
                            multi_uniq_id: mui,
                            ltime,
                            status,
                        }],
                    ));
                    break;
                }
            }

            Some((r_rec.0.into(), r_rec.1))
        } else {
            None
        }
    }
}

pub(crate) struct MoreSpecificPrefixIter<
    'a,
    AF: AddressFamily + 'a,
    M: Meta + 'a,
    const PREFIX_SIZE: usize,
    const KEY_SIZE: usize,
> {
    next_rec: Option<(PrefixId<AF>, Vec<PublicRecord<M>>)>,
    store: &'a PersistTree<AF, PREFIX_SIZE, KEY_SIZE>,
    search_prefix: PrefixId<AF>,
    search_lengths: Vec<u8>,
    cur_range: Box<
        dyn DoubleEndedIterator<
            Item = lsm_tree::Result<(lsm_tree::Slice, lsm_tree::Slice)>,
        >,
    >,
    mui: Option<u32>,
    global_withdrawn_bmin: &'a RoaringBitmap,
    include_withdrawn: bool,
}

impl<
        'a,
        AF: AddressFamily + 'a,
        M: Meta + 'a,
        const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > Iterator for MoreSpecificPrefixIter<'a, AF, M, PREFIX_SIZE, KEY_SIZE>
{
    type Item = (PrefixId<AF>, Vec<PublicRecord<M>>);
    fn next(&mut self) -> Option<Self::Item> {
        let mut cur_pfx = None;
        let mut recs =
            if let Some(next_rec) = std::mem::take(&mut self.next_rec) {
                cur_pfx = Some(next_rec.0);
                next_rec.1
            } else {
                vec![]
            };
        loop {
            if let Some(Ok((k, v))) = self.cur_range.next() {
                let (pfx, mui, ltime, mut status) =
                    PersistTree::<AF, PREFIX_SIZE, KEY_SIZE>::parse_key(
                        k.as_ref(),
                    );

                if !self.include_withdrawn
                    && (status == RouteStatus::Withdrawn)
                {
                    continue;
                }

                if self.global_withdrawn_bmin.contains(mui) {
                    if !self.include_withdrawn {
                        continue;
                    } else {
                        status = RouteStatus::Withdrawn;
                    }
                }

                if let Some(m) = self.mui {
                    if m != mui {
                        continue;
                    }
                }

                cur_pfx = if cur_pfx.is_some() {
                    cur_pfx
                } else {
                    Some(pfx)
                };

                if cur_pfx.is_some_and(|c| c == pfx) {
                    recs.push(PublicRecord::new(
                        mui,
                        ltime,
                        status,
                        v.as_ref().to_vec().into(),
                    ));
                } else {
                    self.next_rec = cur_pfx.map(|_p| {
                        (
                            pfx,
                            vec![PublicRecord::new(
                                mui,
                                ltime,
                                status,
                                v.as_ref().to_vec().into(),
                            )],
                        )
                    });
                    return Some((pfx, recs));
                }
            } else {
                // See if there's a next prefix length to iterate over
                if let Some(len) = self.search_lengths.pop() {
                    self.cur_range = self
                        .store
                        .get_records_for_more_specific_prefix_in_len(
                            self.search_prefix,
                            len,
                        );
                } else {
                    return cur_pfx.map(|p| (p, recs));
                }
            }
        }
    }
}

pub(crate) struct LessSpecificPrefixIter<
    'a,
    AF: AddressFamily + 'a,
    M: Meta + 'a,
    const PREFIX_SIZE: usize,
    const KEY_SIZE: usize,
> {
    store: &'a PersistTree<AF, PREFIX_SIZE, KEY_SIZE>,
    search_lengths: Vec<PrefixId<AF>>,
    mui: Option<u32>,
    global_withdrawn_bmin: &'a RoaringBitmap,
    include_withdrawn: bool,
    _m: PhantomData<M>,
}

impl<
        'a,
        AF: AddressFamily + 'a,
        M: Meta + 'a,
        const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > Iterator for LessSpecificPrefixIter<'a, AF, M, PREFIX_SIZE, KEY_SIZE>
{
    type Item = (PrefixId<AF>, Vec<PublicRecord<M>>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(lp) = self.search_lengths.pop() {
                let recs = self.store.get_records_for_prefix(
                    lp,
                    self.mui,
                    self.include_withdrawn,
                    self.global_withdrawn_bmin,
                );
                // .into_iter()
                // .filter(|r| self.mui.is_none_or(|m| m == r.multi_uniq_id))
                // .filter(|r| {
                //     self.include_withdrawn
                //         || (!self
                //             .global_withdrawn_bmin
                //             .contains(r.multi_uniq_id)
                //             && r.status != RouteStatus::Withdrawn)
                // })
                // .collect::<Vec<_>>();

                if !recs.is_empty() {
                    return Some((lp, recs));
                }
            } else {
                return None;
            }
        }
    }
}
