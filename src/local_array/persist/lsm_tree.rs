//------------ PersistTree ---------------------------------------------------

use std::marker::PhantomData;
use std::path::Path;

use inetnum::addr::Prefix;
use log::trace;
use lsm_tree::{AbstractTree, KvPair};
use roaring::RoaringBitmap;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, NativeEndian, TryFromBytes,
    Unaligned, U32, U64,
};

use crate::local_array::types::{PrefixId, RouteStatus};
use crate::prefix_record::{ValueHeader, ZeroCopyRecord};
use crate::rib::Counters;
use crate::{AddressFamily, Meta, PublicRecord};

type ZeroCopyError<'a, T> = zerocopy::ConvertError<
    zerocopy::AlignmentError<&'a [u8], T>,
    zerocopy::SizeError<&'a [u8], T>,
    zerocopy::ValidityError<&'a [u8], T>,
>;
type ZeroCopyMutError<'a, T> = zerocopy::ConvertError<
    zerocopy::AlignmentError<&'a mut [u8], T>,
    zerocopy::SizeError<&'a mut [u8], T>,
    zerocopy::ValidityError<&'a mut [u8], T>,
>;

pub trait KeySize<AF: AddressFamily, const KEY_SIZE: usize>:
    TryFromBytes + KnownLayout + IntoBytes + Unaligned + Immutable
{
    fn mut_from_bytes(
        bytes: &mut [u8],
    ) -> std::result::Result<&mut Self, ZeroCopyMutError<'_, Self>> {
        Self::try_mut_from_bytes(bytes.as_mut_bytes())
    }

    fn from_bytes(
        bytes: &[u8],
    ) -> std::result::Result<&Self, ZeroCopyError<'_, Self>> {
        Self::try_ref_from_bytes(bytes.as_bytes())
    }

    fn header(bytes: &[u8]) -> &LongKey<AF> {
        LongKey::try_ref_from_bytes(bytes.as_bytes()).unwrap()
    }

    fn header_mut(bytes: &mut [u8]) -> &mut LongKey<AF> {
        trace!("key size {}", KEY_SIZE);
        trace!("bytes len {}", bytes.len());
        LongKey::try_mut_from_bytes(bytes.as_mut_bytes()).unwrap()
    }

    fn short_key(bytes: &[u8]) -> &ShortKey<AF> {
        trace!("short key from bytes {:?}", bytes);
        let s_b = &bytes[..(AF::BITS as usize / 8) + 6];
        trace!("short key {:?}", s_b);
        ShortKey::try_ref_from_prefix(bytes).unwrap().0
    }
}

#[derive(Debug, KnownLayout, Immutable, FromBytes, Unaligned, IntoBytes)]
#[repr(C)]
pub struct ShortKey<AF: AddressFamily> {
    prefix: PrefixId<AF>,
    mui: U32<NativeEndian>,
}

#[derive(
    Copy,
    Clone,
    Debug,
    KnownLayout,
    Immutable,
    TryFromBytes,
    Unaligned,
    IntoBytes,
)]
#[repr(C)]
pub struct LongKey<AF: AddressFamily> {
    prefix: PrefixId<AF>,     // 4 or 16
    mui: U32<NativeEndian>,   // 4
    ltime: U64<NativeEndian>, // 8
    status: RouteStatus,      // 1
} // 17 or 29

impl<AF: AddressFamily, const KEY_SIZE: usize> KeySize<AF, KEY_SIZE>
    for ShortKey<AF>
{
    // fn new_write_key(
    //     prefix: PrefixId<AF>,
    //     mui: u32,
    //     _ltime: u64,
    //     _status: RouteStatus,
    // ) -> [u8; KEY_SIZE] {
    //     *Self::from((prefix, mui))
    //         .as_bytes()
    //         .first_chunk::<KEY_SIZE>()
    //         .unwrap()
    // }
}

impl<AF: AddressFamily> From<(PrefixId<AF>, u32)> for ShortKey<AF> {
    fn from(value: (PrefixId<AF>, u32)) -> Self {
        Self {
            prefix: value.0,
            mui: value.1.into(),
        }
    }
}

impl<AF: AddressFamily, const KEY_SIZE: usize> KeySize<AF, KEY_SIZE>
    for LongKey<AF>
{
    // fn new_write_key(
    //     prefix: PrefixId<AF>,
    //     mui: u32,
    //     ltime: u64,
    //     status: RouteStatus,
    // ) -> [u8; KEY_SIZE] {
    //     *Self::from((prefix, mui, ltime, status))
    //         .as_bytes()
    //         .first_chunk::<KEY_SIZE>()
    //         .unwrap()
    // }
}

impl<AF: AddressFamily> From<(PrefixId<AF>, u32, u64, RouteStatus)>
    for LongKey<AF>
{
    fn from(value: (PrefixId<AF>, u32, u64, RouteStatus)) -> Self {
        Self {
            prefix: value.0,
            mui: value.1.into(),
            ltime: value.2.into(),
            status: value.3,
        }
    }
}

pub struct PersistTree<
    AF: AddressFamily,
    K: KeySize<AF, KEY_SIZE>,
    // The size in bytes of the prefix in the persisted storage (disk), this
    // amounnts to the bytes for the addres (4 for IPv4, 16 for IPv6) and 1
    // bytefor the prefix length.
    // const PREFIX_SIZE: usize,
    // The size in bytes of the complete key in the persisted storage, this
    // is PREFIX_SIZE bytes (4; 16) + mui size (4) + ltime (8)
    const KEY_SIZE: usize,
> {
    tree: lsm_tree::Tree,
    counters: Counters,
    _af: PhantomData<AF>,
    _k: PhantomData<K>,
}

impl<
        AF: AddressFamily,
        K: KeySize<AF, KEY_SIZE>,
        // const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > PersistTree<AF, K, KEY_SIZE>
{
    pub fn new(persist_path: &Path) -> PersistTree<AF, K, KEY_SIZE> {
        PersistTree::<AF, K, KEY_SIZE> {
            tree: lsm_tree::Config::new(persist_path).open().unwrap(),
            counters: Counters::default(),
            _af: PhantomData,
            _k: PhantomData,
        }
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> (u32, u32) {
        self.tree.insert::<&[u8], &[u8]>(key, value, 0)
    }

    pub fn remove(&self, key: &[u8]) {
        self.tree.remove_weak(key, 0);
        // the first byte of the prefix holds the length of the prefix.
        self.counters.dec_prefixes_count(key[0]);
    }

    pub fn get_records_for_prefix(
        &self,
        prefix: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        withdrawn_muis_bmin: &RoaringBitmap,
    ) -> Option<Vec<Vec<u8>>> {
        match (mui, include_withdrawn) {
            // Specific mui, include withdrawn routes
            (Some(mui), true) => {
                // get the records from the persist store for the (prefix,
                // mui) tuple only.
                let prefix_b = ShortKey::from((prefix, mui));
                self.tree
                    .prefix(prefix_b.as_bytes(), None, None)
                    .map(|kv| {
                        kv.map(|kv| {
                            trace!("mui i persist kv pair found: {:?}", kv);
                            let mut bytes = [kv.0, kv.1].concat();
                            // let key: &mut LongKey<AF> =
                            //     LongKey::try_mut_from_bytes(
                            //         bytes.as_mut_bytes(),
                            //     )
                            //     .unwrap();
                            let key = K::header_mut(&mut bytes[..KEY_SIZE]);
                            // If mui is in the global withdrawn muis table,
                            // then rewrite the routestatus of the record
                            // to withdrawn.
                            if withdrawn_muis_bmin.contains(key.mui.into()) {
                                key.status = RouteStatus::Withdrawn;
                            }
                            bytes
                        })
                    })
                    .collect::<Vec<lsm_tree::Result<Vec<u8>>>>()
                    .into_iter()
                    .collect::<lsm_tree::Result<Vec<Vec<u8>>>>()
                    .ok()
                    .and_then(
                        |recs| {
                            if recs.is_empty() {
                                None
                            } else {
                                Some(recs)
                            }
                        },
                    )
            }
            // Al muis, include withdrawn routes
            (None, true) => {
                // get all records for this prefix
                // let prefix_b = &prefix.to_len_first_bytes::<PREFIX_SIZE>();
                self.tree
                    .prefix(prefix.as_bytes(), None, None)
                    // .into_iter()
                    .map(|kv| {
                        kv.map(|kv| {
                            trace!("n i persist kv pair found: {:?}", kv);
                            // let kv = kv.unwrap();
                            // let (_, r_mui, ltime, mut status) =
                            //     Self::parse_key(kv.0.as_ref());

                            // If mui is in the global withdrawn muis table, then
                            // rewrite the routestatus of the record to withdrawn.
                            let mut bytes = [kv.0, kv.1].concat();
                            trace!("bytes {:?}", bytes);
                            // let key: &mut LongKey<AF> =
                            //     LongKey::try_mut_from_bytes(
                            //         bytes.as_mut_bytes(),
                            //     )
                            //     .unwrap();
                            let key = K::header_mut(&mut bytes[..KEY_SIZE]);
                            trace!("key {:?}", key);
                            trace!("wm_bmin {:?}", withdrawn_muis_bmin);
                            if withdrawn_muis_bmin.contains(key.mui.into()) {
                                trace!("rewrite status");
                                key.status = RouteStatus::Withdrawn;
                            }
                            // PublicRecord::new(
                            //     r_mui,
                            //     ltime,
                            //     status,
                            //     kv.1.as_ref().to_vec().into(),
                            // )
                            bytes
                        })
                    })
                    .collect::<Vec<lsm_tree::Result<Vec<u8>>>>()
                    .into_iter()
                    .collect::<lsm_tree::Result<Vec<Vec<u8>>>>()
                    .ok()
                    .and_then(
                        |recs| {
                            if recs.is_empty() {
                                None
                            } else {
                                Some(recs)
                            }
                        },
                    )
            }
            // All muis, exclude withdrawn routes
            (None, false) => {
                // get all records for this prefix
                // let prefix_b = &prefix.to_len_first_bytes::<PREFIX_SIZE>();
                self.tree
                    .prefix(prefix.as_bytes(), None, None)
                    .filter_map(|kv| {
                        kv.map(|kv| {
                            trace!("n f persist kv pair found: {:?}", kv);
                            let mut bytes = [kv.0, kv.1].concat();
                            // let key: &mut LongKey<AF> =
                            //     LongKey::try_mut_from_bytes(
                            //         bytes.as_mut_bytes(),
                            //     )
                            //     .unwrap();
                            let header =
                                K::header_mut(&mut bytes[..KEY_SIZE]);
                            // If mui is in the global withdrawn muis table,
                            // then skip this record
                            trace!("header {}", Prefix::from(header.prefix));
                            trace!(
                                "status {}",
                                header.status == RouteStatus::Withdrawn
                            );
                            if header.status == RouteStatus::Withdrawn
                                || withdrawn_muis_bmin
                                    .contains(header.mui.into())
                            {
                                trace!(
                                    "NOT returning {} {}",
                                    Prefix::from(header.prefix),
                                    header.mui
                                );
                                return None;
                            }
                            trace!(
                                "RETURNING {} {}",
                                Prefix::from(header.prefix),
                                header.mui
                            );
                            Some(bytes)
                        })
                        .transpose()
                    })
                    .collect::<Vec<lsm_tree::Result<Vec<u8>>>>()
                    .into_iter()
                    .collect::<lsm_tree::Result<Vec<Vec<u8>>>>()
                    .ok()
                    .and_then(
                        |recs| {
                            if recs.is_empty() {
                                None
                            } else {
                                Some(recs)
                            }
                        },
                    )
            }
            // Specific mui, exclude withdrawn routes
            (Some(mui), false) => {
                // get the records from the persist store for the (prefix,
                // mui) tuple only.
                let prefix_b = ShortKey::<AF>::from((prefix, mui));
                self.tree
                    .prefix(prefix_b.as_bytes(), None, None)
                    // .into_iter()
                    .filter_map(|kv| {
                        // let kv = kv.unwrap();
                        kv.map(|kv| {
                            trace!("mui f persist kv pair found: {:?}", kv);
                            let bytes = [kv.0, kv.1].concat();
                            // let (_, r_mui, ltime, status) =
                            //     Self::parse_key(kv.0.as_ref());

                            // let key: &mut LongKey<AF> =
                            //     LongKey::try_mut_from_bytes(
                            //         bytes.as_mut_bytes(),
                            //     )
                            //     .unwrap();
                            let key = K::header(&bytes[..KEY_SIZE]);
                            // If mui is in the global withdrawn muis table, then
                            // skip this record
                            if key.status == RouteStatus::Withdrawn
                                || withdrawn_muis_bmin
                                    .contains(key.mui.into())
                            {
                                return None;
                            }
                            // Some(PublicRecord::new(
                            //     mui,
                            //     ltime,
                            //     status,
                            //     kv.1.as_ref().to_vec().into(),
                            // ))
                            Some(bytes)
                        })
                        .transpose()
                    })
                    .collect::<Vec<lsm_tree::Result<Vec<u8>>>>()
                    .into_iter()
                    .collect::<lsm_tree::Result<Vec<Vec<u8>>>>()
                    .ok()
                    .and_then(
                        |recs| {
                            if recs.is_empty() {
                                None
                            } else {
                                Some(recs)
                            }
                        },
                    )
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

    pub fn get_most_recent_record_for_prefix_mui(
        &self,
        prefix: PrefixId<AF>,
        mui: u32,
    ) -> Option<Vec<u8>> {
        trace!("get most recent record for prefix mui combo");
        let key_b = ShortKey::from((prefix, mui));

        (*self.tree.prefix(key_b.as_bytes(), None, None))
            .into_iter()
            .map(move |kv| {
                let kv = kv.unwrap();
                [kv.0, kv.1].concat()
            })
            .max_by(|b0, b1| K::header(b0).ltime.cmp(&K::header(b1).ltime))
    }

    pub(crate) fn get_records_with_keys_for_prefix_mui(
        &self,
        prefix: PrefixId<AF>,
        mui: u32,
    ) -> Vec<Vec<u8>> {
        // let key_b: [u8; KEY_SIZE] =
        //     ShortKey::from((prefix, mui)).as_key_size_bytes();

        let key_b = ShortKey::from((prefix, mui));

        (*self.tree.prefix(key_b.as_bytes(), None, None))
            .into_iter()
            .map(|kv| {
                let kv = kv.unwrap();
                [kv.0, kv.1].concat()
                // let (_, mui, ltime, status) = Self::parse_key(kv.0.as_ref());
                // (
                //     kv.0.to_vec(),
                //     PublicRecord::new(
                //         mui,
                //         ltime,
                //         status,
                //         kv.1.as_ref().to_vec().into(),
                //     ),
                // )
            })
            .collect::<Vec<_>>()
    }

    // fn get_records_for_more_specific_prefix_in_len(
    //     &self,
    //     prefix: PrefixId<AF>,
    //     len: u8,
    // ) -> Box<
    //     dyn DoubleEndedIterator<
    //         Item = Result<
    //             (lsm_tree::Slice, lsm_tree::Slice),
    //             lsm_tree::Error,
    //         >,
    //     >,
    // > {
    //     let start = PrefixId::new(prefix.get_net(), len);
    //     let end: [u8; PREFIX_SIZE] = start.inc_len().to_len_first_bytes();

    //     self.tree.range(start.to_len_first_bytes()..end, None, None)
    // }

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

    // fn enrich_prefixes<M: Meta>(
    //     &self,
    //     prefixes: Option<Vec<PrefixId<AF>>>,
    //     mui: Option<u32>,
    //     include_withdrawn: bool,
    //     bmin: &RoaringBitmap,
    // ) -> Option<FamilyRecord<AF, M>> {
    //     prefixes.map(|recs| {
    //         recs.iter()
    //             .flat_map(move |pfx| {
    //                 Some((
    //                     *pfx,
    //                     self.get_records_for_prefix(
    //                         *pfx,
    //                         mui,
    //                         include_withdrawn,
    //                         bmin,
    //                     )
    //                     .into_iter()
    //                     .filter_map(|mut r| {
    //                         if bmin.contains(r.multi_uniq_id) {
    //                             if !include_withdrawn {
    //                                 return None;
    //                             }
    //                             r.status = RouteStatus::Withdrawn;
    //                         }
    //                         Some(r)
    //                     })
    //                     .collect(),
    //                 ))
    //             })
    //             .collect()
    //     })
    // }

    // fn sparse_record_set<M: Meta>(
    //     &self,
    //     prefixes: Option<Vec<PrefixId<AF>>>,
    // ) -> Option<FamilyRecord<AF, M>> {
    //     prefixes.map(|recs| {
    //         recs.iter().flat_map(|pfx| Some((*pfx, vec![]))).collect()
    //     })
    // }

    // pub(crate) fn match_prefix<M: Meta>(
    //     &self,
    //     search_pfxs: TreeQueryResult<AF>,
    //     options: &MatchOptions,
    //     bmin: &RoaringBitmap,
    // ) -> FamilyQueryResult<AF, M> {
    //     let (prefix, prefix_meta) = if let Some(prefix) = search_pfxs.prefix {
    //         (
    //             prefix,
    //             self.get_records_for_prefix(
    //                 prefix,
    //                 options.mui,
    //                 options.include_withdrawn,
    //                 bmin,
    //             ),
    //         )
    //     } else {
    //         return FamilyQueryResult {
    //             match_type: MatchType::EmptyMatch,
    //             prefix: None,
    //             prefix_meta: vec![],
    //             less_specifics: if options.include_less_specifics {
    //                 search_pfxs.less_specifics.map(|v| {
    //                     v.into_iter().map(|p| (p, vec![])).collect::<Vec<_>>()
    //                 })
    //             } else {
    //                 None
    //             },
    //             more_specifics: if options.include_more_specifics {
    //                 search_pfxs.more_specifics.map(|v| {
    //                     v.into_iter().map(|p| (p, vec![])).collect::<Vec<_>>()
    //                 })
    //             } else {
    //                 None
    //             },
    //         };
    //     };

    //     let mut res = match options.include_history {
    //         // All the records for all the prefixes
    //         IncludeHistory::All => FamilyQueryResult {
    //             prefix: Some(prefix),
    //             prefix_meta,
    //             match_type: search_pfxs.match_type,
    //             less_specifics: self.enrich_prefixes(
    //                 search_pfxs.less_specifics,
    //                 options.mui,
    //                 options.include_withdrawn,
    //                 bmin,
    //             ),
    //             more_specifics: search_pfxs.more_specifics.map(|ms| {
    //                 self.more_specific_prefix_iter_from(
    //                     prefix,
    //                     ms.iter().map(|p| p.get_len()).collect::<Vec<_>>(),
    //                     options.mui,
    //                     bmin,
    //                     options.include_withdrawn,
    //                 )
    //                 .collect::<Vec<_>>()
    //             }),
    //         },
    //         // Only the search prefix itself has historical records attached
    //         // to it, other prefixes (less|more specifics), have no records
    //         // attached. Not useful with the MemoryOnly strategy (historical
    //         // records are neve kept in memory).
    //         IncludeHistory::SearchPrefix => FamilyQueryResult {
    //             prefix: Some(prefix),
    //             prefix_meta,
    //             match_type: search_pfxs.match_type,
    //             less_specifics: self
    //                 .sparse_record_set(search_pfxs.less_specifics),
    //             more_specifics: self
    //                 .sparse_record_set(search_pfxs.more_specifics),
    //         },
    //         // Only the most recent record of the search prefix is returned
    //         // with the prefixes. This is used for the PersistOnly strategy.
    //         IncludeHistory::None => {
    //             println!("Include history: None");
    //             FamilyQueryResult {
    //                 prefix: Some(prefix),
    //                 prefix_meta,
    //                 match_type: search_pfxs.match_type,
    //                 less_specifics: search_pfxs.less_specifics.map(|ls| {
    //                     self.less_specific_prefix_iter_from(
    //                         ls,
    //                         options.mui,
    //                         bmin,
    //                         options.include_withdrawn,
    //                     )
    //                     .collect::<Vec<_>>()
    //                 }),
    //                 more_specifics: search_pfxs.more_specifics.map(|ms| {
    //                     self.more_specific_prefix_iter_from(
    //                         prefix,
    //                         ms.iter()
    //                             .map(|p| p.get_len())
    //                             .collect::<Vec<_>>(),
    //                         options.mui,
    //                         bmin,
    //                         options.include_withdrawn,
    //                     )
    //                     .collect::<Vec<_>>()
    //                 }),
    //             }
    //         }
    //     };

    //     res.match_type = match (options.match_type, &res) {
    //         (_, res) if !res.prefix_meta.is_empty() => MatchType::ExactMatch,
    //         (MatchType::LongestMatch | MatchType::EmptyMatch, _) => {
    //             if res
    //                 .less_specifics
    //                 .as_ref()
    //                 .is_some_and(|lp| !lp.is_empty())
    //             {
    //                 MatchType::LongestMatch
    //             } else {
    //                 MatchType::EmptyMatch
    //             }
    //         }
    //         (MatchType::ExactMatch, _) => MatchType::EmptyMatch,
    //     };

    //     res
    // }

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

    pub(crate) fn persist_record_w_long_key<M: Meta>(
        &self,
        prefix: PrefixId<AF>,
        record: &PublicRecord<M>,
    ) {
        self.insert(
            LongKey::from((
                prefix,
                record.multi_uniq_id,
                record.ltime,
                record.status,
            ))
            .as_bytes(),
            record.meta.as_ref(),
        );
    }

    pub(crate) fn persist_record_w_short_key<M: Meta>(
        &self,
        prefix: PrefixId<AF>,
        record: &PublicRecord<M>,
    ) {
        trace!("Record to persist {}", record);
        let mut value = ValueHeader {
            ltime: record.ltime,
            status: record.status,
        }
        .as_bytes()
        .to_vec();

        trace!("header in bytes {:?}", value);

        value.extend_from_slice(record.meta.as_ref());

        trace!("value complete {:?}", value);

        self.insert(
            ShortKey::from((prefix, record.multi_uniq_id)).as_bytes(),
            &value,
        );
    }

    pub(crate) fn rewrite_header_for_record(
        &self,
        header: ValueHeader,
        record_b: &[u8],
    ) {
        let record = ZeroCopyRecord::<AF>::try_ref_from_prefix(record_b)
            .unwrap()
            .0;
        let key = ShortKey::from((record.prefix, record.multi_uniq_id));
        trace!("insert key {:?}", key);

        header
            .as_bytes()
            .to_vec()
            .extend_from_slice(record.meta.as_ref());

        self.insert(key.as_bytes(), header.as_bytes());
    }

    pub(crate) fn insert_empty_record(
        &self,
        prefix: PrefixId<AF>,
        mui: u32,
        ltime: u64,
    ) {
        self.insert(
            LongKey::from((prefix, mui, ltime, RouteStatus::Withdrawn))
                .as_bytes(),
            &[],
        );
    }

    pub(crate) fn prefixes_iter(
        &self,
    ) -> impl Iterator<Item = Vec<Vec<u8>>> + '_ {
        PersistedPrefixIter::<AF, K, KEY_SIZE> {
            tree_iter: self.tree.iter(None, None),
            cur_rec: None,
            _af: PhantomData,
            _k: PhantomData,
        }
    }

    // pub(crate) fn more_specific_prefix_iter_from<'a, M: Meta + 'a>(
    //     &'a self,
    //     search_prefix: PrefixId<AF>,
    //     mut search_lengths: Vec<u8>,
    //     mui: Option<u32>,
    //     global_withdrawn_bmin: &'a RoaringBitmap,
    //     include_withdrawn: bool,
    // ) -> impl Iterator<Item = (PrefixId<AF>, Vec<Vec<u8>>)> + 'a {
    //     trace!("search more specifics in the persist store.");
    //     if search_lengths.is_empty() {
    //         for l in search_prefix.get_len() + 1..=AF::BITS {
    //             search_lengths.push(l);
    //         }
    //     }
    //     println!("more specific prefix lengths {:?}", search_lengths);

    //     let len = search_lengths.pop().unwrap();
    //     let cur_range = self
    //         .get_records_for_more_specific_prefix_in_len(search_prefix, len);

    //     MoreSpecificPrefixIter {
    //         store: self,
    //         search_prefix,
    //         search_lengths,
    //         mui,
    //         global_withdrawn_bmin,
    //         include_withdrawn,
    //         cur_range,
    //         next_rec: None,
    //     }
    // }

    // pub(crate) fn less_specific_prefix_iter_from<'a, M: Meta + 'a>(
    //     &'a self,
    //     search_lengths: Vec<PrefixId<AF>>,
    //     mui: Option<u32>,
    //     global_withdrawn_bmin: &'a RoaringBitmap,
    //     include_withdrawn: bool,
    // ) -> impl Iterator<Item = (PrefixId<AF>, Vec<PublicRecord<M>>)> + 'a {
    //     LessSpecificPrefixIter {
    //         store: self,
    //         search_lengths,
    //         mui,
    //         global_withdrawn_bmin,
    //         include_withdrawn,
    //         _m: PhantomData,
    //     }
    // }
}

impl<
        AF: AddressFamily,
        K: KeySize<AF, KEY_SIZE>,
        // const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > std::fmt::Debug for PersistTree<AF, K, KEY_SIZE>
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
    K: KeySize<AF, KEY_SIZE>,
    // M: Meta,
    // const PREFIX_SIZE: usize,
    const KEY_SIZE: usize,
> {
    cur_rec: Option<Vec<Vec<u8>>>,
    tree_iter:
        Box<dyn DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>>,
    _af: PhantomData<AF>,
    // _m: PhantomData<M>,
    _k: PhantomData<K>,
}

impl<
        AF: AddressFamily,
        K: KeySize<AF, KEY_SIZE>,
        // M: Meta,
        // const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > Iterator for PersistedPrefixIter<AF, K, KEY_SIZE>
{
    type Item = Vec<Vec<u8>>;
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
                    // let p_k =
                    //     PersistTree::<AF, K, PREFIX_SIZE, KEY_SIZE>::parse_key(
                    //         k.as_ref(),
                    //     );
                    // rec = Some((
                    //     p_k.0,
                    //     vec![PublicRecord::<M> {
                    //         multi_uniq_id: p_k.1,
                    //         ltime: p_k.2,
                    //         status: p_k.3,
                    //         meta: v.to_vec().into(),
                    //     }],
                    // ));
                    rec = Some(vec![[k, v].concat()]);
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
            let outer_pfx = K::header(&r_rec[0]).prefix;

            for (k, v) in self.tree_iter.by_ref().flatten() {
                // let (pfx, mui, ltime, status) =
                //     PersistTree::<AF, PREFIX_SIZE, KEY_SIZE>::parse_key(
                //         k.as_ref(),
                //     );
                let header = K::header(&k);

                if header.prefix == outer_pfx {
                    r_rec.push([k, v].concat());
                    // r_rec.1.push(PublicRecord {
                    //     meta: v.to_vec().into(),
                    //     multi_uniq_id: header.mui.into(),
                    //     ltime: header.ltime.into(),
                    //     status: header.status,
                    // });
                } else {
                    self.cur_rec = Some(vec![[k, v].concat()]);
                    // self.cur_rec = Some((
                    //     header.prefix,
                    //     vec![PublicRecord {
                    //         meta: v.to_vec().into(),
                    //         multi_uniq_id: header.mui.into(),
                    //         ltime: header.ltime.into(),
                    //         status: header.status.into(),
                    //     }],
                    // ));
                    break;
                }
            }

            Some(r_rec)
        } else {
            None
        }
    }
}

// pub(crate) struct MoreSpecificPrefixIter<
//     'a,
//     AF: AddressFamily + 'a,
//     K: KeySize<AF, KEY_SIZE> + 'a,
//     // M: Meta + 'a,
//     const PREFIX_SIZE: usize,
//     const KEY_SIZE: usize,
// > {
//     next_rec: Option<(PrefixId<AF>, Vec<Vec<u8>>)>,
//     store: &'a PersistTree<AF, K, PREFIX_SIZE, KEY_SIZE>,
//     search_prefix: PrefixId<AF>,
//     search_lengths: Vec<u8>,
//     cur_range: Box<
//         dyn DoubleEndedIterator<
//             Item = lsm_tree::Result<(lsm_tree::Slice, lsm_tree::Slice)>,
//         >,
//     >,
//     mui: Option<u32>,
//     global_withdrawn_bmin: &'a RoaringBitmap,
//     include_withdrawn: bool,
// }

// impl<
//         'a,
//         AF: AddressFamily + 'a,
//         K: KeySize<AF, KEY_SIZE> + 'a,
//         // M: Meta + 'a,
//         const PREFIX_SIZE: usize,
//         const KEY_SIZE: usize,
//     > Iterator for MoreSpecificPrefixIter<'a, AF, K, PREFIX_SIZE, KEY_SIZE>
// {
//     type Item = (PrefixId<AF>, Vec<Vec<u8>>);
//     fn next(&mut self) -> Option<Self::Item> {
//         let mut cur_pfx = None;
//         let mut recs =
//             if let Some(next_rec) = std::mem::take(&mut self.next_rec) {
//                 cur_pfx = Some(next_rec.0);
//                 next_rec.1
//             } else {
//                 vec![]
//             };
//         loop {
//             if let Some(Ok((k, v))) = self.cur_range.next() {
//                 // let (pfx, mui, ltime, mut status) =
//                 let mut v = [k, v].concat();
//                 let key = K::header_mut(&mut v);

//                 if !self.include_withdrawn
//                     && (key.status == RouteStatus::Withdrawn)
//                 {
//                     continue;
//                 }

//                 if self.global_withdrawn_bmin.contains(key.mui.into()) {
//                     if !self.include_withdrawn {
//                         continue;
//                     } else {
//                         key.status = RouteStatus::Withdrawn;
//                     }
//                 }

//                 if let Some(m) = self.mui {
//                     if m != key.mui.into() {
//                         continue;
//                     }
//                 }

//                 cur_pfx = if cur_pfx.is_some() {
//                     cur_pfx
//                 } else {
//                     Some(key.prefix)
//                 };

//                 if cur_pfx.is_some_and(|c| c == key.prefix) {
//                     // recs.push(PublicRecord::new(
//                     //     mui,
//                     //     ltime,
//                     //     status,
//                     //     v.as_ref().to_vec().into(),
//                     // ));
//                     recs.push(v);
//                 } else {
//                     self.next_rec = cur_pfx.map(|_| {
//                         (key.prefix, vec![v])
//                         // vec![PublicRecord::new(
//                         //     mui,
//                         //     ltime,
//                         //     status,
//                         //     v.as_ref().to_vec().into(),
//                         // )],
//                     });
//                     return Some((key.prefix, recs));
//                 }
//             } else {
//                 // See if there's a next prefix length to iterate over
//                 if let Some(len) = self.search_lengths.pop() {
//                     self.cur_range = self
//                         .store
//                         .get_records_for_more_specific_prefix_in_len(
//                             self.search_prefix,
//                             len,
//                         );
//                 } else {
//                     return cur_pfx.map(|p| (p, recs));
//                 }
//             }
//         }
//     }
// }

// pub(crate) struct LessSpecificPrefixIter<
//     'a,
//     AF: AddressFamily + 'a,
//     K: KeySize<AF, KEY_SIZE> + 'a,
//     M: Meta + 'a,
//     const PREFIX_SIZE: usize,
//     const KEY_SIZE: usize,
// > {
//     store: &'a PersistTree<AF, K, PREFIX_SIZE, KEY_SIZE>,
//     search_lengths: Vec<PrefixId<AF>>,
//     mui: Option<u32>,
//     global_withdrawn_bmin: &'a RoaringBitmap,
//     include_withdrawn: bool,
//     _m: PhantomData<M>,
// }

// impl<
//         'a,
//         AF: AddressFamily + 'a,
//         K: KeySize<AF, KEY_SIZE> + 'a,
//         M: Meta + 'a,
//         const PREFIX_SIZE: usize,
//         const KEY_SIZE: usize,
//     > Iterator
//     for LessSpecificPrefixIter<'a, AF, K, M, PREFIX_SIZE, KEY_SIZE>
// {
//     type Item = (PrefixId<AF>, Vec<PublicRecord<M>>);
//     fn next(&mut self) -> Option<Self::Item> {
//         loop {
//             if let Some(lp) = self.search_lengths.pop() {
//                 let recs = self.store.get_records_for_prefix(
//                     lp,
//                     self.mui,
//                     self.include_withdrawn,
//                     self.global_withdrawn_bmin,
//                 );
//                 // .into_iter()
//                 // .filter(|r| self.mui.is_none_or(|m| m == r.multi_uniq_id))
//                 // .filter(|r| {
//                 //     self.include_withdrawn
//                 //         || (!self
//                 //             .global_withdrawn_bmin
//                 //             .contains(r.multi_uniq_id)
//                 //             && r.status != RouteStatus::Withdrawn)
//                 // })
//                 // .collect::<Vec<_>>();

//                 if !recs.is_empty() {
//                     return Some((lp, recs));
//                 }
//             } else {
//                 return None;
//             }
//         }
//     }
// }
