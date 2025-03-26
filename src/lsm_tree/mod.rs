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

use crate::errors::{FatalError, FatalResult, PrefixStoreError};
use crate::prefix_record::Meta;
use crate::stats::Counters;
use crate::types::prefix_record::{ValueHeader, ZeroCopyRecord};
use crate::types::{AddressFamily, Record};
use crate::types::{PrefixId, RouteStatus};

//------------ Key -----------------------------------------------------------

// The type of key used to create entries in the LsmTree. Can be short or
// long. Short keys overwrite existing values for existing (prefix, mui)
// pairs, whereas long keys append values with existing (prefix, mui), thus
// creating persisted historical records.

pub(crate) trait Key<AF: AddressFamily, const KEY_SIZE: usize>:
    TryFromBytes + KnownLayout + IntoBytes + Unaligned + Immutable
{
    // Try to extract a header from the bytes for reading only. If this
    // somehow fails, we don't know what to do anymore. Data may be corrupted,
    // so it probably should not be retried.
    fn header(bytes: &[u8]) -> Result<&LongKey<AF>, FatalError> {
        LongKey::try_ref_from_bytes(bytes.as_bytes()).map_err(|_| FatalError)
    }

    // Try to extract a header for writing. If this somehow fails, we most
    //probably cannot write to it anymore. This is fatal. The application
    //should exit, data integrity (on disk) should be verified.
    fn header_mut(bytes: &mut [u8]) -> Result<&mut LongKey<AF>, FatalError> {
        trace!("key size {}", KEY_SIZE);
        trace!("bytes len {}", bytes.len());
        LongKey::try_mut_from_bytes(bytes.as_mut_bytes())
            .map_err(|_| FatalError)
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
    prefix: PrefixId<AF>,     // 1 + (4 or 16)
    mui: U32<NativeEndian>,   // 4
    ltime: U64<NativeEndian>, // 8
    status: RouteStatus,      // 1
} // 18 or 30

impl<AF: AddressFamily, const KEY_SIZE: usize> Key<AF, KEY_SIZE>
    for ShortKey<AF>
{
}

impl<AF: AddressFamily> From<(PrefixId<AF>, u32)> for ShortKey<AF> {
    fn from(value: (PrefixId<AF>, u32)) -> Self {
        Self {
            prefix: value.0,
            mui: value.1.into(),
        }
    }
}

impl<AF: AddressFamily, const KEY_SIZE: usize> Key<AF, KEY_SIZE>
    for LongKey<AF>
{
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

//------------ LsmTree -------------------------------------------------------

// The log-structured merge tree that backs the persistent store (on disk).

pub struct LsmTree<
    // The address family that this tree stores. IPv4 or IPv6.
    AF: AddressFamily,
    // The Key type for this tree. This can basically be a long key, if the
    // store needs to store historical records, or a short key, if it should
    // overwrite records for (prefix, mui) pairs, effectively only keeping the
    // current state.
    K: Key<AF, KEY_SIZE>,
    // The size in bytes of the complete key in the persisted storage, this
    // is PREFIX_SIZE bytes (4; 16) + mui size (4) + ltime (8)
    const KEY_SIZE: usize,
> {
    tree: lsm_tree::Tree,
    counters: Counters,
    _af: PhantomData<AF>,
    _k: PhantomData<K>,
}

impl<AF: AddressFamily, K: Key<AF, KEY_SIZE>, const KEY_SIZE: usize>
    LsmTree<AF, K, KEY_SIZE>
{
    pub fn new(persist_path: &Path) -> FatalResult<LsmTree<AF, K, KEY_SIZE>> {
        if let Ok(tree) = lsm_tree::Config::new(persist_path).open() {
            Ok(LsmTree::<AF, K, KEY_SIZE> {
                tree,
                counters: Counters::default(),
                _af: PhantomData,
                _k: PhantomData,
            })
        } else {
            Err(FatalError)
        }
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> (u32, u32) {
        self.tree.insert::<&[u8], &[u8]>(key, value, 0)
    }

    // This is not production code yet. To be re-evaluated if it does become
    // production code.
    #[allow(clippy::indexing_slicing)]
    pub fn _remove(&self, key: &[u8]) {
        self.tree.remove_weak(key, 0);
        // the first byte of the prefix holds the length of the prefix.
        self.counters._dec_prefixes_count(key[0]);
    }

    // Based on the properties of the lsm_tree we can assume that the key and
    // value concatenated in this method always has a length of greater than
    // KEYS_SIZE, a global constant for the store per AF.
    #[allow(clippy::indexing_slicing)]
    pub fn get_records_for_prefix(
        &self,
        prefix: PrefixId<AF>,
        mui: Option<u32>,
        include_withdrawn: bool,
        withdrawn_muis_bmin: &RoaringBitmap,
    ) -> Option<Vec<FatalResult<Vec<u8>>>> {
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
                            let key = K::header_mut(&mut bytes[..KEY_SIZE])?;
                            // If mui is in the global withdrawn muis table,
                            // then rewrite the routestatus of the record
                            // to withdrawn.
                            if withdrawn_muis_bmin.contains(key.mui.into()) {
                                key.status = RouteStatus::Withdrawn;
                            }
                            Ok(bytes)
                        })
                    })
                    .collect::<Vec<lsm_tree::Result<FatalResult<Vec<u8>>>>>()
                    .into_iter()
                    .collect::<lsm_tree::Result<Vec<FatalResult<Vec<u8>>>>>()
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
            // All muis, include withdrawn routes
            (None, true) => {
                // get all records for this prefix
                self.tree
                    .prefix(prefix.as_bytes(), None, None)
                    .map(|kv| {
                        kv.map(|kv| {
                            trace!("n i persist kv pair found: {:?}", kv);

                            // If mui is in the global withdrawn muis table,
                            // then rewrite the routestatus of the record
                            // to withdrawn.
                            let mut bytes = [kv.0, kv.1].concat();
                            trace!("bytes {:?}", bytes);
                            let key = K::header_mut(&mut bytes[..KEY_SIZE])?;
                            trace!("key {:?}", key);
                            trace!("wm_bmin {:?}", withdrawn_muis_bmin);
                            if withdrawn_muis_bmin.contains(key.mui.into()) {
                                trace!("rewrite status");
                                key.status = RouteStatus::Withdrawn;
                            }
                            Ok(bytes)
                        })
                    })
                    .collect::<Vec<lsm_tree::Result<FatalResult<Vec<u8>>>>>()
                    .into_iter()
                    .collect::<lsm_tree::Result<Vec<FatalResult<Vec<u8>>>>>()
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
                self.tree
                    .prefix(prefix.as_bytes(), None, None)
                    .filter_map(|r| {
                        r.map(|kv| {
                            trace!("n f persist kv pair found: {:?}", kv);
                            let mut bytes = [kv.0, kv.1].concat();
                            if let Ok(header) =
                                K::header_mut(&mut bytes[..KEY_SIZE])
                            {
                                // If mui is in the global withdrawn muis
                                // table, then skip this record
                                trace!(
                                    "header {}",
                                    Prefix::from(header.prefix)
                                );
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
                                Some(Ok(bytes))
                            } else {
                                Some(Err(FatalError))
                            }
                        })
                        .transpose()
                    })
                    .collect::<Vec<lsm_tree::Result<FatalResult<Vec<u8>>>>>()
                    .into_iter()
                    .collect::<lsm_tree::Result<Vec<FatalResult<Vec<u8>>>>>()
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
                    .filter_map(|kv| {
                        kv.map(|kv| {
                            trace!("mui f persist kv pair found: {:?}", kv);
                            let bytes = [kv.0, kv.1].concat();
                            if let Ok(key) = K::header(&bytes[..KEY_SIZE]) {
                                // If mui is in the global withdrawn muis
                                // table, then skip this record
                                if key.status == RouteStatus::Withdrawn
                                    || withdrawn_muis_bmin
                                        .contains(key.mui.into())
                                {
                                    return None;
                                }
                                Some(Ok(bytes))
                            } else {
                                Some(Err(FatalError))
                            }
                        })
                        .transpose()
                    })
                    .collect::<Vec<lsm_tree::Result<FatalResult<Vec<u8>>>>>()
                    .into_iter()
                    .collect::<lsm_tree::Result<Vec<FatalResult<Vec<u8>>>>>()
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
    }

    pub fn get_most_recent_record_for_prefix_mui(
        &self,
        prefix: PrefixId<AF>,
        mui: u32,
    ) -> FatalResult<Option<Vec<u8>>> {
        trace!("get most recent record for prefix mui combo");
        let key_b = ShortKey::from((prefix, mui));
        let mut res: FatalResult<Vec<u8>> = Err(FatalError);

        for rkv in self.tree.prefix(key_b.as_bytes(), None, None) {
            if let Ok(kvs) = rkv {
                let kv = [kvs.0, kvs.1].concat();
                if let Ok(h) = K::header(&kv) {
                    if let Ok(r) = &res {
                        if let Ok(h_res) = K::header(r) {
                            if h_res.ltime < h.ltime {
                                res = Ok(kv);
                            }
                        }
                    } else {
                        res = Ok(kv);
                    }
                } else {
                    return Err(FatalError);
                }
            } else {
                return Err(FatalError);
            }
        }

        res.map(|r| Some(r.to_vec()))
    }

    pub(crate) fn get_records_with_keys_for_prefix_mui(
        &self,
        prefix: PrefixId<AF>,
        mui: u32,
    ) -> Vec<FatalResult<Vec<u8>>> {
        let key_b = ShortKey::from((prefix, mui));

        (*self.tree.prefix(key_b.as_bytes(), None, None))
            .into_iter()
            .map(|rkv| {
                if let Ok(kv) = rkv {
                    Ok([kv.0, kv.1].concat())
                } else {
                    Err(FatalError)
                }
            })
            .collect::<Vec<_>>()
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

    #[allow(clippy::indexing_slicing)]
    pub fn get_prefixes_count_for_len(
        &self,
        len: u8,
    ) -> Result<usize, PrefixStoreError> {
        if len <= AF::BITS {
            Ok(self.counters.get_prefixes_count()[len as usize])
        } else {
            Err(PrefixStoreError::StoreNotReadyError)
        }
    }

    pub(crate) fn persist_record_w_long_key<M: Meta>(
        &self,
        prefix: PrefixId<AF>,
        record: &Record<M>,
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
        record: &Record<M>,
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
    ) -> FatalResult<()> {
        let record = ZeroCopyRecord::<AF>::try_ref_from_prefix(record_b)
            .map_err(|_| FatalError)?
            .0;
        let key = ShortKey::from((record.prefix, record.multi_uniq_id));
        trace!("insert key {:?}", key);

        header
            .as_bytes()
            .to_vec()
            .extend_from_slice(record.meta.as_ref());

        self.insert(key.as_bytes(), header.as_bytes());

        Ok(())
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
    ) -> impl Iterator<Item = Vec<FatalResult<Vec<u8>>>> + '_ {
        PersistedPrefixIter::<AF, K, KEY_SIZE> {
            tree_iter: self.tree.iter(None, None),
            cur_rec: None,
            _af: PhantomData,
            _k: PhantomData,
        }
    }
}

impl<
        AF: AddressFamily,
        K: Key<AF, KEY_SIZE>,
        // const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > std::fmt::Debug for LsmTree<AF, K, KEY_SIZE>
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
    K: Key<AF, KEY_SIZE>,
    const KEY_SIZE: usize,
> {
    cur_rec: Option<Vec<FatalResult<Vec<u8>>>>,
    tree_iter:
        Box<dyn DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>>,
    _af: PhantomData<AF>,
    _k: PhantomData<K>,
}

impl<AF: AddressFamily, K: Key<AF, KEY_SIZE>, const KEY_SIZE: usize> Iterator
    for PersistedPrefixIter<AF, K, KEY_SIZE>
{
    type Item = Vec<FatalResult<Vec<u8>>>;
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
                    rec = Some(vec![Ok([k, v].concat())]);
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
            let outer_pfx = if let Some(Ok(Ok(rr))) =
                r_rec.first().map(|v| v.as_ref().map(|h| K::header(h)))
            {
                rr.prefix
            } else {
                return Some(vec![Err(FatalError)]);
            };

            for (k, v) in self.tree_iter.by_ref().flatten() {
                let header = K::header(&k);

                if let Ok(h) = header {
                    if h.prefix == outer_pfx {
                        r_rec.push(Ok([k, v].concat()));
                    } else {
                        self.cur_rec = Some(vec![Ok([k, v].concat())]);
                        break;
                    }
                } else {
                    r_rec.push(Err(FatalError));
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
