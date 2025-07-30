use std::marker::PhantomData;
use std::path::Path;

use log::{debug, trace};
use lsm_tree::{AbstractTree, KvPair};
use roaring::RoaringBitmap;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, NativeEndian, TryFromBytes,
    Unaligned, U64,
};

use crate::errors::{FatalError, FatalResult, PrefixStoreError};
use crate::prefix_cht::map_type::KeyExtensions;
use crate::prefix_record::Meta;
use crate::stats::Counters;
use crate::types::prefix_record::{ValueHeader, ZeroCopyRecord};
use crate::types::RouteStatus;
use crate::types::{Nlri, Record};

//------------ Key -----------------------------------------------------------

// The type of key used to create entries in the LsmTree. Can be short or
// long. Short keys overwrite existing values for existing (prefix, mui)
// pairs, whereas long keys append values with existing (prefix, mui), thus
// creating persisted historical records.

pub trait Key<N: Nlri, K: KeyExtensions>:
    Copy
    + std::fmt::Debug
    + TryFromBytes
    + KnownLayout
    + IntoBytes
    + Unaligned
    + Immutable
{
    // Try to extract a header from the bytes for reading only. If this
    // somehow fails, we don't know what to do anymore. Data may be corrupted,
    // so it probably should not be retried.
    fn from_header(bytes: &[u8]) -> Result<&Self, FatalError> {
        // trace!("key size {}", KEY_SIZE);
        trace!("bytes len {}", bytes.len());
        trace!("bytes {bytes:?}");
        trace!("key size {}", size_of::<K>());
        Self::try_ref_from_bytes(bytes).map_err(|e| {
            debug!("header error {e}");
            FatalError
        })
    }

    // Try to extract a header for writing. If this somehow fails, we most
    // probably cannot write to it anymore. This is fatal. The application
    // should exit, data integrity (on disk) should be verified.
    fn from_header_mut(bytes: &mut [u8]) -> Result<&mut Self, FatalError> {
        // trace!("key size {}", KEY_SIZE);
        trace!("bytes len {}", bytes.len());
        trace!("bytes {bytes:?}");
        let lk = Self::try_mut_from_bytes(bytes.as_mut_bytes())
            .map_err(|_| FatalError);
        // trace!("long key {lk:?}");
        lk
    }

    fn long_key_from_header(
        bytes: &[u8],
    ) -> Result<&LongKey<N, K>, FatalError> {
        trace!("bytes len {}", bytes.len());
        trace!("bytes {bytes:?}");
        let lk = LongKey::<N, K>::try_ref_from_bytes(bytes)
            .map_err(|_| FatalError);
        // trace!("long key {lk:?}");
        lk
    }

    fn long_key_from_header_mut(
        bytes: &mut [u8],
    ) -> Result<&mut LongKey<N, K>, FatalError> {
        trace!("bytes len {}", bytes.len());
        trace!("bytes {bytes:?}");
        let lk = LongKey::<N, K>::try_mut_from_bytes(bytes.as_mut_bytes())
            .map_err(|_| FatalError);
        // trace!("long key {lk:?}");
        lk
    }

    fn prefix(&self) -> N;

    fn mui(&self) -> K;
}

#[derive(
    Copy,
    Clone,
    Debug,
    KnownLayout,
    Immutable,
    FromBytes,
    Unaligned,
    IntoBytes,
    Hash,
)]
#[repr(C)]
pub struct ShortKey<N: Nlri, K: KeyExtensions> {
    nlri: N,
    mui: K,
}

const fn key_size<N: Nlri, K: KeyExtensions>() -> usize {
    size_of::<N>() + size_of::<K>() + 8 + 1
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
pub struct LongKey<N: Nlri, K: KeyExtensions> {
    nlri: N,                  // 1 + (4 or 16) or hashed blob (4 bytes)
    key_ext: K, // 4 (mui), 4 + 5 (mui + path_id), 4 + 5 + 8 (mui + path_id + rd)
    ltime: U64<NativeEndian>, // 8
    status: RouteStatus, // 1
} // (18, or 23, or 31) for IPv4, and (30, or 35, or 43) for IPv6

impl<N: Nlri, K: KeyExtensions> Key<N, K> for ShortKey<N, K> {
    fn prefix(&self) -> N {
        self.nlri
    }
    fn mui(&self) -> K {
        self.mui
    }
}

impl<N: Nlri, K: KeyExtensions> From<(N, K)> for ShortKey<N, K> {
    fn from(value: (N, K)) -> Self {
        Self {
            nlri: value.0,
            mui: value.1,
        }
    }
}

impl<N: Nlri, K: KeyExtensions> Key<N, K> for LongKey<N, K> {
    fn prefix(&self) -> N {
        self.nlri
    }

    fn mui(&self) -> K {
        self.key_ext
    }
}

impl<N: Nlri, K: KeyExtensions> From<(N, K, u64, RouteStatus)>
    for LongKey<N, K>
{
    fn from(value: (N, K, u64, RouteStatus)) -> Self {
        Self {
            nlri: value.0,
            key_ext: value.1,
            ltime: value.2.into(),
            status: value.3,
        }
    }
}

//------------ LsmTree -------------------------------------------------------

// The log-structured merge tree that backs the persistent store (on disk).

pub struct LsmTree<
    // The address family that this tree stores. IPv4 or IPv6.
    N: Nlri,
    // Manages how much extra information goes into the key. The options are
    // (mui), (mui, path_id), and (mui, route distuinghisher, path_id)
    KE: KeyExtensions,
    // The Key type for this tree. This can basically be a long key, if the
    // store needs to store historical records, or a short key, if it should
    // overwrite records for (prefix, mui) pairs, effectively only keeping the
    // current state.
    K: Key<N, KE>,
    // The size in bytes of the complete key in the persisted storage, this
    // is PREFIX_SIZE bytes (4; 16) + mui size (4) + ltime (8)
    // const KEY_SIZE: usize,
> {
    tree: lsm_tree::Tree,
    counters: Counters,
    _af: PhantomData<N>,
    _k: PhantomData<K>,
    _rk: PhantomData<KE>,
}

impl<N: Nlri, RK: KeyExtensions, K: Key<N, RK>> LsmTree<N, RK, K> {
    pub fn new(persist_path: &Path) -> FatalResult<LsmTree<N, RK, K>> {
        if let Ok(tree) = lsm_tree::Config::new(persist_path).open() {
            Ok(LsmTree::<N, RK, K> {
                tree,
                counters: Counters::default(),
                _af: PhantomData,
                _k: PhantomData,
                _rk: PhantomData,
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

    pub fn contains_prefix(
        &self,
        prefix: N,
    ) -> Result<bool, PrefixStoreError> {
        self.tree
            .contains_key(prefix.as_bytes(), None)
            .map_err(|_| PrefixStoreError::StoreNotReadyError)
    }

    pub fn contains_key(
        &self,
        prefix: N,
        key: RK,
    ) -> Result<bool, PrefixStoreError> {
        for kv in self.tree.prefix(prefix.as_bytes(), None, None).flatten() {
            let mut bytes = [kv.0, kv.1].concat();
            let b = &mut bytes
                .get_mut(..const { key_size::<N, RK>() })
                .ok_or(PrefixStoreError::FatalError)?;
            let k = K::from_header_mut(b)?;
            if k.mui() == key {
                return Ok(true);
            }
        }

        Ok(false)
    }

    // Based on the properties of the lsm_tree we can assume that the key and
    // value concatenated in this method always has a length of greater than
    // KEYS_SIZE, a global constant for the store per AF.
    #[allow(clippy::indexing_slicing)]
    pub fn records_for_prefix<YK: KeyExtensions>(
        &self,
        prefix: N,
        mui: Option<YK>,
        include_withdrawn: bool,
        withdrawn_muis_bmin: &RoaringBitmap,
    ) -> Option<Vec<FatalResult<Vec<u8>>>> {
        match (mui, include_withdrawn) {
            // Specific mui, include withdrawn routes
            (Some(mui), true) => {
                // get the records from the persist store for the (prefix,
                // mui) tuple only.
                let prefix_b = ShortKey::from((prefix, mui));
                println!("search key {prefix_b:?}");
                self.tree
                    .prefix(prefix_b.as_bytes(), None, None)
                    .map(|kv| {
                        kv.map(|kv| {
                            trace!("mui i persist kv pair found: {kv:?}");
                            let mut bytes = [kv.0, kv.1].concat();
                            let key = K::long_key_from_header_mut(
                                &mut bytes[..const { key_size::<N, RK>() }],
                            )?;
                            // If mui is in the global withdrawn muis table,
                            // then rewrite the routestatus of the record
                            // to withdrawn.
                            if withdrawn_muis_bmin
                                .contains(key.key_ext.mui().into())
                            {
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
                            trace!("n i persist kv pair found: {kv:?}");

                            // If mui is in the global withdrawn muis table,
                            // then rewrite the routestatus of the record
                            // to withdrawn.
                            let mut bytes = [kv.0, kv.1].concat();
                            trace!("bytes {bytes:?}");
                            let key = K::long_key_from_header_mut(
                                &mut bytes[..const { key_size::<N, RK>() }],
                            )?;
                            trace!("key {key:?}");
                            trace!("wm_bmin {withdrawn_muis_bmin:?}");
                            if withdrawn_muis_bmin
                                .contains(key.key_ext.mui().into())
                            {
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
                            trace!("n f persist kv pair found: {kv:?}");
                            let bytes = [kv.0, kv.1].concat();
                            if let Ok(header) = K::long_key_from_header(
                                &bytes[..const { key_size::<N, RK>() }],
                            ) {
                                // If mui is in the global withdrawn muis
                                // table, then skip this record
                                // trace!(
                                //     "header {}",
                                //     Prefix::from(header.nlri)
                                // );
                                trace!(
                                    "status {}",
                                    header.status == RouteStatus::Withdrawn
                                );
                                if header.status == RouteStatus::Withdrawn
                                    || withdrawn_muis_bmin
                                        .contains(header.key_ext.mui().into())
                                {
                                    // trace!(
                                    //     "NOT returning {} {}",
                                    //     Prefix::from(header.nlri),
                                    //     header.key_ext
                                    // );
                                    return None;
                                }
                                // trace!(
                                //     "RETURNING {} {}",
                                //     Prefix::from(header.nlri),
                                //     header.key_ext
                                // );
                                Some(Ok(bytes))
                            } else {
                                println!("no header; size {}", bytes.len());
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
                let prefix_b = ShortKey::<N, YK>::from((prefix, mui));
                self.tree
                    .prefix(prefix_b.as_bytes(), None, None)
                    .filter_map(|kv| {
                        kv.map(|kv| {
                            trace!("mui f persist kv pair found: {kv:?}");
                            let bytes = [kv.0, kv.1].concat();
                            if let Ok(key) = K::long_key_from_header(
                                &bytes[..const { key_size::<N, RK>() }],
                            ) {
                                // If mui is in the global withdrawn muis
                                // table, then skip this record
                                if key.status == RouteStatus::Withdrawn
                                    || withdrawn_muis_bmin
                                        .contains(key.key_ext.mui().into())
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

    pub fn most_recent_record_for_prefix_mui(
        &self,
        prefix: N,
        mui: impl KeyExtensions,
    ) -> FatalResult<Option<Vec<u8>>> {
        trace!("get most recent record for prefix mui combo");
        let key_b = ShortKey::from((prefix, mui));
        let mut res: FatalResult<Vec<u8>> = Err(FatalError);

        for rkv in self.tree.prefix(key_b.as_bytes(), None, None) {
            if let Ok(kvs) = rkv {
                let kv = [kvs.0, kvs.1].concat();
                if let Ok(h) = K::long_key_from_header(&kv) {
                    if let Ok(r) = &res {
                        if let Ok(h_res) = K::long_key_from_header(r) {
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

    pub(crate) fn records_with_keys_for_prefix_mui(
        &self,
        prefix: N,
        mui: impl KeyExtensions,
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

    pub fn prefixes_count(&self) -> usize {
        self.counters.prefixes_count().iter().sum()
    }

    pub fn routes_count(&self) -> usize {
        self.counters.routes_count()
    }

    #[allow(clippy::indexing_slicing)]
    pub fn prefixes_count_for_len(
        &self,
        len: u8,
    ) -> Result<usize, PrefixStoreError> {
        if len <= N::BITS {
            Ok(self.counters.prefixes_count()[len as usize])
        } else {
            Err(PrefixStoreError::StoreNotReadyError)
        }
    }

    pub(crate) fn persist_record_w_long_key<M: Meta>(
        &self,
        prefix: N,
        record: &Record<RK, M>,
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
        prefix: N,
        record: &Record<RK, M>,
    ) {
        trace!("Record to persist {record}");
        let mut value = ValueHeader {
            ltime: record.ltime,
            status: record.status,
        }
        .as_bytes()
        .to_vec();

        trace!("header in bytes {value:?}");

        value.extend_from_slice(record.meta.as_ref());

        trace!("value complete {value:?}");

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
        let record = ZeroCopyRecord::<N, RK>::try_ref_from_prefix(record_b)
            .map_err(|_| FatalError)?
            .0;
        let key = ShortKey::from((record.nlri, record.ext_key));
        trace!("insert key {key:?}");

        header
            .as_bytes()
            .to_vec()
            .extend_from_slice(record.meta.as_ref());

        self.insert(key.as_bytes(), header.as_bytes());

        Ok(())
    }

    pub(crate) fn insert_empty_record(
        &self,
        prefix: N,
        mui: impl KeyExtensions,
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
        PersistedPrefixIter::<N, RK, K> {
            tree_iter: self.tree.iter(None, None),
            cur_rec: None,
            _k: PhantomData,
            _rk: PhantomData,
            _n: PhantomData,
        }
    }
}

impl<N: Nlri, RK: KeyExtensions, K: Key<N, RK>> std::fmt::Debug
    for LsmTree<N, RK, K>
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

// Iterator for all items in a lsm tree partition. The iterator used for
// this will scan through the entire tree, and there's no way to start at a
// specified offset.
pub(crate) struct PersistedPrefixIter<
    N: Nlri,
    RK: KeyExtensions,
    K: Key<N, RK>,
> {
    cur_rec: Option<Vec<FatalResult<Vec<u8>>>>,
    tree_iter:
        Box<dyn DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>>,
    _n: PhantomData<N>,
    _k: PhantomData<K>,
    _rk: PhantomData<RK>,
}

impl<N: Nlri, RK: KeyExtensions, K: Key<N, RK>> Iterator
    for PersistedPrefixIter<N, RK, K>
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
                r_rec.first().map(|v| v.as_ref().map(|h| K::from_header(h)))
            {
                rr.prefix()
            } else {
                return Some(vec![Err(FatalError)]);
            };

            for (k, v) in self.tree_iter.by_ref().flatten() {
                let kv = [k, v].concat();
                let key = K::long_key_from_header(&kv);
                debug!("header {key:?}");

                if let Ok(h) = key {
                    if h.nlri == outer_pfx {
                        r_rec.push(Ok(kv));
                    } else {
                        self.cur_rec = Some(vec![Ok(kv)]);
                        break;
                    }
                } else {
                    debug!("boem error pushed");
                    r_rec.push(Err(FatalError));
                }
            }

            Some(r_rec)
        } else {
            None
        }
    }
}
