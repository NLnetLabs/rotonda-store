//------------ StoredBlob ----------------------------------------------------

use std::{
    hash::Hash,
    io,
    mem::MaybeUninit,
    path::Path,
    sync::{atomic::Ordering, MutexGuard},
};

use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use hash32::{FnvHasher, Hasher as _};
use log::{debug, info, trace};
use roaring::RoaringBitmap;
use zerocopy::{IntoBytes, NetworkEndian, U32};

use crate::lsm_tree::LongKey;
use crate::{
    epoch,
    errors::{FatalError, FatalResult, PrefixStoreError},
    prefix_cht::{
        cht::{MultiMapValue, PrefixCht},
        iterators_cp::PrefixIter,
        map_type::{KeyExtensions, MapType, Mui, MuiRdPathId},
        rd_multi_map::RdPathIdMultiMap,
    },
    prefix_record::{Meta, ValueHeader, ZeroCopyRecord},
    rib::config::{Config, PersistStrategy},
    stats::{Counters, UpsertCounters, UpsertReport},
    types::{Record, RouteStatus},
    LsmTree,
};

// type BlobCht<M, MT> = PrefixCht<U32<NetworkEndian>, M, MT, 9, 33>;

// 4 8 16 32 64 128 256 512 1024 2048 4096
// 0 1 2  3  4  5   6   7   8    9    10

struct BucketedBlobCht<
    M: Meta,
    MT: MapType<M>,
    const ROOT_SIZE: usize,
    const STRIDE_PER_BUCKET: usize,
> {
    buckets: (
        PrefixCht<[u8; 8], M, MT, ROOT_SIZE, STRIDE_PER_BUCKET>,
        PrefixCht<[u8; 16], M, MT, ROOT_SIZE, STRIDE_PER_BUCKET>,
        PrefixCht<[u8; 32], M, MT, ROOT_SIZE, STRIDE_PER_BUCKET>,
        PrefixCht<[u8; 64], M, MT, ROOT_SIZE, STRIDE_PER_BUCKET>,
        PrefixCht<[u8; 128], M, MT, ROOT_SIZE, STRIDE_PER_BUCKET>,
        PrefixCht<[u8; 256], M, MT, ROOT_SIZE, STRIDE_PER_BUCKET>,
        PrefixCht<[u8; 512], M, MT, ROOT_SIZE, STRIDE_PER_BUCKET>,
        PrefixCht<[u8; 1024], M, MT, ROOT_SIZE, STRIDE_PER_BUCKET>,
        PrefixCht<[u8; 2048], M, MT, ROOT_SIZE, STRIDE_PER_BUCKET>,
        PrefixCht<[u8; 4096], M, MT, ROOT_SIZE, STRIDE_PER_BUCKET>,
    ),
    counters: Counters,
}

impl<
        M: Meta,
        MT: MapType<M>,
        const ROOT_SIZE: usize,
        const STRIDES_PER_BUCKET: usize,
    > BucketedBlobCht<M, MT, ROOT_SIZE, STRIDES_PER_BUCKET>
{
    pub(crate) fn init() -> Self {
        BucketedBlobCht {
            buckets: (PrefixCht::<
                [u8; 8],
                M,
                MT,
                ROOT_SIZE,
                STRIDES_PER_BUCKET,
            >::init(),
            PrefixCht::<
                [u8; 16],
                M,
                MT,
                ROOT_SIZE,
                STRIDES_PER_BUCKET,
            >::init(),
            PrefixCht::<
                [u8; 32],
                M,
                MT,
                ROOT_SIZE,
                STRIDES_PER_BUCKET,
            >::init(),
            PrefixCht::<
                [u8; 64],
                M,
                MT,
                ROOT_SIZE,
                STRIDES_PER_BUCKET,
            >::init(),
            PrefixCht::<
                [u8; 128],
                M,
                MT,
                ROOT_SIZE,
                STRIDES_PER_BUCKET,
            >::init(),
            PrefixCht::<
                [u8; 256],
                M,
                MT,
                ROOT_SIZE,
                STRIDES_PER_BUCKET,
            >::init(),
            PrefixCht::<
                [u8; 512],
                M,
                MT,
                ROOT_SIZE,
                STRIDES_PER_BUCKET,
            >::init(),
            PrefixCht::<
                [u8; 1024],
                M,
                MT,
                ROOT_SIZE,
                STRIDES_PER_BUCKET,
            >::init(),
            PrefixCht::<
                [u8; 2048],
                M,
                MT,
                ROOT_SIZE,
                STRIDES_PER_BUCKET,
            >::init(),
            PrefixCht::<
                [u8; 4096],
                M,
                MT,
                ROOT_SIZE,
                STRIDES_PER_BUCKET,
            >::init()),
            counters: Counters::default(),
        }
    }

    pub(crate) fn upsert_prefix(
        &self,
        prefix: &[u8],
        record: Record<MT::Key, M>,
        update_path_selections: Option<M::TBI>,
        guard: &Guard,
    ) -> Result<(UpsertReport, Option<MultiMapValue<M>>), PrefixStoreError>
    {
        trace!("upserting prefix");
        match prefix.len() {
            l if l <= 6 => {
                let ba = MaybeUninit::<[u8; 8]>::uninit();
                let mut ba = unsafe { ba.assume_init() };
                // write the length of the slice in the first two bytes
                let mut l;
                let mut _r;
                (l, _r) = ba.split_at_mut(2);
                l.copy_from_slice(&(prefix.len() as u16).to_le_bytes());
                (l, _r) = ba[2..].split_at_mut(prefix.len());
                l.copy_from_slice(prefix);
                self.buckets.0.upsert_prefix(
                    ba,
                    record,
                    update_path_selections,
                    guard,
                )
            }
            l if l <= 14 => {
                let ba = MaybeUninit::<[u8; 16]>::uninit();
                let mut ba = unsafe { ba.assume_init() };

                let mut l;
                let mut _r;
                (l, _r) = ba.split_at_mut(2);
                l.copy_from_slice(&(prefix.len() as u16).to_le_bytes());
                (l, _r) = ba[2..].split_at_mut(prefix.len());
                l.copy_from_slice(prefix);
                self.buckets.1.upsert_prefix(
                    ba,
                    record,
                    update_path_selections,
                    guard,
                )
            }
            l if l <= 32 => {
                let ba = MaybeUninit::<[u8; 32]>::uninit();
                let mut ba = unsafe { ba.assume_init() };
                let mut l;
                let mut _r;
                (l, _r) = ba.split_at_mut(2);
                l.copy_from_slice(&(prefix.len() as u16).to_le_bytes());
                (l, _r) = ba[2..].split_at_mut(prefix.len());
                l.copy_from_slice(prefix);
                self.buckets.2.upsert_prefix(
                    ba,
                    record,
                    update_path_selections,
                    guard,
                )
            }
            l if l <= 64 => {
                let ba = MaybeUninit::<[u8; 64]>::uninit();
                let mut ba = unsafe { ba.assume_init() };
                let mut l;
                let mut _r;
                (l, _r) = ba.split_at_mut(2);
                l.copy_from_slice(&(prefix.len() as u16).to_le_bytes());
                (l, _r) = ba[2..].split_at_mut(prefix.len());
                l.copy_from_slice(prefix);
                self.buckets.3.upsert_prefix(
                    ba,
                    record,
                    update_path_selections,
                    guard,
                )
            }
            l if l <= 128 => {
                let ba = MaybeUninit::<[u8; 128]>::uninit();
                let mut ba = unsafe { ba.assume_init() };
                let mut l;
                let mut _r;
                (l, _r) = ba.split_at_mut(2);
                l.copy_from_slice(&(prefix.len() as u16).to_le_bytes());
                (l, _r) = ba[2..].split_at_mut(prefix.len());
                l.copy_from_slice(prefix);
                self.buckets.4.upsert_prefix(
                    ba,
                    record,
                    update_path_selections,
                    guard,
                )
            }
            l if l <= 256 => {
                let ba = MaybeUninit::<[u8; 256]>::uninit();
                let mut ba = unsafe { ba.assume_init() };
                let mut l;
                let mut _r;
                (l, _r) = ba.split_at_mut(2);
                l.copy_from_slice(&(prefix.len() as u16).to_le_bytes());
                (l, _r) = ba[2..].split_at_mut(prefix.len());
                l.copy_from_slice(prefix);
                self.buckets.5.upsert_prefix(
                    ba,
                    record,
                    update_path_selections,
                    guard,
                )
            }
            l if l <= 512 => {
                let ba = MaybeUninit::<[u8; 512]>::uninit();
                let mut ba = unsafe { ba.assume_init() };
                let mut l;
                let mut _r;
                (l, _r) = ba.split_at_mut(2);
                l.copy_from_slice(&(prefix.len() as u16).to_le_bytes());
                (l, _r) = ba[2..].split_at_mut(prefix.len());
                l.copy_from_slice(prefix);
                self.buckets.6.upsert_prefix(
                    ba,
                    record,
                    update_path_selections,
                    guard,
                )
            }
            l if l <= 1024 => {
                let ba = MaybeUninit::<[u8; 1024]>::uninit();
                let mut ba = unsafe { ba.assume_init() };
                let mut l;
                let mut _r;
                (l, _r) = ba.split_at_mut(2);
                l.copy_from_slice(&(prefix.len() as u16).to_le_bytes());
                (l, _r) = ba[2..].split_at_mut(prefix.len());
                l.copy_from_slice(prefix);
                self.buckets.7.upsert_prefix(
                    ba,
                    record,
                    update_path_selections,
                    guard,
                )
            }
            l if l <= 2048 => {
                let ba = MaybeUninit::<[u8; 2048]>::uninit();
                let mut ba = unsafe { ba.assume_init() };
                let mut l;
                let mut _r;
                (l, _r) = ba.split_at_mut(2);
                l.copy_from_slice(&(prefix.len() as u16).to_le_bytes());
                (l, _r) = ba.split_at_mut(prefix.len());
                l.copy_from_slice(prefix);
                self.buckets.8.upsert_prefix(
                    ba,
                    record,
                    update_path_selections,
                    guard,
                )
            }
            l if l <= 4096 => {
                let ba = MaybeUninit::<[u8; 4096]>::uninit();
                let mut ba = unsafe { ba.assume_init() };
                let mut l;
                let mut _r;
                (l, _r) = ba.split_at_mut(2);
                l.copy_from_slice(&(prefix.len() as u16).to_le_bytes());
                (l, _r) = ba[2..].split_at_mut(prefix.len());
                l.copy_from_slice(prefix);
                self.buckets.9.upsert_prefix(
                    ba,
                    record,
                    update_path_selections,
                    guard,
                )
            }
            _ => Err(PrefixStoreError::NlriTooBig),
        }
    }

    #[allow(clippy::unwrap_used)]
    fn contains_nlri(&self, nlri: &[u8]) -> Result<bool, PrefixStoreError> {
        match nlri.len() {
            l if l <= 8 => Ok(self
                .buckets
                .0
                .contains_prefix(*nlri.first_chunk::<8>().unwrap())),
            l if l <= 16 => Ok(self
                .buckets
                .1
                .contains_prefix(*nlri.first_chunk::<16>().unwrap())),
            _ => Err(PrefixStoreError::NlriTooBig),
        }
    }

    fn contains_key(&self, nlri: &[u8], mui: impl KeyExtensions) -> bool {
        todo!()
    }

    pub(crate) fn get_records_for_prefix<FK: KeyExtensions>(
        &self,
        nlri: &[u8],
        mui: Option<FK>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Vec<Record<MT::Key, M>>>
    where
        MT::Key: From<FK>,
    {
        match nlri.len() {
            l if l <= 8 => self.buckets.0.get_records_for_prefix(
                #[allow(clippy::unwrap_used)]
                *nlri.first_chunk::<8>().unwrap(),
                mui,
                include_withdrawn,
                bmin,
            ),
            l if l <= 16 => self.buckets.1.get_records_for_prefix(
                #[allow(clippy::unwrap_used)]
                *nlri.first_chunk::<16>().unwrap(),
                mui,
                include_withdrawn,
                bmin,
            ),
            _ => None,
        }
    }

    pub(crate) fn non_recursive_retrieve_prefix_mut(
        &'_ self,
        search_nlri: &[u8],
    ) -> Result<(MutexGuard<'_, MT>, bool), PrefixStoreError> {
        match search_nlri.len() {
            l if l <= 8 => {
                let spp = self.buckets.0.non_recursive_retrieve_prefix_mut(
                    #[allow(clippy::unwrap_used)]
                    *search_nlri.first_chunk::<8>().unwrap(),
                );
                Ok((spp.0.acquire_read_guard(), spp.1))
            }
            l if l <= 16 => {
                let spp = self.buckets.1.non_recursive_retrieve_prefix_mut(
                    #[allow(clippy::unwrap_used)]
                    *search_nlri.first_chunk::<16>().unwrap(),
                );
                Ok((spp.0.acquire_read_guard(), spp.1))
            }
            _ => Err(PrefixStoreError::NlriTooBig),
        }
    }

    pub fn iter<'a>(
        &'a self,
        bmin: Option<&'a RoaringBitmap>,
        start_len: u8,
    ) -> PrefixIter<'a, [u8; 8], M, MT, ROOT_SIZE, STRIDES_PER_BUCKET> {
        PrefixIter {
            prefixes: &self.buckets.0.bush,
            bmin,
            cur_len: start_len,
            cur_bucket: self.buckets.0.bush.root_for_len(0),
            cur_level: 0,
            parents: [None; 8],
            cursor: 0,
        }
    }
}

type BlobLsmTree<const BLOB_SIZE: usize> = LsmTree<
    [u8; BLOB_SIZE],
    MuiRdPathId,
    LongKey<[u8; BLOB_SIZE], MuiRdPathId>,
>;

pub struct BlobRib<M: Meta, const BLOB_SIZE: usize, C: Config> {
    pub config: C,
    pub(crate) blob_cht: BucketedBlobCht<M, RdPathIdMultiMap<M>, 9, 33>,
    pub(crate) persist_tree: Option<BlobLsmTree<BLOB_SIZE>>,
    withdrawn_muis_bmin: Atomic<RoaringBitmap>,
    pub counters: Counters,
}

pub fn prefix_hash(blob: &[u8]) -> U32<NetworkEndian> {
    let mut prefix_hash: FnvHasher = Default::default();
    blob.hash(&mut prefix_hash);
    prefix_hash.finish32().into()
}

impl<M: Meta, const BLOB_SIZE: usize, C: Config> BlobRib<M, BLOB_SIZE, C> {
    /// Create a new RIB with a default configuration.
    ///
    /// The default configuration uses the `MemoryOnly` persistence strategy.
    ///
    /// This method is really infallible, but we return a result anyway to be
    /// in line with the `new_with_config` method.
    pub fn try_default() -> Result<Self, PrefixStoreError> {
        let config = C::default();
        Self::new_with_config(config)
            .map_err(|_| PrefixStoreError::StoreNotReadyError)
    }

    /// Create a new RIB with the specified [configuration](
    /// crate::rib::config).
    ///
    /// Creation may fail for all strategies that persist to disk, e.g.
    /// the persistence path does not exist, it doesn't have the correct
    /// permissions, etc.
    pub(crate) fn new_with_config(
        config: C,
    ) -> Result<BlobRib<M, BLOB_SIZE, C>, Box<dyn std::error::Error>> {
        BlobRib::<M, BLOB_SIZE, C>::init(config)
    }

    fn init(config: C) -> Result<Self, Box<dyn std::error::Error>> {
        info!("store: initialize store {}", 32);

        let persist_tree = match config.persist_strategy() {
            PersistStrategy::MemoryOnly => None,
            _ => {
                let persist_path = if let Some(pp) = config.persist_path() {
                    pp
                } else {
                    return Err(io::Error::other(
                        "Missing persistence path".to_string(),
                    )
                    .into());
                };
                let pp_ref = &Path::new(&persist_path);
                Some(LsmTree::new(pp_ref).map_err(|_| {
                    io::Error::other("Cannot create persistence store")
                })?)
            }
        };

        let store = BlobRib {
            config,
            persist_tree,
            counters: Counters::default(),
            blob_cht: BucketedBlobCht::<M, RdPathIdMultiMap<M>, 9, 33>::init(
            ),
            withdrawn_muis_bmin: Atomic::new(RoaringBitmap::new()),
        };

        Ok(store)
    }

    pub fn insert(
        &self,
        key: &[u8],
        record: Record<impl KeyExtensions, M>,
        update_path_selections: Option<M::TBI>,
    ) -> Result<UpsertReport, PrefixStoreError> {
        // let prefix = prefix_hash(key);
        trace!("try inserting {key:?}",);
        let retry_count = 0;
        let guard = &epoch::pin();
        self.upsert_prefix(key, record, update_path_selections, guard)
            .map(|mut report| {
                if report.mui_new {
                    self.counters.inc_routes_count();
                }
                report.cas_count += retry_count as usize;
                if report.prefix_new {
                    self.counters.inc_prefixes_count(32);
                }
                report
            })
    }

    fn upsert_prefix(
        &self,
        key: &[u8],
        record: Record<impl KeyExtensions, M>,
        update_path_selections: Option<M::TBI>,
        guard: &Guard,
    ) -> Result<UpsertReport, PrefixStoreError> {
        // let prefix = prefix_hash(key);
        let mui_rd_path_id = MuiRdPathId::from(&record.multi_uniq_id);
        let rec = Record::<MuiRdPathId, M>::from((mui_rd_path_id, &record));
        match self.config.persist_strategy() {
            PersistStrategy::WriteAhead => {
                if let Some(persist_tree) = &self.persist_tree {
                    persist_tree.persist_record_w_long_bucket_key(key, &rec);

                    self.blob_cht
                        .upsert_prefix(
                            key,
                            rec,
                            update_path_selections,
                            guard,
                        )
                        .map(|(report, _old_rec)| report)
                } else {
                    Err(PrefixStoreError::StoreNotReadyError)
                }
            }
            PersistStrategy::PersistHistory => self
                .blob_cht
                .upsert_prefix(key, rec, update_path_selections, guard)
                .map(|(report, old_rec)| {
                    if let Some(rec) = old_rec {
                        if let Some(persist_tree) = &self.persist_tree {
                            persist_tree.persist_record_w_long_bucket_key(
                                key,
                                &Record::from((mui_rd_path_id, &rec)),
                            );
                        }
                    }
                    report
                }),
            PersistStrategy::MemoryOnly => self
                .blob_cht
                .upsert_prefix(key, rec, update_path_selections, guard)
                .map(|(report, _)| report),
            PersistStrategy::PersistOnly => {
                if let Some(persist_tree) = &self.persist_tree {
                    let exists = persist_tree.contains_prefix(key)?;
                    persist_tree.persist_record_w_short_bucket_key(key, &rec);
                    Ok(UpsertReport {
                        cas_count: 0,
                        prefix_new: exists,
                        mui_new: true,
                        mui_count: 0,
                    })
                } else {
                    Err(PrefixStoreError::PersistFailed)
                }
            }
        }
    }

    pub fn contains(
        &self,
        prefix: &[u8],
        mui: Option<MuiRdPathId>,
    ) -> Result<bool, PrefixStoreError> {
        match self.config.persist_strategy() {
            PersistStrategy::PersistOnly => {
                if let Some(persist_tree) = &self.persist_tree {
                    if let Some(mui) = mui {
                        persist_tree.contains_key(prefix, mui)
                    } else {
                        persist_tree.contains_prefix(prefix)
                    }
                } else {
                    Err(PrefixStoreError::StoreNotReadyError)
                }
            }
            _ => {
                if let Some(mui) = mui {
                    Ok(self.blob_cht.contains_key(prefix, mui))
                } else {
                    self.blob_cht.contains_nlri(prefix)
                }
            }
        }
    }

    pub fn get(
        &self,
        key: &[u8],
        mui: Option<MuiRdPathId>,
        include_withdrawn: bool,
    ) -> Result<Vec<Record<MuiRdPathId, M>>, PrefixStoreError> {
        let guard = &epoch::pin();
        let mut recs_vec = vec![];
        // let prefix = prefix_hash(key);
        match self.config.persist_strategy() {
            PersistStrategy::PersistOnly => {
                if let Some(persist_tree) = &self.persist_tree {
                    if let Some(recs) = persist_tree.records_for_blob_key(
                        key,
                        include_withdrawn,
                        self.withdrawn_muis_bmin(guard),
                    ) {
                        for rec in recs {
                            let Ok(r) = rec else {
                                return Err(PrefixStoreError::FatalError);
                            };
                            trace!("bytes w/o nlri blob {:?}", &r);
                            #[allow(clippy::indexing_slicing)]
                            if let Ok(rec) = ZeroCopyRecord::<
                                [u8; 0],
                                MuiRdPathId,
                            >::from_bytes(
                                &r
                            ) {
                                let mui = rec.ext_key;
                                recs_vec.push(Record::new(
                                    mui,
                                    rec.ltime,
                                    rec.status,
                                    rec.meta.to_vec().into(),
                                ))
                            } else {
                                return Err(PrefixStoreError::FatalError);
                            }
                        }
                    }
                    Ok(recs_vec)
                } else {
                    Err(PrefixStoreError::StoreNotReadyError)
                }
            }
            _ => {
                // let mui = mui.map(MuiRdPathId::from);
                Ok(self
                    .blob_cht
                    .get_records_for_prefix(
                        key,
                        mui,
                        include_withdrawn,
                        self.withdrawn_muis_bmin(guard),
                    )
                    .unwrap_or_default())
            }
        }
    }

    pub fn get_nodes_count(&self) -> usize {
        self.counters.nodes_count()
    }

    pub(crate) fn withdrawn_muis_bmin<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> &'a RoaringBitmap {
        unsafe {
            self.withdrawn_muis_bmin
                .load(Ordering::Acquire, guard)
                .deref()
        }
    }

    // Change the status of the record for the specified (prefix, mui)
    // combination  to Withdrawn.
    pub fn mark_mui_as_withdrawn_for_prefix(
        &self,
        prefix: &[u8],
        mui: Mui,
        ltime: u64,
    ) -> Result<(), PrefixStoreError> {
        match self.persist_strategy() {
            PersistStrategy::WriteAhead | PersistStrategy::MemoryOnly => {
                let (mut stored_prefix, exists) = self
                    .blob_cht
                    .non_recursive_retrieve_prefix_mut(prefix)?;

                if !exists {
                    return Err(PrefixStoreError::PrefixNotFound);
                }
                // let mut record_map = stored_prefix.acquire_read_guard();
                stored_prefix
                    .mark_as_withdrawn_for_mui(mui.mui().into(), ltime);
            }
            PersistStrategy::PersistOnly => {
                if let Some(p_tree) = self.persist_tree.as_ref() {
                    let stored_prefixes = p_tree
                        .records_with_bucket_keys_for_prefix_mui(prefix, mui);

                    for rkv in stored_prefixes {
                        if let Ok(r) = rkv {
                            let header = ValueHeader {
                                ltime,
                                status: RouteStatus::Withdrawn,
                            };
                            p_tree
                                .rewrite_header_for_record(header, &r)
                                .map_err(|_| {
                                    PrefixStoreError::StoreNotReadyError
                                })?;
                        } else {
                            return Err(PrefixStoreError::StoreNotReadyError);
                        }
                    }
                } else {
                    return Err(PrefixStoreError::StoreNotReadyError);
                }
            }
            PersistStrategy::PersistHistory => {
                // First do the in-memory part
                let (mut stored_prefix, exists) = self
                    .blob_cht
                    .non_recursive_retrieve_prefix_mut(prefix)?;

                if !exists {
                    return Err(PrefixStoreError::StoreNotReadyError);
                }
                // let mut record_map = stored_prefix.acquire_read_guard();
                stored_prefix
                    .mark_as_withdrawn_for_mui(mui.mui().into(), ltime);

                // Use the record from the in-memory RIB to persist.
                if let Some(_record) =
                    stored_prefix.get_record_for_key(mui, true)
                {
                    self._insert_empty_record(prefix, mui, ltime)?;
                }
            }
        }

        Ok(())
    }

    fn _insert_empty_record(
        &self,
        prefix: &[u8],
        mui: Mui,
        ltime: u64,
    ) -> Result<(), PrefixStoreError> {
        match prefix.len() {
            l if l <= 8 => {
                let buf = &mut [0; 8];
                self._insert_empty_record_w_bucket_key(
                    prefix, mui, ltime, buf,
                )?;
            }
            _ => {
                return Err(PrefixStoreError::NlriTooBig);
            }
        };
        Ok(())
    }

    fn _insert_empty_record_w_bucket_key<const SIZE: usize>(
        &self,
        key: &[u8],
        mui: Mui,
        ltime: u64,
        buf: &mut [u8; SIZE],
    ) -> Result<(), PrefixStoreError> {
        let p_tree = if let Some(p_tree) = self.persist_tree.as_ref() {
            p_tree
        } else {
            return Err(PrefixStoreError::StoreNotReadyError);
        };
        buf.copy_from_slice(key);
        let lk = LongKey::from((*buf, mui, ltime, RouteStatus::Withdrawn));
        p_tree.insert_empty_record(lk.as_bytes());
        Ok(())
    }

    // Change the status of the record for the specified (prefix, mui)
    // combination  to Active.
    pub fn mark_mui_as_active_for_prefix(
        &self,
        prefix: &[u8],
        mui: Mui,
        ltime: u64,
    ) -> FatalResult<()> {
        match self.persist_strategy() {
            PersistStrategy::WriteAhead | PersistStrategy::MemoryOnly => {
                let (mut stored_prefix, exists) = self
                    .blob_cht
                    .non_recursive_retrieve_prefix_mut(prefix)
                    .map_err(|_| FatalError)?;

                if !exists {
                    return Err(FatalError);
                }
                // let mut record_map = stored_prefix.acquire_read_guard();
                // record_map.mark_as_withdrawn_for_mui(mui, ltime);
                stored_prefix.mark_as_active_for_mui(mui.mui().into(), ltime);
            }
            PersistStrategy::PersistOnly => {
                if let Some(p_tree) = self.persist_tree.as_ref() {
                    // let sk = ShortKey::from((prefix.into(), mui));
                    if let Ok(Some(record_b)) = p_tree
                        .most_recent_record_for_bucket_key_mui(prefix, mui)
                    {
                        let header = ValueHeader {
                            ltime,
                            status: RouteStatus::Active,
                        };
                        p_tree
                            .rewrite_header_for_record(header, &record_b)?;
                    }
                } else {
                    return Err(FatalError);
                }
            }
            PersistStrategy::PersistHistory => {
                // First do the in-memory part
                let (mut stored_prefix, exists) = self
                    .blob_cht
                    .non_recursive_retrieve_prefix_mut(prefix)
                    .map_err(|_| FatalError)?;

                if !exists {
                    return Err(FatalError);
                }
                // let mut record_map = stored_prefix.acquire_read_guard();
                stored_prefix.mark_as_active_for_mui(mui.mui().into(), ltime);

                // Use the record from the in-memory RIB to persist.
                if let Some(_record) =
                    stored_prefix.get_record_for_key(mui, true)
                {
                    let p_tree =
                        if let Some(p_tree) = self.persist_tree.as_ref() {
                            p_tree
                        } else {
                            return Err(FatalError);
                        };

                    // Here we are keeping persisted history, so no removal of
                    // old (prefix, mui) records.
                    // We are inserting an empty record, since this is a
                    // withdrawal.
                    self._insert_empty_record(prefix, mui, ltime)
                        .map_err(|_| FatalError)?;
                    // p_tree.insert_empty_record(prefix, mui, ltime);
                }
            }
        }

        Ok(())
    }

    // Change the status of the mui globally to Withdrawn. Iterators and match
    // functions will by default not return any records for this mui.
    pub fn mark_mui_as_withdrawn(
        &self,
        mui: u32,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        let current = self.withdrawn_muis_bmin.load(Ordering::Acquire, guard);

        let mut new = unsafe { current.as_ref() }
            .ok_or(PrefixStoreError::StoreNotReadyError)?
            .clone();

        new.insert(mui);

        self.update_withdrawn_muis_bmin(current, new, guard)
    }

    pub(crate) fn update_withdrawn_muis_bmin<'a>(
        &self,
        current: Shared<'a, RoaringBitmap>,
        mut new: RoaringBitmap,
        guard: &'a Guard,
    ) -> Result<(), PrefixStoreError> {
        loop {
            match self.withdrawn_muis_bmin.compare_exchange(
                current,
                Owned::new(new),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(updated) => {
                    new = unsafe { updated.current.as_ref() }
                        .ok_or(PrefixStoreError::StoreNotReadyError)?
                        .clone();
                }
            }
        }
    }

    // Change the status of the mui globally to Active. Iterators and match
    // functions will default to the status on the record itself.
    pub fn mark_mui_as_active(
        &self,
        mui: u32,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        let current = self.withdrawn_muis_bmin.load(Ordering::Acquire, guard);

        let mut new = unsafe { current.as_ref() }
            .ok_or(PrefixStoreError::StoreNotReadyError)?
            .clone();

        new.remove(mui);
        self.update_withdrawn_muis_bmin(current, new, guard)
    }

    // Whether this mui is globally withdrawn. Note that this overrules
    // (by default) any (prefix, mui) combination in iterators and match
    // functions.
    pub fn mui_is_withdrawn(&self, mui: Mui, guard: &Guard) -> bool {
        // unsafe {
        self.withdrawn_muis_bmin(guard).contains(mui.mui().into())
    }

    // Whether this mui is globally active. Note that the local statuses of
    // records (prefix, mui) may be set to withdrawn in iterators and match
    // functions.
    pub(crate) fn is_mui_active(&self, mui: Mui, guard: &Guard) -> bool {
        // !unsafe {
        !self.withdrawn_muis_bmin(guard).contains(mui.mui().into())
    }

    pub(crate) fn prefixes_count(&self) -> UpsertCounters {
        UpsertCounters {
            in_memory_count: self
                .blob_cht
                .counters
                .prefixes_count()
                .iter()
                .sum(),
            persisted_count: self
                .persist_tree
                .as_ref()
                .map_or(0, |p| p.prefixes_count()),
            total_count: self.counters.prefixes_count().iter().sum(),
        }
    }

    pub(crate) fn routes_count(&self) -> UpsertCounters {
        UpsertCounters {
            in_memory_count: self.blob_cht.counters.routes_count(),
            persisted_count: self
                .persist_tree
                .as_ref()
                .map_or(0, |p| p.routes_count()),
            total_count: self.counters.routes_count(),
        }
    }

    // the len check does it all.
    #[allow(clippy::indexing_slicing, clippy::unwrap_used)]
    pub fn prefixes_count_for_len(
        &self,
        len: u8,
    ) -> Result<UpsertCounters, PrefixStoreError> {
        if len <= 32 {
            Ok(UpsertCounters {
                in_memory_count: self.counters.prefixes_count_for_len(len),
                persisted_count: self
                    .persist_tree
                    .as_ref()
                    .map_or(0, |p| p.prefixes_count_for_len(len).unwrap()),
                total_count: self.counters.prefixes_count()[len as usize],
            })
        } else {
            Err(PrefixStoreError::PrefixLengthInvalid)
        }
    }

    pub fn nlri_iter<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Box<
        dyn Iterator<
                Item = FatalResult<(Vec<u8>, Vec<Record<MuiRdPathId, M>>)>,
            > + 'a,
    > {
        trace!("start nlri_iter");
        match self.config.persist_strategy() {
            PersistStrategy::MemoryOnly
            | PersistStrategy::PersistHistory
            | PersistStrategy::WriteAhead => Box::new(
                self.blob_cht
                    .iter(Some(self.withdrawn_muis_bmin(guard)), 32)
                    .map(|p| {
                        #[allow(clippy::unwrap_used)]
                        Ok((
                            #[allow(clippy::unwrap_used)]
                            p.0.to_vec(),
                            p.1.into_iter()
                                .map(|r| {
                                    println!("R {}", r);
                                    r
                                })
                                .collect::<Vec<_>>(),
                        ))
                    }),
            ),
            PersistStrategy::PersistOnly => {
                trace!("persist only");
                Box::new(self.persist_prefixes_iter().map(|r| {
                    #[allow(clippy::unwrap_used)]
                    let p = r.unwrap();
                    trace!("p {:?}", p);
                    #[allow(clippy::unwrap_used)]
                    Ok(p)
                }))
            }
        }
    }

    pub fn records_iter<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Record<MuiRdPathId, M>)> + 'a>
    {
        match self.persist_strategy() {
            PersistStrategy::WriteAhead
            | PersistStrategy::MemoryOnly
            | PersistStrategy::PersistHistory => {
                let iter = self
                    .blob_cht
                    .iter(Some(self.withdrawn_muis_bmin(guard)), 32);

                Box::new(iter.flat_map(|p| {
                    p.1.into_iter().map(move |r| (p.0.to_vec(), r.clone()))
                }))
            }
            PersistStrategy::PersistOnly => {
                let iter = self.persist_prefixes_iter();

                Box::new(iter.flat_map(|p| {
                    #[allow(clippy::unwrap_used)]
                    let pp = p.unwrap();
                    pp.1.into_iter().map(move |r| (pp.0.to_vec(), r))
                }))
            }
        }
    }

    //-------- Persistence ---------------------------------------------------

    pub fn persist_strategy(&self) -> PersistStrategy {
        self.config.persist_strategy()
    }

    // TODO: create iterator over bundled records per NLRI from the flat iterator
    // over contiguous records.
    #[allow(clippy::complexity)]
    pub(crate) fn persist_prefixes_iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = FatalResult<(Vec<u8>, Vec<Record<MuiRdPathId, M>>)>,
    > + 'a {
        trace!("start persist_prefixes_iter");
        let tree_iter = self
            .persist_tree
            .as_ref()
            .map(|tree| {
                trace!("Tree");
                tree.nlri_blob_iter().map(|recs| {
                    trace!("tree");
                    if let Some(Ok(first_rec)) = recs.first() {
                        trace!("first_rec {first_rec:?}");
                        #[allow(clippy::unwrap_used)]
                        let nlri_blob_len = <u16>::from_le_bytes(
                            *first_rec.first_chunk::<2>().unwrap(),
                        ) + 2;
                        match nlri_blob_len {
                            l if l <= 8 => {
                                if let Ok(pfx) = ZeroCopyRecord::<
                                    [u8; 8],
                                    MuiRdPathId,
                                >::from_bytes(
                                    first_rec
                                ) {
                                    let blob = pfx.ext_key;

                                    let mut rec_vec: Vec<
                                        Record<MuiRdPathId, M>,
                                    > = vec![];
                                    for res_rec in recs.iter() {
                                        match res_rec {
                                            Ok(rec) => {
                                                debug!("rec {rec:?}");
                                                if let Ok(rec) =
                                                    ZeroCopyRecord::<
                                                        [u8; 8],
                                                        MuiRdPathId,
                                                    >::from_bytes(
                                                        rec
                                                    )
                                                {
                                                    // debug!("recrec {rec}");
                                                    rec_vec.push(Record {
                                                        multi_uniq_id: rec
                                                            .ext_key,
                                                        ltime: rec.ltime,
                                                        status: rec.status,
                                                        meta: rec
                                                            .meta
                                                            .to_vec()
                                                            .into(),
                                                    });
                                                }
                                            }
                                            Err(e) => {
                                                debug!(
                                                    "recrec_vec {rec_vec:?}"
                                                );
                                                debug!("A. {e:?}");
                                                // return Err(FatalError);
                                            }
                                        }
                                    }
                                    debug!("done");
                                    #[allow(clippy::unwrap_used)]
                                    Ok((
                                        blob.blob().unwrap().to_vec(),
                                        rec_vec,
                                    ))
                                } else {
                                    debug!("B8.");
                                    Err(FatalError)
                                }
                            }
                            l if l <= 16 => {
                                if let Ok(nlri_blob) = ZeroCopyRecord::<
                                    [u8; 16],
                                    MuiRdPathId,
                                >::from_bytes(
                                    first_rec
                                ) {
                                    // let blob = pfx.ext_key;

                                    let mut rec_vec: Vec<
                                        Record<MuiRdPathId, M>,
                                    > = vec![];
                                    for res_rec in recs.iter() {
                                        match res_rec {
                                            Ok(rec) => {
                                                debug!("rec {rec:?}");
                                                if let Ok(rec) =
                                                    ZeroCopyRecord::<
                                                        [u8; 16],
                                                        MuiRdPathId,
                                                    >::from_bytes(
                                                        rec
                                                    )
                                                {
                                                    // debug!("recrec {rec}");
                                                    rec_vec.push(Record {
                                                        multi_uniq_id: rec
                                                            .ext_key,
                                                        ltime: rec.ltime,
                                                        status: rec.status,
                                                        meta: rec
                                                            .meta
                                                            .to_vec()
                                                            .into(),
                                                    });
                                                }
                                            }
                                            Err(e) => {
                                                debug!(
                                                    "recrec_vec {rec_vec:?}"
                                                );
                                                debug!("A. {e:?}");
                                                // return Err(FatalError);
                                            }
                                        }
                                    }
                                    debug!("done");
                                    #[allow(clippy::unwrap_used)]
                                    Ok((nlri_blob.nlri.to_vec(), rec_vec))
                                } else {
                                    debug!("B16.");
                                    Err(FatalError)
                                }
                            }
                            l => {
                                trace!("length {}", l);
                                debug!("C.");
                                Err(FatalError)
                            }
                        }
                    } else {
                        debug!("D.");
                        Err(FatalError)
                    }
                })
            })
            .into_iter()
            .flatten();
        println!("NOTHGIN!!");
        tree_iter
    }

    pub(crate) fn flush_to_disk(&self) -> Result<(), PrefixStoreError> {
        if let Some(p) = &self.persist_tree {
            p.flush_to_disk()
                .map_err(|_| PrefixStoreError::PersistFailed)
        } else {
            Err(PrefixStoreError::PersistFailed)
        }
    }

    pub fn approx_persisted_items(&self) -> usize {
        if let Some(p) = &self.persist_tree {
            p.approximate_len()
        } else {
            0
        }
    }

    pub fn disk_space(&self) -> u64 {
        if let Some(p) = &self.persist_tree {
            p.disk_space()
        } else {
            0
        }
    }
}

//------------ FlowSpecRib ---------------------------------------------------

pub type FlowSpecRib<M, C> = BlobRib<M, 4096, C>;
