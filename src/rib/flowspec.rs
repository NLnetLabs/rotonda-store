//------------ StoredBlob ----------------------------------------------------

use std::{hash::Hash, io, path::Path, sync::atomic::Ordering};

use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use hash32::{FnvHasher, Hasher as _};
use log::{debug, info, trace};
use roaring::RoaringBitmap;
use zerocopy::{NetworkEndian, U32};

use crate::{
    epoch,
    errors::{FatalError, FatalResult, PrefixStoreError},
    lsm_tree::LongKey,
    prefix_cht::{
        blob_map::RdPathIdBlobMap,
        cht::PrefixCht,
        map_type::{
            KeyExtensions, MapType, Mui, MuiRdPathId, MuiRdPathIdBlob,
        },
    },
    prefix_record::{Meta, ValueHeader, ZeroCopyRecord},
    rib::config::{Config, PersistStrategy},
    stats::{Counters, UpsertCounters, UpsertReport},
    types::{PrefixId, Record, RouteStatus},
    IPv4, LsmTree,
};

type BlobCht<M, MT> = PrefixCht<IPv4, M, MT, 9, 33>;

type BlobLsmTree<const BLOB_SIZE: usize> = LsmTree<
    IPv4,
    MuiRdPathIdBlob<BLOB_SIZE>,
    LongKey<IPv4, MuiRdPathIdBlob<BLOB_SIZE>>,
>;

pub struct BlobRib<M: Meta, const BLOB_SIZE: usize, C: Config> {
    pub config: C,
    pub(crate) blob_cht: BlobCht<M, RdPathIdBlobMap<M, BLOB_SIZE>>,
    pub(crate) persist_tree: Option<BlobLsmTree<BLOB_SIZE>>,
    withdrawn_muis_bmin: Atomic<RoaringBitmap>,
    pub counters: Counters,
}

impl<const BLOB_SIZE: usize, M: Meta>
    From<Record<MuiRdPathIdBlob<BLOB_SIZE>, M>>
    for ([u8; BLOB_SIZE], Record<MuiRdPathId, M>)
{
    fn from(value: Record<MuiRdPathIdBlob<BLOB_SIZE>, M>) -> Self {
        (
            #[allow(clippy::unwrap_used)]
            *value
                .multi_uniq_id
                .blob()
                .unwrap()
                .first_chunk::<BLOB_SIZE>()
                .unwrap(),
            Record::new(
                MuiRdPathId::from(&value.multi_uniq_id),
                value.ltime,
                value.status,
                value.meta,
            ),
        )
    }
}

pub fn prefix_hash(blob: &[u8]) -> U32<NetworkEndian> {
    let mut prefix_hash: FnvHasher = Default::default();
    blob.hash(&mut prefix_hash);
    let bits: u32 = prefix_hash.finish32();
    bits.into()
    // PrefixId::<IPv4>::from((bits, 32))
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
            blob_cht: BlobCht::<M, RdPathIdBlobMap<M, BLOB_SIZE>>::init(),
            withdrawn_muis_bmin: Atomic::new(RoaringBitmap::new()),
        };

        Ok(store)
    }

    pub fn insert(
        &self,
        key: &[u8; BLOB_SIZE],
        record: Record<impl KeyExtensions, M>,
        update_path_selections: Option<M::TBI>,
    ) -> Result<UpsertReport, PrefixStoreError> {
        let prefix = prefix_hash(key);
        trace!("try inserting {prefix:?}",);
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
        key: &[u8; BLOB_SIZE],
        record: Record<impl KeyExtensions, M>,
        update_path_selections: Option<M::TBI>,
        guard: &Guard,
    ) -> Result<UpsertReport, PrefixStoreError> {
        let prefix = prefix_hash(key);
        let mui_rd_path_id =
            MuiRdPathIdBlob::from((record.multi_uniq_id, key.as_ref()));
        let rec = Record::<MuiRdPathIdBlob<BLOB_SIZE>, M>::from((
            mui_rd_path_id,
            &record,
        ));
        match self.config.persist_strategy() {
            PersistStrategy::WriteAhead => {
                if let Some(persist_tree) = &self.persist_tree {
                    persist_tree
                        .persist_record_w_long_key(prefix, &rec);

                    self.blob_cht
                        .upsert_prefix(
                            PrefixId::new(prefix, 32),
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
                .upsert_prefix(
                    PrefixId::new(prefix, 32),
                    rec,
                    update_path_selections,
                    guard,
                )
                .map(|(report, old_rec)| {
                    if let Some(rec) = old_rec {
                        if let Some(persist_tree) = &self.persist_tree {
                            persist_tree.persist_record_w_long_key(
                                prefix,
                                &Record::from((mui_rd_path_id, &rec)),
                            );
                        }
                    }
                    report
                }),
            PersistStrategy::MemoryOnly => self
                .blob_cht
                .upsert_prefix(
                    PrefixId::new(prefix, 32),
                    rec,
                    update_path_selections,
                    guard,
                )
                .map(|(report, _)| report),
            PersistStrategy::PersistOnly => {
                if let Some(persist_tree) = &self.persist_tree {
                    let exists = persist_tree.contains_prefix(prefix)?;
                    persist_tree.persist_record_w_short_key(prefix, &rec);
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
        prefix: U32<NetworkEndian>,
        mui: Option<MuiRdPathIdBlob<BLOB_SIZE>>,
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
                    Ok(self
                        .blob_cht
                        .contains_key(PrefixId::new(prefix, 32), mui))
                } else {
                    Ok(self
                        .blob_cht
                        .contains_prefix(PrefixId::new(prefix, 32)))
                }
            }
        }
    }

    pub fn get(
        &self,
        key: &[u8; BLOB_SIZE],
        mui: Option<MuiRdPathId>,
        include_withdrawn: bool,
    ) -> Result<Vec<Record<MuiRdPathIdBlob<BLOB_SIZE>, M>>, PrefixStoreError>
    {
        let guard = &epoch::pin();
        let mut recs_vec = vec![];
        let prefix = prefix_hash(key);
        match self.config.persist_strategy() {
            PersistStrategy::PersistOnly => {
                if let Some(persist_tree) = &self.persist_tree {
                    if let Some(recs) = persist_tree.records_for_prefix(
                        prefix,
                        mui,
                        include_withdrawn,
                        self.withdrawn_muis_bmin(guard),
                    ) {
                        for rec in recs {
                            let Ok(r) = rec else {
                                return Err(PrefixStoreError::FatalError);
                            };
                            if let Ok(rec) = ZeroCopyRecord::<
                                IPv4,
                                MuiRdPathIdBlob<BLOB_SIZE>,
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
                let mui =
                    mui.map(|mui| MuiRdPathIdBlob::from((mui, key.as_ref())));
                Ok(self
                    .blob_cht
                    .get_records_for_prefix(
                        PrefixId::new(prefix, 32),
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
        prefix: U32<NetworkEndian>,
        mui: Mui,
        ltime: u64,
    ) -> Result<(), PrefixStoreError> {
        match self.persist_strategy() {
            PersistStrategy::WriteAhead | PersistStrategy::MemoryOnly => {
                let (stored_prefix, exists) =
                    self.blob_cht.non_recursive_retrieve_prefix_mut(
                        PrefixId::new(prefix, 43),
                    );

                if !exists {
                    return Err(PrefixStoreError::PrefixNotFound);
                }
                let mut record_map = stored_prefix.acquire_read_guard();
                record_map.mark_as_withdrawn_for_mui(mui.mui().into(), ltime);
            }
            PersistStrategy::PersistOnly => {
                if let Some(p_tree) = self.persist_tree.as_ref() {
                    let stored_prefixes =
                        p_tree.records_with_keys_for_prefix_mui(prefix, mui);

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
                let (stored_prefix, exists) =
                    self.blob_cht.non_recursive_retrieve_prefix_mut(
                        PrefixId::new(prefix, 32),
                    );

                if !exists {
                    return Err(PrefixStoreError::StoreNotReadyError);
                }
                let mut record_map = stored_prefix.acquire_read_guard();
                record_map.mark_as_withdrawn_for_mui(mui.mui().into(), ltime);

                // Use the record from the in-memory RIB to persist.
                if let Some(_record) =
                    record_map.get_record_for_key(mui, true)
                {
                    let p_tree =
                        if let Some(p_tree) = self.persist_tree.as_ref() {
                            p_tree
                        } else {
                            return Err(PrefixStoreError::StoreNotReadyError);
                        };

                    p_tree.insert_empty_record(prefix, mui, ltime);
                }
            }
        }

        Ok(())
    }

    // Change the status of the record for the specified (prefix, mui)
    // combination  to Active.
    pub fn mark_mui_as_active_for_prefix(
        &self,
        prefix: U32<NetworkEndian>,
        mui: Mui,
        ltime: u64,
    ) -> FatalResult<()> {
        match self.persist_strategy() {
            PersistStrategy::WriteAhead | PersistStrategy::MemoryOnly => {
                let (stored_prefix, exists) =
                    self.blob_cht.non_recursive_retrieve_prefix_mut(
                        PrefixId::new(prefix, 32),
                    );

                if !exists {
                    return Err(FatalError);
                }
                let mut record_map = stored_prefix.acquire_read_guard();
                // record_map.mark_as_withdrawn_for_mui(mui, ltime);
                record_map.mark_as_active_for_mui(mui.mui().into(), ltime);
            }
            PersistStrategy::PersistOnly => {
                if let Some(p_tree) = self.persist_tree.as_ref() {
                    if let Ok(Some(record_b)) =
                        p_tree.most_recent_record_for_prefix_mui(prefix, mui)
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
                let (stored_prefix, exists) =
                    self.blob_cht.non_recursive_retrieve_prefix_mut(
                        PrefixId::new(prefix, 32),
                    );

                if !exists {
                    return Err(FatalError);
                }
                let mut record_map = stored_prefix.acquire_read_guard();
                record_map.mark_as_active_for_mui(mui.mui().into(), ltime);

                // Use the record from the in-memory RIB to persist.
                if let Some(_record) =
                    record_map.get_record_for_key(mui, true)
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
                    p_tree.insert_empty_record(prefix, mui, ltime);
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
            in_memory_count: self.blob_cht.prefixes_count(),
            persisted_count: self
                .persist_tree
                .as_ref()
                .map_or(0, |p| p.prefixes_count()),
            total_count: self.counters.prefixes_count().iter().sum(),
        }
    }

    pub(crate) fn routes_count(&self) -> UpsertCounters {
        UpsertCounters {
            in_memory_count: self.blob_cht.routes_count(),
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
                Item = FatalResult<(
                    [u8; BLOB_SIZE],
                    Vec<Record<MuiRdPathId, M>>,
                )>,
            > + 'a,
    > {
        match self.config.persist_strategy() {
            PersistStrategy::MemoryOnly
            | PersistStrategy::PersistHistory
            | PersistStrategy::WriteAhead => Box::new(
                self.blob_cht
                    .iter(Some(self.withdrawn_muis_bmin(guard)), 32)
                    .map(|p| {
                        #[allow(clippy::unwrap_used)]
                        let b =
                            *p.1.first()
                                .unwrap()
                                .multi_uniq_id
                                .blob()
                                .unwrap()
                                .first_chunk::<BLOB_SIZE>()
                                .unwrap();
                        Ok((
                            #[allow(clippy::unwrap_used)]
                            b,
                            p.1.into_iter()
                                .map(|r| {
                                    <(
                                        [u8; BLOB_SIZE],
                                        Record<MuiRdPathId, M>,
                                    )>::from(
                                        r
                                    )
                                    .1
                                })
                                .collect::<Vec<_>>(),
                        ))
                    }),
            ),
            PersistStrategy::PersistOnly => {
                Box::new(self.persist_prefixes_iter().map(|r| {
                    #[allow(clippy::unwrap_used)]
                    let p = r.unwrap();
                    #[allow(clippy::unwrap_used)]
                    let b =
                        *p.1.first()
                            .unwrap()
                            .multi_uniq_id
                            .blob()
                            .unwrap()
                            .first_chunk::<BLOB_SIZE>()
                            .unwrap();
                    Ok((
                        #[allow(clippy::unwrap_used)]
                        b,
                        p.1.into_iter()
                            .map(|r| {
                                <(
                                        [u8; BLOB_SIZE],
                                        Record<MuiRdPathId, M>,
                                    )>::from(
                                        r
                                    )
                                    .1
                            })
                            .collect::<Vec<_>>(),
                    ))
                }))
            }
        }
    }

    pub fn records_iter<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> Box<
        dyn Iterator<Item = ([u8; BLOB_SIZE], Record<MuiRdPathId, M>)> + 'a,
    > {
        match self.persist_strategy() {
            PersistStrategy::WriteAhead
            | PersistStrategy::MemoryOnly
            | PersistStrategy::PersistHistory => {
                let iter = self
                    .blob_cht
                    .iter(Some(self.withdrawn_muis_bmin(guard)), 32);

                Box::new(iter.flat_map(|p| p.1.into_iter().map(|r| r.into())))
            }
            PersistStrategy::PersistOnly => {
                let iter = self.persist_prefixes_iter();

                Box::new(iter.flat_map(|p| {
                    #[allow(clippy::unwrap_used)]
                    p.unwrap().1.into_iter().map(|r| r.into())
                }))
            }
        }
    }

    //-------- Persistence ---------------------------------------------------

    pub fn persist_strategy(&self) -> PersistStrategy {
        self.config.persist_strategy()
    }

    pub(crate) fn persist_prefixes_iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = FatalResult<(
            [u8; BLOB_SIZE],
            Vec<Record<MuiRdPathIdBlob<BLOB_SIZE>, M>>,
        )>,
    > + 'a {
        self.persist_tree
            .as_ref()
            .map(|tree| {
                tree.prefixes_iter().map(|recs| {
                    debug!("tree");
                    if let Some(Ok(first_rec)) = recs.first() {
                        debug!("first_rec {first_rec:?}");
                        if let Ok(pfx) = ZeroCopyRecord::<
                            IPv4,
                            MuiRdPathIdBlob<BLOB_SIZE>,
                        >::from_bytes(
                            first_rec
                        ) {
                            #[allow(clippy::unwrap_used)]
                            let blob = pfx.ext_key;

                            let mut rec_vec: Vec<
                                Record<MuiRdPathIdBlob<BLOB_SIZE>, M>,
                            > = vec![];
                            for res_rec in recs.iter() {
                                match res_rec {
                                    Ok(rec) => {
                                        debug!("rec {rec:?}");
                                        if let Ok(rec) = ZeroCopyRecord::<
                                            IPv4,
                                            MuiRdPathIdBlob<BLOB_SIZE>,
                                        >::from_bytes(
                                            rec
                                        ) {
                                            debug!("recrec {rec}");
                                            rec_vec.push(Record {
                                                multi_uniq_id: rec.ext_key,
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
                                        debug!("recrec_vec {rec_vec:?}");
                                        debug!("A. {e:?}");
                                        // return Err(FatalError);
                                    }
                                }
                            }
                            debug!("done");
                            Ok((
                                #[allow(clippy::unwrap_used)]
                                *blob
                                    .blob()
                                    .unwrap()
                                    .first_chunk::<BLOB_SIZE>()
                                    .unwrap(),
                                rec_vec,
                            ))
                        } else {
                            debug!("B.");
                            Err(FatalError)
                        }
                    } else {
                        debug!("C.");
                        Err(FatalError)
                    }
                })
            })
            .into_iter()
            .flatten()
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
