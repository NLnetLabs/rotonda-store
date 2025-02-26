use super::super::persist::lsm_tree::PersistTree;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use inetnum::addr::Prefix;
use log::{info, trace};

use crossbeam_epoch::{self as epoch};
use epoch::{Guard, Owned};
use zerocopy::TryFromBytes;

use crate::local_array::in_memory::tree::TreeBitMap;
use crate::local_array::persist::lsm_tree::KeySize;
use crate::local_array::prefix_cht::cht::PrefixCHT;
use crate::local_array::types::PrefixId;
use crate::prefix_record::{ValueHeader, ZeroCopyRecord};
use crate::prelude::multi::RouteStatus;
use crate::stats::CreatedNodes;
use crate::{
    local_array::errors::PrefixStoreError, prefix_record::PublicRecord,
};

use crate::local_array::in_memory::atomic_types::NodeBuckets;
use crate::local_array::in_memory::atomic_types::PrefixBuckets;

// Make sure to also import the other methods for the Rib, so the proc macro
// create_store can use them.
pub use crate::local_array::in_memory::iterators;
pub use crate::local_array::query;

use crate::{IPv4, IPv6, Meta};

use crate::AddressFamily;

//------------ Config --------------------------------------------------------

/// Defines where records are stored, in-memory and/or persisted (to disk),
/// and, whether new records for a unique (prefix, mui) pair are overwritten
/// or persisted.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PersistStrategy {
    /// Current records are stored both in-memory and persisted. Additionally
    /// historical records are persisted.
    WriteAhead,
    /// Current records are stored in-memory, historical records are pesisted.
    PersistHistory,
    /// Current records are stored in-memory, historical records are discarded
    /// when nwer records appear.
    MemoryOnly,
    /// Both current and historical records are persisted.
    PersistOnly,
}

pub trait Config: Clone + Default + std::fmt::Debug {
    fn persist_strategy(&self) -> PersistStrategy;
    fn persist_path(&self) -> Option<String>;
    fn set_persist_path(&mut self, path: String);
}

//------------ MemoryOnlyConfig ----------------------------------------------

#[derive(Copy, Clone, Debug)]
pub struct MemoryOnlyConfig;

impl Config for MemoryOnlyConfig {
    fn persist_strategy(&self) -> PersistStrategy {
        PersistStrategy::MemoryOnly
    }

    fn persist_path(&self) -> Option<String> {
        None
    }

    fn set_persist_path(&mut self, _: String) {
        unimplemented!()
    }
}

impl Default for MemoryOnlyConfig {
    fn default() -> Self {
        Self
    }
}

//------------ PeristOnlyConfig ----------------------------------------------

impl Default for PersistOnlyConfig {
    fn default() -> Self {
        Self {
            persist_path: "/tmp/rotonda/".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PersistOnlyConfig {
    persist_path: String,
}

impl Config for PersistOnlyConfig {
    fn persist_strategy(&self) -> PersistStrategy {
        PersistStrategy::PersistOnly
    }

    fn persist_path(&self) -> Option<String> {
        Some(self.persist_path.clone())
    }

    fn set_persist_path(&mut self, path: String) {
        self.persist_path = path;
    }
}

//------------ WriteAheadConfig ----------------------------------------------

impl Default for WriteAheadConfig {
    fn default() -> Self {
        Self {
            persist_path: "/tmp/rotonda/".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WriteAheadConfig {
    persist_path: String,
}

impl Config for WriteAheadConfig {
    fn persist_strategy(&self) -> PersistStrategy {
        PersistStrategy::WriteAhead
    }

    fn persist_path(&self) -> Option<String> {
        Some(self.persist_path.clone())
    }

    fn set_persist_path(&mut self, path: String) {
        self.persist_path = path;
    }
}

//------------ PersistHistoryConfig ------------------------------------------

#[derive(Clone, Debug)]
pub struct PersistHistoryConfig {
    persist_path: String,
}

impl Config for PersistHistoryConfig {
    fn persist_strategy(&self) -> PersistStrategy {
        PersistStrategy::PersistHistory
    }

    fn persist_path(&self) -> Option<String> {
        Some(self.persist_path.clone())
    }

    fn set_persist_path(&mut self, path: String) {
        self.persist_path = path;
    }
}

impl Default for PersistHistoryConfig {
    fn default() -> Self {
        Self {
            persist_path: "/tmp/rotonda/".to_string(),
        }
    }
}

//------------ Counters -----------------------------------------------------

#[derive(Debug)]
pub struct Counters {
    // number of created nodes in the in-mem tree
    nodes: AtomicUsize,
    // number of unique prefixes in the store
    prefixes: [AtomicUsize; 129],
    // number of unique (prefix, mui) values inserted in the in-mem tree
    routes: AtomicUsize,
}

impl Counters {
    pub fn get_nodes_count(&self) -> usize {
        self.nodes.load(Ordering::Relaxed)
    }

    pub fn inc_nodes_count(&self) {
        self.nodes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_prefixes_count(&self) -> Vec<usize> {
        self.prefixes
            .iter()
            .map(|pc| pc.load(Ordering::Relaxed))
            .collect::<Vec<_>>()
    }

    pub fn inc_prefixes_count(&self, len: u8) {
        self.prefixes[len as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_prefixes_count(&self, len: u8) {
        self.prefixes[len as usize].fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get_prefix_stats(&self) -> Vec<CreatedNodes> {
        self.prefixes
            .iter()
            .enumerate()
            .filter_map(|(len, count)| {
                let count = count.load(Ordering::Relaxed);
                if count != 0 {
                    Some(CreatedNodes {
                        depth_level: len as u8,
                        count,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn inc_routes_count(&self) {
        self.routes.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for Counters {
    fn default() -> Self {
        let mut prefixes: Vec<AtomicUsize> = Vec::with_capacity(129);
        for _ in 0..=128 {
            prefixes.push(AtomicUsize::new(0));
        }

        Self {
            nodes: AtomicUsize::new(0),
            prefixes: prefixes.try_into().unwrap(),
            routes: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug)]
pub struct UpsertCounters {
    // number of unique inserted prefixes|routes in the in-mem tree
    in_memory_count: usize,
    // number of unique persisted prefixes|routes
    persisted_count: usize,
    // total number of unique inserted prefixes|routes in the RIB
    total_count: usize,
}

impl UpsertCounters {
    pub fn in_memory(&self) -> usize {
        self.in_memory_count
    }

    pub fn persisted(&self) -> usize {
        self.persisted_count
    }

    pub fn total(&self) -> usize {
        self.total_count
    }
}

impl std::fmt::Display for UpsertCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unique Items in-memory:\t{}", self.in_memory_count)?;
        write!(f, "Unique persisted Items:\t{}", self.persisted_count)?;
        write!(f, "Total inserted Items:\t{}", self.total_count)
    }
}

impl std::ops::AddAssign for UpsertCounters {
    fn add_assign(&mut self, rhs: Self) {
        self.in_memory_count += rhs.in_memory_count;
        self.persisted_count += rhs.persisted_count;
        self.total_count += rhs.total_count;
    }
}

impl std::ops::Add for UpsertCounters {
    type Output = UpsertCounters;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            in_memory_count: self.in_memory_count + rhs.in_memory_count,
            persisted_count: self.persisted_count + rhs.persisted_count,
            total_count: self.total_count + rhs.total_count,
        }
    }
}

//------------ StoreStats ----------------------------------------------

#[derive(Debug)]
pub struct StoreStats {
    pub v4: Vec<CreatedNodes>,
    pub v6: Vec<CreatedNodes>,
}

//------------ UpsertReport --------------------------------------------------

#[derive(Debug)]
pub struct UpsertReport {
    // Indicates the number of Atomic Compare-and-Swap operations were
    // necessary to create/update the Record entry. High numbers indicate
    // contention.
    pub cas_count: usize,
    // Indicates whether this was the first mui record for this prefix was
    // created. So, the prefix did not exist before hand.
    pub prefix_new: bool,
    // Indicates whether this mui was new for this prefix. False means an old
    // value was overwritten.
    pub mui_new: bool,
    // The number of mui records for this prefix after the upsert operation.
    pub mui_count: usize,
}

// ----------- Rib -----------------------------------------------------------
//
// A Routing Information Base that consists of multiple different trees for
// in-memory and on-disk (persisted storage).
#[derive(Debug)]
pub struct Rib<
    AF: AddressFamily,
    M: Meta,
    K: KeySize<AF, KEY_SIZE>,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, M>,
    C: Config,
    const KEY_SIZE: usize,
> {
    pub config: C,
    pub(crate) in_memory_tree: TreeBitMap<AF, NB>,
    pub(crate) prefix_cht: PrefixCHT<AF, M, PB>,
    pub(in crate::local_array) persist_tree:
        Option<PersistTree<AF, K, KEY_SIZE>>,
    pub counters: Counters,
}

impl<
        AF: AddressFamily,
        M: crate::prefix_record::Meta,
        K: KeySize<AF, KEY_SIZE>,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, M>,
        C: Config,
        const KEY_SIZE: usize,
    > Rib<AF, M, K, NB, PB, C, KEY_SIZE>
{
    #[allow(clippy::type_complexity)]
    pub(crate) fn new(
        config: C,
    ) -> Result<Rib<AF, M, K, NB, PB, C, KEY_SIZE>, Box<dyn std::error::Error>>
    {
        Rib::<AF, M, K, NB, PB, C, KEY_SIZE>::init(config)
    }

    // pub(crate) fn new_short_key(
    //     config: StoreConfig,
    // ) -> Result<
    //     Rib<AF, M, ShortKey<AF>, NB, PB, KEY_SIZE>,
    //     Box<dyn std::error::Error>,
    // > {
    //     Rib::<AF, M, ShortKey<AF>, NB, PB, KEY_SIZE>::init(config)
    // }

    // pub(crate) fn new_long_key(
    //     config: StoreConfig,
    // ) -> Result<
    //     Rib<AF, M, LongKey<AF>, NB, PB, KEY_SIZE>,
    //     Box<dyn std::error::Error>,
    // > {
    //     Rib::<AF, M, LongKey<AF>, NB, PB, KEY_SIZE>::init(config)
    // }

    fn init(config: C) -> Result<Self, Box<dyn std::error::Error>> {
        info!("store: initialize store {}", AF::BITS);

        let persist_tree = match config.persist_strategy() {
            PersistStrategy::MemoryOnly => None,
            _ => {
                let persist_path = &config.persist_path().unwrap();
                let pp_ref = &Path::new(persist_path);
                Some(PersistTree::new(pp_ref))
            }
        };

        let store = Rib {
            config,
            in_memory_tree: TreeBitMap::<AF, NB>::new()?,
            persist_tree,
            counters: Counters::default(),
            prefix_cht: PrefixCHT::<AF, M, PB>::new(),
        };

        Ok(store)
    }

    pub(crate) fn insert(
        &self,
        prefix: PrefixId<AF>,
        record: PublicRecord<M>,
        update_path_selections: Option<M::TBI>,
    ) -> Result<UpsertReport, PrefixStoreError> {
        trace!("try insertingf {:?}", prefix);
        let guard = &epoch::pin();
        self.in_memory_tree
            .set_prefix_exists(prefix, record.multi_uniq_id)
            .and_then(|(retry_count, exists)| {
                trace!("exists, upsert it");
                self.upsert_prefix(
                    prefix,
                    record,
                    update_path_selections,
                    guard,
                )
                .map(|mut report| {
                    if report.mui_new {
                        self.counters.inc_routes_count();
                    }
                    report.cas_count += retry_count as usize;
                    if !exists {
                        self.counters.inc_prefixes_count(prefix.get_len());
                        report.prefix_new = true;
                    }
                    report
                })
            })
    }

    fn upsert_prefix(
        &self,
        prefix: PrefixId<AF>,
        record: PublicRecord<M>,
        update_path_selections: Option<M::TBI>,
        guard: &Guard,
    ) -> Result<UpsertReport, PrefixStoreError> {
        let mui = record.multi_uniq_id;
        match self.config.persist_strategy() {
            PersistStrategy::WriteAhead => {
                if let Some(persist_tree) = &self.persist_tree {
                    persist_tree.persist_record_w_short_key(prefix, &record);

                    self.prefix_cht
                        .upsert_prefix(
                            prefix,
                            record,
                            update_path_selections,
                            guard,
                        )
                        .map(|(report, _old_rec)| report)
                } else {
                    Err(PrefixStoreError::StoreNotReadyError)
                }
            }
            PersistStrategy::PersistHistory => self
                .prefix_cht
                .upsert_prefix(prefix, record, update_path_selections, guard)
                .map(|(report, old_rec)| {
                    if let Some(rec) = old_rec {
                        if let Some(persist_tree) = &self.persist_tree {
                            persist_tree.persist_record_w_long_key(
                                prefix,
                                &PublicRecord::from((mui, &rec)),
                            );
                        }
                    }
                    report
                }),
            PersistStrategy::MemoryOnly => self
                .prefix_cht
                .upsert_prefix(prefix, record, update_path_selections, guard)
                .map(|(report, _)| report),
            PersistStrategy::PersistOnly => {
                if let Some(persist_tree) = &self.persist_tree {
                    let (retry_count, exists) = self
                        .in_memory_tree
                        .set_prefix_exists(prefix, record.multi_uniq_id)?;
                    persist_tree.persist_record_w_short_key(prefix, &record);
                    Ok(UpsertReport {
                        cas_count: retry_count as usize,
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

    pub fn contains(&self, prefix: PrefixId<AF>, mui: Option<u32>) -> bool {
        if let Some(mui) = mui {
            self.in_memory_tree.prefix_exists_for_mui(prefix, mui)
        } else {
            self.in_memory_tree.prefix_exists(prefix)
        }
    }

    pub fn get_nodes_count(&self) -> usize {
        self.in_memory_tree.get_nodes_count()
    }

    // Change the status of the record for the specified (prefix, mui)
    // combination  to Withdrawn.
    pub fn mark_mui_as_withdrawn_for_prefix(
        &self,
        prefix: PrefixId<AF>,
        mui: u32,
        ltime: u64,
    ) -> Result<(), PrefixStoreError> {
        match self.persist_strategy() {
            PersistStrategy::WriteAhead | PersistStrategy::MemoryOnly => {
                let (stored_prefix, exists) =
                    self.prefix_cht.non_recursive_retrieve_prefix_mut(prefix);

                if !exists {
                    return Err(PrefixStoreError::PrefixNotFound);
                }
                stored_prefix
                    .record_map
                    .mark_as_withdrawn_for_mui(mui, ltime);
            }
            PersistStrategy::PersistOnly => {
                println!(
                    "mark as wd in persist tree {:?} for mui {:?}",
                    prefix, mui
                );
                let p_tree = self.persist_tree.as_ref().unwrap();
                let stored_prefixes =
                    p_tree.get_records_with_keys_for_prefix_mui(prefix, mui);

                for s in stored_prefixes {
                    // let key: [u8; KEY_SIZE] = PersistTree::<
                    //     AF,
                    //     // PREFIX_SIZE,
                    //     K,
                    //     KEY_SIZE,
                    // >::persistence_key(
                    //     prefix,
                    //     mui,
                    //     ltime,
                    //     RouteStatus::Withdrawn,
                    // );

                    // let key = K::new_write_key(
                    //     prefix,
                    //     mui,
                    //     ltime,
                    //     RouteStatus::Withdrawn,
                    // );
                    // let record =
                    //     ZeroCopyRecord::<AF>::try_ref_from_prefix(&s)
                    //         .unwrap()
                    //         .0;
                    // let key = ShortKey::from((record.prefix, mui));
                    // trace!("insert key {:?}", key);

                    // // new header for this value
                    // let mut value = ValueHeader {
                    //     ltime,
                    //     status: RouteStatus::Withdrawn,
                    // }
                    // .as_bytes()
                    // .to_vec();
                    // value.extend_from_slice(record.meta.as_ref());

                    // p_tree.insert(key.as_bytes(), &value);
                    let header = ValueHeader {
                        ltime,
                        status: RouteStatus::Withdrawn,
                    };
                    p_tree.rewrite_header_for_record(header, &s);

                    // remove the entry for the same (prefix, mui), but with
                    // an older logical time
                    // let key = K::short_key(&s);
                    // p_tree.remove(key.as_bytes());
                }
            }
            PersistStrategy::PersistHistory => {
                // First do the in-memory part
                let (stored_prefix, exists) =
                    self.prefix_cht.non_recursive_retrieve_prefix_mut(prefix);

                if !exists {
                    return Err(PrefixStoreError::StoreNotReadyError);
                }
                stored_prefix
                    .record_map
                    .mark_as_withdrawn_for_mui(mui, ltime);

                // Use the record from the in-memory RIB to persist.
                if let Some(_record) =
                    stored_prefix.record_map.get_record_for_mui(mui, true)
                {
                    let p_tree =
                        if let Some(p_tree) = self.persist_tree.as_ref() {
                            p_tree
                        } else {
                            return Err(PrefixStoreError::StoreNotReadyError);
                        };

                    // let key: [u8; KEY_SIZE] = PersistTree::<
                    //     AF,
                    //     K,
                    //     // PREFIX_SIZE,
                    //     KEY_SIZE,
                    // >::persistence_key(
                    //     prefix,
                    //     mui,
                    //     ltime,
                    //     RouteStatus::Withdrawn,
                    // );
                    // let key = K::new_write_key(
                    //     prefix,
                    //     mui,
                    //     ltime,
                    //     RouteStatus::Withdrawn,
                    // );
                    //

                    // Here we are keeping persisted history, so no removal of
                    // old (prefix, mui) records.
                    // We are inserting an empty record, since this is a
                    // withdrawal.
                    // p_tree.insert(
                    //     LongKey::from((
                    //         prefix,
                    //         mui,
                    //         ltime,
                    //         RouteStatus::Withdrawn,
                    //     ))
                    //     .as_bytes(),
                    //     &[],
                    // );
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
        prefix: PrefixId<AF>,
        mui: u32,
        ltime: u64,
    ) -> Result<(), PrefixStoreError> {
        match self.persist_strategy() {
            PersistStrategy::WriteAhead | PersistStrategy::MemoryOnly => {
                let (stored_prefix, exists) =
                    self.prefix_cht.non_recursive_retrieve_prefix_mut(prefix);

                if !exists {
                    return Err(PrefixStoreError::PrefixNotFound);
                }
                stored_prefix.record_map.mark_as_active_for_mui(mui, ltime);
            }
            PersistStrategy::PersistOnly => {
                let p_tree = self.persist_tree.as_ref().unwrap();
                // let stored_prefixes = p_tree
                //     .get_records_with_keys_for_prefix_mui::<M>(prefix, mui)

                if let Some(record_b) =
                    p_tree.get_most_recent_record_for_prefix_mui(prefix, mui)
                {
                    // let new_key: [u8; KEY_SIZE] = PersistTree::<
                    //     AF,
                    //     // PREFIX_SIZE,
                    //     KEY_SIZE,
                    // >::persistence_key(
                    //     prefix,
                    //     mui,
                    //     ltime,
                    //     RouteStatus::Active,
                    // );

                    // let record: &mut ZeroCopyRecord<AF> =
                    //     ZeroCopyRecord::try_mut_from_bytes(&mut record_b)
                    //         .unwrap();
                    // // record.prefix = prefix;
                    // // record.multi_uniq_id = mui;
                    // record.ltime = ltime;
                    // record.status = RouteStatus::Active;

                    let header = ValueHeader {
                        ltime,
                        status: RouteStatus::Active,
                    };
                    // .as_bytes()
                    // .to_vec();
                    // value.extend_from_slice(record.meta.as_ref());

                    // p_tree.insert(
                    //     ShortKey::from((prefix, mui)).as_bytes(),
                    //     value.as_bytes(),
                    // );
                    p_tree.rewrite_header_for_record(header, &record_b);
                    // // remove the entry for the same (prefix, mui), but with
                    // // an older logical time
                    // let old_key = PersistTree::<
                    //     AF,
                    //     // PREFIX_SIZE,
                    //     KEY_SIZE,
                    // >::persistence_key(
                    //     prefix,
                    //     mui,
                    //     record.ltime,
                    //     record.status,
                    // );
                    // p_tree.remove(old_key);
                }
            }
            PersistStrategy::PersistHistory => {
                // First do the in-memory part
                let (stored_prefix, exists) =
                    self.prefix_cht.non_recursive_retrieve_prefix_mut(prefix);

                if !exists {
                    return Err(PrefixStoreError::PrefixNotFound);
                }
                stored_prefix.record_map.mark_as_active_for_mui(mui, ltime);

                // Use the record from the in-memory RIB to persist.
                if let Some(_record) =
                    stored_prefix.record_map.get_record_for_mui(mui, true)
                {
                    let p_tree =
                        if let Some(p_tree) = self.persist_tree.as_ref() {
                            p_tree
                        } else {
                            return Err(PrefixStoreError::StoreNotReadyError);
                        };

                    // let key: [u8; KEY_SIZE] = PersistTree::<
                    //     AF,
                    //     // PREFIX_SIZE,
                    //     KEY_SIZE,
                    // >::persistence_key(
                    //     prefix,
                    //     mui,
                    //     ltime,
                    //     RouteStatus::Active,
                    // );
                    // let key = K::new_write_key(
                    //     prefix,
                    //     mui,
                    //     ltime,
                    //     RouteStatus::Active,
                    // );
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
        let current = self
            .in_memory_tree
            .withdrawn_muis_bmin
            .load(Ordering::Acquire, guard);

        let mut new = unsafe { current.as_ref() }.unwrap().clone();
        new.insert(mui);

        #[allow(clippy::assigning_clones)]
        loop {
            match self.in_memory_tree.withdrawn_muis_bmin.compare_exchange(
                current,
                Owned::new(new),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(updated) => {
                    new =
                        unsafe { updated.current.as_ref() }.unwrap().clone();
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
        let current = self
            .in_memory_tree
            .withdrawn_muis_bmin
            .load(Ordering::Acquire, guard);

        let mut new = unsafe { current.as_ref() }.unwrap().clone();
        new.remove(mui);

        #[allow(clippy::assigning_clones)]
        loop {
            match self.in_memory_tree.withdrawn_muis_bmin.compare_exchange(
                current,
                Owned::new(new),
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(updated) => {
                    new =
                        unsafe { updated.current.as_ref() }.unwrap().clone();
                }
            }
        }
    }

    // Whether this mui is globally withdrawn. Note that this overrules
    // (by default) any (prefix, mui) combination in iterators and match
    // functions.
    pub fn mui_is_withdrawn(&self, mui: u32, guard: &Guard) -> bool {
        unsafe {
            self.in_memory_tree
                .withdrawn_muis_bmin
                .load(Ordering::Acquire, guard)
                .as_ref()
        }
        .unwrap()
        .contains(mui)
    }

    // Whether this mui is globally active. Note that the local statuses of
    // records (prefix, mui) may be set to withdrawn in iterators and match
    // functions.
    pub fn mui_is_active(&self, mui: u32, guard: &Guard) -> bool {
        !unsafe {
            self.in_memory_tree
                .withdrawn_muis_bmin
                .load(Ordering::Acquire, guard)
                .as_ref()
        }
        .unwrap()
        .contains(mui)
    }

    pub fn get_prefixes_count(&self) -> UpsertCounters {
        UpsertCounters {
            in_memory_count: self.in_memory_tree.get_prefixes_count(),
            persisted_count: self
                .persist_tree
                .as_ref()
                .map_or(0, |p| p.get_prefixes_count()),
            total_count: self.counters.get_prefixes_count().iter().sum(),
        }
    }

    pub fn get_prefixes_count_for_len(&self, len: u8) -> UpsertCounters {
        UpsertCounters {
            in_memory_count: self
                .in_memory_tree
                .get_prefixes_count_for_len(len),
            persisted_count: self
                .persist_tree
                .as_ref()
                .map_or(0, |p| p.get_prefixes_count_for_len(len)),
            total_count: self.counters.get_prefixes_count()[len as usize],
        }
    }

    pub fn prefixes_iter<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> impl Iterator<Item = (Prefix, Vec<PublicRecord<M>>)> + 'a {
        self.in_memory_tree.prefixes_iter().map(|p| {
            (
                p,
                self.get_value(p.into(), None, true, guard)
                    .unwrap_or_default(),
            )
        })
    }

    //-------- Persistence ---------------------------------------------------

    pub fn persist_strategy(&self) -> PersistStrategy {
        self.config.persist_strategy()
    }

    pub fn persist_prefixes_iter(
        &self,
    ) -> impl Iterator<Item = (Prefix, Vec<PublicRecord<M>>)> + '_ {
        self.persist_tree
            .as_ref()
            .map(|tree| {
                tree.prefixes_iter().map(|recs| {
                    (
                        Prefix::from(
                            ZeroCopyRecord::<AF>::try_ref_from_bytes(
                                &recs[0],
                            )
                            .unwrap()
                            .prefix,
                        ),
                        recs.iter()
                            .map(|rec| {
                                let rec =
                                    ZeroCopyRecord::<AF>::try_ref_from_bytes(
                                        rec,
                                    )
                                    .unwrap();
                                PublicRecord {
                                    multi_uniq_id: rec.multi_uniq_id,
                                    ltime: rec.ltime,
                                    status: rec.status,
                                    meta: rec.meta.to_vec().into(),
                                }
                            })
                            .collect::<Vec<PublicRecord<M>>>(),
                    )
                })
            })
            .into_iter()
            .flatten()
    }
    // pub fn get_records_for_prefix(
    //     &self,
    //     prefix: &Prefix,
    //     mui: Option<u32>,
    //     include_withdrawn: bool,
    // ) -> Vec<PublicRecord<M>> {
    //     trace!("get records for prefix in the right store");
    //     let guard = epoch::pin();
    //     match self.persist_strategy() {
    //         PersistStrategy::PersistOnly => self
    //             .persist_tree
    //             .as_ref()
    //             .map(|tree| {
    //                 tree.get_records_for_prefix(
    //                     PrefixId::from(*prefix),
    //                     mui,
    //                     include_withdrawn,
    //                     self.in_memory_tree.withdrawn_muis_bmin(&guard),
    //                 )
    //             })
    //             .unwrap_or_default(),
    //         _ => self
    //             .prefix_cht
    //             .get_records_for_prefix(
    //                 PrefixId::from(*prefix),
    //                 mui,
    //                 include_withdrawn,
    //                 self.in_memory_tree.withdrawn_muis_bmin(&guard),
    //             )
    //             .unwrap_or_default(),
    //     }
    // }

    pub fn flush_to_disk(&self) -> Result<(), PrefixStoreError> {
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

impl<
        M: Meta,
        K: KeySize<IPv4, KEY_SIZE>,
        NB: NodeBuckets<IPv4>,
        PB: PrefixBuckets<IPv4, M>,
        // const PREFIX_SIZE: usize,
        C: Config,
        const KEY_SIZE: usize,
    > std::fmt::Display for Rib<IPv4, M, K, NB, PB, C, KEY_SIZE>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Rib<IPv4, {}>", std::any::type_name::<M>())
    }
}

impl<
        M: Meta,
        K: KeySize<IPv6, KEY_SIZE>,
        NB: NodeBuckets<IPv6>,
        PB: PrefixBuckets<IPv6, M>,
        C: Config,
        // const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > std::fmt::Display for Rib<IPv6, M, K, NB, PB, C, KEY_SIZE>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Rib<IPv6, {}>", std::any::type_name::<M>())
    }
}
