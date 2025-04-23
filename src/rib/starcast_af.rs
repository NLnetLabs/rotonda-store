use std::path::Path;

use inetnum::addr::Prefix;
use log::{info, trace};

use crate::prefix_record::Meta;
use crate::rib::config::PersistStrategy;
use crate::stats::{Counters, UpsertCounters, UpsertReport};
use crate::{epoch, Guard};

use crate::errors::{FatalError, FatalResult};
use crate::prefix_cht::cht::PrefixCht;
use crate::types::prefix_record::{ValueHeader, ZeroCopyRecord};
use crate::types::{PrefixId, RouteStatus};
use crate::TreeBitMap;
use crate::{lsm_tree::LongKey, LsmTree};
use crate::{types::errors::PrefixStoreError, types::prefix_record::Record};

use crate::{IPv4, IPv6};

use crate::AddressFamily;

use super::config::Config;

//------------ StarCastAfRib -------------------------------------------------

// A Routing Information Base that consists of multiple different trees for
// in-memory and on-disk (persisted storage) for one address family. Most of
// the methods on this struct are meant to be publicly available, however they
// are all behind the StarCastRib interface, that abstracts over the address
// family.
#[derive(Debug)]
pub(crate) struct StarCastAfRib<
    AF: AddressFamily,
    // The type that stores the route-like data
    M: Meta,
    // The number of root nodes for the tree bitmap (one for each 4 prefix
    // lengths, so that's 9 for IPv4, 33 for IPv6)
    const N_ROOT_SIZE: usize,
    // The number of root nodes for the prefix CHT (one for each prefix length
    // that can exists, so that's 33 for IPv4, and 129 for IPv6).
    const P_ROOT_SIZE: usize,
    // The configuration, each persistence strategy implements its own type.
    C: Config,
    // The size of the key in the persistence store, this varies per address
    // family. This is 18 for IPv4 (1 octet prefix length, 4 octets address
    // part prefix, 4 octets mui, 8 octets ltime, 1 octet RouteStatus). This
    // corresponds to the `LongKey` struct. It's 30 for IPv6.
    const KEY_SIZE: usize,
> {
    pub config: C,
    pub(crate) tree_bitmap: TreeBitMap<AF, N_ROOT_SIZE>,
    pub(crate) prefix_cht: PrefixCht<AF, M, P_ROOT_SIZE>,
    pub(crate) persist_tree: Option<LsmTree<AF, LongKey<AF>, KEY_SIZE>>,
    pub counters: Counters,
}

impl<
        AF: AddressFamily,
        M: Meta,
        const P_ROOT_SIZE: usize,
        const N_ROOT_SIZE: usize,
        C: Config,
        const KEY_SIZE: usize,
    > StarCastAfRib<AF, M, N_ROOT_SIZE, P_ROOT_SIZE, C, KEY_SIZE>
{
    pub(crate) fn new(
        config: C,
    ) -> Result<
        StarCastAfRib<AF, M, N_ROOT_SIZE, P_ROOT_SIZE, C, KEY_SIZE>,
        Box<dyn std::error::Error>,
    > {
        StarCastAfRib::<AF, M, N_ROOT_SIZE, P_ROOT_SIZE, C, KEY_SIZE>::init(
            config,
        )
    }

    fn init(config: C) -> Result<Self, Box<dyn std::error::Error>> {
        info!("store: initialize store {}", AF::BITS);

        let persist_tree = match config.persist_strategy() {
            PersistStrategy::MemoryOnly => None,
            _ => {
                let persist_path = if let Some(pp) = config.persist_path() {
                    pp
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Missing persistence path".to_string(),
                    )
                    .into());
                };
                let pp_ref = &Path::new(&persist_path);
                Some(LsmTree::new(pp_ref).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Cannot create persistence store",
                    )
                })?)
            }
        };

        let store = StarCastAfRib {
            config,
            tree_bitmap: TreeBitMap::<AF, N_ROOT_SIZE>::new()?,
            persist_tree,
            counters: Counters::default(),
            prefix_cht: PrefixCht::<AF, M, P_ROOT_SIZE>::init(),
        };

        Ok(store)
    }

    pub(crate) fn insert(
        &self,
        prefix: PrefixId<AF>,
        record: Record<M>,
        update_path_selections: Option<M::TBI>,
    ) -> Result<UpsertReport, PrefixStoreError> {
        trace!("try insertingf {:?}", prefix);
        let guard = &epoch::pin();
        self.tree_bitmap
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
                        self.counters.inc_prefixes_count(prefix.len());
                        report.prefix_new = true;
                    }
                    report
                })
            })
    }

    fn upsert_prefix(
        &self,
        prefix: PrefixId<AF>,
        record: Record<M>,
        update_path_selections: Option<M::TBI>,
        guard: &Guard,
    ) -> Result<UpsertReport, PrefixStoreError> {
        let mui = record.multi_uniq_id;
        match self.config.persist_strategy() {
            PersistStrategy::WriteAhead => {
                if let Some(persist_tree) = &self.persist_tree {
                    persist_tree.persist_record_w_long_key(prefix, &record);

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
                                &Record::from((mui, &rec)),
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
                        .tree_bitmap
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
            self.tree_bitmap.prefix_exists_for_mui(prefix, mui)
        } else {
            self.tree_bitmap.prefix_exists(prefix)
        }
    }

    pub fn get_nodes_count(&self) -> usize {
        self.tree_bitmap.nodes_count()
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
    ) -> FatalResult<()> {
        match self.persist_strategy() {
            PersistStrategy::WriteAhead | PersistStrategy::MemoryOnly => {
                let (stored_prefix, exists) =
                    self.prefix_cht.non_recursive_retrieve_prefix_mut(prefix);

                if !exists {
                    return Err(FatalError);
                }
                stored_prefix.record_map.mark_as_active_for_mui(mui, ltime);
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
                    self.prefix_cht.non_recursive_retrieve_prefix_mut(prefix);

                if !exists {
                    return Err(FatalError);
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
        self.tree_bitmap.mark_mui_as_withdrawn(mui, guard)
    }

    // Change the status of the mui globally to Active. Iterators and match
    // functions will default to the status on the record itself.
    pub fn mark_mui_as_active(
        &self,
        mui: u32,
        guard: &Guard,
    ) -> Result<(), PrefixStoreError> {
        self.tree_bitmap.mark_mui_as_active(mui, guard)
    }

    // Whether this mui is globally withdrawn. Note that this overrules
    // (by default) any (prefix, mui) combination in iterators and match
    // functions.
    pub fn mui_is_withdrawn(&self, mui: u32, guard: &Guard) -> bool {
        // unsafe {
        self.tree_bitmap.withdrawn_muis_bmin(guard).contains(mui)
    }

    // Whether this mui is globally active. Note that the local statuses of
    // records (prefix, mui) may be set to withdrawn in iterators and match
    // functions.
    pub(crate) fn is_mui_active(&self, mui: u32, guard: &Guard) -> bool {
        // !unsafe {
        !self.tree_bitmap.withdrawn_muis_bmin(guard).contains(mui)
    }

    pub(crate) fn prefixes_count(&self) -> UpsertCounters {
        UpsertCounters {
            in_memory_count: self.prefix_cht.prefixes_count(),
            persisted_count: self
                .persist_tree
                .as_ref()
                .map_or(0, |p| p.prefixes_count()),
            total_count: self.counters.prefixes_count().iter().sum(),
        }
    }

    pub(crate) fn routes_count(&self) -> UpsertCounters {
        UpsertCounters {
            in_memory_count: self.prefix_cht.routes_count(),
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
        if len <= AF::BITS {
            Ok(UpsertCounters {
                in_memory_count: self
                    .tree_bitmap
                    .prefixes_count_for_len(len)?,
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

    pub fn prefixes_iter<'a>(
        &'a self,
        guard: &'a Guard,
    ) -> impl Iterator<Item = FatalResult<(Prefix, Vec<Record<M>>)>> + 'a
    {
        self.tree_bitmap.prefixes_iter().map(|p| {
            if let Ok(r) = self.get_value(p.into(), None, true, guard) {
                Ok((p, r.unwrap_or_default()))
            } else {
                Err(FatalError)
            }
        })
    }

    //-------- Persistence ---------------------------------------------------

    pub fn persist_strategy(&self) -> PersistStrategy {
        self.config.persist_strategy()
    }

    pub(crate) fn persist_prefixes_iter(
        &self,
    ) -> impl Iterator<Item = FatalResult<(Prefix, Vec<Record<M>>)>> + '_
    {
        self.persist_tree
            .as_ref()
            .map(|tree| {
                tree.prefixes_iter().map(|recs| {
                    if let Some(Ok(first_rec)) = recs.first() {
                        if let Ok(pfx) =
                            ZeroCopyRecord::<AF>::from_bytes(first_rec)
                        {
                            let mut rec_vec: Vec<Record<M>> = vec![];
                            for res_rec in recs.iter() {
                                if let Ok(rec) = res_rec {
                                    if let Ok(rec) =
                                        ZeroCopyRecord::<AF>::from_bytes(rec)
                                    {
                                        rec_vec.push(Record {
                                            multi_uniq_id: rec.multi_uniq_id,
                                            ltime: rec.ltime,
                                            status: rec.status,
                                            meta: rec.meta.to_vec().into(),
                                        });
                                    }
                                } else {
                                    return Err(FatalError);
                                }
                            }
                            Ok((Prefix::from(pfx.prefix), rec_vec))
                        } else {
                            Err(FatalError)
                        }
                    } else {
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

impl<
        M: Meta,
        const N_ROOT_SIZE: usize,
        const P_ROOT_SIZE: usize,
        C: Config,
        const KEY_SIZE: usize,
    > std::fmt::Display
    for StarCastAfRib<IPv4, M, N_ROOT_SIZE, P_ROOT_SIZE, C, KEY_SIZE>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Rib<IPv4, {}>", std::any::type_name::<M>())
    }
}

impl<
        M: Meta,
        const N_ROOT_SIZE: usize,
        const P_ROOT_SIZE: usize,
        C: Config,
        const KEY_SIZE: usize,
    > std::fmt::Display
    for StarCastAfRib<IPv6, M, N_ROOT_SIZE, P_ROOT_SIZE, C, KEY_SIZE>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Rib<IPv6, {}>", std::any::type_name::<M>())
    }
}
