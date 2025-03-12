use crossbeam_epoch::Guard;
use inetnum::addr::Prefix;
use rand::prelude::*;

use crate::{
    epoch,
    match_options::{MatchOptions, QueryResult},
    prefix_record::{Meta, PrefixRecord, Record},
    rib::config::Config,
    types::{errors::PrefixStoreError, PrefixId},
    AddressFamily, IPv4, IPv6,
};

use super::starcast_af::StarCastAfRib;
use crate::rib::config::PersistStrategy;
use crate::stats::{StoreStats, UpsertCounters, UpsertReport};

pub const STRIDE_SIZE: u8 = 4;
pub const BIT_SPAN_SIZE: u8 = 32;

/// A RIB that stores routes (and/or other data) for [`IPv4`,
/// `IPv6`]/[`Unicast`, `Multicast`], i.e. AFI/SAFI types `{1,2}/{1,2}`.
///
/// Routes can be kept in memory, persisted to disk, or both. Also, historical
/// records can be persisted.
pub struct StarCastRib<M: Meta, C: Config> {
    v4: StarCastAfRib<IPv4, M, 9, 33, C, 18>,
    v6: StarCastAfRib<IPv6, M, 33, 129, C, 30>,
    config: C,
}

impl<'a, M: Meta, C: Config> StarCastRib<M, C> {
    /// Create a new RIB with a default configuration. The default
    /// configuration uses the `MemoryOnly` persistence strategy.
    ///
    /// This method is really infallible, but we return a result anyway to be
    /// in line with the `new_with_config` method.
    pub fn try_default() -> Result<Self, PrefixStoreError> {
        let config = C::default();
        Self::new_with_config(config)
            .map_err(|_| PrefixStoreError::StoreNotReadyError)
    }

    /// Create a new RIB with the specified configuration.
    ///
    /// Creation may fail for all strategies that persist to disk, e.g.
    /// the persistence path does not exist, it doesn't have the correct
    /// permissions, etc.
    pub fn new_with_config(
        config: C,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rng = rand::rng();
        let uuid: String = rng
            .sample_iter(rand::distr::Alphanumeric)
            .take(12)
            .map(char::from)
            .collect();
        let mut config_v4 = config.clone();
        let mut config_v6 = config.clone();

        if let Some(path) = config_v4.persist_path() {
            let pp = format!("{}/{}/ipv4/", path, uuid);
            config_v4.set_persist_path(pp);
        };

        if let Some(path) = config_v6.persist_path() {
            config_v6.set_persist_path(format!("{}/{}/ipv6/", path, uuid));
        }

        Ok(Self {
            v4: StarCastAfRib::new(config_v4)?,
            v6: StarCastAfRib::new(config_v6)?,
            config,
        })
    }

    pub fn match_prefix(
        &'a self,
        search_pfx: &Prefix,
        options: &MatchOptions,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => self.v4.match_prefix(
                PrefixId::<IPv4>::new(
                    <IPv4 as AddressFamily>::from_ipaddr(addr),
                    search_pfx.len(),
                ),
                options,
                guard,
            ),
            std::net::IpAddr::V6(addr) => self.v6.match_prefix(
                PrefixId::<IPv6>::new(
                    <IPv6 as AddressFamily>::from_ipaddr(addr),
                    search_pfx.len(),
                ),
                options,
                guard,
            ),
        }
    }

    pub fn contains(&'a self, prefix: &Prefix, mui: Option<u32>) -> bool {
        match prefix.addr() {
            std::net::IpAddr::V4(_addr) => {
                self.v4.contains(PrefixId::<IPv4>::from(*prefix), mui)
            }
            std::net::IpAddr::V6(_addr) => {
                self.v6.contains(PrefixId::<IPv6>::from(*prefix), mui)
            }
        }
    }

    pub fn best_path(
        &'a self,
        search_pfx: &Prefix,
        guard: &Guard,
    ) -> Option<Result<Record<M>, PrefixStoreError>> {
        match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => self.v4.best_path(
                PrefixId::<IPv4>::new(
                    <IPv4 as AddressFamily>::from_ipaddr(addr),
                    search_pfx.len(),
                ),
                guard,
            ),
            std::net::IpAddr::V6(addr) => self.v6.best_path(
                PrefixId::<IPv6>::new(
                    <IPv6 as AddressFamily>::from_ipaddr(addr),
                    search_pfx.len(),
                ),
                guard,
            ),
        }
    }

    pub fn calculate_and_store_best_and_backup_path(
        &self,
        search_pfx: &Prefix,
        tbi: &<M as Meta>::TBI,
        guard: &Guard,
    ) -> Result<(Option<u32>, Option<u32>), PrefixStoreError> {
        match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => {
                self.v4.calculate_and_store_best_and_backup_path(
                    PrefixId::<IPv4>::new(
                        <IPv4 as AddressFamily>::from_ipaddr(addr),
                        search_pfx.len(),
                    ),
                    tbi,
                    guard,
                )
            }
            std::net::IpAddr::V6(addr) => {
                self.v6.calculate_and_store_best_and_backup_path(
                    PrefixId::<IPv6>::new(
                        <IPv6 as AddressFamily>::from_ipaddr(addr),
                        search_pfx.len(),
                    ),
                    tbi,
                    guard,
                )
            }
        }
    }

    pub fn is_ps_outdated(
        &self,
        search_pfx: &Prefix,
        guard: &Guard,
    ) -> Result<bool, PrefixStoreError> {
        match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => self.v4.is_ps_outdated(
                PrefixId::<IPv4>::new(
                    <IPv4 as AddressFamily>::from_ipaddr(addr),
                    search_pfx.len(),
                ),
                guard,
            ),
            std::net::IpAddr::V6(addr) => self.v6.is_ps_outdated(
                PrefixId::<IPv6>::new(
                    <IPv6 as AddressFamily>::from_ipaddr(addr),
                    search_pfx.len(),
                ),
                guard,
            ),
        }
    }

    pub fn more_specifics_from(
        &'a self,
        search_pfx: &Prefix,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => self.v4.more_specifics_from(
                PrefixId::<IPv4>::new(
                    <IPv4 as AddressFamily>::from_ipaddr(addr),
                    search_pfx.len(),
                ),
                mui,
                include_withdrawn,
                guard,
            ),
            std::net::IpAddr::V6(addr) => self.v6.more_specifics_from(
                PrefixId::<IPv6>::new(
                    <IPv6 as AddressFamily>::from_ipaddr(addr),
                    search_pfx.len(),
                ),
                mui,
                include_withdrawn,
                guard,
            ),
        }
    }

    pub fn less_specifics_from(
        &'a self,
        search_pfx: &Prefix,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> QueryResult<M> {
        match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => self.v4.less_specifics_from(
                PrefixId::<IPv4>::new(
                    <IPv4 as AddressFamily>::from_ipaddr(addr),
                    search_pfx.len(),
                ),
                mui,
                include_withdrawn,
                guard,
            ),
            std::net::IpAddr::V6(addr) => self.v6.less_specifics_from(
                PrefixId::<IPv6>::new(
                    <IPv6 as AddressFamily>::from_ipaddr(addr),
                    search_pfx.len(),
                ),
                mui,
                include_withdrawn,
                guard,
            ),
        }
    }

    pub fn less_specifics_iter_from(
        &'a self,
        search_pfx: &Prefix,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> impl Iterator<Item = PrefixRecord<M>> + 'a {
        let (left, right) = match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => (
                Some(
                    self.v4
                        .less_specifics_iter_from(
                            PrefixId::<IPv4>::new(
                                <IPv4 as AddressFamily>::from_ipaddr(addr),
                                search_pfx.len(),
                            ),
                            mui,
                            include_withdrawn,
                            guard,
                        )
                        .map(PrefixRecord::from),
                ),
                None,
            ),
            std::net::IpAddr::V6(addr) => (
                None,
                Some(
                    self.v6
                        .less_specifics_iter_from(
                            PrefixId::<IPv6>::new(
                                <IPv6 as AddressFamily>::from_ipaddr(addr),
                                search_pfx.len(),
                            ),
                            mui,
                            include_withdrawn,
                            guard,
                        )
                        .map(PrefixRecord::from),
                ),
            ),
        };

        left.into_iter()
            .flatten()
            .chain(right.into_iter().flatten())
    }

    pub fn more_specifics_iter_from(
        &'a self,
        search_pfx: &Prefix,
        mui: Option<u32>,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> impl Iterator<Item = PrefixRecord<M>> + 'a {
        let (left, right) = match search_pfx.addr() {
            std::net::IpAddr::V4(addr) => (
                Some(
                    self.v4
                        .more_specifics_iter_from(
                            PrefixId::<IPv4>::new(
                                <IPv4 as AddressFamily>::from_ipaddr(addr),
                                search_pfx.len(),
                            ),
                            mui,
                            include_withdrawn,
                            guard,
                        )
                        .map(PrefixRecord::from),
                ),
                None,
            ),
            std::net::IpAddr::V6(addr) => (
                None,
                Some(
                    self.v6
                        .more_specifics_iter_from(
                            PrefixId::<IPv6>::new(
                                <IPv6 as AddressFamily>::from_ipaddr(addr),
                                search_pfx.len(),
                            ),
                            mui,
                            include_withdrawn,
                            guard,
                        )
                        .map(PrefixRecord::from),
                ),
            ),
        };

        left.into_iter()
            .flatten()
            .chain(right.into_iter().flatten())
    }

    pub fn iter_records_for_mui_v4(
        &'a self,
        mui: u32,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> impl Iterator<Item = PrefixRecord<M>> + 'a {
        if self.v4.mui_is_withdrawn(mui, guard) && !include_withdrawn {
            None
        } else {
            Some(
                self.v4
                    .more_specifics_iter_from(
                        PrefixId::<IPv4>::new(
                            <IPv4 as AddressFamily>::zero(),
                            0,
                        ),
                        Some(mui),
                        include_withdrawn,
                        guard,
                    )
                    .map(PrefixRecord::from),
            )
        }
        .into_iter()
        .flatten()
    }

    pub fn iter_records_for_mui_v6(
        &'a self,
        mui: u32,
        include_withdrawn: bool,
        guard: &'a Guard,
    ) -> impl Iterator<Item = PrefixRecord<M>> + 'a {
        if self.v6.mui_is_withdrawn(mui, guard) && !include_withdrawn {
            None
        } else {
            Some(
                self.v6
                    .more_specifics_iter_from(
                        PrefixId::<IPv6>::new(
                            <IPv6 as AddressFamily>::zero(),
                            0,
                        ),
                        Some(mui),
                        include_withdrawn,
                        guard,
                    )
                    .map(PrefixRecord::from),
            )
        }
        .into_iter()
        .flatten()
    }

    pub fn insert(
        &self,
        prefix: &Prefix,
        record: Record<M>,
        update_path_selections: Option<M::TBI>,
    ) -> Result<UpsertReport, PrefixStoreError> {
        match prefix.addr() {
            std::net::IpAddr::V4(_addr) => self.v4.insert(
                PrefixId::<IPv4>::from(*prefix),
                record,
                update_path_selections,
            ),
            std::net::IpAddr::V6(_addr) => self.v6.insert(
                PrefixId::<IPv6>::from(*prefix),
                record,
                update_path_selections,
            ),
        }
    }

    pub fn prefixes_iter(
        &'a self,
        guard: &'a Guard,
    ) -> impl Iterator<Item = PrefixRecord<M>> + 'a {
        self.v4
            .prefixes_iter(guard)
            .map(PrefixRecord::from)
            .chain(self.v6.prefixes_iter(guard).map(PrefixRecord::from))
    }

    pub fn prefixes_iter_v4(
        &'a self,
        guard: &'a Guard,
    ) -> impl Iterator<Item = PrefixRecord<M>> + 'a {
        self.v4.prefixes_iter(guard).map(PrefixRecord::from)
    }

    pub fn prefixes_iter_v6(
        &'a self,
        guard: &'a Guard,
    ) -> impl Iterator<Item = PrefixRecord<M>> + 'a {
        self.v6.prefixes_iter(guard).map(PrefixRecord::from)
    }

    pub fn persist_prefixes_iter(
        &'a self,
    ) -> impl Iterator<Item = PrefixRecord<M>> + 'a {
        self.v4
            .persist_prefixes_iter()
            .map(PrefixRecord::from)
            .chain(self.v6.persist_prefixes_iter().map(PrefixRecord::from))
    }

    pub fn persist_prefixes_iter_v4(
        &'a self,
    ) -> impl Iterator<Item = PrefixRecord<M>> + 'a {
        self.v4.persist_prefixes_iter().map(PrefixRecord::from)
    }

    pub fn persist_prefixes_iter_v6(
        &'a self,
    ) -> impl Iterator<Item = PrefixRecord<M>> + 'a {
        self.v6.persist_prefixes_iter().map(PrefixRecord::from)
    }

    pub fn is_mui_active(&self, mui: u32) -> bool {
        let guard = &epoch::pin();
        self.v4.is_mui_active(mui, guard) || self.v6.is_mui_active(mui, guard)
    }

    pub fn is_mui_active_v4(&self, mui: u32) -> bool {
        let guard = &epoch::pin();
        self.v4.is_mui_active(mui, guard)
    }

    pub fn is_mui_active_v6(&self, mui: u32) -> bool {
        let guard = &epoch::pin();
        self.v6.is_mui_active(mui, guard)
    }

    /// Change the local status of the record for the combination of
    /// (prefix, multi_uniq_id) to Withdrawn. Note that by default the
    /// global `Withdrawn` status for a mui overrides the local status
    /// of a record.
    pub fn mark_mui_as_withdrawn_for_prefix(
        &self,
        prefix: &Prefix,
        mui: u32,
        ltime: u64,
    ) -> Result<(), PrefixStoreError> {
        match prefix.addr() {
            std::net::IpAddr::V4(_addr) => {
                self.v4.mark_mui_as_withdrawn_for_prefix(
                    PrefixId::<IPv4>::from(*prefix),
                    mui,
                    ltime,
                )
            }
            std::net::IpAddr::V6(_addr) => {
                self.v6.mark_mui_as_withdrawn_for_prefix(
                    PrefixId::<IPv6>::from(*prefix),
                    mui,
                    ltime,
                )
            }
        }
    }

    /// Change the local status of the record for the combination of
    /// (prefix, multi_uniq_id) to Active. Note that by default the
    /// global `Withdrawn` status for a mui overrides the local status
    /// of a record.
    pub fn mark_mui_as_active_for_prefix(
        &self,
        prefix: &Prefix,
        mui: u32,
        ltime: u64,
    ) -> Result<(), PrefixStoreError> {
        match prefix.addr() {
            std::net::IpAddr::V4(_addr) => {
                self.v4.mark_mui_as_active_for_prefix(
                    PrefixId::<IPv4>::from(*prefix),
                    mui,
                    ltime,
                )
            }
            std::net::IpAddr::V6(_addr) => {
                self.v6.mark_mui_as_active_for_prefix(
                    PrefixId::<IPv6>::from(*prefix),
                    mui,
                    ltime,
                )
            }
        }
    }

    /// Change the status of all records for IPv4 prefixes for this
    /// `multi_uniq_id` globally to Active.  Note that the global
    /// `Active` status will be overridden by the local status of the
    /// record.
    pub fn mark_mui_as_active_v4(
        &self,
        mui: u32,
    ) -> Result<(), PrefixStoreError> {
        let guard = &epoch::pin();

        self.v4.mark_mui_as_active(mui, guard)
    }

    /// Change the status of all records for IPv4 prefixes for this
    /// `multi_uniq_id` globally to Withdrawn. A global `Withdrawn`
    /// status for a `multi_uniq_id` overrides the local status of
    /// prefixes for this mui. However the local status can still be
    /// modified. This modification will take effect if the global
    /// status is changed to `Active`.
    pub fn mark_mui_as_withdrawn_v4(
        &self,
        mui: u32,
    ) -> Result<(), PrefixStoreError> {
        let guard = &epoch::pin();

        self.v4.mark_mui_as_withdrawn(mui, guard)
    }

    /// Change the status of all records for IPv6 prefixes for this
    /// `multi_uniq_id` globally to Active.  Note that the global
    /// `Active` status will be overridden by the local status of the
    /// record.
    pub fn mark_mui_as_active_v6(
        &self,
        mui: u32,
    ) -> Result<(), PrefixStoreError> {
        let guard = &epoch::pin();

        self.v6.mark_mui_as_active(mui, guard)
    }

    /// Change the status of all records for IPv6 prefixes for this
    /// `multi_uniq_id` globally to Withdrawn. A global `Withdrawn`
    /// status for a `multi_uniq_id` overrides the local status of
    /// prefixes for this mui. However the local status can still be
    /// modified. This modification will take effect if the global
    /// status is changed to `Active`.
    pub fn mark_mui_as_withdrawn_v6(
        &self,
        mui: u32,
    ) -> Result<(), PrefixStoreError> {
        let guard = &epoch::pin();

        self.v6.mark_mui_as_withdrawn(mui, guard)
    }

    /// Change the status of all records for this `multi_uniq_id` to
    /// Withdrawn.
    ///
    /// This method tries to mark all records: first the IPv4 records,
    /// then the IPv6 records. If marking of the IPv4 records fails,
    /// the method continues and tries to mark the IPv6 records. If
    /// either or both fail, an error is returned.
    pub fn mark_mui_as_withdrawn(
        &self,
        mui: u32,
    ) -> Result<(), PrefixStoreError> {
        let guard = &epoch::pin();

        let res_v4 = self.v4.mark_mui_as_withdrawn(mui, guard);
        let res_v6 = self.v6.mark_mui_as_withdrawn(mui, guard);

        res_v4.and(res_v6)
    }

    // Whether the global status for IPv4 prefixes and the specified
    // `multi_uniq_id` is set to `Withdrawn`.
    pub fn mui_is_withdrawn_v4(&self, mui: u32) -> bool {
        let guard = &epoch::pin();

        self.v4.mui_is_withdrawn(mui, guard)
    }

    // Whether the global status for IPv6 prefixes and the specified
    // `multi_uniq_id` is set to `Active`.
    pub fn mui_is_withdrawn_v6(&self, mui: u32) -> bool {
        let guard = &epoch::pin();

        self.v6.mui_is_withdrawn(mui, guard)
    }

    /// Returns the number of all prefixes in the store.
    ///
    /// Note that this method will actually traverse the complete
    /// tree.
    pub fn prefixes_count(&self) -> UpsertCounters {
        self.v4.get_prefixes_count() + self.v6.get_prefixes_count()
    }

    /// Returns the number of all IPv4 prefixes in the store.
    ///
    /// Note that this counter may be lower than the actual
    /// number in the store, due to contention at the time of
    /// reading the value.
    pub fn prefixes_v4_count(&self) -> UpsertCounters {
        self.v4.get_prefixes_count()
    }

    /// Returns the number of all IPv4 prefixes with the
    /// supplied prefix length in the store.
    ///
    /// Note that this counter may be lower than the actual
    /// number in the store, due to contention at the time of
    /// reading the value.
    pub fn prefixes_v4_count_for_len(&self, len: u8) -> UpsertCounters {
        self.v4.get_prefixes_count_for_len(len)
    }

    /// Returns the number of all IPv6 prefixes in the store.
    ///
    /// Note that this counter may be lower than the actual
    /// number in the store, due to contention at the time of
    /// reading the value.
    pub fn prefixes_v6_count(&self) -> UpsertCounters {
        self.v6.get_prefixes_count()
    }

    /// Returns the number of all IPv6 prefixes with the
    /// supplied prefix length in the store.
    ///
    /// Note that this counter may be lower than the actual
    /// number in the store, due to contention at the time of
    /// reading the value.
    pub fn prefixes_v6_count_for_len(&self, len: u8) -> UpsertCounters {
        self.v6.get_prefixes_count_for_len(len)
    }

    /// Returns the number of nodes in the store.
    ///
    /// Note that this counter may be lower than the actual
    /// number in the store, due to contention at the time of
    /// reading the value.
    pub fn nodes_count(&self) -> usize {
        self.v4.get_nodes_count() + self.v6.get_nodes_count()
    }

    /// Returns the number of IPv4 nodes in the store.
    ///
    /// Note that this counter may be lower than the actual
    /// number in the store, due to contention at the time of
    /// reading the value.
    pub fn nodes_v4_count(&self) -> usize {
        self.v4.get_nodes_count()
    }

    /// Returns the number of IPv6 nodes in the store.
    ///
    /// Note that this counter may be lower than the actual
    /// number in the store, due to contention at the time of
    /// reading the value.
    pub fn nodes_v6_count(&self) -> usize {
        self.v6.get_nodes_count()
    }

    /// Print the store statistics to the standard output.
    #[cfg(feature = "cli")]
    pub fn print_funky_stats(&self) {
        println!("\nStats for IPv4 multi-threaded store\n");
        println!("{}", self.v4.tree_bitmap);
        println!("Stats for IPv6 multi-threaded store\n");
        println!("{}", self.v6.tree_bitmap);
    }

    // The Store statistics.
    pub fn stats(&self) -> StoreStats {
        StoreStats {
            v4: self.v4.counters.get_prefix_stats(),
            v6: self.v6.counters.get_prefix_stats(),
        }
    }

    // Disk Persistence

    pub fn persist_strategy(&self) -> PersistStrategy {
        self.config.persist_strategy()
    }

    pub fn get_records_for_prefix(
        &self,
        prefix: &Prefix,
        mui: Option<u32>,
        include_withdrawn: bool,
    ) -> Option<Vec<Record<M>>> {
        let guard = &epoch::pin();

        match prefix.is_v4() {
            true => self.v4.get_value(
                PrefixId::<IPv4>::from(*prefix),
                mui,
                include_withdrawn,
                guard,
            ),
            false => self.v6.get_value(
                PrefixId::<IPv6>::from(*prefix),
                mui,
                include_withdrawn,
                guard,
            ),
        }
    }

    /// Persist all the non-unique (prefix, mui, ltime) tuples
    /// with their values to disk
    pub fn flush_to_disk(&self) -> Result<(), PrefixStoreError> {
        self.v4.flush_to_disk()?;
        self.v6.flush_to_disk()?;

        Ok(())
    }

    /// Return the approximate number of items that are persisted
    /// to disk, for IPv4 and IPv6 respectively.
    pub fn approx_persisted_items(&self) -> (usize, usize) {
        (
            self.v4.approx_persisted_items(),
            self.v6.approx_persisted_items(),
        )
    }

    /// Return an estimation of the disk space currently used by the
    /// store in bytes.
    pub fn disk_space(&self) -> u64 {
        self.v4.disk_space() + self.v6.disk_space()
    }
}
