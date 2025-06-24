use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::sync::{Arc, Mutex, MutexGuard};

use crossbeam_utils::Backoff;
use roaring::RoaringBitmap;

use crate::cht::{nodeset_size, prev_node_size};
use crate::errors::{FatalError, FatalResult};
use crate::prefix_record::Meta;
use crate::stats::{Counters, UpsertReport};
#[cfg(test)]
use crate::test_types::NoMeta;
use crate::types::RouteStatus;
#[cfg(test)]
use crate::IPv6;
use crate::{
    cht::{Cht, OnceBoxSlice, Value},
    types::{
        errors::PrefixStoreError, prefix_record::Record, AddressFamily,
        PrefixId,
    },
};

use super::cht::MultiMapValue;

pub trait MapType {
    type Inner;
}

impl<M: Meta> MapType for AddPathMultiMap<M> {
    type Inner = (u32, [u8; 4]);
}

//------------ MultiMap ------------------------------------------------------
//
// This is the collection of records or a given prefix, keyed on the multi
// unique identifier ("mui"). Note that the record contains more than just
// the meta-data typed value ("M").

#[derive(Debug)]
pub struct AddPathMultiMap<M: Meta>(
    Arc<Mutex<BTreeMap<(u32, [u8; 4]), MultiMapValue<M>>>>,
);

impl<M: Send + Sync + Debug + Display + Meta> AddPathMultiMap<M> {
    pub(crate) fn new(
        record_map: BTreeMap<(u32, [u8; 4]), MultiMapValue<M>>,
    ) -> Self {
        Self(Arc::new(Mutex::new(record_map)))
    }

    #[allow(clippy::type_complexity)]
    fn acquire_write_lock(
        &self,
    ) -> FatalResult<(
        MutexGuard<BTreeMap<(u32, [u8; 4]), MultiMapValue<M>>>,
        usize,
    )> {
        let mut retry_count: usize = 0;
        let backoff = Backoff::new();

        loop {
            // We're using lock(), which returns an Error only if another
            // thread has panicked while holding the lock. In that situtation
            // we are certainly not going to write anything.
            if let Ok(guard) = self.0.lock().map_err(|_| FatalError) {
                return Ok((guard, retry_count));
            }

            backoff.spin();
            retry_count += 1;
        }
    }

    fn acquire_read_guard(
        &self,
    ) -> MutexGuard<BTreeMap<(u32, [u8; 4]), MultiMapValue<M>>> {
        let backoff = Backoff::new();

        loop {
            if let Ok(guard) = self.0.try_lock() {
                return guard;
            }

            backoff.spin();
        }
    }

    pub fn _len(&self) -> usize {
        let record_map = self.acquire_read_guard();
        record_map.len()
    }

    pub fn get_records_for_mui(
        &self,
        mui: u32,
        include_withdrawn: bool,
    ) -> Vec<([u8; 4], Record<M>)> {
        let record_map = self.acquire_read_guard();
        let mut res = vec![];

        let range = (
            std::ops::Bound::Included((mui, [0; 4])),
            std::ops::Bound::Excluded((mui + 1, [0; 4])),
        );

        for r in record_map.range(range) {
            if include_withdrawn || r.1.route_status() == RouteStatus::Active
            {
                res.push((r.0 .1, Record::from((mui, r.1))))
            }
        }

        res
    }

    pub fn get_record_for_mui_and_path_id(
        &self,
        mui: u32,
        path_id: [u8; 4],
        include_withdrawn: bool,
    ) -> Option<Record<M>> {
        let record_map = self.acquire_read_guard();

        record_map
            .get(&(mui, path_id))
            .and_then(|r| -> Option<Record<M>> {
                if include_withdrawn
                    || r.route_status() == RouteStatus::Active
                {
                    Some(Record::from((mui, r)))
                } else {
                    None
                }
            })
    }

    pub fn best_backup(
        &self,
        tbi: M::TBI,
    ) -> (Option<(u32, [u8; 4])>, Option<(u32, [u8; 4])>) {
        let record_map = self.acquire_read_guard();
        let ord_routes = record_map
            .iter()
            .map(|r| (r.1.meta().as_orderable(tbi), *r.0));
        let (best, bckup) =
            routecore::bgp::path_selection::best_backup_generic(ord_routes);
        (best.map(|b| b.1), bckup.map(|b| b.1))
    }

    pub(crate) fn get_records_for_mui_with_rewritten_status(
        &self,
        mui: u32,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Vec<([u8; 4], Record<M>)> {
        let record_map = self.acquire_read_guard();
        let mut res = vec![];
        let range = (
            std::ops::Bound::Included((mui, [0; 4])),
            std::ops::Bound::Excluded((mui + 1, [0; 4])),
        );
        record_map.range(range).map(|r| {
            // We'll return a cloned record: the record in the store remains
            // untouched.
            let mut rec = r.1.clone();
            if bmin.contains(mui) {
                rec.set_route_status(rewrite_status);
            }
            res.push((r.0 .1, Record::from((mui, &rec))));
        });

        res
    }

    pub fn get_filtered_record_for_mui(
        &self,
        mui: u32,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Vec<([u8; 4], Record<M>)> {
        match include_withdrawn {
            false => self.get_records_for_mui(mui, include_withdrawn),
            true => self.get_records_for_mui_with_rewritten_status(
                mui,
                bmin,
                RouteStatus::Withdrawn,
            ),
        }
    }

    // Helper to filter out records that are not-active (Inactive or
    // Withdrawn), or whose mui appears in the global withdrawn index.
    pub fn get_filtered_records(
        &self,
        mui: Option<u32>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Vec<([u8; 4], Record<M>)>> {
        if let Some(mui) = mui {
            Some(self.get_filtered_record_for_mui(
                mui,
                include_withdrawn,
                bmin,
            ))
            // .into_iter().map(|r| vec![r])
        } else {
            match include_withdrawn {
                false => {
                    let recs = self.as_active_records_not_in_bmin(bmin);
                    if recs.is_empty() {
                        None
                    } else {
                        Some(recs)
                    }
                }
                true => {
                    let recs = self.as_records_with_rewritten_status(
                        bmin,
                        RouteStatus::Withdrawn,
                    );
                    if recs.is_empty() {
                        None
                    } else {
                        Some(recs)
                    }
                }
            }
        }
    }

    // return all records regardless of their local status, or any globally
    // set status for the mui of the record. However, the local status for a
    // record whose mui appears in the specified bitmap index, will be
    // rewritten with the specified RouteStatus.
    pub fn as_records_with_rewritten_status(
        &self,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Vec<([u8; 4], Record<M>)> {
        let record_map = self.acquire_read_guard();
        record_map
            .iter()
            .map(move |r| {
                let mut rec = r.1.clone();
                if bmin.contains(r.0 .0) {
                    rec.set_route_status(rewrite_status);
                }
                (r.0 .1, Record::from((r.0 .0, &rec)))
            })
            .collect::<Vec<_>>()
    }

    pub fn _as_records(&self) -> Vec<([u8; 4], Record<M>)> {
        let record_map = self.acquire_read_guard();
        record_map
            .iter()
            .map(|r| (r.0 .1, Record::from((r.0 .0, r.1))))
            .collect::<Vec<_>>()
    }

    // Returns a vec of records whose keys are not in the supplied bitmap
    // index, and whose local Status is set to Active. Used to filter out
    // withdrawn routes.
    pub fn as_active_records_not_in_bmin(
        &self,
        bmin: &RoaringBitmap,
    ) -> Vec<([u8; 4], Record<M>)> {
        let record_map = self.acquire_read_guard();
        record_map
            .iter()
            .filter_map(|r| {
                if r.1.route_status() == RouteStatus::Active
                    && !bmin.contains(r.0 .0)
                {
                    Some((r.0 .1, Record::from((r.0 .0, r.1))))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    // Change the local status of the record for this mui to Withdrawn.
    pub fn mark_as_withdrawn_for_mui(&self, mui: u32, ltime: u64) {
        let mut record_map = self.acquire_read_guard();
        let range = (
            std::ops::Bound::Included((mui, [0_u8; 4])),
            std::ops::Bound::Excluded((mui + 1, [0_u8; 4])),
        );
        for rec in record_map.range_mut(range) {
            rec.1.set_route_status(RouteStatus::Withdrawn);
            rec.1.set_logical_time(ltime);
        }
    }

    // Change the local status of the record for this mui to Active.
    pub fn mark_as_active_for_mui(&self, mui: u32, ltime: u64) {
        let mut record_map = self.acquire_read_guard();
        let range = (
            std::ops::Bound::Included((mui, [0_u8; 4])),
            std::ops::Bound::Excluded((mui + 1, [0_u8; 4])),
        );

        for rec in record_map.range_mut(range) {
            rec.1.set_route_status(RouteStatus::Active);
            rec.1.set_logical_time(ltime);
        }
    }

    // Insert or replace the PublicRecord in the HashMap for the key of
    // record.multi_uniq_id. Returns the number of entries in the HashMap
    // after updating it, if it's more than 1. Returns None if this is the
    // first entry.
    #[allow(clippy::type_complexity)]
    pub(crate) fn upsert_record(
        &self,
        path_id: [u8; 4],
        new_rec: Record<M>,
    ) -> FatalResult<(Option<(MultiMapValue<M>, usize)>, usize)> {
        let (mut record_map, retry_count) = self.acquire_write_lock()?;
        let key = (new_rec.multi_uniq_id, path_id);

        match record_map.contains_key(&key) {
            true => {
                let old_rec = record_map
                    .insert(key, MultiMapValue::from(new_rec))
                    .map(|r| (r, record_map.len()));
                Ok((old_rec, retry_count))
            }
            false => {
                let new_rec = MultiMapValue::from(new_rec);
                let old_rec = record_map.insert(key, new_rec);
                assert!(old_rec.is_none());
                Ok((None, retry_count))
            }
        }
    }
}

impl<M: Meta> Clone for AddPathMultiMap<M> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}
