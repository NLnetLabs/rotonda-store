//------------ RdPatIdhMultiMap ----------------------------------------------
//
// This is the collection of records or a given prefix, keyed on the multi
// unique identifier ("mui"). Note that the record contains more than just the
// meta-data typed value ("M").

use std::{
    collections::BTreeMap,
    fmt::Display,
    sync::{Arc, Mutex, MutexGuard},
};

use crossbeam_utils::Backoff;
use roaring::RoaringBitmap;

use crate::{
    errors::{FatalError, FatalResult},
    prefix_record::Meta,
    types::{Record, RouteStatus},
};

use super::{
    cht::MultiMapValue,
    map_type::{MapType, MuiRdPathId, RecordKey},
};

#[derive(Debug)]
pub struct RdPathIdMultiMap<M: Meta>(
    Arc<Mutex<BTreeMap<MuiRdPathId, MultiMapValue<M>>>>,
);

impl<M: Meta> MapType<M> for RdPathIdMultiMap<M> {
    type Key = MuiRdPathId;

    fn new() -> Self {
        let m = BTreeMap::new();
        Self(Arc::new(Mutex::new(m)))
    }

    fn best_backup(
        &self,
        tbi: M::TBI,
    ) -> (Option<MuiRdPathId>, Option<MuiRdPathId>) {
        let record_map = self.acquire_read_guard();
        let ord_routes = record_map
            .iter()
            .map(|r| (r.1.meta().as_orderable(tbi), *r.0));
        let (best, bckup) =
            routecore::bgp::path_selection::best_backup_generic(ord_routes);
        (best.map(|b| b.1), bckup.map(|b| b.1))
    }

    // Change the local status of the record for this mui to Withdrawn.
    fn mark_as_withdrawn_for_mui(&self, mui: Self::Key, ltime: u64) {
        let mut record_map = self.acquire_read_guard();
        let range = Self::Key::mui_range(mui);
        for rec in record_map.range_mut(range) {
            rec.1.set_route_status(RouteStatus::Withdrawn);
            rec.1.set_logical_time(ltime);
        }
    }

    // Change the local status of the record for this mui to Active.
    fn mark_as_active_for_mui(&self, mui: Self::Key, ltime: u64) {
        let mut record_map = self.acquire_read_guard();
        let range = Self::Key::mui_range(mui);

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
    fn upsert_record(
        &self,
        new_rec: Record<Self::Key, M>,
    ) -> FatalResult<(Option<(MultiMapValue<M>, usize)>, usize)> {
        let (mut record_map, retry_count) = self.acquire_write_lock()?;
        let key = new_rec.multi_uniq_id;

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

    // Helper to filter out records that are not-active (Inactive or
    // Withdrawn), or whose mui appears in the global withdrawn index.
    fn get_filtered_records(
        &self,
        mui: Option<MuiRdPathId>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Vec<Record<Self::Key, M>>> {
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

    fn get_records_for_mui(
        &self,
        mui: Self::Key,
        include_withdrawn: bool,
    ) -> Vec<Record<Self::Key, M>> {
        let record_map = self.acquire_read_guard();
        let mut res = vec![];

        let range = Self::Key::mui_range(mui);

        for r in record_map.range(range) {
            if include_withdrawn || r.1.route_status() == RouteStatus::Active
            {
                res.push(Record::<MuiRdPathId, M>::from((mui, r.1)))
            }
        }

        res
    }

    fn get_record_for_key(
        &self,
        mui: MuiRdPathId,
        include_withdrawn: bool,
    ) -> Option<Record<Self::Key, M>> {
        let record_map = self.acquire_read_guard();

        record_map
            .get(&mui)
            .and_then(|r| -> Option<Record<MuiRdPathId, M>> {
                if include_withdrawn
                    || r.route_status() == RouteStatus::Active
                {
                    Some(Record::<MuiRdPathId, M>::from((mui, r)))
                } else {
                    None
                }
            })
    }
}

impl<M: Send + Sync + std::fmt::Debug + Display + Meta> RdPathIdMultiMap<M> {
    #[allow(clippy::type_complexity)]
    fn acquire_write_lock(
        &self,
    ) -> FatalResult<(
        MutexGuard<BTreeMap<MuiRdPathId, MultiMapValue<M>>>,
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
    ) -> MutexGuard<BTreeMap<MuiRdPathId, MultiMapValue<M>>> {
        let backoff = Backoff::new();

        loop {
            if let Ok(guard) = self.0.try_lock() {
                return guard;
            }

            backoff.spin();
        }
    }

    fn _len(&self) -> usize {
        let record_map = self.acquire_read_guard();
        record_map.len()
    }

    fn get_records_for_mui_with_rewritten_status(
        &self,
        mui: MuiRdPathId,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Vec<Record<MuiRdPathId, M>> {
        let record_map = self.acquire_read_guard();
        let mut res = vec![];
        let range = MuiRdPathId::mui_range(mui);
        for r in record_map.range(range) {
            // We'll return a cloned record: the record in the store remains
            // untouched.
            let mut rec = r.1.clone();
            if bmin.contains(mui.mui().into()) {
                rec.set_route_status(rewrite_status);
            }
            res.push(Record::<MuiRdPathId, M>::from((mui, &rec)));
        }

        res
    }

    fn get_filtered_record_for_mui(
        &self,
        mui: MuiRdPathId,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Vec<Record<MuiRdPathId, M>> {
        match include_withdrawn {
            false => self.get_records_for_mui(mui, include_withdrawn),
            true => self.get_records_for_mui_with_rewritten_status(
                mui,
                bmin,
                RouteStatus::Withdrawn,
            ),
        }
    }

    // return all records regardless of their local status, or any globally
    // set status for the mui of the record. However, the local status for a
    // record whose mui appears in the specified bitmap index, will be
    // rewritten with the specified RouteStatus.
    fn as_records_with_rewritten_status(
        &self,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Vec<Record<MuiRdPathId, M>> {
        let record_map = self.acquire_read_guard();
        record_map
            .iter()
            .map(move |r| {
                let mut rec = r.1.clone();
                if bmin.contains(r.0.mui().into()) {
                    rec.set_route_status(rewrite_status);
                }
                Record::<MuiRdPathId, M>::from((*r.0, &rec))
            })
            .collect::<Vec<_>>()
    }

    fn _as_records(&self) -> Vec<Record<MuiRdPathId, M>> {
        let record_map = self.acquire_read_guard();
        record_map
            .iter()
            .map(|r| Record::<MuiRdPathId, M>::from((*r.0, r.1)))
            .collect::<Vec<_>>()
    }

    // Returns a vec of records whose keys are not in the supplied bitmap
    // index, and whose local Status is set to Active. Used to filter out
    // withdrawn routes.
    fn as_active_records_not_in_bmin(
        &self,
        bmin: &RoaringBitmap,
    ) -> Vec<Record<MuiRdPathId, M>> {
        let record_map = self.acquire_read_guard();
        record_map
            .iter()
            .filter_map(|r| {
                if r.1.route_status() == RouteStatus::Active
                    && !bmin.contains(r.0.mui().into())
                {
                    Some(Record::<MuiRdPathId, M>::from((*r.0, r.1)))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}
