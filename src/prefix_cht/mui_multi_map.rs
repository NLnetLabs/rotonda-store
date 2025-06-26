use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::{Arc, Mutex, MutexGuard};

use crossbeam_utils::Backoff;
use roaring::RoaringBitmap;

use crate::errors::{FatalError, FatalResult};
use crate::prefix_record::Meta;
use crate::types::prefix_record::Record;
use crate::types::RouteStatus;

use crate::prefix_cht::map_type::{MapType, Mui, RecordKey};

use super::cht::MultiMapValue;

//------------ MultiMap ------------------------------------------------------
//
// This is the collection of records or a given prefix, keyed on the multi
// unique identifier ("mui"). Note that the record contains more than just
// the meta-data typed value ("M").

#[derive(Debug)]
pub struct MuiMultiMap<M: Meta>(
    Arc<Mutex<std::collections::HashMap<Mui, MultiMapValue<M>>>>,
);

impl<M: Send + Sync + Debug + Display + Meta> MuiMultiMap<M> {
    #[allow(clippy::type_complexity)]
    fn acquire_write_lock(
        &self,
    ) -> FatalResult<(MutexGuard<HashMap<Mui, MultiMapValue<M>>>, usize)>
    {
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
    ) -> MutexGuard<HashMap<Mui, MultiMapValue<M>>> {
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

    pub(crate) fn get_record_for_mui_with_rewritten_status(
        &self,
        mui: Mui,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Option<Record<Mui, M>> {
        let record_map = self.acquire_read_guard();
        record_map.get(&mui).map(|r| {
            // We'll return a cloned record: the record in the store remains
            // untouched.
            let mut r = r.clone();
            if bmin.contains(mui.mui().into()) {
                r.set_route_status(rewrite_status);
            }
            Record::<Mui, M>::from((mui, &r))
        })
    }

    pub fn get_filtered_record_for_mui(
        &self,
        mui: Mui,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Record<Mui, M>> {
        match include_withdrawn {
            false => self.get_record_for_key(mui, include_withdrawn),
            true => self.get_record_for_mui_with_rewritten_status(
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
    pub fn as_records_with_rewritten_status(
        &self,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Vec<Record<Mui, M>> {
        let record_map = self.acquire_read_guard();
        record_map
            .iter()
            .map(move |r| {
                let mut rec = r.1.clone();
                if bmin.contains(r.0.mui().into()) {
                    rec.set_route_status(rewrite_status);
                }
                Record::<Mui, M>::from((*r.0, &rec))
            })
            .collect::<Vec<_>>()
    }

    pub fn _as_records(&self) -> Vec<Record<Mui, M>> {
        let record_map = self.acquire_read_guard();
        record_map
            .iter()
            .map(|r| Record::<Mui, M>::from((*r.0, r.1)))
            .collect::<Vec<_>>()
    }

    // Returns a vec of records whose keys are not in the supplied bitmap
    // index, and whose local Status is set to Active. Used to filter out
    // withdrawn routes.
    pub fn as_active_records_not_in_bmin(
        &self,
        bmin: &RoaringBitmap,
    ) -> Vec<Record<Mui, M>> {
        let record_map = self.acquire_read_guard();
        record_map
            .iter()
            .filter_map(|r| {
                if r.1.route_status() == RouteStatus::Active
                    && !bmin.contains(r.0.mui().into())
                {
                    Some(Record::<Mui, M>::from((*r.0, r.1)))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}

impl<M: Meta> MapType<M> for MuiMultiMap<M> {
    type Key = Mui;

    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    fn best_backup(&self, tbi: M::TBI) -> (Option<Mui>, Option<Mui>) {
        let record_map = self.acquire_read_guard();
        let ord_routes = record_map
            .iter()
            .map(|r| (r.1.meta().as_orderable(tbi), *r.0));
        let (best, bckup) =
            routecore::bgp::path_selection::best_backup_generic(ord_routes);
        (best.map(|b| b.1), bckup.map(|b| b.1))
    }

    // Change the local status of the record for this mui to Withdrawn.
    fn mark_as_withdrawn_for_mui(&self, mui: Mui, ltime: u64) {
        let mut record_map = self.acquire_read_guard();
        if let Some(rec) = record_map.get_mut(&mui) {
            rec.set_route_status(RouteStatus::Withdrawn);
            rec.set_logical_time(ltime);
        }
    }

    // Change the local status of the record for this mui to Active.
    fn mark_as_active_for_mui(&self, mui: Mui, ltime: u64) {
        let mut record_map = self.acquire_read_guard();
        if let Some(rec) = record_map.get_mut(&mui) {
            rec.set_route_status(RouteStatus::Active);
            rec.set_logical_time(ltime);
        }
    }

    // Insert or replace the PublicRecord in the HashMap for the key of
    // record.multi_uniq_id. Returns the number of entries in the HashMap
    // after updating it, if it's more than 1. Returns None if this is the
    // first entry.
    #[allow(clippy::type_complexity)]
    fn upsert_record(
        &self,
        new_rec: Record<Mui, M>,
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
        mui: Option<Mui>,
        include_withdrawn: bool,
        bmin: &RoaringBitmap,
    ) -> Option<Vec<Record<Mui, M>>> {
        if let Some(mui) = mui {
            self.get_filtered_record_for_mui(mui, include_withdrawn, bmin)
                .map(|r| vec![r])
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
        mui: Mui,
        include_withdrawn: bool,
    ) -> Vec<Record<Mui, M>> {
        let record_map = self.acquire_read_guard();
        let mut res = vec![];

        if let Some(r) = record_map.get(&mui) {
            if include_withdrawn || r.route_status() == RouteStatus::Active {
                res.push(Record::<Mui, M>::from((mui, r)));
            }
        }

        res
    }

    fn get_record_for_key(
        &self,
        mui: Mui,
        include_withdrawn: bool,
    ) -> Option<Record<Mui, M>> {
        let record_map = self.acquire_read_guard();

        record_map
            .get(&mui)
            .and_then(|r| -> Option<Record<Mui, M>> {
                if include_withdrawn
                    || r.route_status() == RouteStatus::Active
                {
                    Some(Record::<Mui, M>::from((mui, r)))
                } else {
                    None
                }
            })
    }
}

// impl<M: Meta> From<(u32, &MultiMapValue<M>)> for Record<u32, M> {
//     fn from(value: (u32, &MultiMapValue<M>)) -> Self {
//         Self {
//             multi_uniq_id: value.0,
//             meta: value.1.meta().clone(),
//             ltime: value.1.ltime,
//             status: value.1.route_status,
//         }
//     }
// }

impl<M: Meta> Clone for MuiMultiMap<M> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}
