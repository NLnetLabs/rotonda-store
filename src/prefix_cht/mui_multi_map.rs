use std::collections::HashMap;
use std::fmt::{Debug, Display};

use roaring::RoaringBitmap;

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
pub struct MuiMultiMap<M: Meta>(HashMap<Mui, MultiMapValue<M>>);

impl<M: Send + Sync + Debug + Display + Meta> MuiMultiMap<M> {
    fn get_record_for_mui_with_rewritten_status(
        &self,
        mui: Mui,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Option<Record<Mui, M>> {
        self.0.get(&mui).map(|r| {
            // We'll return a cloned record: the record in the store remains
            // untouched.
            let mut r = r.clone();
            if bmin.contains(mui.mui().into()) {
                r.set_route_status(rewrite_status);
            }
            Record::<Mui, M>::from((mui, &r))
        })
    }

    fn get_filtered_record_for_mui(
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
}

impl<M: Meta> MapType<M> for MuiMultiMap<M> {
    type Key = Mui;
    type Inner = HashMap<Mui, MultiMapValue<M>>;

    fn new() -> Self {
        Self(HashMap::new())
    }

    fn inner(&self) -> &Self::Inner {
        &self.0
    }

    fn inner_mut(&mut self) -> &mut Self::Inner {
        &mut self.0
    }

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = (&'a Mui, &'a MultiMapValue<M>)>
    where
        Self::Inner: 'a,
        M: 'a,
    {
        self.0.iter()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn get(&self, key: &Self::Key) -> Option<&MultiMapValue<M>> {
        self.0.get(key)
    }

    fn contains_key(&self, key: &Mui) -> bool {
        self.0.contains_key(key)
    }

    fn insert(
        &mut self,
        key: Mui,
        value: MultiMapValue<M>,
    ) -> Option<MultiMapValue<M>> {
        self.0.insert(key, value)
    }

    // Change the local status of the record for this mui to Withdrawn.
    fn mark_as_withdrawn_for_mui(&mut self, mui: Mui, ltime: u64) {
        // let mut record_map = self.acquire_read_guard();
        if let Some(rec) = self.0.get_mut(&mui) {
            rec.set_route_status(RouteStatus::Withdrawn);
            rec.set_logical_time(ltime);
        }
    }

    // Change the local status of the record for this mui to Active.
    fn mark_as_active_for_mui(&mut self, mui: Mui, ltime: u64) {
        // let mut record_map = self.acquire_read_guard();
        if let Some(rec) = self.0.get_mut(&mui) {
            rec.set_route_status(RouteStatus::Active);
            rec.set_logical_time(ltime);
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
        let mut res = vec![];

        if let Some(r) = self.0.get(&mui) {
            if include_withdrawn || r.route_status() == RouteStatus::Active {
                res.push(Record::<Mui, M>::from((mui, r)));
            }
        }

        res
    }

    fn get_records_for_mui_with_rewritten_status(
        &self,
        mui: Mui,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Vec<Record<Mui, M>> {
        self.get(&mui)
            .map(|r| {
                // We'll return a cloned record: the record in the store
                // remains untouched.
                let mut r = r.clone();
                if bmin.contains(mui.mui().into()) {
                    r.set_route_status(rewrite_status);
                }
                Record::<Mui, M>::from((mui, &r))
            })
            .into_iter()
            .collect::<Vec<_>>()
    }
}
