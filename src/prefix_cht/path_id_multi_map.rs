use std::collections::BTreeMap;

use roaring::RoaringBitmap;

use crate::prefix_record::Meta;
use crate::types::prefix_record::Record;
use crate::types::RouteStatus;

use super::cht::MultiMapValue;
use super::map_type::{KeyExtensions, MapType, Mui, MuiPathId};

//------------ PatIdhMultiMap ------------------------------------------------
//
// This is the collection of records or a given prefix, keyed on the multi
// unique identifier ("mui"). Note that the record contains more than just the
// meta-data typed value ("M").

#[derive(Debug)]
pub struct PathIdMultiMap<M: Meta>(BTreeMap<MuiPathId, MultiMapValue<M>>);

impl<M: Meta> MapType<M> for PathIdMultiMap<M> {
    type Key = MuiPathId;
    type Inner = BTreeMap<MuiPathId, MultiMapValue<M>>;

    fn new() -> Self {
        Self(BTreeMap::new())
    }

    fn inner(&self) -> &Self::Inner {
        &self.0
    }

    fn inner_mut(&mut self) -> &mut Self::Inner {
        &mut self.0
    }

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = (&'a MuiPathId, &'a MultiMapValue<M>)>
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

    fn get<FK: KeyExtensions>(&self, key: FK) -> Option<&MultiMapValue<M>>
    where
        MuiPathId: From<FK>,
    {
        let key = key.into();
        self.0.get(&key)
    }

    fn contains_key(&self, key: &MuiPathId) -> bool {
        self.0.contains_key(key)
    }

    fn insert(
        &mut self,
        key: MuiPathId,
        value: MultiMapValue<M>,
    ) -> Option<MultiMapValue<M>> {
        self.0.insert(key, value)
    }

    // Change the local status of the record for this mui to Withdrawn.
    fn mark_as_withdrawn_for_mui(&mut self, mui: Mui, ltime: u64) {
        let range = Self::Key::mui_range(mui);
        for rec in self.inner_mut().range_mut(range) {
            rec.1.set_route_status(RouteStatus::Withdrawn);
            rec.1.set_logical_time(ltime);
        }
    }

    // Change the local status of the record for this mui to Active.
    fn mark_as_active_for_mui(&mut self, mui: Mui, ltime: u64) {
        let range = Self::Key::mui_range(mui);

        for rec in self.inner_mut().range_mut(range) {
            rec.1.set_route_status(RouteStatus::Active);
            rec.1.set_logical_time(ltime);
        }
    }

    fn get_records_for_mui(
        &self,
        mui: Mui,
        include_withdrawn: bool,
    ) -> Vec<Record<Self::Key, M>> {
        let mut res = vec![];

        let range = Self::Key::mui_range(mui);

        for r in self.0.range(range) {
            if include_withdrawn || r.1.route_status() == RouteStatus::Active
            {
                res.push(Record::<Self::Key, M>::from((*r.0, r.1)))
            }
        }

        res
    }

    fn get_records_for_mui_with_rewritten_status(
        &self,
        mui: Mui,
        bmin: &RoaringBitmap,
        rewrite_status: RouteStatus,
    ) -> Vec<Record<MuiPathId, M>> {
        let mut res = vec![];
        let range = Self::Key::mui_range(mui);
        for r in self.0.range(range) {
            // We'll return a cloned record: the record in the store remains
            // untouched.
            let mut rec = r.1.clone();
            if bmin.contains(mui.mui().into()) {
                rec.set_route_status(rewrite_status);
            }
            res.push(Record::<Self::Key, M>::from((*r.0, &rec)));
        }

        res
    }
}
