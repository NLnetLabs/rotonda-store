//------------ Metadata Types -----------------------------------------------

use routecore::record::MergeUpdate;

#[derive(Debug, Clone)]
pub struct ComplexPrefixAs(pub Vec<u32>);

impl MergeUpdate for ComplexPrefixAs {
    fn merge_update(
        &mut self,
        update_record: ComplexPrefixAs,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.0 = update_record.0;
        Ok(())
    }

    fn clone_merge_update(
        &self,
        update_meta: &Self,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: std::marker::Sized
    {
        let mut new_meta = update_meta.0.clone();
        new_meta.push(self.0[0]);
        Ok(ComplexPrefixAs(new_meta))
    }
}

impl std::fmt::Display for ComplexPrefixAs {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AS{:?}", self.0)
    }
}
