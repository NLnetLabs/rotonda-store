use std::fmt;

use crate::prelude::*;

// The default stride sizes for IPv4, IPv6, resp.
#[create_store((
    [5, 5, 4, 3, 3, 3, 3, 3, 3, 3], 
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 4]
))]
struct Store;

impl<
        Meta: routecore::record::Meta + MergeUpdate,
        Buckets: FamilyBuckets<IPv4>,
    > fmt::Display for CustomAllocStorage<IPv4, Meta, Buckets>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CustomAllocStorage<IPv4, {}>",
            std::any::type_name::<Meta>()
        )
    }
}

impl<
        Meta: routecore::record::Meta + MergeUpdate,
        Buckets: FamilyBuckets<IPv6>,
    > fmt::Display for CustomAllocStorage<IPv6, Meta, Buckets>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CustomAllocStorage<IPv6, {}>",
            std::any::type_name::<Meta>()
        )
    }
}
