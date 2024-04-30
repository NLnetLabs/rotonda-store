use std::fmt;
use crate::prelude::*;
use crate::prelude::multi::*;

// The default stride sizes for IPv4, IPv6, resp.
#[create_store((
    [5, 5, 4, 3, 3, 3, 3, 3, 3, 3], 
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 4]
))]
struct DefaultStore;

impl<
        M: Meta + MergeUpdate,
        NB: NodeBuckets<IPv4>,
        PB: PrefixBuckets<IPv4, M>
    > fmt::Display for CustomAllocStorage<IPv4, M, NB, PB>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CustomAllocStorage<IPv4, {}>",
            std::any::type_name::<M>()
        )
    }
}

impl<
        M: Meta + MergeUpdate,
        NB: NodeBuckets<IPv6>,
        PB: PrefixBuckets<IPv6, M>
    > fmt::Display for CustomAllocStorage<IPv6, M, NB, PB>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CustomAllocStorage<IPv6, {}>",
            std::any::type_name::<M>()
        )
    }
}
