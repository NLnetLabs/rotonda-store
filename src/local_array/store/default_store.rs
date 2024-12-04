use crate::prelude::multi::*;
use crate::prelude::*;
use std::fmt;

// The default stride sizes for IPv4, IPv6, resp.
#[create_store((
    ([5, 5, 4, 3, 3, 3, 3, 3, 3, 3], 5, 18),
    ([4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 4], 17, 30)
))]
struct DefaultStore;

impl<
        M: Meta,
        NB: NodeBuckets<IPv4>,
        PB: PrefixBuckets<IPv4, M>,
        const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > fmt::Display
    for CustomAllocStorage<IPv4, M, NB, PB, PREFIX_SIZE, KEY_SIZE>
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
        M: Meta,
        NB: NodeBuckets<IPv6>,
        PB: PrefixBuckets<IPv6, M>,
        const PREFIX_SIZE: usize,
        const KEY_SIZE: usize,
    > fmt::Display
    for CustomAllocStorage<IPv6, M, NB, PB, PREFIX_SIZE, KEY_SIZE>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CustomAllocStorage<IPv6, {}>",
            std::any::type_name::<M>()
        )
    }
}
