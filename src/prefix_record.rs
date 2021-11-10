use num::PrimInt;

use std::cmp::Ordering;
use std::fmt;
use std::fmt::Debug;

use routecore::addr::AddressFamily;
use routecore::record::{MergeUpdate, Meta};

//------------ InternalPrefixRecord -----------------------------------------

#[derive(Clone, Copy)]
pub(crate) struct InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily,
{
    pub net: AF,
    pub len: u8,
    pub meta: Option<T>,
}

impl<T, AF> InternalPrefixRecord<AF, T>
where
    T: Meta + MergeUpdate,
    AF: AddressFamily + PrimInt + Debug,
{
    pub fn new(net: AF, len: u8) -> InternalPrefixRecord<AF, T> {
        Self {
            net,
            len,
            meta: None,
        }
    }
    pub fn new_with_meta(
        net: AF,
        len: u8,
        meta: T,
    ) -> InternalPrefixRecord<AF, T> {
        Self {
            net,
            len,
            meta: Some(meta),
        }
    }
}

impl<T, AF> std::fmt::Display for InternalPrefixRecord<AF, T>
where
    T: Meta + MergeUpdate,
    AF: AddressFamily + PrimInt + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{} {}",
            AddressFamily::fmt_net(self.net),
            self.len,
            self.meta.as_ref().unwrap().summary()
        )
    }
}

impl<AF, T> Ord for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (self.net >> (AF::BITS - self.len) as usize)
            .cmp(&(other.net >> ((AF::BITS - other.len) % 32) as usize))
    }
}

impl<AF, T> PartialEq for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    fn eq(&self, other: &Self) -> bool {
        self.net >> (AF::BITS - self.len) as usize
            == other.net >> ((AF::BITS - other.len) % 32) as usize
    }
}

impl<AF, T> PartialOrd for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            (self.net >> (AF::BITS - self.len) as usize)
                .cmp(&(other.net >> ((AF::BITS - other.len) % 32) as usize)),
        )
    }
}

impl<AF, T> Eq for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
}

impl<T, AF> Debug for InternalPrefixRecord<AF, T>
where
    AF: AddressFamily + PrimInt + Debug,
    T: Meta,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "{}/{} with {:?}",
            AddressFamily::fmt_net(self.net),
            self.len,
            self.meta
        ))
    }
}
