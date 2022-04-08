use std::cmp::Ordering;
use std::fmt;
use std::fmt::Debug;

use crate::{af::AddressFamily, local_array::node::PrefixId};
use routecore::record::{MergeUpdate, Meta, Record};

//------------ InternalPrefixRecord -----------------------------------------

#[derive(Clone, Copy)]
pub struct InternalPrefixRecord<AF, T>
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
    AF: AddressFamily,
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
    // This should never fail, since there shouldn't be a invalid prefix in
    // this record in the first place.
    pub fn prefix_into_pub(&self) -> routecore::addr::Prefix {
        routecore::addr::Prefix::new(self.net.into_ipaddr(), self.len)
            .unwrap_or_else(|p| panic!("can't convert {:?} into prefix.", p))
    }
}

impl<T, AF> std::fmt::Display for InternalPrefixRecord<AF, T>
where
    T: Meta + MergeUpdate,
    AF: AddressFamily,
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
    AF: AddressFamily,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (self.net >> (AF::BITS - self.len))
            .cmp(&(other.net >> ((AF::BITS - other.len) % 32)))
    }
}

impl<AF, T> PartialEq for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily,
{
    fn eq(&self, other: &Self) -> bool {
        self.net >> (AF::BITS - self.len)
            == other.net >> ((AF::BITS - other.len) % 32)
    }
}

impl<AF, T> PartialOrd for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            (self.net >> (AF::BITS - self.len))
                .cmp(&(other.net >> ((AF::BITS - other.len) % 32))),
        )
    }
}

impl<AF, T> Eq for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily,
{
}

impl<T, AF> Debug for InternalPrefixRecord<AF, T>
where
    AF: AddressFamily,
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

// impl<AF, T> std::hash::Hash for InternalPrefixRecord<AF, T>
// where
//     AF: AddressFamily + PrimInt + Debug,
//     T: Meta,
// {
//     fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
//         self.net.hash(state);
//         self.len.hash(state);
//     }
// }

impl<AF, T> From<InternalPrefixRecord<AF, T>> for PrefixId<AF>
where
    AF: AddressFamily,
    T: Meta,
{
    fn from(record: InternalPrefixRecord<AF, T>) -> Self {
        Self(Some((record.net, record.len)))
    }
}

impl<AF, T> From<&InternalPrefixRecord<AF, T>> for PrefixId<AF>
where
    AF: AddressFamily,
    T: Meta,
{
    fn from(record: &InternalPrefixRecord<AF, T>) -> Self {
        Self(Some((record.net, record.len)))
    }
}

impl<'a, AF, M> From<&'a InternalPrefixRecord<AF, M>>
    for routecore::bgp::PrefixRecord<'a, M>
where
    AF: AddressFamily,
    M: Meta,
{
    fn from(record: &'a InternalPrefixRecord<AF, M>) -> Self {
        routecore::bgp::PrefixRecord::new(
            routecore::addr::Prefix::new(
                record.net.into_ipaddr(),
                record.len,
            )
            .unwrap(),
            record.meta.as_ref().unwrap(),
        )
    }
}

impl<'a, AF, M> From<routecore::bgp::PrefixRecord<'a, M>>
    for InternalPrefixRecord<AF, M>
where
    AF: AddressFamily,
    M: Meta,
{
    fn from(record: routecore::bgp::PrefixRecord<'a, M>) -> Self {
        Self {
            net: AF::from_ipaddr(record.key().addr()),
            len: record.key().len(),
            meta: Some(record.meta().into_owned()),
        }
    }
}
