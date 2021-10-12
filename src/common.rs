use num::PrimInt;
#[cfg(feature = "dynamodb")]
use rpki::repository::resources::Addr;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Debug, Display};

//------------ Metadata Types --------------------------------------------------------

#[derive(Debug, Copy, Clone)]
pub struct PrefixAs(pub u32);

impl MergeUpdate for PrefixAs {
    fn merge_update(&mut self, update_record: PrefixAs) -> Result<(), Box<dyn std::error::Error>> {
        self.0 = update_record.0;
        Ok(())
    }
}

impl Display for PrefixAs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AS{}", self.0)
    }
}

pub struct NoMeta;

impl fmt::Debug for NoMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("")
    }
}

impl Display for NoMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("")
    }
}

impl MergeUpdate for NoMeta {
    fn merge_update(&mut self, _: NoMeta) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

pub trait Meta
where
    Self: Debug + Sized + Display,
{
    fn with_meta<AF: AddressFamily + PrimInt + Debug>(
        net: AF,
        len: u8,
        meta: Option<Self>,
    ) -> Prefix<AF, Self> {
        Prefix { net, len, meta }
    }

    fn summary(&self) -> String;
}
pub trait MergeUpdate {
    fn merge_update(&mut self, update_meta: Self) -> Result<(), Box<dyn std::error::Error>>;
}

//------------ Address Family (trait) --------------------------------------------------------

pub trait AddressFamily: PrimInt + Debug {
    const BITMASK: Self;
    const BITS: u8;
    fn fmt_net(net: Self) -> String;
    // returns the specified nibble from `start_bit` to (and
    // including) `start_bit + len` and shifted to the right.
    fn get_nibble(net: Self, start_bit: u8, len: u8) -> u32;

    #[cfg(feature = "dynamodb")]
    fn from_addr(net: Addr) -> Self;

    #[cfg(feature = "dynamodb")]
    fn into_addr(self) -> Addr;

    fn into_ipaddr(self) -> std::net::IpAddr;
}

impl AddressFamily for u32 {
    const BITMASK: u32 = 0x1u32.rotate_right(1);
    const BITS: u8 = 32;

    fn fmt_net(net: Self) -> String {
        std::net::Ipv4Addr::from(net).to_string()
    }

    fn get_nibble(net: Self, start_bit: u8, len: u8) -> u32 {
        (net << start_bit) >> ((32 - len) % 32)
    }

    #[cfg(feature = "dynamodb")]
    fn from_addr(net: Addr) -> u32 {
        net.to_bits() as u32
    }

    #[cfg(feature = "dynamodb")]
    fn into_addr(self) -> Addr {
        Addr::from_bits(self as u128)
    }

    fn into_ipaddr(self) -> std::net::IpAddr {
        std::net::IpAddr::V4(std::net::Ipv4Addr::from(self))
    }
}

impl AddressFamily for u128 {
    const BITMASK: u128 = 0x1u128.rotate_right(1);
    const BITS: u8 = 128;
    fn fmt_net(net: Self) -> String {
        std::net::Ipv6Addr::from(net).to_string()
    }

    fn get_nibble(net: Self, start_bit: u8, len: u8) -> u32 {
        ((net << start_bit) >> ((128 - len) % 128)) as u32
    }

    #[cfg(feature = "dynamodb")]
    fn from_addr(net: Addr) -> u128 {
        net.to_bits()
    }

    #[cfg(feature = "dynamodb")]
    fn into_addr(self) -> Addr {
        Addr::from_bits(self)
    }

    fn into_ipaddr(self) -> std::net::IpAddr {
        std::net::IpAddr::V6(std::net::Ipv6Addr::from(self))
    }
}

//------------ Prefix --------------------------------------------------------

#[derive(Clone, Copy)]
pub struct Prefix<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    pub net: AF,
    pub len: u8,
    pub meta: Option<T>,
}

impl<T, AF> Prefix<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    pub fn new(net: AF, len: u8) -> Prefix<AF, T> {
        T::with_meta(net, len, None)
    }
    pub fn new_with_meta(net: AF, len: u8, meta: T) -> Prefix<AF, T> {
        T::with_meta(net, len, Some(meta))
    }
    pub fn strip_meta(&self) -> Prefix<AF, NoMeta> {
        Prefix::<AF, NoMeta> {
            net: self.net,
            len: self.len,
            meta: None,
        }
    }
}

impl<T, AF> std::fmt::Display for Prefix<AF, T>
where
    T: Meta,
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

pub(crate) type PrefixNodeIter<'a, AF, Meta> =
    Result<std::slice::IterMut<'a, Prefix<AF, Meta>>, Box<dyn std::error::Error>>;

impl<T> Meta for T
where
    T: Debug + Display,
{
    fn with_meta<AF: AddressFamily + PrimInt + Debug>(
        net: AF,
        len: u8,
        meta: Option<T>,
    ) -> Prefix<AF, T> {
        Prefix::<AF, T> { net, len, meta }
    }

    fn summary(&self) -> String {
        format!("{}", self)
    }
}

impl<AF, T> Ord for Prefix<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (self.net >> (AF::BITS - self.len) as usize)
            .cmp(&(other.net >> ((AF::BITS - other.len) % 32) as usize))
    }
}

impl<AF, T> PartialEq for Prefix<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    fn eq(&self, other: &Self) -> bool {
        self.net >> (AF::BITS - self.len) as usize
            == other.net >> ((AF::BITS - other.len) % 32) as usize
    }
}

impl<AF, T> PartialOrd for Prefix<AF, T>
where
    T: Debug + Display,
    AF: AddressFamily + PrimInt + Debug,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            (self.net >> (AF::BITS - self.len) as usize)
                .cmp(&(other.net >> ((AF::BITS - other.len) % 32) as usize)),
        )
    }
}

impl<AF, T> Eq for Prefix<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
}

impl<T, AF> Debug for Prefix<AF, T>
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

//------------ TrieLevelStats --------------------------------------------------------

pub struct TrieLevelStats {
    pub level: u8,
    pub nodes_num: u32,
    pub prefixes_num: u32,
}

impl fmt::Debug for TrieLevelStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{\"level\":{},\"nodes_num\":{},\"prefixes_num\":{}}}",
            self.level, self.nodes_num, self.prefixes_num
        )
    }
}
