use num::PrimInt;

use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Debug, Display};

use routecore::record::{MergeUpdate, Meta, NoMeta};
use routecore::addr::AddressFamily;

//------------ MatchOptions ----------------------------------------------------------

pub struct MatchOptions {
    pub match_type: MatchType,
    pub include_less_specifics: bool,
    pub include_more_specifics: bool,
}

#[derive(Debug, Clone)]
pub enum MatchType {
    ExactMatch,
    LongestMatch,
    EmptyMatch,
}

impl std::fmt::Display for MatchType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MatchType::ExactMatch => write!(f, "exact-match"),
            MatchType::LongestMatch => write!(f, "longest-match"),
            MatchType::EmptyMatch => write!(f, "empty-match"),
        }
    }
}

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

// pub enum NoMeta { Empty }

// impl fmt::Debug for NoMeta {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.write_str("")
//     }
// }

// impl Display for NoMeta {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.write_str("")
//     }
// }

// impl MergeUpdate for NoMeta {
//     fn merge_update(&mut self, _: NoMeta) -> Result<(), Box<dyn std::error::Error>> {
//         Ok(())
//     }
// }

// pub trait Meta
// where
//     Self: Debug + Sized + Display,
// {
//     fn with_meta<AF: AddressFamily + PrimInt + Debug>(
//         net: AF,
//         len: u8,
//         meta: Option<Self>,
//     ) -> PrefixInfoUnit<AF, Self> {
//         PrefixInfoUnit { net, len, meta }
//     }

//     fn summary(&self) -> String;
// }
// pub trait MergeUpdate {
//     fn merge_update(&mut self, update_meta: Self) -> Result<(), Box<dyn std::error::Error>>;
// }

//------------ Address Family (trait) --------------------------------------------------------

// pub trait AddressFamily: PrimInt + Debug {
//     const BITMASK: Self;
//     const BITS: u8;
//     fn fmt_net(net: Self) -> String;
//     // returns the specified nibble from `start_bit` to (and
//     // including) `start_bit + len` and shifted to the right.
//     fn get_nibble(net: Self, start_bit: u8, len: u8) -> u32;

//     #[cfg(feature = "dynamodb")]
//     fn from_addr(net: Addr) -> Self;

//     #[cfg(feature = "dynamodb")]
//     fn into_addr(self) -> Addr;

//     fn into_ipaddr(self) -> std::net::IpAddr;
// }

// pub(crate) type IPv4 = u32;

// impl AddressFamily for IPv4 {
//     const BITMASK: u32 = 0x1u32.rotate_right(1);
//     const BITS: u8 = 32;

//     fn fmt_net(net: Self) -> String {
//         std::net::Ipv4Addr::from(net).to_string()
//     }

//     fn get_nibble(net: Self, start_bit: u8, len: u8) -> u32 {
//         (net << start_bit) >> ((32 - len) % 32)
//     }

//     #[cfg(feature = "dynamodb")]
//     fn from_addr(net: Addr) -> u32 {
//         net.to_bits() as u32
//     }

//     #[cfg(feature = "dynamodb")]
//     fn into_addr(self) -> Addr {
//         Addr::from_bits(self as u128)
//     }

//     fn into_ipaddr(self) -> std::net::IpAddr {
//         std::net::IpAddr::V4(std::net::Ipv4Addr::from(self))
//     }
// }

// pub(crate) type IPv6 = u128;

// impl AddressFamily for IPv6 {
//     const BITMASK: u128 = 0x1u128.rotate_right(1);
//     const BITS: u8 = 128;
//     fn fmt_net(net: Self) -> String {
//         std::net::Ipv6Addr::from(net).to_string()
//     }

//     fn get_nibble(net: Self, start_bit: u8, len: u8) -> u32 {
//         ((net << start_bit) >> ((128 - len) % 128)) as u32
//     }

//     #[cfg(feature = "dynamodb")]
//     fn from_addr(net: Addr) -> u128 {
//         net.to_bits()
//     }

//     #[cfg(feature = "dynamodb")]
//     fn into_addr(self) -> Addr {
//         Addr::from_bits(self)
//     }

//     fn into_ipaddr(self) -> std::net::IpAddr {
//         std::net::IpAddr::V6(std::net::Ipv6Addr::from(self))
//     }
// }

//------------ Addr ----------------------------------------------------------

// #[derive(Clone, Copy, Debug)]
// pub enum Addr {
//     V4(u32),
//     V6(u128),
// }

// impl From<Ipv4Addr> for Addr {
//     fn from(addr: Ipv4Addr) -> Self {
//         Self::V4(addr.into())
//     }
// }

// impl From<Ipv6Addr> for Addr {
//     fn from(addr: Ipv6Addr) -> Self {
//         Self::V6(addr.into())
//     }
// }

// impl From<IpAddr> for Addr {
//     fn from(addr: IpAddr) -> Self {
//         match addr {
//             IpAddr::V4(addr) => addr.into(),
//             IpAddr::V6(addr) => addr.into(),
//         }
//     }
// }

// impl From<Addr> for IpAddr {
//     fn from(addr: Addr) -> Self {
//         match addr {
//             Addr::V4(addr) => IpAddr::V4(addr.into()),
//             Addr::V6(addr) => IpAddr::V6(addr.into()),
//         }
//     }
// }

// impl From<u32> for Addr {
//     fn from(addr: u32) -> Self {
//         addr.into()
//     }
// }

// impl FromStr for Addr {
//     type Err = <IpAddr as FromStr>::Err;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         IpAddr::from_str(s).map(Into::into)
//     }
// }

// impl fmt::Display for Addr {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         match self {
//             Addr::V4(addr) => write!(f, "{}", std::net::Ipv4Addr::from(*addr)),
//             Addr::V6(addr) => write!(f, "{}", std::net::Ipv6Addr::from(*addr)),
//         }
//     }
// }

// impl Addr {
//     pub fn to_bits(&self) -> u128 {
//         match self {
//             Addr::V4(addr) => *addr as u128,
//             Addr::V6(addr) => *addr,
//         }
//     }

//     pub fn to_ipaddr(&self) -> std::net::IpAddr {
//         match self {
//             Addr::V4(addr) => IpAddr::V4(std::net::Ipv4Addr::from(*addr)),
//             Addr::V6(addr) => IpAddr::V6(std::net::Ipv6Addr::from(*addr)),
//         }
//     }
// }

//------------ PrefixInfoUnit --------------------------------------------------------

#[derive(Clone, Copy)]
pub struct PrefixInfoUnit<AF, T>
where
    T: Meta,
    AF: AddressFamily,
{
    pub net: AF,
    pub len: u8,
    pub meta: Option<T>,
}

impl<T, AF> PrefixInfoUnit<AF, T>
where
    T: Meta + MergeUpdate,
    AF: AddressFamily + PrimInt + Debug,
{
    pub fn new(net: AF, len: u8) -> PrefixInfoUnit<AF, T> {
        Self {
            net,
            len,
            meta: None,
        }
    }
    pub fn new_with_meta(net: AF, len: u8, meta: T) -> PrefixInfoUnit<AF, T> {
        Self {
            net,
            len,
            meta: Some(meta),
        }
    }
    pub fn strip_meta(&self) -> PrefixInfoUnit<AF, NoMeta> {
        PrefixInfoUnit::<AF, NoMeta> {
            net: self.net,
            len: self.len,
            meta: None,
        }
    }
}

impl<T, AF> std::fmt::Display for PrefixInfoUnit<AF, T>
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

// impl<T> Meta for T
// where
//     T: Debug + Display,
// {
//     // fn with_meta<AF: AddressFamily + PrimInt + Debug>(
//     //     net: AF,
//     //     len: u8,
//     //     meta: Option<T>,
//     // ) -> PrefixInfoUnit<AF, T> {
//     //     PrefixInfoUnit::<AF, T> { net, len, meta }
//     // }

//     fn summary(&self) -> String {
//         format!("{}", self)
//     }
// }

impl<AF, T> Ord for PrefixInfoUnit<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (self.net >> (AF::BITS - self.len) as usize)
            .cmp(&(other.net >> ((AF::BITS - other.len) % 32) as usize))
    }
}

impl<AF, T> PartialEq for PrefixInfoUnit<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    fn eq(&self, other: &Self) -> bool {
        self.net >> (AF::BITS - self.len) as usize
            == other.net >> ((AF::BITS - other.len) % 32) as usize
    }
}

impl<AF, T> PartialOrd for PrefixInfoUnit<AF, T>
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

impl<AF, T> Eq for PrefixInfoUnit<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
}

impl<T, AF> Debug for PrefixInfoUnit<AF, T>
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
