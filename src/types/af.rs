use log::trace;
use zerocopy::{NetworkEndian, U128, U32};

use crate::types::BitSpan;

//------------ AddressFamily (trait) ----------------------------------------
//
/// The address family of an IP address as a Trait.
///
/// The idea of this trait is that each family will have a separate type to be
/// able to only take the exact amount of memory needed. Useful when building
/// trees with large amounts of addresses/prefixes. Used by rotonda-store for
/// this purpose.
pub trait AddressFamily:
    std::fmt::Binary
    + std::fmt::Debug
    + std::hash::Hash
    + std::fmt::Display
    + Eq
    + std::ops::BitAnd<Output = Self>
    + std::ops::BitOr<Output = Self>
    + std::ops::Shr<Self, Output = Self>
    + std::ops::Shl<Output = Self>
    + std::ops::Shl<Self, Output = Self>
    + std::ops::Sub<Output = Self>
    + Copy
    + Ord
    + zerocopy::FromBytes
    + zerocopy::IntoBytes
    + zerocopy::KnownLayout
    + zerocopy::Immutable
    + zerocopy::Unaligned
{
    /// The number of bits in the byte representation of the family.
    const BITS: u8;

    /// The type actually holding the value, u32 for IPv4, and u128 for IPv6.
    type Inner: Into<Self> + From<u32> + From<u8>;

    /// The std::net that the value of self belongs to. So,
    /// [std::net::Ipv4Addr], and [std::net::Ipv6Addr] for IPv4, and IPv6
    /// respectively.
    type InnerIpAddr;

    fn new(value: Self::Inner) -> Self {
        value.into()
    }

    fn from_ipaddr(ip_addr: Self::InnerIpAddr) -> Self;

    fn from_u32(value: u32) -> Self;
    fn from_u8(value: u8) -> Self;

    fn zero() -> Self;

    // returns the specified nibble from `start_bit` to (and including)
    // `start_bit + len` and shifted to the right.
    fn into_bit_span(net: Self, start_bit: u8, len: u8) -> BitSpan;

    /// Treat self as a prefix and append the given bitspan to it.
    fn add_bit_span(self, len: u8, bs: BitSpan) -> (Self, u8);

    /// fill the bits after the specified len with zeros. Interpreted as an IP
    /// Prefix, this means that self will be truncated to the specified len.
    fn truncate_to_len(self, len: u8) -> Self;

    /// Turn self in to a [std::net::IpAddr].
    fn into_ipaddr(self) -> std::net::IpAddr;

    /// Truncate self to a u32. For IPv4 this is a NOP. For IPv6 this
    /// truncates to 32 bits.
    fn dangerously_truncate_to_u32(self) -> u32;

    // For the sake of searching for 0/0, check the the right shift, since
    // since shifting with MAXLEN (32 in Ipv4, or 128 in IPv6) will panic
    // in debug mode. A failed check will simply retutrn zero. Used in
    // finding node_ids (always zero for 0/0).
    fn checked_shr_or_zero(self, rhs: u32) -> Self;
    fn checked_shl_or_zero(self, rhs: u32) -> Self;

    // These checked shifts are for use in debug asserts only.
    fn checked_shr(self, rhs: u32) -> Option<Self::Inner>;
    fn checked_shl(self, rhs: u32) -> Option<Self::Inner>;
}

//-------------- Ipv4 Type --------------------------------------------------

/// Exactly fitting IPv4 bytes (4 octets).
pub type IPv4 = zerocopy::U32<NetworkEndian>;

impl AddressFamily for IPv4 {
    const BITS: u8 = 32;
    type Inner = u32;
    type InnerIpAddr = std::net::Ipv4Addr;

    fn zero() -> Self {
        0.into()
    }

    fn from_u8(value: u8) -> Self {
        IPv4::from([0, 0, 0, value])
    }

    fn from_u32(value: u32) -> Self {
        IPv4::from(value)
    }

    fn from_ipaddr(ip_addr: Self::InnerIpAddr) -> Self {
        IPv4::from(ip_addr.octets())
    }

    fn into_bit_span(net: Self, start_bit: u8, len: u8) -> BitSpan {
        BitSpan {
            bits: ((net << <U32<NetworkEndian>>::from(start_bit as u32))
                >> <U32<NetworkEndian>>::from(((32 - len) % 32) as u32))
            .into(),
            len,
        }
    }

    fn add_bit_span(self, len: u8, bs: BitSpan) -> (U32<NetworkEndian>, u8) {
        let res = self | (bs.bits << (32 - len - bs.len) as usize);
        (res, len + bs.len)
    }

    fn into_ipaddr(self) -> std::net::IpAddr {
        std::net::IpAddr::V4(std::net::Ipv4Addr::from(u32::from(self)))
    }

    fn dangerously_truncate_to_u32(self) -> u32 {
        // not dangerous at all.
        self.into()
    }

    fn truncate_to_len(self, len: u8) -> Self {
        self & ((1_u32.rotate_right(len as u32)
            ^ 1_u32.saturating_sub(len as u32))
        .wrapping_sub(1)
            ^ u32::MAX)
    }

    fn checked_shr_or_zero(self, rhs: u32) -> Self {
        trace!("CHECKED_SHR_OR_ZERO {} >> {}", u32::from(self), rhs);
        if rhs == 0 || rhs == 32 {
            return 0.into();
        }
        self >> U32::<NetworkEndian>::from(rhs)
    }

    fn checked_shl_or_zero(self, rhs: u32) -> Self {
        trace!("CHECKED_SHL_OR_ZERO {} >> {}", u32::from(self), rhs);
        if rhs == 0 || rhs >= 32 {
            return 0.into();
        }
        self << U32::<NetworkEndian>::from(rhs)
    }

    fn checked_shr(self, rhs: u32) -> Option<u32> {
        u32::from(self).checked_shr(rhs)
    }

    fn checked_shl(self, rhs: u32) -> Option<u32> {
        u32::from(self).checked_shl(rhs)
    }
}

//-------------- Ipv6 Type --------------------------------------------------

/// Exactly fitting IPv6 bytes (16 octets).
pub type IPv6 = U128<NetworkEndian>;

impl AddressFamily for IPv6 {
    // const BITMASK: u128 = 0x1u128.rotate_right(1);
    const BITS: u8 = 128;
    type Inner = u128;
    type InnerIpAddr = std::net::Ipv6Addr;

    fn zero() -> Self {
        0.into()
    }

    fn from_ipaddr(ip_addr: Self::InnerIpAddr) -> Self {
        IPv6::from(ip_addr.octets())
    }

    fn into_bit_span(net: Self, start_bit: u8, len: u8) -> BitSpan {
        BitSpan {
            bits: u128::from(
                (net << <U128<NetworkEndian>>::from(start_bit as u128))
                    >> (<U128<NetworkEndian>>::from(128 - len as u128) % 128),
            ) as u32,
            len,
        }
    }

    fn add_bit_span(self, len: u8, bs: BitSpan) -> (Self, u8) {
        let res = self | ((bs.bits as u128) << (128 - len - bs.len) as usize);
        (res, len + bs.len)
    }

    fn truncate_to_len(self, len: u8) -> Self {
        self & ((1_u128.rotate_right(len as u32)
            ^ 1_u128.saturating_sub(len as u128))
        .wrapping_sub(1)
            ^ u128::MAX)
    }

    fn into_ipaddr(self) -> std::net::IpAddr {
        std::net::IpAddr::V6(std::net::Ipv6Addr::from(u128::from(self)))
    }

    fn dangerously_truncate_to_u32(self) -> u32 {
        // this will chop off the high bits.
        u128::from(self) as u32
    }

    fn checked_shr_or_zero(self, rhs: u32) -> Self {
        if rhs == 0 || rhs == 128 {
            return U128::from(0);
        };

        self >> U128::from(rhs as u128)
    }

    fn checked_shl_or_zero(self, rhs: u32) -> Self {
        if rhs >= 128 {
            return U128::from(0);
        };

        self << U128::from(rhs as u128)
    }

    fn checked_shr(self, rhs: u32) -> Option<u128> {
        u128::from(self).checked_shr(rhs)
    }

    fn checked_shl(self, rhs: u32) -> Option<u128> {
        u128::from(self).checked_shl(rhs)
    }

    fn from_u8(value: u8) -> Self {
        IPv6::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, value])
    }

    fn from_u32(value: u32) -> Self {
        (value as u128).into()
    }
}

pub trait IntoIpAddr {
    fn into_ipaddr(self) -> std::net::IpAddr;
}

impl IntoIpAddr for u32 {
    fn into_ipaddr(self) -> std::net::IpAddr {
        std::net::IpAddr::V4(std::net::Ipv4Addr::from(self))
    }
}

impl IntoIpAddr for u128 {
    fn into_ipaddr(self) -> std::net::IpAddr {
        std::net::IpAddr::V6(std::net::Ipv6Addr::from(self))
    }
}
