use num::PrimInt;
use std::fmt::Debug;

//------------ AddressFamily (trait) ----------------------------------------
/// The address family of an IP address as a Trait.
///
/// The idea of this trait is that each family will have a separate type to
/// be able to only take the amount of memory needs. Useful when building
/// trees with large amounts of addresses/prefixes. Used by rotonda-store for
/// this purpose.
pub trait AddressFamily:
    PrimInt
    + std::fmt::Binary
    + Debug
    + std::hash::Hash
    + std::fmt::Display
    + From<u32>
    + From<u16>
{
    /// The byte representation of the family filled with 1s.
    const BITMASK: Self;
    /// The number of bits in the byte representation of the family.
    const BITS: u8;
    fn fmt_net(net: Self) -> String;
    // returns the specified nibble from `start_bit` to (and including)
    // `start_bit + len` and shifted to the right.
    fn get_nibble(net: Self, start_bit: u8, len: u8) -> u32;

    fn add_nibble(self, len: u8, nibble: u32, nibble_len: u8) -> (Self, u8);

    fn truncate_to_len(self, len: u8) -> Self;

    #[cfg(feature = "dynamodb")]
    fn from_addr(net: Addr) -> Self;

    #[cfg(feature = "dynamodb")]
    fn into_addr(self) -> Addr;

    fn into_ipaddr(self) -> std::net::IpAddr;

    // temporary function, this will botch IPv6 completely.
    fn dangerously_truncate_to_u32(self) -> u32;

    // temporary function, this will botch IPv6 completely.
    fn dangerously_truncate_to_usize(self) -> usize;
}

//-------------- Ipv4 Type --------------------------------------------------

/// Exactly fitting IPv4 bytes (4 octets).
pub type IPv4 = u32;

impl AddressFamily for IPv4 {
    const BITMASK: u32 = 0x1u32.rotate_right(1);
    const BITS: u8 = 32;

    fn fmt_net(net: Self) -> String {
        std::net::Ipv4Addr::from(net).to_string()
    }

    fn get_nibble(net: Self, start_bit: u8, len: u8) -> u32 {
        (net << start_bit) >> ((32 - len) % 32)
    }

    // You can't shift with the number of bits of self, so we'll just return
    // zero for that case.
    //
    // Panics if len is greater than 32 (the number of bits of self).
    fn truncate_to_len(self, len: u8) -> Self {
        match len {
            0 => 0,
            1..=31 => (self >> ((32 - len) as usize)) << (32 - len) as usize,
            32 => self,
            _ => panic!("Can't truncate to more than 32 bits"),
        }
    }

    fn add_nibble(self, len: u8, nibble: u32, nibble_len: u8) -> (u32, u8) {
        let res =
            self | ((nibble << (32 - len - nibble_len) as usize) as u32);
        (res, len + nibble_len)
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

    fn dangerously_truncate_to_u32(self) -> u32 {
        // not dangerous at all.
        self as u32
    }

    fn dangerously_truncate_to_usize(self) -> usize {
        // not dangerous at all.
        self as usize
    }
}

//-------------- Ipv6 Type --------------------------------------------------

/// Exactly fitting IPv6 bytes (16 octets).
pub type IPv6 = u128;

impl AddressFamily for IPv6 {
    const BITMASK: u128 = 0x1u128.rotate_right(1);
    const BITS: u8 = 128;
    fn fmt_net(net: Self) -> String {
        std::net::Ipv6Addr::from(net).to_string()
    }

    fn get_nibble(net: Self, start_bit: u8, len: u8) -> u32 {
        ((net << start_bit) >> ((128 - len) % 128)) as u32
    }

    fn add_nibble(self, len: u8, nibble: u32, nibble_len: u8) -> (Self, u8) {
        let res =
            self | ((nibble << (128 - len - nibble_len) as usize) as u128);
        (res, len + nibble_len)
    }

    fn truncate_to_len(self, len: u8) -> Self {
        if (128 - len) == 0 {
            0
        } else {
            (self >> ((128 - len) as usize)) << (128 - len) as usize
        }
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

    fn dangerously_truncate_to_u32(self) -> u32 {
        // this will chop off the high bits.
        self as u32
    }

    fn dangerously_truncate_to_usize(self) -> usize {
        // this will chop off the high bits.
        self as usize
    }
}
