use log::trace;
use zerocopy::{IntoBytes, NetworkEndian, U128, U32};

use crate::types::BitSpan;

//------------ AddressFamily (trait) ----------------------------------------
/// The address family of an IP address as a Trait.
///
/// The idea of this trait is that each family will have a separate type to
/// be able to only take the amount of memory needs. Useful when building
/// trees with large amounts of addresses/prefixes. Used by rotonda-store for
/// this purpose.
pub trait AddressFamily:
    std::fmt::Binary
    + std::fmt::Debug
    + std::hash::Hash
    + std::fmt::Display
    // + From<u32>
    // + From<u16>
    // + From<u8>
    + Eq
    + std::ops::BitAnd<Output = Self>
    + std::ops::BitOr<Output = Self>
    + std::ops::Shr<Self, Output = Self>
    + std::ops::Shl<Output = Self>
    + std::ops::Shl<Self, Output = Self>
    + std::ops::Sub<Output = Self>
    // + Zero
    + Copy
    + Ord
    + zerocopy::TryFromBytes
    + zerocopy::IntoBytes
    + zerocopy::KnownLayout
    + zerocopy::Immutable
    + zerocopy::Unaligned
{
    /// The byte representation of the family filled with 1s.
    // const BITMASK: Self;
    /// The number of bits in the byte representation of the family.
    const BITS: u8;
    type Inner: Into<Self> + From<u32> + From<u8>;
    type InnerIpAddr;

    fn new(value: Self::Inner) -> Self {
        value.into()
    }

    fn from_ipaddr(ip_addr: Self::InnerIpAddr) -> Self;

    fn from_u32(value: u32) -> Self;
    fn from_u8(value: u8) -> Self;

    fn zero() -> Self;

    fn fmt_net(net: Self) -> String;
    // returns the specified nibble from `start_bit` to (and including)
    // `start_bit + len` and shifted to the right.
    fn into_bit_span(net: Self, start_bit: u8, len: u8) -> BitSpan;

    /// Treat self as a prefix and append the given nibble to it.
    fn add_bit_span(self, len: u8, bs: BitSpan) -> (Self, u8);

    fn truncate_to_len(self, len: u8) -> Self;

    // fn from_ipaddr(net: std::net::IpAddr) -> Self;

    fn into_ipaddr(self) -> std::net::IpAddr;

    // temporary function, this will botch IPv6 completely.
    fn dangerously_truncate_to_u32(self) -> u32;

    // temporary function, this will botch IPv6 completely.
    // fn dangerously_truncate_to_usize(self) -> usize;

    // For the sake of searching for 0/0, check the the right shift, since
    // since shifting with MAXLEN (32 in Ipv4, or 128 in IPv6) will panic
    // in debug mode. A failed check will simply retutrn zero. Used in
    // finding node_ids (always zero for 0/0).
    fn checked_shr_or_zero(self, rhs: u32) -> Self;

    fn to_be_bytes<const PREFIX_SIZE: usize>(&self) -> [u8; PREFIX_SIZE];
}

//-------------- Ipv4 Type --------------------------------------------------

/// Exactly fitting IPv4 bytes (4 octets).
pub type IPv4 = zerocopy::U32<NetworkEndian>;

impl AddressFamily for IPv4 {
    // const BITMASK: u32 = 0x1u32.rotate_right(1);
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

    fn fmt_net(net: Self) -> String {
        std::net::Ipv4Addr::from(u32::from(net)).to_string()
    }

    fn into_bit_span(net: Self, start_bit: u8, len: u8) -> BitSpan {
        BitSpan {
            bits: ((net << <U32<NetworkEndian>>::from(start_bit as u32))
                >> <U32<NetworkEndian>>::from(((32 - len) % 32) as u32))
            .into(),
            len,
        }
    }

    // You can't shift with the number of bits of self, so we'll just return
    // zero for that case.
    //
    // Panics if len is greater than 32 (the number of bits of self).
    // fn truncate_to_len(self, len: u8) -> Self {
    //     match len {
    //         0 => U32::new(0),
    //         1..=31 => (self >> (32 - len as u32).into()) << (32 - len).into(),
    //         32 => self,
    //         _ => panic!("Can't truncate to more than 32 bits"),
    //     }
    // }

    /// Treat self as a prefix and append the given nibble to it.
    ///
    /// Shifts the rightmost `nibble_len` bits of `nibble` to the left to a
    /// position `len` bits from the left, then ORs the result into self.
    ///
    /// For example:
    ///
    /// ```
    /// # use rotonda_store::IPv4;
    /// # use rotonda_store::AddressFamily;
    /// let prefix = 0b10101010_00000000_00000000_00000000_u32; // 8-bit prefix
    /// let nibble = 0b1100110_u32;                             // 7-bit nibble
    /// let (new_prefix, new_len) = prefix.add_nibble(8, nibble, 7);
    /// assert_eq!(new_len, 8 + 7);
    /// assert_eq!(new_prefix, 0b10101010_11001100_00000000_00000000);
    /// //                       ^^^^^^^^ ^^^^^^^
    /// //                       prefix   nibble
    /// ```
    ///
    /// # Panics in debug mode!
    ///
    /// Will panic if there is insufficient space to add the given nibble,
    /// i.e. if `len + nibble_len >= 32`.
    ///
    /// ```
    /// # use rotonda_store::IPv4;
    /// # use rotonda_store::AddressFamily;
    /// let prefix = 0b10101010_00000000_00000000_00000100_u32; // 30-bit prefix
    /// let nibble = 0b1100110_u32;                             // 7-bit nibble
    /// let (new_prefix, new_len) = prefix.add_nibble(30, nibble, 7);
    /// ```
    fn add_bit_span(self, len: u8, bs: BitSpan) -> (U32<NetworkEndian>, u8) {
        let res = self | (bs.bits << (32 - len - bs.len) as usize);
        (res, len + bs.len)
    }

    // fn from_ipaddr(addr: std::net::IpAddr) -> U32<NetworkEndian> {
    //     // Well, this is awkward.
    //     if let std::net::IpAddr::V4(addr) = addr {
    //         (addr.octets()[0] as u32) << 24
    //             | (addr.octets()[1] as u32) << 16
    //             | (addr.octets()[2] as u32) << 8
    //             | (addr.octets()[3] as u32)
    //     } else {
    //         panic!("Can't convert IPv6 to IPv4");
    //     }
    // }

    fn into_ipaddr(self) -> std::net::IpAddr {
        std::net::IpAddr::V4(std::net::Ipv4Addr::from(u32::from(self)))
    }

    fn dangerously_truncate_to_u32(self) -> u32 {
        // not dangerous at all.
        self.into()
    }

    // fn dangerously_truncate_to_usize(self) -> usize {
    //     // not dangerous at all.
    //     <usize>::from(self)
    // }

    fn truncate_to_len(self, len: u8) -> Self {
        match len {
            0 => U32::new(0),
            1..=31 => {
                (self >> U32::from(32 - len as u32))
                    << U32::from(32 - len as u32)
            }
            32 => self,
            len => panic!("Can't truncate to more than 128 bits: {}", len),
        }
    }

    fn checked_shr_or_zero(self, rhs: u32) -> Self {
        trace!("CHECKED_SHR_OR_ZERO {} >> {}", u32::from(self), rhs);
        if rhs == 0 || rhs == 32 {
            return 0.into();
        }
        self >> U32::<NetworkEndian>::from(rhs)
    }

    fn to_be_bytes<const PREFIX_SIZE: usize>(&self) -> [u8; PREFIX_SIZE] {
        *self.as_bytes().first_chunk::<PREFIX_SIZE>().unwrap()
        // *u32::to_be_bytes(*self)
        //     .first_chunk::<PREFIX_SIZE>()
        //     .unwrap()
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

    fn fmt_net(net: Self) -> String {
        std::net::Ipv6Addr::from(u128::from(net)).to_string()
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

    /// Treat self as a prefix and append the given nibble to it.
    ///
    /// Shifts the rightmost `nibble_len` bits of `nibble` to the left to a
    /// position `len` bits from the left, then ORs the result into self.
    ///
    /// For example:
    ///
    /// ```
    /// # use rotonda_store::IPv6;
    /// # use rotonda_store::AddressFamily;
    /// let prefix = 0xF0F0F0F0_F0000000_00000000_00000000u128; // 36-bit prefix
    /// let nibble = 0xA8A8_u32;                                // 16-bit nibble
    /// let (new_prefix, new_len) = prefix.add_nibble(36, nibble, 16);
    /// assert_eq!(new_len, 36 + 16);
    /// assert_eq!(new_prefix, 0xF0F0F0F0F_A8A8000_00000000_00000000u128);
    /// //                       ^^^^^^^^^ ^^^^
    /// //                       prefix    nibble
    /// ```
    ///
    /// # Panics only in debug mode!
    ///
    /// In release mode this will be UB (Undefined Behaviour)!
    ///
    /// Will panic if there is insufficient space to add the given nibble,
    /// i.e. if `len + nibble_len >= 128`.
    ///
    /// ```
    /// # use rotonda_store::IPv6;
    /// # use rotonda_store::AddressFamily;
    /// let prefix = 0xFFFFFFFF_FFFFFFFF_FFFFFFFF_FFFF0000u128; // 112-bit prefix
    /// let nibble = 0xF00FF00F_u32;                            // 32-bit nibble
    /// let (new_prefix, new_len) = prefix.add_nibble(112, nibble, 32);
    /// ```
    fn add_bit_span(self, len: u8, bs: BitSpan) -> (Self, u8) {
        let res = self | ((bs.bits as u128) << (128 - len - bs.len) as usize);
        (res, len + bs.len)
    }

    fn truncate_to_len(self, len: u8) -> Self {
        match len {
            0 => U128::new(0),
            1..=127 => {
                (self >> U128::from(128 - len as u128))
                    << U128::from(128 - len as u128)
            }
            128 => self,
            len => panic!("Can't truncate to more than 128 bits: {}", len),
        }
    }

    // fn truncate_to_len(self, len: u8) -> Self {
    //     if (128 - len) == 0 {
    //         0
    //     } else {
    //         (self >> (128 - len)) << (128 - len)
    //     }
    // }

    // fn from_ipaddr(net: std::net::IpAddr) -> u128 {
    //     if let std::net::IpAddr::V6(addr) = net {
    //         addr.octets()[15] as u128
    //             | (addr.octets()[14] as u128) << 8
    //             | (addr.octets()[13] as u128) << 16
    //             | (addr.octets()[12] as u128) << 24
    //             | (addr.octets()[11] as u128) << 32
    //             | (addr.octets()[10] as u128) << 40
    //             | (addr.octets()[9] as u128) << 48
    //             | (addr.octets()[8] as u128) << 56
    //             | (addr.octets()[7] as u128) << 64
    //             | (addr.octets()[6] as u128) << 72
    //             | (addr.octets()[5] as u128) << 80
    //             | (addr.octets()[4] as u128) << 88
    //             | (addr.octets()[3] as u128) << 96
    //             | (addr.octets()[2] as u128) << 104
    //             | (addr.octets()[1] as u128) << 112
    //             | (addr.octets()[0] as u128) << 120
    //     } else {
    //         panic!("Can't convert IPv4 to IPv6");
    //     }
    // }

    fn into_ipaddr(self) -> std::net::IpAddr {
        std::net::IpAddr::V6(std::net::Ipv6Addr::from(u128::from(self)))
    }

    fn dangerously_truncate_to_u32(self) -> u32 {
        // this will chop off the high bits.
        u128::from(self) as u32
    }

    // fn dangerously_truncate_to_usize(self) -> usize {
    //     // this will chop off the high bits.
    //     self as usize
    // }

    fn checked_shr_or_zero(self, rhs: u32) -> Self {
        if rhs == 0 || rhs == 128 {
            return U128::from(0);
        };

        self >> U128::from(rhs as u128)
    }

    fn to_be_bytes<const PREFIX_SIZE: usize>(&self) -> [u8; PREFIX_SIZE] {
        // *u128::to_be_bytes(*self)
        //     .first_chunk::<PREFIX_SIZE>()
        //     .unwrap()
        *self.as_bytes().first_chunk::<PREFIX_SIZE>().unwrap()
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
