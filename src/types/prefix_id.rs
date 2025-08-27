use std::mem::MaybeUninit;

use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, NetworkEndian, Unaligned,
    U32,
};

use crate::{
    cht::{nodeset_size, prev_node_size},
    AddressFamily, IPv4,
};

pub trait Nlri:
    Copy
    + Clone
    + std::fmt::Debug
    // + std::fmt::Display
    + Eq
    + std::hash::Hash
    + FromBytes
    + IntoBytes
    + KnownLayout
    + Immutable
    + Unaligned
    + Ord
    + PartialOrd
{
    const BITS: u8;
    fn len(&self) -> u8;
    fn hash_for_level(&self, level: u8) -> usize;
    fn from_u8(value: u8) -> Self;
    fn from_u32(value: u32) -> Self;
}

impl Nlri for U32<NetworkEndian> {
    const BITS: u8 = 32;
    fn len(&self) -> u8 {
        32
    }

    fn hash_for_level(&self, level: u8) -> usize {
        let last_level = prev_node_size(self.len(), level);

        // HASHING FUNCTION
        let size = nodeset_size(self.len(), level);

        // shifting left and right here should never overflow for inputs
        // (NodeId, level) that are valid for IPv4 and IPv6. In release
        // compiles this may NOT be noticable, because the undefined behaviour
        // is most probably the desired behaviour (saturating). But it's UB
        // for a reason, so we should not rely on it, and verify that we are
        // not hitting that behaviour.
        debug_assert!(self.checked_shl(last_level as u32).is_some());
        debug_assert!((*self << <Self as Nlri>::from_u32(last_level as u32))
            .checked_shr(u32::from(
                (<Self as Nlri>::BITS - size) % <Self as Nlri>::BITS
            ))
            .is_some());

        ((*self << <Self as Nlri>::from_u32(last_level as u32))
            >> <Self as Nlri>::from_u8(
                (<Self as Nlri>::BITS - size) % <Self as Nlri>::BITS,
            ))
        .dangerously_truncate_to_u32() as usize
    }

    fn from_u8(value: u8) -> Self {
        Self::from([0, 0, 0, value])
    }

    fn from_u32(value: u32) -> Self {
        Self::from(value.to_be_bytes())
    }
}

impl<const SIZE: usize> Nlri for [u8; SIZE] {
    // We're chopping this array into [u8; 8] to calculate the hashes over, so
    // that's 64 bits.
    const BITS: u8 = 64;

    fn len(&self) -> u8 {
        #[allow(clippy::unwrap_used)]
        *self.first().unwrap()
    }

    #[allow(clippy::unwrap_used)]
    fn hash_for_level(&self, level: u8) -> usize {
        let start = level as usize * 8;
        #[allow(clippy::unwrap_used)]
        let bit_array =
            <u64>::from_be_bytes(*self[start..].first_chunk::<8>().unwrap());
        let last_level = prev_node_size(self.len(), level);
        // let shift = <u64>::from_be_bytes(
        //     *<Self as Nlri>::from_u32(last_level as u32)
        //         .first_chunk::<8>()
        //         .unwrap(),
        // );

        // HASHING FUNCTION
        let size = nodeset_size(self.len(), level);

        // shifting left and right here should never overflow for inputs
        // (NodeId, level) that are valid for IPv4 and IPv6. In release
        // compiles this may NOT be noticable, because the undefined behaviour
        // is most probably the desired behaviour (saturating). But it's UB
        // for a reason, so we should not rely on it, and verify that we are
        // not hitting that behaviour.
        debug_assert!(bit_array.checked_shl(last_level as u32).is_some());
        debug_assert!((bit_array << last_level)
            .checked_shr(u32::from(
                (<Self as Nlri>::BITS - size) % <Self as Nlri>::BITS
            ))
            .is_some());

        (
            (bit_array << last_level) >>
            // >> <u64>::from_be_bytes(
            //     *<Self as Nlri>::from_u8(
                    ((<Self as Nlri>::BITS - size) % <Self as Nlri>::BITS)
            // )
            // .first_chunk::<8>()
            // .unwrap(),
        ) as usize
        // .dangerously_truncate_to_u32() as usize
    }

    #[allow(clippy::unwrap_used)]
    fn from_u8(value: u8) -> Self {
        let mut b = [0; SIZE];
        *b.last_mut().unwrap() = value;
        b
    }

    #[allow(clippy::unwrap_used)]
    fn from_u32(value: u32) -> Self {
        let mut b = [0; SIZE];
        *b.last_chunk_mut::<4>().unwrap() = value.to_be_bytes();
        b
    }
}

impl<AF: AddressFamily> Nlri for PrefixId<AF> {
    const BITS: u8 = AF::BITS;
    fn len(&self) -> u8 {
        self.len
    }

    fn from_u8(value: u8) -> Self {
        Self::from([0, 0, 0, value])
    }

    fn from_u32(value: u32) -> Self {
        Self::from(value.to_be_bytes())
    }

    fn hash_for_level(&self, level: u8) -> usize {
        let last_level = prev_node_size(self.len(), level);

        // HASHING FUNCTION
        let size = nodeset_size(self.len(), level);

        // shifting left and right here should never overflow for inputs
        // (NodeId, level) that are valid for IPv4 and IPv6. In release
        // compiles this may NOT be noticable, because the undefined behaviour
        // is most probably the desired behaviour (saturating). But it's UB
        // for a reason, so we should not rely on it, and verify that we are
        // not hitting that behaviour.
        debug_assert!(self.bits().checked_shl(last_level as u32).is_some());
        debug_assert!((self.bits() << AF::from_u32(last_level as u32))
            .checked_shr(u32::from((Self::BITS - size) % Self::BITS))
            .is_some());

        ((self.bits() << AF::from_u32(last_level as u32))
            >> AF::from_u8((Self::BITS - size) % Self::BITS))
        .dangerously_truncate_to_u32() as usize
    }
}

impl<AF: AddressFamily> std::fmt::Display for PrefixId<AF> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.bits(), self.len())
    }
}

//------------ PrefixId ------------------------------------------------------

// The type that acts both as an id for every prefix node in the prefix CHT,
// and as the internal prefix type. It's cut to size for an AF, unlike the
// inetnum Prefix, as not to waste memory. We use the latter on the public
// API.

#[derive(
    Hash,
    Eq,
    PartialEq,
    Debug,
    Copy,
    Clone,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
    Ord,
    PartialOrd,
)]
#[repr(C)]
pub struct PrefixId<AF: AddressFamily> {
    // DO NOT CHANGE THE ORDER OF THESE FIELDS!
    // zerocopy uses this to concatenate the bytes in this order, and the
    // lsm_tree needs to have `len` first, and `net` second to create keys
    // that are correctly sorted on prefix length.
    len: u8,
    bits: AF,
}

impl<AF: AddressFamily> PrefixId<AF> {
    pub(crate) fn new(net: AF, len: u8) -> Self {
        PrefixId { len, bits: net }
    }

    pub(crate) fn bits(&self) -> AF {
        self.bits
    }

    pub(crate) fn len(&self) -> u8 {
        self.len
    }

    pub(crate) fn truncate_to_len(self, len: u8) -> Self {
        Self {
            bits: self.bits.truncate_to_len(len),
            len,
        }
    }
}

// There is no reasonable way for this to panic, PrefixId and inetnum's Prefix
// represent the same data in slightly different ways.
#[allow(clippy::unwrap_used)]
impl<AF: AddressFamily> From<inetnum::addr::Prefix> for PrefixId<AF> {
    fn from(value: inetnum::addr::Prefix) -> Self {
        Self {
            bits: match value.addr() {
                std::net::IpAddr::V4(addr) => {
                    *AF::try_ref_from_bytes(&addr.octets()).unwrap()
                }
                std::net::IpAddr::V6(addr) => {
                    *AF::try_ref_from_bytes(&addr.octets()).unwrap()
                }
            },
            len: value.len(),
        }
    }
}

impl From<(u32, u8)> for PrefixId<IPv4> {
    fn from(value: (u32, u8)) -> Self {
        Self {
            bits: value.0.into(),
            len: value.1,
        }
    }
}

// There is no reasonable way for this to panic, PrefixId and inetnum's Prefix
// represent the same data in slightly different ways.
#[allow(clippy::unwrap_used)]
impl<AF: AddressFamily> From<PrefixId<AF>> for inetnum::addr::Prefix {
    fn from(value: PrefixId<AF>) -> Self {
        Self::new(value.bits().into_ipaddr(), value.len()).unwrap()
    }
}

#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
impl<AF: AddressFamily, const PREFIX_SIZE: usize> From<[u8; PREFIX_SIZE]>
    for PrefixId<AF>
{
    fn from(value: [u8; PREFIX_SIZE]) -> Self {
        Self {
            // This cannot panic for values of PREFIX_SIZE greater than 1
            bits: *AF::ref_from_bytes(&value.as_slice()[1..]).unwrap(),
            len: value[0],
        }
    }
}

#[allow(clippy::unwrap_used)]
impl<'a, AF: AddressFamily, const PREFIX_SIZE: usize>
    From<&'a [u8; PREFIX_SIZE]> for &'a PrefixId<AF>
{
    fn from(value: &'a [u8; PREFIX_SIZE]) -> Self {
        // This cannot panic for values of PREFIX_SIZE greater than 1
        PrefixId::ref_from_bytes(value.as_slice()).unwrap()
    }
}
