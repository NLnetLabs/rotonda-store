use log::trace;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::AddressFamily;

use super::errors::PrefixStoreError;

#[derive(
    Clone,
    Copy,
    Debug,
    Hash,
    PartialEq,
    Eq,
    TryFromBytes,
    KnownLayout,
    Immutable,
    Unaligned,
    IntoBytes,
)]
#[repr(u8)]
pub enum RouteStatus {
    Active = 1,
    InActive = 2,
    Withdrawn = 3,
}

impl std::fmt::Display for RouteStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteStatus::Active => write!(f, "active"),
            RouteStatus::InActive => write!(f, "inactive"),
            RouteStatus::Withdrawn => write!(f, "withdrawn"),
        }
    }
}

impl From<RouteStatus> for u8 {
    fn from(value: RouteStatus) -> Self {
        match value {
            RouteStatus::Active => 1,
            RouteStatus::InActive => 2,
            RouteStatus::Withdrawn => 3,
        }
    }
}

impl TryFrom<u8> for RouteStatus {
    type Error = PrefixStoreError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(RouteStatus::Active),
            2 => Ok(RouteStatus::InActive),
            3 => Ok(RouteStatus::Withdrawn),
            _ => Err(PrefixStoreError::StoreNotReadyError),
        }
    }
}

//------------ PrefixId ------------------------------------------------------
#[derive(
    Hash,
    Eq,
    PartialEq,
    Debug,
    Copy,
    Clone,
    zerocopy::TryFromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct PrefixId<AF: AddressFamily> {
    len: u8,
    net: AF,
}

impl<AF: AddressFamily> PrefixId<AF> {
    pub fn new(net: AF, len: u8) -> Self {
        PrefixId { len, net }
    }

    pub fn get_net(&self) -> AF {
        self.net
    }

    pub fn get_len(&self) -> u8 {
        self.len
    }

    // Increment the length of the prefix without changing the bits part.
    // This is used to iterate over more-specific prefixes for this prefix,
    // since the more specifics iterator includes the requested `base_prefix`
    // itself.
    pub fn inc_len(self) -> Self {
        Self {
            net: self.net,
            len: self.len + 1,
        }
    }

    pub fn truncate_to_len(self, len: u8) -> Self {
        // trace!("orig {:032b}", self.net);
        // trace!(
        //     "new  {:032b}",
        //     self.net >> (AF::BITS - len).into() << (AF::BITS - len).into()
        // );
        // trace!(
        //     "truncate to net {} len {}",
        //     self.net >> (AF::BITS - len).into() << (AF::BITS - len).into(),
        //     len
        // );
        Self {
            // net: (self.net >> (AF::BITS - len)) << (AF::BITS - len),
            net: self.net.truncate_to_len(len),
            len,
        }
    }

    // The lsm tree, used for persistence, stores the prefix in the key with
    // len first, so that key range lookups can be made for more-specifics in
    // each prefix length.
    pub fn to_len_first_bytes<const PREFIX_SIZE: usize>(
        &self,
    ) -> [u8; PREFIX_SIZE] {
        let bytes = &mut [0_u8; PREFIX_SIZE];
        *bytes.last_chunk_mut::<4>().unwrap() = self.net.to_be_bytes();
        bytes[0] = self.len;
        *bytes
    }
}

impl<AF: AddressFamily> From<inetnum::addr::Prefix> for PrefixId<AF> {
    fn from(value: inetnum::addr::Prefix) -> Self {
        Self {
            net: match value.addr() {
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

impl<AF: AddressFamily> From<PrefixId<AF>> for inetnum::addr::Prefix {
    fn from(value: PrefixId<AF>) -> Self {
        Self::new(value.get_net().into_ipaddr(), value.get_len()).unwrap()
    }
}

impl<AF: AddressFamily, const PREFIX_SIZE: usize> From<[u8; PREFIX_SIZE]>
    for PrefixId<AF>
{
    fn from(value: [u8; PREFIX_SIZE]) -> Self {
        Self {
            net: *AF::try_ref_from_bytes(&value.as_slice()[1..]).unwrap(),
            len: value[0],
        }
    }
}

impl<'a, AF: AddressFamily, const PREFIX_SIZE: usize>
    From<&'a [u8; PREFIX_SIZE]> for &'a PrefixId<AF>
{
    fn from(value: &'a [u8; PREFIX_SIZE]) -> Self {
        PrefixId::try_ref_from_bytes(value.as_slice()).unwrap()
    }
}
