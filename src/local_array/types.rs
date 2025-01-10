use crate::AddressFamily;

use super::errors::PrefixStoreError;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum RouteStatus {
    Active,
    InActive,
    Withdrawn,
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

#[derive(Hash, Eq, PartialEq, Debug, Copy, Clone)]
pub struct PrefixId<AF: AddressFamily> {
    net: AF,
    len: u8,
}

impl<AF: AddressFamily> PrefixId<AF> {
    pub fn new(net: AF, len: u8) -> Self {
        PrefixId { net, len }
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
            net: AF::from_ipaddr(value.addr()),
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
            net: u32::from_be_bytes(*value.last_chunk::<4>().unwrap()).into(),
            len: value[0],
        }
    }
}
