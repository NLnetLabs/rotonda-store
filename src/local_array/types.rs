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
pub struct PrefixId<AF: AddressFamily>(Option<(AF, u8)>);

impl<AF: AddressFamily> PrefixId<AF> {
    pub fn new(net: AF, len: u8) -> Self {
        PrefixId(Some((net, len)))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    pub fn get_net(&self) -> AF {
        self.0.unwrap().0
    }

    pub fn get_len(&self) -> u8 {
        self.0.unwrap().1
    }

    // This should never fail, since there shouldn't be a invalid prefix in
    // this prefix id in the first place.
    pub fn into_pub(&self) -> inetnum::addr::Prefix {
        inetnum::addr::Prefix::new(
            self.get_net().into_ipaddr(),
            self.get_len(),
        )
        .unwrap_or_else(|p| panic!("can't convert {:?} into prefix.", p))
    }

    // Increment the length of the prefix without changing the bits part.
    // This is used to iterate over more-specific prefixes for this prefix,
    // since the more specifics iterator includes the requested `base_prefix`
    // itself.
    pub fn inc_len(self) -> Self {
        Self(self.0.map(|(net, len)| (net, len + 1)))
    }

    pub fn as_bytes<const PREFIX_SIZE: usize>(&self) -> [u8; PREFIX_SIZE] {
        match self.0 {
            Some(r) => r.0.as_prefix_bytes(r.1),
            _ => [255; PREFIX_SIZE],
        }
    }
}

impl<AF: AddressFamily> std::default::Default for PrefixId<AF> {
    fn default() -> Self {
        PrefixId(None)
    }
}

impl<AF: AddressFamily> From<inetnum::addr::Prefix> for PrefixId<AF> {
    fn from(value: inetnum::addr::Prefix) -> Self {
        Self(Some((AF::from_ipaddr(value.addr()), value.len())))
    }
}

impl<AF: AddressFamily> From<PrefixId<AF>> for inetnum::addr::Prefix {
    fn from(value: PrefixId<AF>) -> Self {
        value.into_pub()
    }
}

impl<AF: AddressFamily, const PREFIX_SIZE: usize> From<PrefixId<AF>>
    for [u8; PREFIX_SIZE]
{
    fn from(value: PrefixId<AF>) -> Self {
        value.as_bytes::<PREFIX_SIZE>()
    }
}

impl<AF: AddressFamily, const PREFIX_SIZE: usize> From<[u8; PREFIX_SIZE]>
    for PrefixId<AF>
{
    fn from(value: [u8; PREFIX_SIZE]) -> Self {
        PrefixId(Some(AF::from_prefix_bytes(value)))
    }
}
