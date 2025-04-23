use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

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
