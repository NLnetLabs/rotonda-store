use zerocopy::FromBytes;

use crate::AddressFamily;

//------------ PrefixId ------------------------------------------------------
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
)]
#[repr(C)]
pub struct PrefixId<AF: AddressFamily> {
    len: u8,
    net: AF,
}

impl<AF: AddressFamily> PrefixId<AF> {
    pub(crate) fn new(net: AF, len: u8) -> Self {
        PrefixId { len, net }
    }

    pub(crate) fn get_net(&self) -> AF {
        self.net
    }

    pub(crate) fn get_len(&self) -> u8 {
        self.len
    }

    pub(crate) fn truncate_to_len(self, len: u8) -> Self {
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
            net: self.net.truncate_to_len(len),
            len,
        }
    }

    // The lsm tree, used for persistence, stores the prefix in the key with
    // len first, so that key range lookups can be made for more-specifics in
    // each prefix length.
    // pub fn len_first_bytes<const PREFIX_SIZE: usize>(
    //     &self,
    // ) -> [u8; PREFIX_SIZE] {
    //     let bytes = &mut [0_u8; PREFIX_SIZE];
    //     *bytes.last_chunk_mut::<4>().unwrap() = self.net.to_be_bytes();
    //     bytes[0] = self.len;
    //     *bytes
    // }
}

#[allow(clippy::unwrap_used)]
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

// There is no reasonable way for this to panic, PrefixId and inetnum's Prefix
// represent the same data in slightly different ways.
#[allow(clippy::unwrap_used)]
impl<AF: AddressFamily> From<PrefixId<AF>> for inetnum::addr::Prefix {
    fn from(value: PrefixId<AF>) -> Self {
        Self::new(value.get_net().into_ipaddr(), value.get_len()).unwrap()
    }
}

#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
impl<AF: AddressFamily, const PREFIX_SIZE: usize> From<[u8; PREFIX_SIZE]>
    for PrefixId<AF>
{
    fn from(value: [u8; PREFIX_SIZE]) -> Self {
        Self {
            // This cannot panic for values of PREFIX_SIZE greater than 1
            net: *AF::ref_from_bytes(&value.as_slice()[1..]).unwrap(),
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
