use inetnum::asn::Asn;

use crate::Meta;

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct BeBytesAsn(pub [u8; 4]);

impl AsRef<[u8]> for BeBytesAsn {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<Vec<u8>> for BeBytesAsn {
    fn from(value: Vec<u8>) -> Self {
        Self(*value.first_chunk::<4>().unwrap())
    }
}

impl Meta for BeBytesAsn {
    type Orderable<'a> = Asn;
    type TBI = ();

    fn as_orderable(&self, _tbi: Self::TBI) -> Asn {
        u32::from_be_bytes(self.0).into()
    }
}

impl std::fmt::Display for BeBytesAsn {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AS{:?}", self.0)
    }
}

impl From<Asn> for BeBytesAsn {
    fn from(value: Asn) -> Self {
        Self(u32::from_be_bytes(value.to_raw()).to_le_bytes())
    }
}

impl From<u32> for BeBytesAsn {
    fn from(value: u32) -> Self {
        Self(value.to_le_bytes())
    }
}
