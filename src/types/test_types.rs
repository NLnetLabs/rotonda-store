use inetnum::asn::Asn;

use super::prefix_record::Meta;

#[derive(Clone, Copy, Hash)]
pub enum NoMeta {
    Empty,
}

impl std::fmt::Debug for NoMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("")
    }
}

impl std::fmt::Display for NoMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NoMeta")
    }
}

impl Meta for NoMeta {
    type Orderable<'a> = ();
    type TBI = ();
    fn as_orderable(&self, _tbi: Self::TBI) {}
}

impl AsRef<[u8]> for NoMeta {
    fn as_ref(&self) -> &[u8] {
        &[]
    }
}

impl From<Vec<u8>> for NoMeta {
    fn from(_value: Vec<u8>) -> Self {
        Self::Empty
    }
}

//------------ BeBytesAsn ----------------------------------------------------
//
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct BeBytesAsn(pub [u8; 4]);

impl AsRef<[u8]> for BeBytesAsn {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<Vec<u8>> for BeBytesAsn {
    fn from(value: Vec<u8>) -> Self {
        if let Some(value) = value.first_chunk::<4>() {
            Self(*value)
        } else {
            Self([0; 4])
        }
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
        write!(f, "AS{}", <u32>::from_le_bytes(self.0))
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

//------------ PrefixAs ------------------------------------------------------

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PrefixAs([u8; 4]);

impl PrefixAs {
    pub fn new(asn: Asn) -> Self {
        PrefixAs(u32::from_be_bytes(asn.to_raw()).to_le_bytes())
    }

    pub fn new_from_u32(value: u32) -> Self {
        PrefixAs(value.to_le_bytes())
    }

    pub fn asn(&self) -> Asn {
        Asn::from_u32(u32::from_le_bytes(self.0))
    }
}

impl Meta for PrefixAs {
    type Orderable<'a> = Asn;
    type TBI = ();
    fn as_orderable(&self, _tbi: Self::TBI) -> Asn {
        u32::from_le_bytes(self.0).into()
    }
}

impl AsRef<[u8]> for PrefixAs {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<Vec<u8>> for PrefixAs {
    fn from(value: Vec<u8>) -> Self {
        Self(*value.first_chunk::<4>().unwrap())
    }
}

impl std::fmt::Display for PrefixAs {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AS{}", u32::from_le_bytes(self.0))
    }
}
