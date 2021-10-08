use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::{Binary, Debug};

#[derive(Copy, Clone)]
pub struct U256(pub u128, pub u128);

impl U256 {
    pub fn to_be_bytes(self) -> [u8; 32] {
        [self.0.to_be_bytes(), self.1.to_be_bytes()]
            .concat()
            .try_into()
            .expect("U256 with incorrect length.")
    }

    pub fn from_bytes(bytes: &[u8]) -> U256 {
        let nibble1: u128 = u128::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let nibble2: u128 = u128::from_be_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
            bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31],
        ]);
        U256(nibble1, nibble2)
    }
}

impl Debug for U256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{:0128b}\n             {:0128b}",
            self.0, self.1
        ))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct U512(pub u128, pub u128, pub u128, pub u128);

impl U512 {
    pub fn to_be_bytes(self) -> [u8; 64] {
        [
            self.0.to_be_bytes(),
            self.1.to_be_bytes(),
            self.2.to_be_bytes(),
            self.3.to_be_bytes(),
        ]
        .concat()
        .try_into()
        .expect("U512 with incorrect length.")
    }

    pub fn from_bytes(bytes: &[u8]) -> U512 {
        let nibble1: u128 = u128::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let nibble2: u128 = u128::from_be_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
            bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31],
        ]);
        let nibble3: u128 = u128::from_be_bytes([
            bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39],
            bytes[40], bytes[41], bytes[42], bytes[43], bytes[44], bytes[45], bytes[46], bytes[47],
        ]);
        let nibble4: u128 = u128::from_be_bytes([
            bytes[48], bytes[49], bytes[50], bytes[51], bytes[52], bytes[53], bytes[54], bytes[55],
            bytes[56], bytes[57], bytes[58], bytes[59], bytes[60], bytes[61], bytes[62], bytes[63],
        ]);
        U512(nibble1, nibble2, nibble3, nibble4)
    }
}

impl PartialEq for U256 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl Eq for U256 {}

impl Zero for U256 {
    fn zero() -> Self {
        U256(0_u128, 0_u128)
    }
}

impl Binary for U256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Binary::fmt(&self, f)
    }
}

impl PartialOrd for U256 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.0, &other.0) {
            (a, b) if &a > b => Some(self.0.cmp(&other.0)),
            _ => Some(self.1.cmp(&other.1)),
        }
    }
}

impl Ord for U256 {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0, &other.0) {
            (a, b) if &a > b => self.0.cmp(&other.0),
            _ => self.1.cmp(&other.1),
        }
    }
}

impl std::ops::BitOr<Self> for U256 {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0, self.1 | rhs.1)
    }
}

impl std::ops::BitAnd<Self> for U256 {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self::Output
    where
        Self: Eq,
    {
        Self(self.0 & rhs.0, self.1 & rhs.1)
    }
}

impl PartialOrd for U512 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.0, &other.0) {
            (a, b) if &a > b => Some(self.0.cmp(&other.0)),
            _ => match (self.1, &other.1) {
                (a, b) if &a > b => Some(self.1.cmp(&other.1)),
                _ => match (self.2, &other.2) {
                    (a, b) if &a > b => Some(self.2.cmp(&other.2)),
                    _ => Some(self.3.cmp(&other.3)),
                },
            },
        }
    }
}

impl PartialEq for U512 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1 && self.2 == other.2 && self.3 == other.3
    }
}

impl Eq for U512 {}

impl Zero for U512 {
    fn zero() -> Self {
        U512(0_u128, 0_u128, 0_u128, 0_u128)
    }
}

impl Binary for U512 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Binary::fmt(&self, f)
    }
}

impl std::ops::BitOr<Self> for U512 {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(
            self.0 | rhs.0,
            self.1 | rhs.1,
            self.2 | rhs.2,
            self.3 | rhs.3,
        )
    }
}

impl std::ops::BitAnd<Self> for U512 {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self::Output
    where
        Self: Eq,
    {
        Self(
            self.0 & rhs.0,
            self.1 & rhs.1,
            self.2 & rhs.2,
            self.3 & rhs.3,
        )
    }
}

pub trait Zero {
    fn zero() -> Self;
}

impl Zero for u8 {
    fn zero() -> u8 {
        0
    }
}

impl Zero for u16 {
    fn zero() -> u16 {
        0
    }
}

impl Zero for u32 {
    fn zero() -> u32 {
        0
    }
}

impl Zero for u64 {
    fn zero() -> u64 {
        0
    }
}

impl Zero for u128 {
    fn zero() -> u128 {
        0
    }
}