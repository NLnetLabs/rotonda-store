use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::{Binary, Debug};
use std::sync::atomic::AtomicU64;

//------------ U256 synthetic integer type ----------------------------------

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
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
            bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let nibble2: u128 = u128::from_be_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
            bytes[22], bytes[23], bytes[24], bytes[25], bytes[26], bytes[27],
            bytes[28], bytes[29], bytes[30], bytes[31],
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

impl Eq for U256 {}

impl std::ops::Add for U256 {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        U256(self.0.wrapping_add(other.0), self.1.wrapping_add(other.1))
    }
}

impl num::Zero for U256 {
    fn zero() -> Self {
        U256(0_u128, 0_u128)
    }
    fn is_zero(&self) -> bool {
        self.0 == 0_u128 && self.1 == 0_u128
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

impl PartialEq for U256 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

//------------ U512 Synthetic Integer Type ----------------------------------

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
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
            bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let nibble2: u128 = u128::from_be_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
            bytes[22], bytes[23], bytes[24], bytes[25], bytes[26], bytes[27],
            bytes[28], bytes[29], bytes[30], bytes[31],
        ]);
        let nibble3: u128 = u128::from_be_bytes([
            bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37],
            bytes[38], bytes[39], bytes[40], bytes[41], bytes[42], bytes[43],
            bytes[44], bytes[45], bytes[46], bytes[47],
        ]);
        let nibble4: u128 = u128::from_be_bytes([
            bytes[48], bytes[49], bytes[50], bytes[51], bytes[52], bytes[53],
            bytes[54], bytes[55], bytes[56], bytes[57], bytes[58], bytes[59],
            bytes[60], bytes[61], bytes[62], bytes[63],
        ]);
        U512(nibble1, nibble2, nibble3, nibble4)
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
        self.0 == other.0
            && self.1 == other.1
            && self.2 == other.2
            && self.3 == other.3
    }
}

impl Eq for U512 {}

impl std::ops::Add for U512 {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Self(
            self.0 + rhs.0,
            self.1 + rhs.1,
            self.2 + rhs.2,
            self.3 + rhs.3,
        )
    }
}

impl num::Zero for U512 {
    fn zero() -> Self {
        U512(0_u128, 0_u128, 0_u128, 0_u128)
    }
    fn is_zero(&self) -> bool {
        self.0 == 0_u128
            && self.1 == 0_u128
            && self.2 == 0_u128
            && self.3 == 0_u128
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

//------------ Atomic U128 Synthetic Integer Type -------------------------------------

#[allow(dead_code)]
pub struct AtomicU128(pub AtomicU64, pub AtomicU64);

#[allow(dead_code)]
impl AtomicU128 {
    pub fn new(value: u128) -> Self {
        let (hi, lo) =
            (((value << 64) >> 64) as u64, ((value >> 64) << 64) as u64);
        AtomicU128(AtomicU64::new(hi), AtomicU64::new(lo))
    }

    pub fn into_be_bytes(self) -> [u8; 16] {
        [
            self.0.into_inner().to_be_bytes(),
            self.1.into_inner().to_be_bytes(),
        ]
        .concat()
        .try_into()
        .expect("AtomicU128 with incorrect length.")
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let hi = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
            bytes[6], bytes[7],
        ]);
        let lo = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13],
            bytes[14], bytes[15],
        ]);
        AtomicU128(AtomicU64::new(hi), AtomicU64::new(lo))
    }
}

impl Debug for AtomicU128 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{:016x}\n{:016x}",
            self.0.load(std::sync::atomic::Ordering::SeqCst),
            self.1.load(std::sync::atomic::Ordering::SeqCst)
        ))
    }
}

//------------ Atomic U256 Synthetic Integer Type -------------------------------------

#[allow(dead_code)]
pub struct AtomicU256(
    pub AtomicU64,
    pub AtomicU64,
    pub AtomicU64,
    pub AtomicU64,
);

#[allow(dead_code)]
impl AtomicU256 {
    pub fn new(value: U256) -> Self {
        let (hihi, hilo, lohi, lolo) = (
            ((value.0 << 64) >> 64) as u64,
            ((value.0 >> 64) << 64) as u64,
            ((value.1 << 64) >> 64) as u64,
            ((value.1 >> 64) << 64) as u64,
        );
        AtomicU256(
            AtomicU64::new(hihi),
            AtomicU64::new(hilo),
            AtomicU64::new(lohi),
            AtomicU64::new(lolo),
        )
    }

    pub fn into_be_bytes(self) -> [u8; 32] {
        [
            self.0.into_inner().to_be_bytes(),
            self.1.into_inner().to_be_bytes(),
            self.2.into_inner().to_be_bytes(),
            self.3.into_inner().to_be_bytes(),
        ]
        .concat()
        .try_into()
        .expect("AtomicU256 with incorrect length.")
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let hihi = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
            bytes[6], bytes[7],
        ]);
        let hilo = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13],
            bytes[14], bytes[15],
        ]);
        let lohi = u64::from_be_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
            bytes[22], bytes[23],
        ]);
        let lolo = u64::from_be_bytes([
            bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29],
            bytes[30], bytes[31],
        ]);
        AtomicU256(
            AtomicU64::new(hihi),
            AtomicU64::new(hilo),
            AtomicU64::new(lohi),
            AtomicU64::new(lolo),
        )
    }
}

//------------ Atomic U512 Synthetic Integer Type -------------------------------------

#[allow(dead_code)]
pub struct AtomicU512(
    pub AtomicU64,
    pub AtomicU64,
    pub AtomicU64,
    pub AtomicU64,
    pub AtomicU64,
    pub AtomicU64,
    pub AtomicU64,
    pub AtomicU64,
);

#[allow(dead_code)]
impl AtomicU512 {
    pub fn new(value: U512) -> Self {
        let (hihihi, hihilo, hilohi, hilolo, lohihi, lohilo, lolohi, lololo) = (
            ((value.0 << 64) >> 64) as u64,
            ((value.0 >> 64) << 64) as u64,
            ((value.1 << 64) >> 64) as u64,
            ((value.1 >> 64) << 64) as u64,
            ((value.2 << 64) >> 64) as u64,
            ((value.2 >> 64) << 64) as u64,
            ((value.3 << 64) >> 64) as u64,
            ((value.3 >> 64) << 64) as u64,
        );
        AtomicU512(
            AtomicU64::new(hihihi),
            AtomicU64::new(hihilo),
            AtomicU64::new(hilohi),
            AtomicU64::new(hilolo),
            AtomicU64::new(lohihi),
            AtomicU64::new(lohilo),
            AtomicU64::new(lolohi),
            AtomicU64::new(lololo),
        )
    }

    pub fn into_be_bytes(self) -> [u8; 64] {
        [
            self.0.into_inner().to_be_bytes(),
            self.1.into_inner().to_be_bytes(),
            self.2.into_inner().to_be_bytes(),
            self.3.into_inner().to_be_bytes(),
            self.4.into_inner().to_be_bytes(),
            self.5.into_inner().to_be_bytes(),
            self.6.into_inner().to_be_bytes(),
            self.7.into_inner().to_be_bytes(),
        ]
        .concat()
        .try_into()
        .expect("AtomicU512 with incorrect length.")
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let hihihi = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
            bytes[6], bytes[7],
        ]);
        let hihilo = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13],
            bytes[14], bytes[15],
        ]);
        let hilohi = u64::from_be_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
            bytes[22], bytes[23],
        ]);
        let hilolo = u64::from_be_bytes([
            bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29],
            bytes[30], bytes[31],
        ]);
        let lohihi = u64::from_be_bytes([
            bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37],
            bytes[38], bytes[39],
        ]);
        let lohilo = u64::from_be_bytes([
            bytes[40], bytes[41], bytes[42], bytes[43], bytes[44], bytes[45],
            bytes[46], bytes[47],
        ]);
        let lolohi = u64::from_be_bytes([
            bytes[48], bytes[49], bytes[50], bytes[51], bytes[52], bytes[53],
            bytes[54], bytes[55],
        ]);
        let lololo = u64::from_be_bytes([
            bytes[56], bytes[57], bytes[58], bytes[59], bytes[60], bytes[61],
            bytes[62], bytes[63],
        ]);
        AtomicU512(
            AtomicU64::new(hihihi),
            AtomicU64::new(hihilo),
            AtomicU64::new(hilohi),
            AtomicU64::new(hilolo),
            AtomicU64::new(lohihi),
            AtomicU64::new(lohilo),
            AtomicU64::new(lolohi),
            AtomicU64::new(lololo),
        )
    }
}
