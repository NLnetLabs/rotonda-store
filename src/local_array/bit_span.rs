#[derive(Copy, Clone, Debug)]
pub(crate) struct BitSpan {
    pub bits: u32,
    pub len: u8,
}

impl BitSpan {
    pub(crate) fn new(bits: u32, len: u8) -> Self {
        Self { bits, len }
    }

    // Deep, dark, black magic. Calculate the bit span from the index in a
    // bitarr. This is used by iterators, so they can have one sequential i
    // loop, that goes over all positions in a bitarr by its indexes.
    pub fn from_bit_pos_index(mut i: u8) -> Self {
        let bits = i as u32;
        i += 1;
        i |= i >> 1;
        i |= i >> 2;
        i |= i >> 3;
        i = (i >> 1).count_ones() as u8;
        Self {
            bits: bits - ((1 << i as u32) - 1),
            len: i,
        }
    }

    pub(crate) fn check(&self) -> bool {
        // println!("check bit span: {:?}", self);
        self.len < 5
            && self.bits < 16
            && (self.bits << (32 - self.len)) >> (32 - self.len) == self.bits
    }
}

impl std::fmt::Binary for BitSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032b} (len {})", self.bits, self.len)
    }
}
