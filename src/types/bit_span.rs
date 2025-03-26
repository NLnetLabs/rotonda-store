use crate::rib::BIT_SPAN_SIZE;

//------------ BitSpan -------------------------------------------------------

// A bitspan is a bunch of bits representing the last stride in a NodeId
// or PrefixId, as such it can have a length of 1, 2, or 3 bits, in a stride
// length of 4 bits (which is the hard-coded value for all of the store
// currently).
//
// We are storing these bits in a u32, which may seem to be wasting space
// on first glance. However:
// - this bitspan is never stored in the store as
// such, it is used for intermediary calculations. The assumption is that
// modern CPUs always throw around values aligned on 4 bytes.
// - even if wanted to optimise for space, we have to take into account that
// we need to shift right and left beyond the size of the final result of a
// series of calculations.
#[derive(Copy, Clone, Debug)]
pub struct BitSpan {
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
        println!("check bit span: {:?}", self);
        if self.len == 0 && self.bits == 0 {
            return true;
        };
        self.len < 5
            && self.bits < 16
            && (self.bits << (32 - self.len)) >> (32 - self.len) == self.bits
    }

    pub(crate) fn into_bit_pos(self) -> u32 {
        1 << (BIT_SPAN_SIZE
            - ((1 << self.len) - 1) as u8
            - self.bits as u8
            - 1)
    }

    pub(crate) fn cursor_from_bit_span(self) -> u8 {
        self.into_bit_pos().leading_zeros() as u8
    }
}

impl std::fmt::Binary for BitSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032b} (len {})", self.bits, self.len)
    }
}
