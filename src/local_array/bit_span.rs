#[derive(Copy, Clone, Debug)]
pub(crate) struct BitSpan {
    pub bits: u32,
    pub len: u8
}

impl BitSpan {
    pub(crate) fn new(bits: u32, len: u8) -> Self {
        Self {
            bits,
            len
        }
    }

    // Increment the bit span by one and calculate the new length.
    #[allow(dead_code)]
    pub(crate) fn inc(&mut self) {
        self.bits += 1;
        self.len = <u8>::max(self.len, (32 - self.bits.leading_zeros()) as u8);
    }

    #[allow(dead_code)]
    pub(crate) fn set_len_to_bits(&mut self) {
        self.len = <u8>::max(self.len, (32 - self.bits.leading_zeros()) as u8);
    }
    
}

impl std::fmt::Binary for BitSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032b} (len {})", self.bits, self.len)
    }
}