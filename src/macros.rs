#[macro_export]
// This macro only works for stride with bitmaps that are <= u128,
// the ones with synthetic integers (U256, U512) don't have the trait
// implementations for left|right shift, counting ones etc.
#[doc(hidden)]
macro_rules! impl_primitive_stride {
    ( $( $len: expr; $bits: expr; $pfxsize:ty; $ptrsize: ty ), * ) => {
            $(
                impl Stride for $pfxsize {
                    type PtrSize = $ptrsize;
                    const BITS: u8 = $bits;
                    const STRIDE_LEN: u8 = $len;

                    fn get_bit_pos(nibble: u32, len: u8) -> $pfxsize {
                        1 << (<Self as Stride>::BITS - ((1 << len) - 1) as u8 - nibble as u8 - 1)
                    }

                    fn get_pfx_index(bitmap: $pfxsize, nibble: u32, len: u8) -> usize {
                        (bitmap >> ((<Self as Stride>::BITS - ((1 << len) - 1) as u8 - nibble as u8 - 1) as usize))
                            .count_ones() as usize
                            - 1
                    }
                    fn get_ptr_index(bitmap: $ptrsize, nibble: u32) -> usize {
                        (bitmap >> ((<Self as Stride>::BITS >> 1) - nibble as u8 - 1) as usize).count_ones()
                            as usize
                            - 1
                    }

                    fn into_stride_size(bitmap: $ptrsize) -> $pfxsize {
                        bitmap as $pfxsize << 1
                    }

                    fn into_ptrbitarr_size(bitmap: $pfxsize) -> $ptrsize {
                        (bitmap >> 1) as $ptrsize
                    }

                    #[inline]
                    fn leading_zeros(self) -> u32 {
                        self.leading_zeros()
                    }
                }
            )*
    };
}
