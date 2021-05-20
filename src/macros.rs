#[macro_export]

// This macro only works for stride with bitmaps that are <= u128,
// the ones with synthetic integers (U256, U512) don't have the trait
// implementations for left|right shift, counting ones etc.

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
                    fn zero() -> Self {
                        0
                    }

                    #[inline]
                    fn one() -> Self {
                        1
                    }

                    #[inline]
                    fn leading_zeros(self) -> u32 {
                        self.leading_zeros()
                    }
                }
            )*
    };
}

#[macro_export]
macro_rules! stride_branch_for {
    ( 
        // $node: ident;
        // $stride: expr; //
        // $next_stride: expr; //
        // $stride_end: expr;
        $nibble_len: expr;
        $nibble: expr;
        $is_last_stride: expr; //
        $pfx: ident;
        // $pfx_net: expr; //
        // $pfx_len: expr; //
        $cur_i: expr;
        $level: expr;
        $self: ident;
        $enum: ident;
        $( $variant: ident; $len: expr ), *
    ) => {
        match std::mem::take($self.retrieve_node_mut($cur_i).unwrap()) {
            $( $enum::$variant(mut current_node) =>
            match current_node.eval_node_or_prefix_at(
                $nibble,
                $nibble_len,
                $self.strides.get(($level + 1) as usize),
                $is_last_stride,
            ) {
                NewNodeOrIndex::NewNode(n, bit_id) => {
                    $self.stats[$len].inc($level); // Stride3 logs to stats[0], Stride4 logs to stats[1], etc.
                    let i = $self.store_node(n);
                    current_node.ptr_vec.push(NodeId::new(bit_id, i));
                    current_node.ptr_vec.sort();
                    (Some(i), $enum::$variant(current_node))
                }
                NewNodeOrIndex::ExistingNode(i) => (Some(i), $enum::$variant(current_node)),
                NewNodeOrIndex::NewPrefix => {
                    let pfx_len = $pfx.len.clone();
                    let pfx_net = $pfx.net.clone();
                    let i = $self.store_prefix($pfx);
                    $self.stats[$len].inc_prefix_count($level);
                    current_node
                        .pfx_vec
                        .push(((pfx_net >> (AF::BITS - pfx_len) as usize), i));
                    current_node.pfx_vec.sort();
                    let _default_val = std::mem::replace(
                        $self.retrieve_node_mut($cur_i).unwrap(),
                        $enum::$variant(current_node),
                    );
                    return Ok(());
                }
                NewNodeOrIndex::ExistingPrefix(pfx_idx) => {
                    // ExitingPrefix is guaranteed to only happen at the last stride,
                    // so we can return from here.
                    // If we don't then we cannot move pfx.meta into the update_prefix_meta function,
                    // since the compiler can't figure out that it will happen only once.
                    $self.update_prefix_meta(pfx_idx, $pfx.meta)?;
                    let _default_val = std::mem::replace(
                        $self.retrieve_node_mut($cur_i).unwrap(),
                        $enum::$variant(current_node),
                    );
                    return Ok(());
                }
            } )*,
        }
    };
}
