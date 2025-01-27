#[macro_export]
// This macro expands into a match node {}
// with match arms for all SizedStrideNode::Stride[3-8]
// for use in insert()
#[doc(hidden)]
macro_rules! insert_match {
    (
        $self: ident;
        $guard: ident;
        $nibble_len: expr;
        $nibble: expr; // nibble is a variable-length bitarray (1,2,4,8,etc)
        $is_last_stride: expr;
        $pfx: ident; // the whole search prefix
        $mui: ident; // the reccord holding the metadata
        // $update_path_selections: ident; // boolean indicate whether to update the path selections for this route
        $truncate_len: ident; // the start of the length of this stride
        $stride_len: ident; // the length of this stride
        $cur_i: expr; // the id of the current node in this stride
        $level: expr;
        $acc_retry_count: expr;
        // $enum: ident;
        // The strides to generate match arms for,
        // $variant is the name of the enum varian (Stride[3..8]) and
        // $len is the index of the stats level, so 0..5
        $( $variant: ident; $stats_level: expr ), *
    ) => {
        // Look up the current node in the store. This should never fail,
        // since we're starting at the root node and retrieve that. If a node
        // does not exist, it is created here. BUT, BUT, in a multi-threaded
        // context, one thread creating the node may be outpaced by a thread
        // reading the same node. Because the creation of a node actually
        // consists of two independent atomic operations (first setting the
        // right bit in the parent bitarray, second storing the node in the
        // store with the meta-data), a thread creating a new node may have
        // altered the parent bitarray, but not it didn't create the node
        // in the store yet. The reading thread, however, saw the bit in the
        // parent and wants to read the node in the store, but that doesn't
        // exist yet. In that case, the reader thread needs to try again
        // until it is actually created

        // This macro counts the number of retries and adds that to the
        // $acc_retry_count variable, to be used by the incorporating
        // function.
        {
            // this counts the number of retry_count for this loop only,
            // but ultimately we will return the accumulated count of all
            // retry_count from this macro.
            let local_retry_count = 0;
            // retrieve_node_mut updates the bitmap index if necessary.
            if let Some(current_node) = $self.retrieve_node_mut(
                $cur_i, $mui) {
                match current_node {
                    $(
                        SizedStrideRef::$variant(current_node) => {
                            // eval_node_or_prefix_at mutates the node to
                            // reflect changes in the ptrbitarr & pfxbitarr.
                            match current_node.eval_node_or_prefix_at(
                                $nibble,
                                $nibble_len,
                                // All the bits of the search prefix, but with
                                // a length set to the start of the current
                                // stride.
                                StrideNodeId::dangerously_new_with_id_as_is(
                                    $pfx.get_net(), $truncate_len),
                                // the length of THIS stride
                                $stride_len,
                                // the length of the next stride
                                $self
                                    .get_stride_sizes()
                                    .get(($level + 1) as usize),
                                $is_last_stride,
                            ) {
                                (NewNodeOrIndex::NewNode(n), retry_count) => {
                                    // Stride3 logs to stats[0], Stride4 logs
                                    // to stats[1], etc.
                                    // $self.stats[$stats_level].inc($level);

                                    // get a new identifier for the node we're
                                    // going to create.
                                    let new_id =
                                        StrideNodeId::new_with_cleaned_id(
                                            $pfx.get_net(),
                                            $truncate_len + $nibble_len
                                       );

                                    // store the new node in the in_memory
                                    // part of the RIB. It returns the created
                                    // id and the number of retries before
                                    // success.
                                    match $self.store_node(
                                        new_id,
                                        $mui, n
                                    ) {
                                        Ok((node_id, s_retry_count)) => {
                                            Ok((
                                                node_id,
                                                $acc_retry_count +
                                                s_retry_count +
                                                retry_count
                                            ))
                                        },
                                        Err(err) => {
                                            Err(err)
                                        }
                                    }
                                }
                                (NewNodeOrIndex::ExistingNode(node_id),
                                    retry_count
                                ) => {
                                    if log_enabled!(log::Level::Trace) {
                                        if local_retry_count > 0 {
                                            trace!("{} contention: Node \
                                                 already exists {}",
                                            std::thread::current()
                                                .name()
                                                .unwrap_or("unnamed-thread"),
                                                node_id
                                            )
                                        }
                                    }
                                    Ok((
                                        node_id,
                                        $acc_retry_count +
                                        local_retry_count +
                                        retry_count
                                    ))
                                },
                                (NewNodeOrIndex::NewPrefix, retry_count) => {
                                    $self.counters.inc_prefixes_count(
                                        $pfx.get_len()
                                    );
                                    break (
                                        $acc_retry_count +
                                        local_retry_count +
                                        retry_count,
                                        false
                                    )

                                },
                                (
                                    NewNodeOrIndex::ExistingPrefix,
                                     retry_count
                                 ) =>
                                     {
                                    break (
                                        $acc_retry_count +
                                        local_retry_count +
                                        retry_count,
                                        true
                                    )


                                }
                            }   // end of eval_node_or_prefix_at
                        }
                    )*,
                }
            } else {
                Err(PrefixStoreError::NodeCreationMaxRetryError)
            }
        }
    }
}

#[macro_export]
// This macro only works for stride with bitmaps that are <= u128,
// the ones with synthetic integers (U256, U512) don't have the trait
// implementations for left|right shift, counting ones etc.
#[doc(hidden)]
macro_rules! impl_primitive_atomic_stride {
    (
        $(
            $len: expr;
            $bits: expr;
            $pfxsize: ty;
            $atomicpfxsize: ty;
            $ptrsize: ty;
            $atomicptrsize: ty
        ),
    *) => {
        $(
            impl Stride for $pfxsize {
                type AtomicPfxSize = $atomicpfxsize;
                type AtomicPtrSize = $atomicptrsize;
                type PtrSize = $ptrsize;
                const BITS: u8 = $bits;
                const STRIDE_LEN: u8 = $len;

                fn get_bit_pos(nibble: u32, len: u8) -> $pfxsize {
                    trace!("nibble {}, len {}, BITS {}", nibble, len, <Self as Stride>::BITS);
                    1 << (
                            <Self as Stride>::BITS - ((1 << len) - 1) as u8
                            - nibble as u8 - 1
                    )
                }

                fn bit_pos_from_index(i: u8) -> $pfxsize {
                    <$pfxsize>::try_from(1).unwrap().rotate_right(1) >> i
                }

                fn ptr_bit_pos_from_index(i: u8) -> $ptrsize {
                    // trace!("pfx {} ptr {} strlen {}",
                    // <$pfxsize>::BITS, <$ptrsize>::BITS, Self::STRIDE_LEN);
                    <$ptrsize>::try_from(1).unwrap().rotate_right(1)
                        >> (i + 1)
                }

                fn cursor_from_bit_span(bs: BitSpan) -> u8 {
                    Self::get_bit_pos(bs.bits, bs.len)
                    .leading_zeros() as u8 - 1
                }

                fn ptr_range(
                    ptrbitarr: $ptrsize,
                    bs: BitSpan
                ) -> ($ptrsize, u8) {
                    let start: u8 = (bs.bits << (4 - bs.len)) as u8;
                    let stop: u8 = start + (1 << (4 - bs.len));
                    let mask: $ptrsize = (
                        (((1_u32 << (stop as u32 - start as u32)) - 1)
                             as u32
                    )
                        .rotate_right(stop as u32) >> 16)
                        .try_into()
                        .unwrap();
                    trace!("- mask      {:032b}", mask);
                    trace!("- ptrbitarr {:032b}", ptrbitarr);
                    trace!("- shl bitar {:032b}", ptrbitarr & mask);

                    // if ptrbitarr & mask == <$ptrsize>::zero() { panic!("stop"); }

                    (ptrbitarr & mask, start as u8)
                }

                // Ptrbitarr searches are only done in the last half of
                // the bitarray, in the len = S::STRIDE_LEN part. We need a
                // complete BitSpan still to figure when to stop.
                fn ptr_cursor_from_bit_span(bs: BitSpan) -> u8 {
                    let p = Self::get_bit_pos(bs.bits << (4 - bs.len), 4)
                        .leading_zeros() as u8;
                    trace!("bs in {:?}", bs);
                    trace!("pos {}", p);
                    p
                }

                fn get_bit_pos_as_u8(nibble: u32, len: u8) -> u8 {
                    1 << (
                            <Self as Stride>::BITS - ((1 << len) - 1) as u8
                            - nibble as u8 - 1
                    )
                }

                fn get_pfx_index(nibble: u32, len: u8)
                -> usize {
                    (Self::get_bit_pos(nibble, len).leading_zeros() - 1) as usize

                }

                fn get_ptr_index(_bitmap: $ptrsize, nibble: u32) -> usize {
                    (nibble as u16).into()
                }

                fn into_node_id<AF: AddressFamily>(
                    addr_bits: AF,
                    len: u8
                ) -> StrideNodeId<AF> {
                    let id = StrideNodeId::new_with_cleaned_id(addr_bits, len);
                    id
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
