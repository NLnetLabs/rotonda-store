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
        $truncate_len: ident; // the start of the length of this stride
        $stride_len: ident; // the length of this stride
        $cur_i: expr; // the id of the current node in this stride
        $level: expr;
        $back_off: expr;
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
        // right bit in the parent bitarry, second storing the node in the
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
            let mut local_retry_count = 0;
            loop {
                if let Some(current_node) = $self.store.retrieve_node_mut_with_guard($cur_i, $guard) {
                    match current_node {
                        $(
                            SizedStrideRefMut::$variant(current_node) => {
                                // eval_node_or_prefix_at mutates the node to reflect changes
                                // in the ptrbitarr & pfxbitarr.
                                match current_node.eval_node_or_prefix_at(
                                    $nibble,
                                    $nibble_len,
                                    // All the bits of the search prefix, but with a length set to
                                    // the start of the current stride.
                                    StrideNodeId::dangerously_new_with_id_as_is($pfx.net, $truncate_len),
                                    // the length of THIS stride
                                    $stride_len,
                                    // the length of the next stride
                                    $self.store.get_stride_sizes().get(($level + 1) as usize),
                                    $is_last_stride,
                                ) {
                                    (NewNodeOrIndex::NewNode(n), retry_count) => {
                                        // Stride3 logs to stats[0], Stride4 logs to stats[1], etc.
                                        // $self.stats[$stats_level].inc($level);

                                        // get a new identifier for the node we're going to create.
                                        let new_id = $self.store.acquire_new_node_id(($pfx.net, $truncate_len + $nibble_len));

                                        // store the new node in the global
                                        // store. It returns the created id
                                        // and the number of retries before
                                        // success.
                                        match $self.store.store_node(new_id, n, $guard) {
                                            Ok((node_id, s_retry_count)) => {
                                                break Ok((node_id, $acc_retry_count + s_retry_count + retry_count));
                                            },
                                            Err(err) => {
                                                break Err(err);
                                            }
                                        };
                                    }
                                    (NewNodeOrIndex::ExistingNode(node_id), retry_count) => {
                                        // $self.store.update_node($cur_i,SizedStrideRefMut::$variant(current_node));
                                        if log_enabled!(log::Level::Trace) {
                                            if local_retry_count > 0 {
                                                trace!("{} contention: Node already exists {}",
                                                std::thread::current().name().unwrap(), node_id
                                                )
                                            }
                                        }
                                        break Ok((node_id, $acc_retry_count + local_retry_count + retry_count))
                                    },
                                    (NewNodeOrIndex::NewPrefix, retry_count) => {
                                        return $self.store.upsert_prefix($pfx, $guard)
                                            .and_then(|r| Ok(r + $acc_retry_count + local_retry_count + retry_count))
                                        // Log
                                        // $self.stats[$stats_level].inc_prefix_count($level);
                                    }
                                    (NewNodeOrIndex::ExistingPrefix, retry_count) => {
                                        return $self.store.upsert_prefix($pfx, $guard)
                                            .and_then(|r| Ok(r + $acc_retry_count + local_retry_count + retry_count))
                                    }
                                }   // end of eval_node_or_prefix_at
                            }
                        )*,
                    }
                } else {
                    if log_enabled!(log::Level::Trace) {
                        trace!("{} contention: Retrying id {} from store l{}. attempt {}",
                                std::thread::current().name().unwrap(),
                                $cur_i,
                                $self.store.get_stride_sizes()[$level as usize],
                                local_retry_count
                            );
                    }
                    local_retry_count += 1;
                    // THIS IS A FAIRLY ARBITRARY NUMBER.
                    // We're giving up after a number of tries.
                    if local_retry_count >= 8 {
                        if log_enabled!(log::Level::Trace) {
                            trace!("{} contention: Max. retry count reached. Giving up for id {} from store l{} after {} attempts.", 
                                std::thread::current().name().unwrap(),
                                $cur_i,
                                $self.store.get_stride_sizes()[$level as usize],
                                local_retry_count
                            );
                        }
                        return Err(PrefixStoreError::NodeCreationMaxRetryError);
                    }
                    $back_off.spin();
                }
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
                ) -> $crate::local_array::node::StrideNodeId<AF> {
                    let id = $crate::local_array::node::StrideNodeId::new_with_cleaned_id(addr_bits, len);
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
