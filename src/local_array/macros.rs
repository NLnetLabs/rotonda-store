#[macro_export]
// This macro expands into a match node {}
// with match arms for all SizedStrideNode::Stride[3-8]
// for use in insert()
#[doc(hidden)]
macro_rules! match_node_for_strides {
    (
        $self: ident;
        $nibble_len: expr;
        $nibble: expr;
        $is_last_stride: expr;
        $pfx: ident;
        $truncate_len: ident;
        $cur_i: expr;
        $level: expr;
        // $enum: ident;
        // The strides to generate match arms for,
        // $variant is the name of the enum varian (Stride[3..8]) and
        // $len is the index of the stats level, so 0..5
        $( $variant: ident; $stats_level: expr ), *
    ) => {
        match $self.retrieve_node_mut($cur_i)? {
            $(
            SizedStrideRefMut::$variant(node) => {
            let mut current_node = std::mem::take(node);
            match current_node.eval_node_or_prefix_at(
                $nibble,
                $nibble_len,
                StrideNodeId::dangerously_new_with_id_as_is($pfx.net, $truncate_len),
                $self.strides.get(($level + 1) as usize),
                $is_last_stride,
            ) {
                NewNodeOrIndex::NewNode(n) => {
                    // Stride3 logs to stats[0], Stride4 logs to stats[1], etc.
                    $self.stats[$stats_level].inc($level);

                    // get a new identifier for the node we're going to create.
                    let new_id = $self.store.acquire_new_node_id(($pfx.net, $truncate_len));

                    // store the node in the global store
                    let i = $self.store_node(new_id, n).unwrap();

                    // update ptrbitarr in the current node
                    $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));

                    Some(i)
                }
                NewNodeOrIndex::ExistingNode(i) => {
                    $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));
                    Some(i)
                },
                NewNodeOrIndex::NewPrefix(sort_id) => {
                    // acquire_new_prefix_id is deterministic, so we can use it
                    // in all threads concurrently.
                    println!("sort_id {}", sort_id);
                    let new_id = $self.store.acquire_new_prefix_id(&$pfx);
                    $self.stats[$stats_level].inc_prefix_count($level);

                    current_node.pfx_vec.insert(sort_id, new_id);

                    $self.store_prefix($pfx)?;
                    $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));

                    break Ok(());
                }
                NewNodeOrIndex::ExistingPrefix((pfx_idx, serial)) => {
                    // THE CRITICAL SECTION
                    //
                    // UPDATING EXISTING METADATA
                    //
                    // We're going through this section using Read-Copy-Update (RCU).
                    // 1. We're incrementing the serial number of the prefix in the local array
                    //    by one atomically.
                    // 2. We're reading the meta-data of the prefix from the global store.
                    // 3. We're updating a Copy of that meta-data with the our new meta-data.
                    // 4. We're writing the updated meta-data back to the global store as a new
                    //    new entry with the new serial number.
                    // 5. We're reading the serial number in the local array atomically, this
                    //    serial number could be either the old serial number plus one (we've
                    //    updated it). Or it could be a higher number.
                    // 6. If it's the old serial number plus one, write the node to the global
                    //    store and remove the prefix entry in the global store with the old
                    //    serial number.
                    // 7. If it's a higher number, we're repeating the Read-Copy-Update cycle.
                    //
                    // fetch_add returns the old serial number, not the result!
                    let mut old_serial = serial.fetch_add(1, Ordering::Acquire);
                    let new_serial = old_serial + 1;

                    if let Some(ref new_meta) = $pfx.meta {
                        // This needs to go in an unsafe block, probably.
                        $self.update_prefix_meta(*pfx_idx, new_serial, new_meta)?;

                        loop {
                            // Try updating the atomic serial number in the
                            // pfx_vec array of the current node
                            match serial.load(Ordering::Acquire) {
                                    // SUCCESS (Step 6) !
                                    // Nobody messed with our prefix meta-data in between us loading the
                                    // serial and creating the entry with that serial. Update the ptrbitarr
                                    // in the current node in the global store and be done with it.
                                    cur_serial if cur_serial == new_serial => {
                                        let pfx_idx_clone = pfx_idx.clone();
                                        $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));
                                        $self.store.remove_prefix(pfx_idx_clone.set_serial(old_serial));
                                        return Ok(());
                                    },
                                    // FAILURE (Step 7)
                                    // Some other thread messed it up. Try again by upping a newly-read serial once
                                    // more, reading the newly-current meta-data, updating it with our meta-data and
                                    // see if it works then. rince-repeat.
                                    newer_serial => {
                                        println!("contention for {:?} with serial {} -> {}", pfx_idx, old_serial, newer_serial);
                                        old_serial = serial.fetch_add(1, Ordering::Acquire);
                                        $self.store.retrieve_prefix(pfx_idx.set_serial(old_serial)).unwrap();
                                        $self.update_prefix_meta(*pfx_idx, newer_serial, &new_meta)?;
                                    }
                            };
                        }
                    };
                    // ExistingPrefix is guaranteed to only happen at the
                    // last stride, so we can return from here. If we don't
                    // then we cannot move pfx.meta into the
                    // update_prefix_meta function, since the compiler can't
                    // figure out that it will happen only once.
                    return Ok(());
                }
            }
            }
        )*,
        }
    };
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
                ) -> crate::local_array::node::StrideNodeId<AF> {
                    let id = crate::local_array::node::StrideNodeId::new_with_cleaned_id(addr_bits, len);
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
