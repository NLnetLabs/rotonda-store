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
                NewNodeOrIndex::NewNode(n, bit_id) => {
                    // Stride3 logs to stats[0], Stride4 logs to stats[1], etc.
                    $self.stats[$stats_level].inc($level);

                    // get a new identifier for the node we're going to create.
                    let new_id = $self.store.acquire_new_node_id(($pfx.net, $truncate_len));

                    // store the node identifier in the local ptr array
                    // current_node.ptr_vec.insert(bit_id, new_id);

                    // store the node in the global store
                    let i = $self.store_node(new_id, n).unwrap();

                    // update ptrbitarr in the current node
                    $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));

                    Some(i)
                }
                NewNodeOrIndex::ExistingNode(i) => {
                    $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));
                    println!("Existing node {:?}", i);
                    // let _default_val = std::mem::replace(
                    //     $self.retrieve_node_mut($cur_i).unwrap(),
                    //     SizedStrideNode::$variant(current_node));
                    Some(i)
                },
                NewNodeOrIndex::NewPrefix(sort_id) => {

                    // let pfx_len = $pfx.len.clone();
                    // let pfx_net = $pfx.net.clone();
                    // let i = $self.store_prefix($pfx)?;
                    // Construct the SortKey by default from the nibble and
                    // nibble_len, so that nibble_len determines the base
                    // position (2^nibble_len) and then nibble is the offset
                    // from the base position.
                    // let new_id = $self.store.acquire_new_prefix_id(&((1 << ($nibble_len - 1)) + $nibble as u16).into());
                    println!("np#1");
                    let new_id = $self.store.acquire_new_prefix_id(&$pfx);
                    $self.stats[$stats_level].inc_prefix_count($level);

                    println!("np#2");
                    current_node
                        .pfx_vec
                        .insert(sort_id, new_id);
                    // current_node.pfx_vec.sort();


                    println!("np#3 {}", $pfx);
                    $self.store_prefix($pfx)?;

                    println!("np#4");
                    $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));

                    println!("np#5 {}", $cur_i);
                    // let _default_val = std::mem::replace(
                    //     $self.retrieve_node_mut($cur_i).unwrap(),
                    //     SizedStrideNode::$variant(current_node),
                    // );
                    break Ok(());
                }
                NewNodeOrIndex::ExistingPrefix(pfx_idx) => {
                    // ExistingPrefix is guaranteed to only happen at the last stride,
                    // so we can return from here.
                    // If we don't then we cannot move pfx.meta into the update_prefix_meta function,
                    // since the compiler can't figure out that it will happen only once.
                    if let Some(meta) = $pfx.meta { $self.update_prefix_meta(pfx_idx, meta)? };
                    $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));

                    // let _default_val = std::mem::replace(
                    //     $self.retrieve_node_mut($cur_i).unwrap(),
                    //     // expands into SizedStrideNode::Stride[3-8](current_node)
                    //     SizedStrideNode::$variant(current_node),
                    // );
                    break Ok(());
                }
            }
            }
        )*,
        }
    };
}

//     example expansion for Stride4:

//     SizedStrideNode::Stride4(mut current_node) => match current_node
//         .eval_node_or_prefix_at(
//             nibble,
//             nibble_len,
//             // No, next_stride.is_none does *not* mean that it's the last stride
//             // There may very well be a Some(next_stride), next_stride goes all the
//             // way to the end of the length of the network address space (like 32 bits for IPv4 etc),
//             // whereas the last stride stops at the end of the prefix length.
//             // `is_last_stride` is an indicator for the upsert function to write the prefix in the
//             // node's vec.
//             next_stride,
//             pfx_len <= stride_end,
//         ) {
//         NewNodeOrIndex::NewNode(n, bit_id) => {
//             self.stats[1].inc(level); // [1] here corresponds to stats for Stride4
//             let i = self.store_node(n);
//             current_node.ptr_vec.push(NodeId::new(bit_id, i));
//             current_node.ptr_vec.sort();
//              let _default_val = std::mem::replace(
//                 self.retrieve_node_mut(cur_i).unwrap(),
//                 SizedStrideNode::Stride4(current_node),
//             );
//             Some(i)
//         }
//         NewNodeOrIndex::ExistingNode(i) => {
//              let _default_val = std::mem::replace(
//                 self.retrieve_node_mut(cur_i).unwrap(),
//                 SizedStrideNode::Stride4(current_node),
//             );
//             Some(i)
//         }
//         NewNodeOrIndex::NewPrefix => {
//             let i = self.store_prefix(pfx);
//             self.stats[1].inc_prefix_count(level);
//             current_node
//                 .pfx_vec
//                 .push(((pfx_net >> (AF::BITS - pfx_len) as usize), i));
//             current_node.pfx_vec.sort();
//             let _default_val = std::mem::replace(
//                 self.retrieve_node_mut(cur_i).unwrap(),
//                 SizedStrideNode::Stride4(current_node),
//             );
//             return Ok(());
//         }
//         NewNodeOrIndex::ExistingPrefix(pfx_idx) => {
//             // ExitingPrefix is guaranteed to only happen at the last stride,
//             // so we can return from here.
//             // If we don't then we cannot move pfx.meta into the update_prefix_meta function,
//             // since the compiler can't figure out that it will happen only once.
//             self.update_prefix_meta(pfx_idx, pfx.meta)?;
//             let _default_val = std::mem::replace(
//                 self.retrieve_node_mut(cur_i).unwrap(),
//                 SizedStrideNode::Stride4(current_node),
//             );
//             return Ok(());
//         }
//     },

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
                    // (
                    //     bitmap >> (
                    //         (
                    //             <Self as Stride>::BITS - ((1 << len) - 1) as u8
                    //             - nibble as u8 - 1
                    //         ) as usize
                    //     )
                    // ).count_ones() as usize
                    // - 1


                    (Self::get_bit_pos(nibble, len).leading_zeros() - 1) as usize

                }
                fn get_ptr_index(_bitmap: $ptrsize, nibble: u32) -> usize {
                    // (
                    //     bitmap >> (
                    //     (<Self as Stride>::BITS >> 1) - nibble as u8 - 1) as usize
                    // ).count_ones() as usize
                    // - 1
                    // ((<Self as Stride>::BITS >> 1) - nibble as u8 - 1) as usize
                    (nibble as u16).into()
                }

                fn into_node_id<AF: AddressFamily>(
                    addr_bits: AF,
                    len: u8
                ) -> crate::local_array::node::StrideNodeId<AF> {
                    let id = crate::local_array::node::StrideNodeId::new_with_cleaned_id(addr_bits, len);
                    println!("search {}/{}", id.get_id().0.into_ipaddr(), len);
                    println!("sub-search {} {}/{}", id, id.get_id().0.into_ipaddr(), len);
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
