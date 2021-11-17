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
                $self.strides.get(($level + 1) as usize),
                $is_last_stride,
            ) {
                NewNodeOrIndex::NewNode(n, bit_id) => {
                    $self.stats[$stats_level].inc($level); // Stride3 logs to stats[0], Stride4 logs to stats[1], etc.
                    // let new_id = Store::NodeType::new(&bit_id,&$cur_i.get_part());
                    let new_id = $self.store.acquire_new_node_id(bit_id, $self.strides[($level + 1) as usize]);
                    // current_node.ptr_vec.push(new_id);
                    current_node.ptr_vec.insert(new_id);
                    // current_node.ptr_vec.sort();
                    let i = $self.store_node(Some(new_id), n).unwrap();

                    $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));

                    Some(i)
                }
                NewNodeOrIndex::ExistingNode(i) => {
                    $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));

                    // let _default_val = std::mem::replace(
                    //     $self.retrieve_node_mut($cur_i).unwrap(),
                    //     SizedStrideNode::$variant(current_node));
                    Some(i)
                },
                NewNodeOrIndex::NewPrefix => {

                    // let pfx_len = $pfx.len.clone();
                    // let pfx_net = $pfx.net.clone();
                    // let i = $self.store_prefix($pfx)?;
                    // Construct the SortKey by default from the nibble and
                    // nibble_len, so that nibble_len determines the base
                    // position (2^nibble_len) and then nibble is the offset
                    // from the base position.
                    let new_id = $self.store.acquire_new_prefix_id(&((1 << $nibble_len) + $nibble as u16).into(), &$pfx);
                    $self.stats[$stats_level].inc_prefix_count($level);

                    current_node
                        .pfx_vec
                        .insert(new_id);
                    // current_node.pfx_vec.sort();

                    $self.store_prefix($pfx)?;
                    $self.store.update_node($cur_i,SizedStrideNode::$variant(current_node));

                    // let _default_val = std::mem::replace(
                    //     $self.retrieve_node_mut($cur_i).unwrap(),
                    //     SizedStrideNode::$variant(current_node),
                    // );
                    return Ok(());
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
                    return Ok(());
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

                fn get_pfx_index(bitmap: $pfxsize, nibble: u32, len: u8) 
                -> usize {
                    (
                        bitmap >> (
                            (
                                <Self as Stride>::BITS - ((1 << len) - 1) as u8 
                                - nibble as u8 - 1
                            ) as usize
                        )
                    ).count_ones() as usize
                    - 1
                }
                fn get_ptr_index(bitmap: $ptrsize, nibble: u32) -> usize {
                    (
                        bitmap >> (
                        (<Self as Stride>::BITS >> 1) - nibble as u8 - 1) as usize
                    ).count_ones() as usize
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
