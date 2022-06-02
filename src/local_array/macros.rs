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
        $i: expr;
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
                                NewNodeOrIndex::NewNode(n) => {
                                    // Stride3 logs to stats[0], Stride4 logs to stats[1], etc.
                                    // $self.stats[$stats_level].inc($level);

                                    // get a new identifier for the node we're going to create.
                                    let new_id = $self.store.acquire_new_node_id(($pfx.net, $truncate_len + $nibble_len));

                                    // store the new node in the global store
                                    // let i: StrideNodeId<Store::AF>;
                                    // if $self.strides[($level + 1) as usize] != $stride_len {
                                    // store_node may return None, which means that another thread
                                    // is busy creating the node.
                                    match $self.store.store_node(new_id, n) {
                                        Ok(node) => {
                                            break Ok(node);
                                        },
                                        Err(err) => {
                                            if log_enabled!(log::Level::Warn) {
                                                warn!("{} backing off {} -> next id {}",
                                                        std::thread::current().name().unwrap(),
                                                        $cur_i,
                                                        new_id
                                                    )
                                            }
                                            warn!("{} c_n2 {} {:?}", 
                                                std::thread::current().name().unwrap(), 
                                                new_id, 
                                                $self.store.retrieve_node_mut_with_guard(new_id, $guard)
                                            );
                                            break Err(err);
                                        }
                                    };
                                    // break $self.store.store_node(new_id, n)
                                }
                                NewNodeOrIndex::ExistingNode(i) => {
                                    // $self.store.update_node($cur_i,SizedStrideRefMut::$variant(current_node));
                                    if $i > 0 { 
                                        warn!("{} existing node {}", std::thread::current().name().unwrap(), i);
                                    }
                                    break Ok(i)
                                },
                                NewNodeOrIndex::NewPrefix => {
                                    return $self.store.upsert_prefix($pfx)
                                    // Log
                                    // $self.stats[$stats_level].inc_prefix_count($level);
                                }
                                NewNodeOrIndex::ExistingPrefix => {
                                    return $self.store.upsert_prefix($pfx)
                                }
                            }   // end of eval_node_or_prefix_at
                        }
                    )*,
                }
            } else {
                if log_enabled!(log::Level::Warn) {
                    warn!("{} Couldn't load id {} from store l{}. Trying again.",
                            std::thread::current().name().unwrap(),
                            $cur_i,
                            $self.store.get_stride_sizes()[$level as usize]);
                }
                $i += 1;
                // THIS IS A FAIRLY ARBITRARY NUMBER.
                // We're giving up after a number of tries.
                if $i >= 3 {
                    warn!("STOP LOOPING {}", $cur_i);
                    return Err(
                        Box::new(
                            crate::local_array::store::errors::PrefixStoreError::NodeCreationMaxRetryError
                        )
                    );
                } 
                $back_off.spin();
            }
            // std::thread::sleep(std::time::Duration::from_millis(1000));
            // warn!("{} loop {} {:?}", std::thread::current().name().unwrap(), $cur_i, $self.store.retrieve_node_mut_with_guard($cur_i, $guard));
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

#[macro_export]
#[doc(hidden)]
macro_rules! impl_search_level {
    (
        $(
            $stride: ident;
            $id: ident;
        ),
    * ) => {
        $(
            SearchLevel {
                f: &|search_level: &SearchLevel<AF, $stride>,
                    nodes,
                    mut level: u8,
                    guard| {
                        // HASHING FUNCTION
                        let index = Self::hash_node_id($id, level);

                        // Read the node from the block pointed to by the
                        // Atomic pointer.
                        let this_node = unsafe {
                            &mut nodes.0.load(Ordering::SeqCst, guard).deref_mut()[index]
                        };
                        // trace!("this node {:?}", this_node);
                        match unsafe { this_node.assume_init_ref().load(Ordering::Acquire, guard).deref() } {
                            // No node exists, here
                            StoredNode::Empty => None,
                            // A node exists, but since we're not using perfect
                            // hashing everywhere, this may be very well a node
                            // we're not searching for, so check that.
                            StoredNode::NodeWithRef((node_id, node, node_set)) => {
                                // trace!("found {} in level {}", node, level);
                                // trace!("search id {}", $id);
                                // trace!("found id {}", node_id);
                                if $id == *node_id {
                                    // YES, It's the one we're looking for!
                                    return Some(SizedStrideRef::$stride(&node));
                                };
                                // Meh, it's not, but we can a go to the next level
                                // and see if it lives there.
                                level += 1;
                                match <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level) {
                                    // on to the next level!
                                    next_bit_shift if next_bit_shift > 0 => {
                                        (search_level.f)(
                                            search_level,
                                            &node_set,
                                            level,
                                            guard,
                                        )
                                    }
                                    // There's no next level, we found nothing.
                                    _ => None,
                                }
                            }
                        }
                    }
            }
        )*
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! impl_search_level_mut {
    (
        $(
            $stride: ident;
            $id: ident;
        ),
    * ) => {
        $(
            SearchLevel {
                f: &|search_level: &SearchLevel<AF, $stride>,
                    nodes,
                    // bits_division: [u8; 10],
                    mut level: u8,
                    guard| {
                        // HASHING FUNCTION
                        let index = Self::hash_node_id($id, level);

                        // Read the node from the block pointed to by the
                        // Atomic pointer.
                        let this_node = unsafe {
                            &mut nodes.0.load(Ordering::Acquire, guard).deref_mut()[index]
                        };
                        match unsafe { this_node.assume_init_mut().load(Ordering::Acquire, guard).deref_mut() } {
                            // No node exists, here
                            StoredNode::Empty => None,
                            // A node exists, but since we're not using perfect
                            // hashing everywhere, this may be very well a node
                            // we're not searching for, so check that.
                            StoredNode::NodeWithRef((node_id, node, node_set)) => {
                                // trace!("found {} in level {}", node, level);
                                // trace!("search id {}", $id);
                                // trace!("found id {}", node_id);
                                if &$id == node_id {
                                    // YES, It's the one we're looking for!
                                    return Some(SizedStrideRefMut::$stride(node));
                                };
                                // Meh, it's not, but we can a go to the next level
                                // and see if it lives there.
                                level += 1;
                                match <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level) {
                                    // on to the next level!
                                    next_bit_shift if next_bit_shift > 0 => {
                                        (search_level.f)(
                                            search_level,
                                            node_set,
                                            level,
                                            guard,
                                        )
                                    }
                                    // There's no next level, we found nothing.
                                    _ => None,
                                }
                            }
                        }
                    }
            }
        )*
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! impl_write_level {
    (
        $(
            $stride: ident;
            $id: ident;
        ),
    * ) => {
        $(
            SearchLevel {
                f: &|search_level: &SearchLevel<AF, $stride>,
                     nodes,
                     new_node: TreeBitMapNode<AF, $stride>,
                     mut level: u8| {
                    let this_level = <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level);
                    trace!("{:032b}", $id.get_id().0);
                    trace!("id {:?}", $id.get_id());

                    // HASHING FUNCTION
                    let index = Self::hash_node_id($id, level);
                    let guard = &epoch::pin();
                    let unwrapped_nodes = nodes.0.load(Ordering::Acquire, guard);
                    // trace!("nodes {:?}", unsafe { unwrapped_nodes.deref_mut().len() });
                    match unwrapped_nodes.is_null() {
                        false => {
                            trace!("unwrapped_nodes {:?}", unwrapped_nodes);
                            let node_ref =
                                unsafe { unwrapped_nodes.deref()[index].assume_init_ref() };
                            let n_r = node_ref.load(Ordering::Acquire, guard);
                            match n_r.is_null() {
                                false => {
                                    match unsafe { n_r.deref() } {
                                        // No node exists, so we create one here.
                                        StoredNode::Empty => {
                                            if log_enabled!(log::Level::Debug) {
                                                debug!("{} Empty node found, creating new node {} len{} lvl{}", 
                                                    std::thread::current().name().unwrap(), 
                                                    $id, $id.get_id().1, level + 1);
                                            }
                                            let next_level = <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level + 1);
                                            trace!("next level {}", next_level);
                                            trace!("creating {} nodes", if next_level >= this_level { 1 << (next_level - this_level) } else { 1 });
                                            let node_set = if next_level > 0 { NodeSet::init((1 << (next_level - this_level)) as usize ) } else { NodeSet(Atomic::null()) };
                                            match node_ref.compare_exchange(
                                                n_r,
                                                Owned::new(StoredNode::NodeWithRef((
                                                    $id,
                                                    new_node,
                                                    node_set,
                                                ))),
                                                Ordering::AcqRel,
                                                Ordering::Acquire,
                                                guard
                                            ) {
                                                Ok(_) => {
                                                    if log_enabled!(log::Level::Debug) {
                                                        debug!("{} created node {}", std::thread::current().name().unwrap(), $id);
                                                    }
                                                    return Some($id);
                                                },
                                                Err(_) => {
                                                    if log_enabled!(log::Level::Warn) {
                                                        warn!(
                                                            "{} failed to create node {}. Someone is busy creating it.",
                                                                std::thread::current().name().unwrap(), 
                                                                $id
                                                        );
                                                    }
                                                    error!("failed to create node {}. Someone is busy creating it.", $id);
                                                    return None;
                                                }
                                            };
                                        }
                                        // A node exists, might be ours, might be another one
                                        StoredNode::NodeWithRef((node_id, _node, node_set)) => {
                                            if log_enabled!(log::Level::Warn) {
                                                warn!("
                                                    {} node here exists {:?}", 
                                                        std::thread::current().name().unwrap(), 
                                                        node_id
                                                    );
                                                }
                                            trace!("node_id {:?}", node_id.get_id());
                                            trace!("node_id {:032b}", node_id.get_id().0);
                                            trace!("id {}", $id);
                                            trace!("     id {:032b}", $id.get_id().0);
                                            // See if somebody beat us to creating our node
                                            // already, if so, we're done. Nodes do not
                                            // carry meta-data (they just "exist"), so we
                                            // don't have to update anything, just return it.
                                            if $id == *node_id {
                                                // yes, it exists
                                                if log_enabled!(log::Level::Warn) {
                                                    warn!(
                                                        "{} node {} already created.", 
                                                        std::thread::current().name().unwrap(), $id
                                                    );
                                                }
                                                return Some($id);
                                            } else {
                                                // it's not "our" node, make a (recursive)
                                                // call to create it.
                                                level += 1;
                                                trace!("Collision with node_id {}, move to next level: {} len{} next_lvl{} index {}", node_id, $id, $id.get_id().1, level, index);
                                                return match <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level) {
                                                    // on to the next level!
                                                    next_bit_shift if next_bit_shift > 0 => {
                                                        (search_level.f)(
                                                            search_level,
                                                            node_set,
                                                            new_node,
                                                            level,
                                                        )
                                                    }
                                                    // There's no next level!
                                                    _ => panic!("out of storage levels, current level is {}", level),
                                                }
                                            }
                                        }
                                    }
                                },
                                true => {
                                    return None;
                                }
                            };
                        },
                        true => { return None; }
                    }

                }
            }

        )*
    };
}
