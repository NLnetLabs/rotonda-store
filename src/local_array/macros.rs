#[macro_export]
// This macro expands into a match node {}
// with match arms for all SizedStrideNode::Stride[3-8]
// for use in insert()
#[doc(hidden)]
macro_rules! match_node_for_strides {
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
        // $enum: ident;
        // The strides to generate match arms for,
        // $variant is the name of the enum varian (Stride[3..8]) and
        // $len is the index of the stats level, so 0..5
        $( $variant: ident; $stats_level: expr ), *
    ) => {
        match $self.store.retrieve_node_mut_with_guard($cur_i, $guard).expect(
                format!(
                    "\x1b[91mCouldn't load id {} from store l{}\x1b[0m",
                    $cur_i,
                    $self.strides[$level as usize]
                ).as_str()) {
            $(
            SizedStrideRefMut::$variant(current_node) => {
            // let mut current_node = std::mem::take(node_store.get_mut(&node).unwrap().value_mut());
            // let cn = node_store.get(&node).expect(
            //     format!(
            //         "\x1b[91mCouldn't load id {} from store node{}\x1b[0m",
            //         $cur_i,
            //         $self.strides[$level as usize]
            //     ).as_str());
            // let mut current_node = cn;
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
                $self.strides.get(($level + 1) as usize),
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
                    // drop(node_store);
                    // let mut new_store = $self.store.get_stride_for_id_with_write_store(new_id).1;
                    let i = $self.store.store_node(new_id, n).unwrap();
                    // drop(new_store);
                        // drop(node_store);
                    // }
                    // else {
                        // i = $self.store.store_node_in_store(&mut StrideStore::$variant(node_store), new_id, n).unwrap();
                    // }
                    // let i = node_store.insert(new_id, n).unwrap();

                    // update ptrbitarr in the current node and move it back into the store
                    // node_store.alter(&$cur_i, |_, n| { n });
                    // let node_store = &mut $self.store.get_stride_for_id_with_write_store($cur_i).1;
                    // $self.store.update_node_in_store(node_store, $cur_i,SizedStrideNode::$variant(current_node));

                    Some(i)
                }
                NewNodeOrIndex::ExistingNode(i) => {
                    // $self.store.update_node($cur_i,SizedStrideRefMut::$variant(current_node));
                    Some(i)
                },
                NewNodeOrIndex::NewPrefix(sort_id) => {
                    // Log
                    // $self.stats[$stats_level].inc_prefix_count($level);

                    // THE CRITICAL SECTION
                    //
                    // CREATING A NEW PREFIX
                    //
                    // 1. Increment the serial number if the current value is
                    //    zero.
                    // 2. If step 1 is OK, Store the prefix with its metadata
                    //    in the global store.
                    // 3. Update ptrbitarr of the current node in the global
                    //    store.
                    // 4. If the result of Step 2 is not zero, go over the
                    //    procedure for updating the prefix.

                    // STEP 1
                    // acquire the Atomic Serial mutably from the local pfx_vec.
                    println!("get serial for node nr. {}", sort_id);
                    let serial = current_node.pfx_vec.get_serial_at(sort_id as usize);
                    // increment the serial number only if its zero right now.
                    let old_serial = serial.compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed);

                    match old_serial {
                        Ok(_) => {
                            // STEP 2
                            // Store the prefix in the global, well, store. The serial number for
                            // this prefix will be set to 1.
                            $self.store_prefix($pfx)?;
                            // STEP 3
                            // update the ptrbitarr bitarray in the current node in
                            // the global store.
                            // $self.store.update_node_in_store(&mut StrideWriteStore::$variant(node_store), $cur_i,SizedStrideNode::$variant(current_node));
                        }
                        // STEP 4
                        //
                        // This is basically the same code as the
                        // ExistingPrefix case, we're repeating the code here
                        // to avoid starting all over again with fetching the
                        // prefix node by node.
                        Err(newer_serial) => {
                            println!("contention while creating node");
                            // Somebody beat us to it. Try again with the new serial number.
                            // let mut old_serial = serial.fetch_add(1, Ordering::Acquire);
                            let new_serial = newer_serial + 1;
                            // No need to set a serial here, it's not going to be used without
                            // it being explicitly set.
                            let found_prefix_id = PrefixId::new($pfx.net, $pfx.len);

                            if let Some(ref new_meta) = $pfx.meta {

                                // RCU the prefix meta-data in the global store
                                $self.update_prefix_meta(found_prefix_id.set_serial(newer_serial), new_serial, new_meta)?;

                                loop {

                                    match serial.load(Ordering::Acquire) {
                                            1 => {
                                                panic!("So-called existing prefix {}/{} does not exist?", found_prefix_id.get_net().into_ipaddr(), found_prefix_id.get_len());
                                            },
                                            // SUCCESS (Step 6) !
                                            // Nobody messed with our prefix meta-data in between us loading the
                                            // serial and creating the entry with that serial. Update the ptrbitarr
                                            // in the current node in the global store and be done with it.
                                            cur_serial if cur_serial == new_serial => {
                                                let found_prefix_id_clone = found_prefix_id.clone();
                                                // $self.store.update_node_in_store(&mut StrideWriteStore::$variant(node_store), $cur_i,SizedStrideNode::$variant(current_node));
                                                // println!(
                                                //     "removing old prefix with serial {}...",
                                                //     newer_serial
                                                // );
                                                $self.store.remove_prefix(found_prefix_id_clone.set_serial(newer_serial));
                                                return Ok(());
                                            },
                                            // FAILURE (Step 7)
                                            // Some other thread messed it up. Try again by upping a newly-read serial once
                                            // more, reading the newly-current meta-data, updating it with our meta-data and
                                            // see if it works then. rinse-repeat.
                                            even_newer_serial => {
                                                println!("Contention for {:?} with serial {} -> {}", found_prefix_id, newer_serial, even_newer_serial);
                                                let old_serial = serial.fetch_add(1, Ordering::Acquire);
                                                $self.store.get_prefixes().get(&found_prefix_id.set_serial(old_serial));
                                                $self.update_prefix_meta(found_prefix_id, even_newer_serial, &new_meta)?;
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

                    break Ok(());
                }
                NewNodeOrIndex::ExistingPrefix(found_prefix_id, serial) => {
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
                    // Try updating the atomic serial number in the pfx_vec
                    // array of the current node fetch_add returns the old
                    // serial number, not the result!
                    let mut old_serial = serial.fetch_add(1, Ordering::Acquire);
                    let new_serial = old_serial + 1;

                    if let Some(ref new_meta) = $pfx.meta {

                        // RCU the prefix meta-data in the global store
                        $self.update_prefix_meta(found_prefix_id.set_serial(old_serial), new_serial, new_meta)?;

                        loop {

                            match serial.load(Ordering::Acquire) {
                                    1 => {
                                        panic!("So-called existing prefix {}/{} does not exist?", found_prefix_id.get_net().into_ipaddr(), found_prefix_id.get_len());
                                    },
                                    // SUCCESS (Step 6) !
                                    // Nobody messed with our prefix meta-data in between us loading the
                                    // serial and creating the entry with that serial. Update the ptrbitarr
                                    // in the current node in the global store and be done with it.
                                    cur_serial if cur_serial == new_serial => {
                                        let found_prefix_id_clone = found_prefix_id.clone();
                                        // current_node.pfx_vec.insert(pfx_vec_index, found_prefix_id_clone.set_serial(new_serial));
                                        // $self.store.update_node_in_store(&mut StrideWriteStore::$variant(node_store), $cur_i,SizedStrideNode::$variant(current_node));
                                        $self.store.remove_prefix(found_prefix_id_clone.set_serial(old_serial));
                                        // println!("current_node.pfx_vec {:?}", current_node.pfx_vec);
                                        return Ok(());
                                    },
                                    // FAILURE (Step 7)
                                    // Some other thread messed it up. Try again by upping a newly-read serial once
                                    // more, reading the newly-current meta-data, updating it with our meta-data and
                                    // see if it works then. rinse-repeat.
                                    newer_serial => {
                                        println!("Contention for {:?} with serial {} -> {}", found_prefix_id, old_serial, newer_serial);
                                        old_serial = serial.fetch_add(1, Ordering::Acquire);
                                        $self.store.get_prefixes().get(&found_prefix_id.set_serial(old_serial));
                                        $self.update_prefix_meta(found_prefix_id, newer_serial, &new_meta)?;
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
                    // bits_division: [u8; 10],
                    mut level: u8,
                    guard| {
                        // Aaaaand, this is all of our hashing function.
                        // I'll explain later.
                        let index = $id.get_id().0.dangerously_truncate_to_usize()
                            >> (AF::BITS - <Buckets as FamilyBuckets<AF>>::len_to_store_bits($id.get_id().1,level).unwrap());

                        // Read the node from the block pointed to by the
                        // Atomic pointer.
                        // let guard = &epoch::pin();
                        let this_node = unsafe {
                            &mut nodes.0.load(Ordering::SeqCst, guard).deref_mut()[index]
                        };
                        println!("this node {:?}", this_node);
                        match unsafe { this_node.assume_init_mut() } {
                            // No node exists, here
                            StoredNode::Empty => None,
                            // A node exists, but since we're not using perfect
                            // hashing everywhere, this may be very well a node
                            // we're not searching for, so check that.
                            StoredNode::NodeWithRef((node_id, node, node_set)) => {
                                println!("found {} in level {}", node, level);
                                println!("search id {}", $id);
                                println!("found id {}", node_id);
                                if &$id == node_id {
                                    // YES, It's the one we're looking for!
                                    return Some(SizedStrideRef::$stride(node));
                                };
                                // Meh, it's not, but we can a go to the next level
                                // and see if it lives there.
                                level += 1;
                                match <Buckets as FamilyBuckets<AF>>::len_to_store_bits($id.get_id().1, level) {
                                    // on to the next level!
                                    Some(next_bit_shift) if next_bit_shift > &0 => {
                                        (search_level.f)(
                                            search_level,
                                            node_set,
                                            // new_node,
                                            // bits_division,
                                            level,
                                            // result_node,
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
                        // Aaaaand, this is all of our hashing function.
                        // I'll explain later.
                        let index = $id.get_id().0.dangerously_truncate_to_usize()
                            >> (AF::BITS - <Buckets as FamilyBuckets<AF>>::len_to_store_bits($id.get_id().1, level).unwrap());

                        // Read the node from the block pointed to by the
                        // Atomic pointer.
                        // let guard = &epoch::pin();
                        let this_node = unsafe {
                            &mut nodes.0.load(Ordering::SeqCst, guard).deref_mut()[index]
                        };
                        println!("this node {:?}", this_node);
                        match unsafe { this_node.assume_init_mut() } {
                            // No node exists, here
                            StoredNode::Empty => None,
                            // A node exists, but since we're not using perfect
                            // hashing everywhere, this may be very well a node
                            // we're not searching for, so check that.
                            StoredNode::NodeWithRef((node_id, node, node_set)) => {
                                println!("found {} in level {}", node, level);
                                println!("search id {}", $id);
                                println!("found id {}", node_id);
                                if &$id == node_id {
                                    // YES, It's the one we're looking for!
                                    return Some(SizedStrideRefMut::$stride(node));
                                };
                                // Meh, it's not, but we can a go to the next level
                                // and see if it lives there.
                                level += 1;
                                match <Buckets as FamilyBuckets<AF>>::len_to_store_bits($id.get_id().1, level) {
                                    // on to the next level!
                                    Some(next_bit_shift) if next_bit_shift > &0 => {
                                        (search_level.f)(
                                            search_level,
                                            node_set,
                                            // new_node,
                                            // bits_division,
                                            level,
                                            // result_node,
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
                    //  bits_division: [u8; 10],
                     mut level: u8| {
                    let index = $id.get_id().0.dangerously_truncate_to_usize()
                        >> (AF::BITS - <Buckets as FamilyBuckets<AF>>::len_to_store_bits($id.get_id().1, level).unwrap());
                    println!("{:032b}", $id.get_id().0.dangerously_truncate_to_usize());
                    println!("id {:?}", $id.get_id());
                    println!("calculated index {}", index);
                    println!("level {}", level);
                    println!("bits_division {}", <Buckets as FamilyBuckets<AF>>::len_to_store_bits($id.get_id().1,level).unwrap());
                    let guard = &epoch::pin();
                    let mut unwrapped_nodes = nodes.0.load(Ordering::SeqCst, guard);
                    println!("nodes {:?}", unsafe { unwrapped_nodes.deref_mut().len() });
                    let node_ref =
                        unsafe { &mut unwrapped_nodes.deref_mut()[index] };
                    match unsafe { node_ref.assume_init_mut() } {
                        // No node exists, so we crate one here.
                        StoredNode::Empty => {
                            println!("Empty node found, creating new node {}", $id);
                            std::mem::swap(
                                node_ref,
                                &mut MaybeUninit::new(StoredNode::NodeWithRef((
                                    $id,
                                    new_node,
                                    NodeSet::init((1 << <Buckets as FamilyBuckets<AF>>::len_to_store_bits($id.get_id().1, level + 1).unwrap()) as usize),
                                ))),
                            );
                            // ABA Baby!
                            match nodes.0.compare_exchange(
                                unwrapped_nodes,
                                unwrapped_nodes,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                guard,
                            ) {
                                Ok(_) => Some($id),
                                Err(_) => {
                                    // TODO: This needs some kind of backoff,
                                    // I guess.
                                    loop {
                                        println!("contention while creating node {}", $id);
                                        match nodes.0.compare_exchange(
                                            unwrapped_nodes,
                                            unwrapped_nodes,
                                            Ordering::SeqCst,
                                            Ordering::SeqCst,
                                            guard,
                                        ) {
                                            Ok(_) => { return Some($id); },
                                            Err(_) => {}
                                        };
                                    };
                                },
                            };
                            Some($id)
                        }
                        // A node exists, since `store_node` only creates new
                        // nodes, we will silently abort insertion if the
                        // specified node already exists.
                        StoredNode::NodeWithRef((node_id, _node, node_set)) => {
                            println!("node here exists {:?}", _node);
                            println!("node_id {:?}", node_id.get_id());
                            println!("node_id {:032b}", node_id.get_id().0);
                            println!("id {}", $id);
                            println!("     id {:032b}", $id.get_id().0);
                            if $id == *node_id {
                                println!("found node {}, stop", $id);
                                // Node already exists, nothing to do
                                return Some($id);
                            };
                            level += 1;
                            match <Buckets as FamilyBuckets<AF>>::len_to_store_bits($id.get_id().1, level) {
                                // on to the next level!
                                Some(next_bit_shift) if next_bit_shift > &0 => {
                                    (search_level.f)(
                                        search_level,
                                        node_set,
                                        new_node,
                                        // bits_division,
                                        level,
                                    )
                                }
                                // There's no next level!
                                _ => panic!("out of storage levels, current level is {}", level),
                            }
                        }
                    }
                }
            }

        )*
    };
}
