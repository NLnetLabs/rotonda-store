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

                    // Read the node from the block pointed to by the Atomic
                    // pointer.
                    let stored_node = unsafe {
                        &mut nodes.0.load(Ordering::SeqCst, guard).deref()[index].assume_init_ref()
                    };
                    let this_node = stored_node.load(Ordering::Acquire, guard);

                    match this_node.is_null() {
                        true => None,
                        false => {
                            let StoredNode { node_id, node, node_set, .. } = unsafe { this_node.deref() };
                            if $id == *node_id {
                                // YES, It's the one we're looking for!
                                return Some(SizedStrideRef::$stride(&node));
                            };
                            // Meh, it's not, but we can a go to the next
                            // level and see if it lives there.
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
macro_rules! impl_search_level_for_mui {
    (
        $(
            $stride: ident;
            $id: ident;
            $mui: ident;
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

                    // Read the node from the block pointed to by the Atomic
                    // pointer.
                    let stored_node = unsafe {
                        &mut nodes.0.load(Ordering::SeqCst, guard).deref()[index].assume_init_ref()
                    };
                    let this_node = stored_node.load(Ordering::Acquire, guard);

                    match this_node.is_null() {
                        true => None,
                        false => {
                            let StoredNode { node_id, node, node_set, .. } = unsafe {
                                this_node.deref()
                            };

                            // early return if the mui is not in the index
                            // stored in this node, meaning the mui does not
                            // appear anywhere in the sub-tree formed from
                            // this node.
                            let bmin: &RoaringBitmap = unsafe {
                                node_set.1.load(Ordering::Acquire, guard).deref()
                            };
                            if !bmin.contains($mui) {
                                return None;
                            }

                            if $id == *node_id {
                                // YES, It's the one we're looking for!
                                return Some(SizedStrideRef::$stride(&node));
                            };
                            // Meh, it's not, but we can a go to the next
                            // level and see if it lives there.
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
macro_rules! retrieve_node_mut_with_guard_closure {
    (
        $(
            $stride: ident;
            $id: ident;
            $multi_uniq_id: ident;
        ),
    * ) => {$(
        SearchLevel {
            f: &|
                search_level: &SearchLevel<AF, $stride>,
                nodes,
                mut level: u8,
                guard
            | {
                // HASHING FUNCTION
                let index = Self::hash_node_id($id, level);

                // Read the node from the block pointed to by the Atomic
                // pointer.
                assert!(unsafe { nodes.0.load(Ordering::SeqCst, guard).deref().get(index).is_some() } );
                let stored_node = unsafe {
                    &mut nodes.0.load(Ordering::SeqCst, guard).deref()[index].assume_init_ref()
                };
                let this_node = stored_node.load(Ordering::Acquire, guard);

                match this_node.is_null() {
                    true => None,
                    false => {
                        let StoredNode { node_id, node, node_set } = unsafe { this_node.deref() };
                        if $id == *node_id {
                            // YES, It's the one we're looking for!

                            // Update the rbm_index in this node with the
                            // multi_uniq_id that the caller specified. This
                            // is the only atomic operation we need to do
                            // here. The NodeSet that the index is attached
                            // to, does not need to be written to, it's part
                            // of a trie, so it just needs to "exist" (and it
                            // already does).
                            let retry_count = node_set.update_rbm_index(
                                $multi_uniq_id, guard
                            ).ok();

                            trace!("Retry_count rbm index {:?}", retry_count);
                            trace!("add multi uniq id to bitmap index {} for node {}", $multi_uniq_id, node);
                            return Some(SizedStrideRef::$stride(node));
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
    )*};
}

#[macro_export]
#[doc(hidden)]
macro_rules! store_node_closure {
    (
        $(
            $stride: ident;
            $id: ident;
            // $multi_uniq_id: ident;
            $guard: ident;
            $back_off: ident;
        ),
    *) => {
        $(
        SearchLevel {
            f: &|
            search_level: &SearchLevel<AF, $stride>,
            nodes,
            new_node: TreeBitMapNode<AF, $stride>,
            multi_uniq_id: u32,
            mut level: u8,
            mut retry_count: u32| {
                println!("-");
                let this_level = <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level);
                trace!("{:032b}", $id.get_id().0);
                trace!("id {:?}", $id.get_id());
                trace!("multi_uniq_id {}", multi_uniq_id);

                std::sync::atomic::fence(Ordering::SeqCst);
                // HASHING FUNCTION
                let index = Self::hash_node_id($id, level);
                let stored_nodes = nodes.0.load(Ordering::Acquire, $guard);

                match stored_nodes.is_null() {
                    false => {
                        print!("NODE HERE: ");
                        assert!(unsafe { stored_nodes.deref().get(index).is_some() });
                        let node_ref =
                            unsafe { stored_nodes.deref()[index].assume_init_ref() };
                        println!("success");
                        let stored_node = node_ref.load(Ordering::Acquire, $guard);

                        match stored_node.is_null() {
                            true => {
                                // No node exists, so we create one here.
                                let next_level = <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level + 1);

                                if log_enabled!(log::Level::Trace) {
                                    trace!("Empty node found, creating new node {} len{} lvl{}",
                                        $id, $id.get_id().1, level + 1
                                    );
                                    trace!("Next level {}",
                                        next_level
                                    );
                                    trace!("Creating space for {} nodes",
                                        if next_level >= this_level { 1 << (next_level - this_level) } else { 1 }
                                    );
                                }

                                trace!("multi uniq id {}", multi_uniq_id);

                                let node_set = if next_level > 0 {
                                    NodeSet::init((1 << (next_level - this_level)) as usize )
                                } else { NodeSet(
                                    Atomic::null(), nodes.1.load(Ordering::Acquire, $guard).into()) };

                                // Update the rbm_index in this node with the
                                // multi_uniq_id that the caller specified. We're
                                // doing this independently from setting the
                                // NodeSet atomically. If we would have done this
                                // in one Atomic CAS operation we would have to
                                // clone the NodeSet. Now we only have to clone
                                // the rbm_index itself. Furthermore, these
                                // operations can be (semi-)independent. Two
                                // out-of-order things can happen:
                                // 1. The rbm_index storing and the NodeSet
                                // storing get interjected with rbm_index value
                                // from another thread. In that case the whole
                                // NodeSet storing operation fails and is retried
                                // with a newly acquired value for both the
                                // rbm_index and the NodeSet.
                                // 2. The rmb_index storing operation succeeds,
                                // but the NodeSet storing operation fails,
                                // because the contention retries hit the
                                // threshold. In that case a false positive is
                                // stored in the index, which leads to more
                                // in-vain searching, but not to data corruption.
                                retry_count += node_set.update_rbm_index(multi_uniq_id, $guard)?;

                                match node_ref.compare_exchange(
                                    Shared::null(),
                                    Owned::new(StoredNode {
                                        node_id: $id,
                                        node: new_node,
                                        node_set,
                                    }),
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                    $guard
                                ) {
                                    Ok(_pfx) => {

                                        if log_enabled!(log::Level::Trace) {
                                            trace!("Created node {}", $id);
                                        }
                                        return Ok(($id, retry_count));
                                    },
                                    Err(crossbeam_epoch::CompareExchangeError { new, .. }) => {
                                        retry_count +=1 ;

                                        if log_enabled!(log::Level::Trace) {
                                            trace!("Failed to create node {}. Someone is busy creating it",$id);
                                        }

                                        let StoredNode { node: cur_node,.. } = *new.into_box();
                                        $back_off.spin();
                                        return (search_level.f)(
                                            search_level,
                                            nodes,
                                            cur_node,
                                            multi_uniq_id,
                                            level,
                                            retry_count
                                        );
                                    }
                                };
                            }
                            false => {
                                // A node exists, might be ours, might be
                                // another one. SAFETY: We tested for null
                                // above and, since we do not remove nodes,
                                // this node can't be null anymore.
                                let stored_node = unsafe { stored_node.deref() };


                                println!("NODE EXISTS");
                                if log_enabled!(log::Level::Trace) {
                                    trace!("
                                        {} store: Node here exists {:?}",
                                            std::thread::current().name().unwrap(),
                                            stored_node.node_id
                                    );
                                    trace!("node_id {:?}", stored_node.node_id.get_id());
                                    trace!("node_id {:032b}", stored_node.node_id.get_id().0);
                                    trace!("id {}", $id);
                                    trace!("     id {:032b}", $id.get_id().0);
                                }

                                // See if somebody beat us to creating our
                                // node already, if so, we still need to do
                                // work: we have to update the bitmap index
                                // with the multi_uniq_id we've got from the
                                // caller.
                                if $id == stored_node.node_id {
                                    println!("NODE WITH ID EXISTS");
                                    // panic!("this should not happen in single-threaded context.");
                                    // yes, it exists
                                    trace!("found node {} in {} attempts",
                                        $id,
                                        retry_count
                                    );

                                    // Same remarks here as the above
                                    // fetch_update.
                                    // stored_node.node_set.update_rbm_index(
                                        // multi_uniq_id, $guard
                                    // )?;


                                stored_node.node.ptrbitarr.merge_with(new_node.ptrbitarr.load());
                                stored_node.node.pfxbitarr.merge_with(new_node.pfxbitarr.load());

                                    return Ok(($id, retry_count));
                                } else {
                                    // it's not "our" node, make a (recursive)
                                    // call to create it.
                                    level += 1;
                                    trace!("Collision with node_id {}, move to next level: {} len{} next_lvl{} index {}",
                                        stored_node.node_id, $id, $id.get_id().1, level, index
                                    );

                                    return match <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level) {
                                        // on to the next level!
                                        next_bit_shift if next_bit_shift > 0 => {
                                            (search_level.f)(
                                                search_level,
                                                &stored_node.node_set,
                                                new_node,
                                                multi_uniq_id,
                                                level,
                                                retry_count
                                            )
                                        }
                                        // There's no next level!
                                        _ => panic!("out of storage levels, current level is {}", level),
                                    }
                                }
                            }
                        }
                    }
                    true => {
                        trace!("Empty node set for {} in {} attempts. Giving up.",
                            $id,
                            retry_count
                        );
                        return Err(super::errors::PrefixStoreError::NodeNotFound);
                    }
                };
            }
        }
        )*
    };
}
