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
            | {
                    // HASHING FUNCTION
                    let index = Self::hash_node_id($id, level);

                    // Read the node from the block pointed to by the Atomic
                    // pointer.
                    // let stored_node = unsafe {
                    //     &mut nodes.0[index]
                    // };
                    // let this_node = stored_node.load(Ordering::Acquire, guard);

                    match nodes.0.get(index) {
                        None => None,
                        Some(stored_node) => {
                            let StoredNode { node_id, node, node_set, .. } = stored_node;
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
                                        // guard,
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
                mut level: u8| {
                    // HASHING FUNCTION
                    let index = Self::hash_node_id($id, level);

                    // Read the node from the block pointed to by the Atomic
                    // pointer.
                    // let stored_node = unsafe {
                    //     &mut nodes.0[index].assume_init_ref()
                    // };
                    // let this_node = stored_node.load(Ordering::Acquire, guard);

                    match nodes.0.get(index) {
                        None => None,
                        Some(this_node) => {
                            let StoredNode { node_id, node, node_set, .. } = this_node;

                            // early return if the mui is not in the index
                            // stored in this node, meaning the mui does not
                            // appear anywhere in the sub-tree formed from
                            // this node.
                            let bmin = node_set.1.read().unwrap(); // load(Ordering::Acquire, guard).deref()
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
                                        // guard,
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

// This macro creates a closure that is used in turn in the macro
// 'eBox', that is used in the public `insert` method on a TreeBitMap.
//
// It retrieves the node specified by $id recursively, creates it if it does
// not exist. It is responsible for setting/updating the RBMIN, but is does
// *not* set/update the pfxbitarr or ptrbitarr of the TreeBitMapNode. The
// `insert_match` takes care of the latter.
//
// This closure should not be called repeatedly to create the same node, if it
// returns `None` that is basically a data race in the store and therefore an
// error. Also the caller should make sure to stay within the limit of the
// defined number of levels, although the closure will return at the end of
// the maximum depth.
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
                // guard
            | {
                // HASHING FUNCTION
                let index = Self::hash_node_id($id, level);

                // assert!(!nodes.0.is_null());

                match nodes.0.get(index) {
                    // This arm only ever gets called in multi-threaded code
                    // where our thread (running this code *now*), andgot ahead
                    // of another thread: After the other thread created the
                    // TreeBitMapNode first, it was overtaken by our thread
                    // running this method, so our thread enounters an empty node
                    // in the store.
                    None => {
                        // There is some code duplicaton in these arms, but we
                        // want to avoid always creating these values and then
                        // not using them.
                        let this_level = <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level);
                        let next_level = <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level + 1);
                        let node_set = NodeSet::init(next_level - this_level);
                        // } else {
                        //     NodeSet(
                        //         Box::new([]),
                        //         std::sync::RwLock::new(RoaringBitmap::new())
                        //     )
                        // };

                        // See if we can create the node
                        let node = nodes.0.get_or_init(index, || StoredNode {
                            node_id: $id,
                            node: TreeBitMapNode {
                                ptrbitarr: <$stride as Stride>::AtomicPtrSize::from(0),
                                pfxbitarr: <$stride as Stride>::AtomicPfxSize::from(0),
                                _af: PhantomData
                            },
                            node_set
                        });

                        // We may have lost, and a different node than we
                        // intended could live here, if so go a level deeper
                        if $id == node.node_id {
                            // Nope, its ours or at least the node we need.
                            let _retry_count = node.node_set.update_rbm_index(
                                $multi_uniq_id
                            ).ok();

                            return Some(SizedStrideRef::$stride(&node.node));
                        };

                        // It isn't ours move one level deeper.
                        level += 1;
                        match <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level) {
                            // on to the next level!
                            next_bit_shift if next_bit_shift > 0 => {
                                (search_level.f)(
                                    search_level,
                                    &node.node_set,
                                    level,
                                    // guard,
                                )
                            }
                            // There's no next level, we found nothing.
                            _ => None,
                        }
                    },
                    Some(this_node) => {
                        let StoredNode { node_id, node, node_set } = this_node;
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
                                $multi_uniq_id
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
                                    // guard,
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
                // println!("-");
                let this_level = <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level);
                trace!("{:032b}", $id.get_id().0);
                trace!("id {:?}", $id.get_id());
                trace!("multi_uniq_id {}", multi_uniq_id);

                std::sync::atomic::fence(Ordering::Acquire);
                // HASHING FUNCTION
                let index = Self::hash_node_id($id, level);
                let stored_nodes = &nodes.0; //.load(Ordering::Acquire, $guard);

                // assert!(!stored_nodes.is_null());
                let node_ref = &stored_nodes; // .get(index);
                // println!("success");
                // let stored_node = node_ref.load(Ordering::Acquire, $guard);

                match node_ref.get(index) {
                    None => {
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

                        // let node_set = if next_level > 0 {
                        let node_set = NodeSet::init(next_level - this_level);
                        // } else {
                        //     NodeSet(
                        //         Box::new([]),
                        //         std::sync::RwLock::new(RoaringBitmap::new())
                        //     )
                        // };

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
                        // retry_count += node_set.update_rbm_index(multi_uniq_id)?;

                        let ptrbitarr = new_node.ptrbitarr.load();
                        let pfxbitarr = new_node.pfxbitarr.load();

                        let stored_node = node_ref.get_or_init(
                            index,
                            || StoredNode {
                                node_id: $id,
                                node: new_node,
                                node_set
                            }
                        );

                        if stored_node.node_id == $id && !its_us {
                            stored_node.node_set.update_rbm_index(
                                multi_uniq_id
                            )?;

                            if !its_us && ptrbitarr != 0 {
                                stored_node.node.ptrbitarr.merge_with(ptrbitarr);
                            }

                            if !its_us && pfxbitarr != 0 {
                                stored_node.node.pfxbitarr.merge_with(pfxbitarr);
                            }
                        }

                        return Ok(($id, retry_count));

                        // match node_ref.compare_exchange(
                        //     Shared::null(),
                        //     Owned::new(StoredNode {
                        //         node_id: $id,
                        //         node: new_node,
                        //         node_set,
                        //     }),
                        //     Ordering::Acquire,
                        //     Ordering::Relaxed,
                        //     $guard
                        // ) {
                        //     Ok(_pfx) => {

                        //         if log_enabled!(log::Level::Trace) {
                        //             trace!("Created node {}", $id);
                        //         }
                        //         return Ok(($id, retry_count));
                        //     },
                        //     Err(crossbeam_epoch::CompareExchangeError { new, .. }) => {
                        //         retry_count +=1 ;

                        //         if log_enabled!(log::Level::Trace) {
                        //             trace!("Failed to create node {}. Someone is busy creating it",$id);
                        //         }

                        //         let StoredNode { node: cur_node,.. } = *new.into_box();
                        //         $back_off.spin();
                        //         return (search_level.f)(
                        //             search_level,
                        //             nodes,
                        //             cur_node,
                        //             multi_uniq_id,
                        //             level,
                        //             retry_count
                        //         );
                        //     }
                        // };
                    }
                    Some(stored_node) => {
                        // A node exists, might be ours, might be
                        // another one. SAFETY: We tested for null
                        // above and, since we do not remove nodes,
                        // this node can't be null anymore.
                        // let stored_node = unsafe { stored_node.get() };

                        // println!("NODE EXISTS");
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
                            // println!("NODE WITH ID EXISTS");
                            // panic!("this should not happen in single-threaded context.");
                            // yes, it exists
                            trace!("found node {} in {} attempts",
                                $id,
                                retry_count
                            );

                            stored_node.node_set.update_rbm_index(
                                multi_uniq_id
                            )?;

                            if new_node.ptrbitarr.load() != 0 {
                                stored_node.node.ptrbitarr.merge_with(new_node.ptrbitarr.load());
                            }
                            if new_node.pfxbitarr.load() != 0 {
                                stored_node.node.pfxbitarr.merge_with(new_node.pfxbitarr.load());
                            }

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
        }
    )*
    };
}
