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
                        let stored_node = unsafe {
                            &mut nodes.0.load(Ordering::SeqCst, guard).deref()[index].assume_init_ref()
                        };
                        // trace!("this node {:?}", this_node);
                        let this_node = stored_node.load(Ordering::Acquire, guard);

                        match this_node.is_null() {
                            true => None,
                            false => {
                                let StoredNode { node_id, node, node_set } = unsafe { this_node.deref() };
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

                        // match unsafe { this_node.assume_init_ref().load(Ordering::SeqCst, guard).deref() } {
                        //     // No node exists, here
                        //     StoredNode::Empty => None,
                        //     // A node exists, but since we're not using perfect
                        //     // hashing everywhere, this may be very well a node
                        //     // we're not searching for, so check that.
                        //     StoredNode::NodeWithRef((node_id, node, node_set)) => {
                        //         // trace!("found {} in level {}", node, level);
                        //         // trace!("search id {}", $id);
                        //         // trace!("found id {}", node_id);
                        //         if $id == *node_id {
                        //             // YES, It's the one we're looking for!
                        //             return Some(SizedStrideRef::$stride(&node));
                        //         };
                        //         // Meh, it's not, but we can a go to the next level
                        //         // and see if it lives there.
                        //         level += 1;
                        //         match <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level) {
                        //             // on to the next level!
                        //             next_bit_shift if next_bit_shift > 0 => {
                        //                 (search_level.f)(
                        //                     search_level,
                        //                     &node_set,
                        //                     level,
                        //                     guard,
                        //                 )
                        //             }
                        //             // There's no next level, we found nothing.
                        //             _ => None,
                        //         }
                        //     }
                        // }
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
        ),
    * ) => {
        $(
            SearchLevel {
                f: &|search_level: &SearchLevel<AF, $stride>,
                    nodes,
                    // bits_division: [u8; 10],
                    mut level: u8,
                    guard| {
                        // // HASHING FUNCTION
                        // let index = Self::hash_node_id($id, level);

                        // // Read the node from the block pointed to by the
                        // // Atomic pointer.
                        // let this_node = unsafe {
                        //     &mut nodes.0.load(Ordering::Acquire, guard).deref_mut()[index]
                        // };
                        // match unsafe { this_node.assume_init_mut().load(Ordering::Acquire, guard).deref_mut() } {
                        //     // No node exists, here
                        //     StoredNode::Empty => {
                        //         warn!("{} empty node {}", std::thread::current().name().unwrap(), $id);
                        //         None
                        //     },
                        //     // A node exists, but since we're not using perfect
                        //     // hashing everywhere, this may be very well a node
                        //     // we're not searching for, so check that.
                        //     StoredNode::NodeWithRef((node_id, node, node_set)) => {
                        //         // trace!("found {} in level {}", node, level);
                        //         // trace!("search id {}", $id);
                        //         // trace!("found id {}", node_id);
                        //         if &$id == node_id {
                        //             // YES, It's the one we're looking for!
                        //             return Some(SizedStrideRefMut::$stride(node));
                        //         };
                        //         // Meh, it's not, but we can a go to the next level
                        //         // and see if it lives there.
                        //         level += 1;
                        //         match <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level) {
                        //             // on to the next level!
                        //             next_bit_shift if next_bit_shift > 0 => {
                        //                 (search_level.f)(
                        //                     search_level,
                        //                     node_set,
                        //                     level,
                        //                     guard,
                        //                 )
                        //             }
                        //             // There's no next level, we found nothing.
                        //             _ => None,
                        //         }
                        //     }
                        // }

                         // HASHING FUNCTION
                         let index = Self::hash_node_id($id, level);

                         // Read the node from the block pointed to by the
                         // Atomic pointer.
                         let stored_node = unsafe {
                             &mut nodes.0.load(Ordering::SeqCst, guard).deref_mut()[index].assume_init_ref()
                         };
                         // trace!("this node {:?}", this_node);
                         let mut this_node = stored_node.load(Ordering::Acquire, guard);

                         match this_node.is_null() {
                             true => None,
                             false => {
                                let StoredNode { node_id, node, node_set } = unsafe { this_node.deref_mut() };
                                if $id == *node_id {
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
macro_rules! store_node_closure {
    (
        $(
            $stride: ident;
            $id: ident;
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
                mut level: u8,
                mut retry_count: u32| {
                    let this_level = <NB as NodeBuckets<AF>>::len_to_store_bits($id.get_id().1, level);
                    trace!("{:032b}", $id.get_id().0);
                    trace!("id {:?}", $id.get_id());

                    // HASHING FUNCTION
                    let index = Self::hash_node_id($id, level);
                    let stored_nodes = nodes.0.load(Ordering::Acquire, $guard);

                    match stored_nodes.is_null() {
                        false => {
                            let node_ref =
                                unsafe { stored_nodes.deref()[index].assume_init_ref() };
                            let stored_node = node_ref.load(Ordering::Acquire, $guard);

                            match stored_node.is_null() {
                                true => {
                                    // No node exists, so we create one here.
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
                                            if log_enabled!(log::Level::Warn) {
                                                debug!("{} created node {}", std::thread::current().name().unwrap(), $id);
                                            }
                                            if log_enabled!(log::Level::Warn) && retry_count > 0 {
                                                debug!("{} contention resolved on node {}", std::thread::current().name().unwrap(), $id);
                                            }
                                            return Ok(($id, retry_count));
                                        },
                                        Err(crossbeam_epoch::CompareExchangeError { current, new }) => {
                                            retry_count +=1 ;
                                            if log_enabled!(log::Level::Trace) {
                                                trace!(
                                                    "{} failed to create node {}. Someone is busy creating it",
                                                        std::thread::current().name().unwrap(),
                                                        $id
                                                );
                                                trace!("{} current {:?}",
                                                    std::thread::current().name().unwrap(),
                                                    unsafe { current.as_ref() }
                                                );
                                            }

                                            let StoredNode { node: cur_node,.. } = *new.into_box();
                                                $back_off.spin();
                                                return (search_level.f)(
                                                    search_level,
                                                    nodes,
                                                    cur_node,
                                                    level,
                                                    retry_count
                                            );
                                        }
                                    };
                                }
                                false => {
                                    // A node exists, might be ours, might be another one
                                    let StoredNode { node_id, node_set, .. } = unsafe { stored_node.deref() };
                                    if log_enabled!(log::Level::Debug) {
                                        debug!("
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
                                        if log_enabled!(log::Level::Debug) {
                                            debug!(
                                                "{} node {} already created.",
                                                std::thread::current().name().unwrap(), $id
                                            );
                                        }
                                        return Ok(($id, retry_count));
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
                            return Err(Box::new(super::errors::PrefixStoreError::NodeNotFound));
                        }
                    };
                }
            }
        )*
    };
}
