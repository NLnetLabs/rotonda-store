#[derive(Debug)]
pub(crate) struct InMemStorage<
    AF: AddressFamily,
    Meta: routecore::record::Meta,
> {
    // each stride in its own vec avoids having to store SizedStrideNode, an enum, that will have
    // the size of the largest variant as its memory footprint (Stride8).
    pub nodes3: DashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride3>>,
    pub nodes4: DashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride4>>,
    pub nodes5: DashMap<StrideNodeId<AF>, TreeBitMapNode<AF, Stride5>>,
    pub prefixes: DashMap<PrefixId<AF>, InternalPrefixRecord<AF, Meta>>,
    pub(crate) len_to_stride_size: [StrideType; 128],
    pub default_route_prefix_serial: AtomicUsize,
}

impl<AF: AddressFamily, Meta: routecore::record::Meta + MergeUpdate>
    StorageBackend for InMemStorage<AF, Meta>
{
    type AF = AF;
    type Meta = Meta;

    fn init(
        len_to_stride_size: [StrideType; 128],
        root_node: SizedStrideNode<Self::AF>,
    ) -> InMemStorage<Self::AF, Self::Meta> {
        let nodes3 =
            DashMap::<StrideNodeId<AF>, TreeBitMapNode<AF, Stride3>>::new();
        let nodes4 =
            DashMap::<StrideNodeId<AF>, TreeBitMapNode<AF, Stride4>>::new();
        let nodes5 =
            DashMap::<StrideNodeId<AF>, TreeBitMapNode<AF, Stride5>>::new();

        let mut store = InMemStorage {
            nodes3,
            nodes4,
            nodes5,
            prefixes: DashMap::new(),
            len_to_stride_size,
            default_route_prefix_serial: AtomicUsize::new(0),
        };
        store.store_node(
            StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0),
            root_node,
        );
        store
    }

    fn acquire_new_node_id(
        &self,
        (prefix_net, sub_prefix_len): (Self::AF, u8),
    ) -> StrideNodeId<AF> {
        StrideNodeId::new_with_cleaned_id(prefix_net, sub_prefix_len)
    }

    fn store_node(
        &mut self,
        id: StrideNodeId<AF>,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId<AF>> {
        match next_node {
            SizedStrideNode::Stride3(node) => {
                self.nodes3.insert(id, node);
                Some(id)
            }
            SizedStrideNode::Stride4(node) => {
                self.nodes4.insert(id, node);
                Some(id)
            }
            SizedStrideNode::Stride5(node) => {
                self.nodes5.insert(id, node);
                Some(id)
            }
        }
    }

    fn store_node_in_store(
        store: &mut StrideWriteStore<Self::AF>,
        id: StrideNodeId<AF>,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId<AF>> {
        match next_node {
            SizedStrideNode::Stride3(node) => {
                if let StrideWriteStore::Stride3(n_store) = store {
                    let _default_val = (*n_store).insert(id, node);
                }
                Some(id)
            }
            SizedStrideNode::Stride4(node) => {
                if let StrideWriteStore::Stride4(n_store) = store {
                    let _default_val = (*n_store).insert(id, node);
                }
                Some(id)
            }
            SizedStrideNode::Stride5(node) => {
                if let StrideWriteStore::Stride5(n_store) = store {
                    let _default_val = (*n_store).insert(id, node);
                }
                Some(id)
            }
        }
    }

    fn get_stride_for_id(
        &self,
        id: StrideNodeId<Self::AF>,
    ) -> crate::local_array::tree::StrideType {
        self.len_to_stride_size[id.get_id().1 as usize]
    }

    fn get_stride_for_id_with_read_store(
        &self,
        id: StrideNodeId<Self::AF>,
    ) -> (StrideNodeId<Self::AF>, StrideReadStore<Self::AF>) {
        match self.len_to_stride_size[id.get_id().1 as usize] {
            crate::local_array::tree::StrideType::Stride3 => {
                (id, StrideReadStore::Stride3(&self.nodes3))
            }
            crate::local_array::tree::StrideType::Stride4 => {
                (id, StrideReadStore::Stride4(&self.nodes4))
            }
            crate::local_array::tree::StrideType::Stride5 => {
                (id, StrideReadStore::Stride5(&self.nodes5))
            }
            _ => panic!("invalid stride size"),
        }
    }

    fn get_stride_for_id_with_write_store(
        &self,
        id: StrideNodeId<Self::AF>,
    ) -> (StrideNodeId<Self::AF>, StrideWriteStore<Self::AF>) {
        match self.len_to_stride_size[id.get_id().1 as usize] {
            crate::local_array::tree::StrideType::Stride3 => {
                (id, StrideWriteStore::Stride3(&self.nodes3))
            }
            crate::local_array::tree::StrideType::Stride4 => {
                (id, StrideWriteStore::Stride4(&self.nodes4))
            }
            crate::local_array::tree::StrideType::Stride5 => {
                (id, StrideWriteStore::Stride5(&self.nodes5))
            }
            _ => panic!("invalid stride size"),
        }
    }

    fn update_node(
        &mut self,
        current_node_id: StrideNodeId<AF>,
        updated_node: SizedStrideRefMut<AF>,
    ) {
        match updated_node {
            SizedStrideRefMut::Stride3(node) => {
                let node = std::mem::take(node);
                let _default_val = self.nodes3.insert(current_node_id, node);
            }
            SizedStrideRefMut::Stride4(node) => {
                let node = std::mem::take(node);
                let _default_val = self.nodes4.insert(current_node_id, node);
            }
            SizedStrideRefMut::Stride5(node) => {
                let node = std::mem::take(node);
                let _default_val = self.nodes5.insert(current_node_id, node);
            }
        }
    }

    fn update_node_in_store(
        &self,
        store: &mut StrideWriteStore<Self::AF>,
        current_node_id: StrideNodeId<Self::AF>,
        updated_node: SizedStrideNode<Self::AF>,
    ) {
        match updated_node {
            SizedStrideNode::Stride3(node) => {
                if let StrideWriteStore::Stride3(n_store) = store {
                    let _default_val =
                        (*n_store).insert(current_node_id, node);
                }
            }
            SizedStrideNode::Stride4(node) => {
                if let StrideWriteStore::Stride4(n_store) = store {
                    let _default_val =
                        (*n_store).insert(current_node_id, node);
                }
            }
            SizedStrideNode::Stride5(node) => {
                if let StrideWriteStore::Stride5(n_store) = store {
                    let _default_val =
                        (*n_store).insert(current_node_id, node);
                }
            }
        }
    }

    fn retrieve_node(
        &self,
        id: StrideNodeId<AF>,
    ) -> SizedNodeRefOption<'_, Self::AF> {
        match self.len_to_stride_size[id.get_id().1 as usize] {
            StrideType::Stride3 => self
                .nodes3
                .get(&id)
                .map(|n| SizedStrideRef::Stride3(n.value())),
            StrideType::Stride4 => self
                .nodes4
                .get(&id)
                .map(|n| SizedStrideRef::Stride4(n.value())),
            StrideType::Stride5 => self
                .nodes5
                .get(&id)
                .map(|n| SizedStrideRef::Stride5(n.value())),
        }
    }

    fn retrieve_node_mut_with_guard<'a>(
        &'a self,
        _id: StrideNodeId<Self::AF>,
        _guard: &'a Guard,
    ) -> Option<SizedStrideRefMut<'a, Self::AF>> {
        unimplemented!()
    }

    fn store_node_with_guard<'a>(
        &'a self,
        _current_node: SizedNodeRefOption<'a, Self::AF>,
        _next_node: SizedStrideNode<Self::AF>,
        _guard: &'a Guard,
    ) -> Option<StrideNodeId<Self::AF>> {
        unimplemented!()
    }

    fn retrieve_node_with_guard<'a>(
        &'a self,
        _id: StrideNodeId<Self::AF>,
        _guard: &'a Guard,
    ) -> Option<SizedStrideRef<'a, Self::AF>> {
        unimplemented!()
    }

    fn get_root_node_id(&self, first_stride_size: u8) -> StrideNodeId<AF> {
        let (addr_bits, len) = match first_stride_size {
            3 => (AF::zero(), 0),
            4 => (AF::zero(), 0),
            5 => (AF::zero(), 0),
            _ => panic!("Invalid stride size {}", first_stride_size),
        };
        StrideNodeId::dangerously_new_with_id_as_is(addr_bits, len)
    }

    fn get_nodes_len(&self) -> usize {
        self.nodes3.len() + self.nodes4.len() + self.nodes5.len()
        // + self.nodes6.len()
        // + self.nodes7.len()
        // + self.nodes8.len()
    }

    fn acquire_new_prefix_id(
        &self,
        prefix: &InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> PrefixId<AF> {
        // The return value the StrideType doesn't matter here,
        // because we store all prefixes in one huge vec (unlike the nodes,
        // which are stored in separate vec for each stride size).
        // We'll return the index to the end of the vec.
        PrefixId::<AF>::new(prefix.net, prefix.len).set_serial(1)
    }

    fn store_prefix(
        &self,
        id: PrefixId<Self::AF>,
        next_node: InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> Result<PrefixId<Self::AF>, Box<dyn std::error::Error>> {
        self.prefixes.insert(id, next_node);
        Ok(id)
    }

    fn retrieve_prefix(
        &self,
        part_id: PrefixId<Self::AF>,
    ) -> Option<&InternalPrefixRecord<Self::AF, Self::Meta>> {
        self.prefixes.get(&part_id).map(|p| p.value())
    }

    fn remove_prefix(
        &self,
        id: PrefixId<Self::AF>,
    ) -> Option<InternalPrefixRecord<Self::AF, Self::Meta>> {
        match id.is_empty() {
            false => self.prefixes.remove(&id).map(|p| p.1),
            true => None,
        }
    }

    fn load_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial.load(Ordering::Acquire)
    }

    // returns the *old* serial in analogy with basic atomic operations
    fn increment_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial
            .fetch_add(1, Ordering::Acquire)
    }

    fn get_prefixes(&'_ self) -> &'_ PrefixHashMap<Self::AF, Self::Meta> {
        &self.prefixes
    }

    fn get_prefixes_clear(&self) -> &PrefixHashMap<Self::AF, Self::Meta> {
        &self.prefixes
    }

    fn get_prefixes_len(&self) -> usize {
        self.prefixes.len()
    }

    #[cfg(feature = "dynamodb")]
    fn prefixes_iter_mut(
        &mut self,
    ) -> Result<
        std::slice::IterMut<'_, InternalPrefixRecord<AF, Meta>>,
        Box<dyn std::error::Error>,
    > {
        Ok(self.prefixes.iter_mut())
    }
}