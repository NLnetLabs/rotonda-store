#[macro_export]
macro_rules! impl_buckets {
    ( $( $len: expr, $stride_size:expr ), * ) => {

    #[derive(Debug)]
    pub(crate) struct NodeBuckets4<AF: AddressFamily> {
        $( $len: NodeSet<AF, $stride_size>, )*
    }

impl<AF: AddressFamily> FamilyBuckets<AF> for NodeBuckets4<AF> {
    fn init() -> Self {
        NodeBuckets4 {
            $( $len: NodeSet::init(1 << Self::len_to_store_bits($level, 0)), )*
        }
    }

    fn len_to_store_bits(len: u8, level: u8) -> Option<&'static u8> {
        // (vert x hor) = len x level -> number of bits
        [
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 0
            [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 1
            [2, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 2 - never exists
            [3, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 3
            [4, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 4
            [5, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 5
            [6, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 6
            [7, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 7
            [8, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 8
            [9, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 9
            [10, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 10
            [11, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 11
            [12, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 12
            [12, 13, 0, 0, 0, 0, 0, 0, 0, 0],     // 13
            [12, 14, 0, 0, 0, 0, 0, 0, 0, 0],     // 14
            [12, 15, 0, 0, 0, 0, 0, 0, 0, 0],     // 15
            [12, 16, 0, 0, 0, 0, 0, 0, 0, 0],     // 16
            [12, 17, 0, 0, 0, 0, 0, 0, 0, 0],     // 17
            [12, 18, 0, 0, 0, 0, 0, 0, 0, 0],     // 18
            [12, 19, 0, 0, 0, 0, 0, 0, 0, 0],     // 19
            [12, 20, 0, 0, 0, 0, 0, 0, 0, 0],     // 20
            [12, 21, 0, 0, 0, 0, 0, 0, 0, 0],     // 21
            [12, 22, 0, 0, 0, 0, 0, 0, 0, 0],     // 22
            [12, 23, 0, 0, 0, 0, 0, 0, 0, 0],     // 23
            [12, 24, 0, 0, 0, 0, 0, 0, 0, 0],     // 24
            [12, 24, 25, 0, 0, 0, 0, 0, 0, 0],    // 25
            [4, 8, 12, 16, 20, 24, 26, 0, 0, 0],  // 26
            [4, 8, 12, 16, 20, 24, 27, 0, 0, 0],  // 27
            [4, 8, 12, 16, 20, 24, 28, 0, 0, 0],  // 28
            [4, 8, 12, 16, 20, 24, 28, 29, 0, 0], // 29
            [4, 8, 12, 16, 20, 24, 28, 30, 0, 0], // 30
        ][len as usize]
            .get(level as usize)
    }

    fn get_store3_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride3> {
        match id.get_id().1 as usize {
            14 => &mut self.l14,
            17 => &mut self.l17,
            20 => &mut self.l20,
            23 => &mut self.l23,
            26 => &mut self.l26,
            29 => &mut self.l29,
            _ => panic!(
                "unexpected sub prefix length {} in stride size 3 ({})",
                id.get_id().1,
                id
            ),
        }
    }

    fn get_store3(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride3> {
        match id.get_id().1 as usize {
            14 => &self.l14,
            17 => &self.l17,
            20 => &self.l20,
            23 => &self.l23,
            26 => &self.l26,
            29 => &self.l29,
            _ => panic!(
                "unexpected sub prefix length {} in stride size 3 ({})",
                id.get_id().1,
                id
            ),
        }
    }

    fn get_store4_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride4> {
        match id.get_id().1 as usize {
            10 => &mut self.l10,
            _ => panic!(
                "unexpected sub prefix length {} in stride size 4 ({})",
                id.get_id().1,
                id
            ),
        }
    }

    fn get_store4(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride4> {
        match id.get_id().1 as usize {
            10 => &self.l10,
            _ => panic!(
                "unexpected sub prefix length {} in stride size 4 ({})",
                id.get_id().1,
                id
            ),
        }
    }

    fn get_store5_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride5> {
        match id.get_id().1 as usize {
            0 => &mut self.l0,
            5 => &mut self.l5,
            _ => panic!(
                "unexpected sub prefix length {} in stride size 3 ({})",
                id.get_id().1,
                id
            ),
        }
    }

    fn get_store5(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride5> {
        match id.get_id().1 as usize {
            0 => &self.l0,
            5 => &self.l5,
            _ => panic!(
                "unexpected sub prefix length {} in stride size 3 ({})",
                id.get_id().1,
                id
            ),
        }
    }
}

}
}