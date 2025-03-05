//------------ Types for Statistics -----------------------------------------

// use crate::stride::{Stride3, Stride4, Stride5, Stride6, Stride7, Stride8};
use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
};

use crate::{
    rib::STRIDE_SIZE, tree_bitmap::TreeBitMapNode, types::AddressFamily,
};

pub struct StrideStats<AF: AddressFamily> {
    pub node_size: usize,
    pub created_nodes: Vec<CreatedNodes>,
    pub prefixes_num: Vec<CreatedNodes>,
    _af: PhantomData<AF>,
}

impl<AF: AddressFamily> StrideStats<AF> {
    pub fn new() -> Self {
        Self {
            node_size: std::mem::size_of::<TreeBitMapNode<AF>>(),
            created_nodes: Self::nodes_vec(AF::BITS / STRIDE_SIZE),
            prefixes_num: Self::nodes_vec(AF::BITS / STRIDE_SIZE),
            _af: PhantomData,
        }
    }

    pub fn mem_usage(&self) -> usize {
        STRIDE_SIZE as usize
            * self.created_nodes.iter().fold(0, |mut acc, c| {
                acc += c.count;
                acc
            })
    }

    fn nodes_vec(num_depth_levels: u8) -> Vec<CreatedNodes> {
        let mut vec: Vec<CreatedNodes> = vec![];
        for n in 0..num_depth_levels {
            vec.push(CreatedNodes {
                depth_level: n,
                count: 0,
            })
        }
        vec
    }

    pub fn inc(&mut self, depth_level: u8) {
        self.created_nodes[depth_level as usize].count += 1;
    }

    pub fn inc_prefix_count(&mut self, depth_level: u8) {
        self.prefixes_num[depth_level as usize].count += 1;
    }
}

impl<AF: AddressFamily> Default for StrideStats<AF> {
    fn default() -> Self {
        Self::new()
    }
}

impl<AF: AddressFamily> Debug for StrideStats<AF> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stride4:{:>8?} {:?} ({}k)",
            &self.created_nodes.iter().fold(0, |mut a, n| {
                a += n.count;
                a
            }),
            &self.created_nodes,
            &self.mem_usage() / 1024
        )
    }
}

impl<AF: AddressFamily> Display for StrideStats<AF> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stride4:{:>8?} {:?} ({}k)",
            &self.created_nodes.iter().fold(0, |mut a, n| {
                a += n.count;
                a
            }),
            &self.created_nodes,
            &self.mem_usage() / 1024
        )
    }
}

#[derive(Copy, Clone)]
pub struct CreatedNodes {
    pub depth_level: u8,
    pub count: usize,
}

impl CreatedNodes {
    pub fn add(mut self, num: usize) {
        self.count += num;
    }
}

impl Debug for CreatedNodes {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_fmt(format_args!("/{}: {}", &self.depth_level, &self.count))
    }
}
