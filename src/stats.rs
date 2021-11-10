//------------ Types for Statistics -----------------------------------------

use crate::stride::{Stride3, Stride4, Stride5, Stride6, Stride7, Stride8};
use std::fmt::{Debug, Display};

#[derive(Debug, Copy, Clone)]
pub enum SizedStride {
    Stride3,
    Stride4,
    Stride5,
    Stride6,
    Stride7,
    Stride8,
}
pub struct StrideStats {
    pub stride_type: SizedStride,
    pub stride_size: usize,
    pub stride_len: u8,
    pub node_size: usize,
    pub created_nodes: Vec<CreatedNodes>,
    pub prefixes_num: Vec<CreatedNodes>,
}

impl StrideStats {
    pub fn new(stride_type: SizedStride, num_depth_levels: u8) -> Self {
        match stride_type {
            SizedStride::Stride3 => Self {
                stride_type: SizedStride::Stride3,
                stride_size: 16,
                stride_len: 3,
                node_size: std::mem::size_of::<Stride3>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
            SizedStride::Stride4 => Self {
                stride_type: SizedStride::Stride4,
                stride_size: 32,
                stride_len: 4,
                node_size: std::mem::size_of::<Stride4>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
            SizedStride::Stride5 => Self {
                stride_type: SizedStride::Stride5,
                stride_size: 64,
                stride_len: 5,
                node_size: std::mem::size_of::<Stride5>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
            SizedStride::Stride6 => Self {
                stride_type: SizedStride::Stride6,
                stride_size: 128,
                stride_len: 6,
                node_size: std::mem::size_of::<Stride6>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
            SizedStride::Stride7 => Self {
                stride_type: SizedStride::Stride7,
                stride_size: 256,
                stride_len: 7,
                node_size: std::mem::size_of::<Stride7>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
            SizedStride::Stride8 => Self {
                stride_type: SizedStride::Stride8,
                stride_size: 512,
                stride_len: 8,
                node_size: std::mem::size_of::<Stride8>(),
                created_nodes: Self::nodes_vec(num_depth_levels),
                prefixes_num: Self::nodes_vec(num_depth_levels),
            },
        }
    }

    pub fn mem_usage(&self) -> usize {
        self.stride_size
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

impl Debug for StrideStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{:>8?} {:?} ({}k)",
            &self.stride_type,
            &self.created_nodes.iter().fold(0, |mut a, n| {
                a += n.count;
                a
            }),
            &self.created_nodes,
            &self.mem_usage() / 1024
        )
    }
}

impl Display for StrideStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{:>8?} {:?} ({}k)",
            &self.stride_type,
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
        f.write_fmt(format_args!("{}", &self.count))
    }
}
