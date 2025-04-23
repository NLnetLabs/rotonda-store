//------------ Types for Statistics -----------------------------------------

use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{rib::STRIDE_SIZE, types::AddressFamily};

pub(crate) struct StrideStats<AF: AddressFamily> {
    pub(crate) created_nodes: Vec<CreatedNodes>,
    pub(crate) _prefixes_num: Vec<CreatedNodes>,
    _af: PhantomData<AF>,
}

impl<AF: AddressFamily> StrideStats<AF> {
    pub fn new() -> Self {
        Self {
            created_nodes: Self::nodes_vec(AF::BITS / STRIDE_SIZE),
            _prefixes_num: Self::nodes_vec(AF::BITS / STRIDE_SIZE),
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

    pub fn _inc(&mut self, depth_level: u8) {
        if let Some(n) = self.created_nodes.get_mut(depth_level as usize) {
            n.count += 1
        }
    }

    pub fn _inc_prefix_count(&mut self, depth_level: u8) {
        if let Some(p) = self._prefixes_num.get_mut(depth_level as usize) {
            p.count += 1;
        }
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

//------------ Counters -----------------------------------------------------
//
// This is the struct that's part of the data structure of each tree type.

#[derive(Debug)]
pub(crate) struct Counters {
    // number of created nodes in the TreeBitMap. Set to 0 for other trees.
    nodes: AtomicUsize,
    // number of unique prefixes in the tree
    prefixes: [AtomicUsize; 129],
    // number of unique (prefix, mui) values inserted in the tree.
    routes: AtomicUsize,
}

impl Counters {
    pub fn nodes_count(&self) -> usize {
        self.nodes.load(Ordering::Relaxed)
    }

    pub fn inc_nodes_count(&self) {
        self.nodes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn prefixes_count(&self) -> Vec<usize> {
        self.prefixes
            .iter()
            .map(|pc| pc.load(Ordering::Relaxed))
            .collect::<Vec<_>>()
    }

    pub fn inc_prefixes_count(&self, len: u8) {
        if let Some(p) = self.prefixes.get(len as usize) {
            p.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn _dec_prefixes_count(&self, len: u8) {
        if let Some(p) = self.prefixes.get(len as usize) {
            p.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn prefix_stats(&self) -> Vec<CreatedNodes> {
        self.prefixes
            .iter()
            .enumerate()
            .filter_map(|(len, count)| -> Option<CreatedNodes> {
                let count = count.load(Ordering::Relaxed);
                if count != 0 {
                    Some(CreatedNodes {
                        depth_level: len as u8,
                        count,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn routes_count(&self) -> usize {
        self.routes.load(Ordering::Relaxed)
    }

    pub fn inc_routes_count(&self) {
        self.routes.fetch_add(1, Ordering::Relaxed);
    }
}

// How can this unwrap in here ever fail?
#[allow(clippy::unwrap_used)]
impl Default for Counters {
    fn default() -> Self {
        let mut prefixes: Vec<AtomicUsize> = Vec::with_capacity(129);
        for _ in 0..=128 {
            prefixes.push(AtomicUsize::new(0));
        }

        Self {
            nodes: AtomicUsize::new(0),
            prefixes: prefixes.try_into().unwrap(),
            routes: AtomicUsize::new(0),
        }
    }
}

//------------ UpsertCounters ------------------------------------------------
//
// The Counters struct holds atomic values, so this struct exists to return a
// set of counters from the RIB to users.

#[derive(Debug)]
pub struct UpsertCounters {
    // number of unique inserted prefixes|routes in the in-mem tree
    pub(crate) in_memory_count: usize,
    // number of unique persisted prefixes|routes
    pub(crate) persisted_count: usize,
    // total number of unique inserted prefixes|routes in the RIB
    pub(crate) total_count: usize,
}

impl UpsertCounters {
    pub fn in_memory(&self) -> usize {
        self.in_memory_count
    }

    pub fn persisted(&self) -> usize {
        self.persisted_count
    }

    pub fn total(&self) -> usize {
        self.total_count
    }
}

// impl std::fmt::Display for UpsertCounters {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         writeln!(f, "Unique Items in-memory:\t{}", self.in_memory_count)?;
//         writeln!(f, "Unique persisted Items:\t{}", self.persisted_count)?;
//         writeln!(f, "Total inserted Items:\t{}", self.total_count)
//     }
// }

impl std::ops::AddAssign for UpsertCounters {
    fn add_assign(&mut self, rhs: Self) {
        self.in_memory_count += rhs.in_memory_count;
        self.persisted_count += rhs.persisted_count;
        self.total_count += rhs.total_count;
    }
}

impl std::ops::Add for UpsertCounters {
    type Output = UpsertCounters;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            in_memory_count: self.in_memory_count + rhs.in_memory_count,
            persisted_count: self.persisted_count + rhs.persisted_count,
            total_count: self.total_count + rhs.total_count,
        }
    }
}

//------------ StoreStats ----------------------------------------------------

#[derive(Debug)]
pub struct StoreStats {
    pub v4: Vec<CreatedNodes>,
    pub v6: Vec<CreatedNodes>,
}

//------------ UpsertReport --------------------------------------------------

#[derive(Debug)]
pub struct UpsertReport {
    // Indicates the number of Atomic Compare-and-Swap operations were
    // necessary to create/update the Record entry. High numbers indicate
    // contention.
    pub cas_count: usize,
    // Indicates whether this was the first mui record for this prefix was
    // created. So, the prefix did not exist before hand.
    pub prefix_new: bool,
    // Indicates whether this mui was new for this prefix. False means an old
    // value was overwritten.
    pub mui_new: bool,
    // The number of mui records for this prefix after the upsert operation.
    pub mui_count: usize,
}
