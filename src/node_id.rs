//------------------------ NodeId Types ------------------------------------------------------------

pub trait SortableNodeId
where
    Self: std::cmp::Ord + std::fmt::Debug + Sized + Default,
    Self::Sort: std::cmp::Ord + std::convert::From<u16> + std::convert::Into<usize>,
    Self::Part: std::cmp::Ord + std::convert::From<u16> + std::marker::Copy + std::fmt::Debug,
{
    type Part;
    type Sort;
    // fn sort(&self, other: &Self) -> std::cmp::Ordering;
    fn new(sort: &Self::Sort, part: &Self::Part) -> Self;
    fn empty() -> Self;
    fn get_sort(&self) -> Self::Sort;
    fn get_part(&self) -> Self::Part;
    fn is_empty(&self) -> bool;
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Copy, Clone, Default)]
pub struct InMemNodeId(pub u16, pub u32);

// This works for both IPv4 and IPv6 up to a certain point.
// the u16 for Sort is used for ordering the local vecs
// inside the nodes.
// The u32 Part is used as an index to the backing global vecs,
// so you CANNOT store all IPv6 prefixes that could exist!
// If you really want that you should implement your own type with trait
// SortableNodeId, e.g., Sort = u16, Part = u128.
impl SortableNodeId for InMemNodeId {
    type Sort = u16;
    type Part = u32;

    // fn sort(&self, other: &Self) -> std::cmp::Ordering {
    //     self.0.cmp(&other.0)
    // }

    fn new(sort: &Self::Sort, part: &Self::Part) -> InMemNodeId {
        InMemNodeId(*sort, *part)
    }

    fn get_sort(&self) -> Self::Sort {
        self.0
    }

    fn get_part(&self) -> Self::Part {
        self.1
    }

    fn is_empty(&self) -> bool {
        self.0 == 0 && self.1 == 0
    }

    fn empty() -> Self {
        Self::new(&0, &0)
    }
}