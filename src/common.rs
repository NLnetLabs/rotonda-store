use num::PrimInt;

use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Debug, Display};

use routecore::record::{MergeUpdate, Meta, NoMeta};
use routecore::addr::AddressFamily;

//------------ MatchOptions ----------------------------------------------------------

pub struct MatchOptions {
    pub match_type: MatchType,
    pub include_less_specifics: bool,
    pub include_more_specifics: bool,
}

#[derive(Debug, Clone)]
pub enum MatchType {
    ExactMatch,
    LongestMatch,
    EmptyMatch,
}

impl std::fmt::Display for MatchType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MatchType::ExactMatch => write!(f, "exact-match"),
            MatchType::LongestMatch => write!(f, "longest-match"),
            MatchType::EmptyMatch => write!(f, "empty-match"),
        }
    }
}


//------------ Metadata Types --------------------------------------------------------

#[derive(Debug, Copy, Clone)]
pub struct PrefixAs(pub u32);

impl MergeUpdate for PrefixAs {
    fn merge_update(&mut self, update_record: PrefixAs) -> Result<(), Box<dyn std::error::Error>> {
        self.0 = update_record.0;
        Ok(())
    }
}

impl Display for PrefixAs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AS{}", self.0)
    }
}


//------------ InternalPrefixRecord --------------------------------------------------------

#[derive(Clone, Copy)]
pub struct InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily,
{
    pub net: AF,
    pub len: u8,
    pub meta: Option<T>,
}

impl<T, AF> InternalPrefixRecord<AF, T>
where
    T: Meta + MergeUpdate,
    AF: AddressFamily + PrimInt + Debug,
{
    pub fn new(net: AF, len: u8) -> InternalPrefixRecord<AF, T> {
        Self {
            net,
            len,
            meta: None,
        }
    }
    pub fn new_with_meta(net: AF, len: u8, meta: T) -> InternalPrefixRecord<AF, T> {
        Self {
            net,
            len,
            meta: Some(meta),
        }
    }
    pub fn strip_meta(&self) -> InternalPrefixRecord<AF, NoMeta> {
        InternalPrefixRecord::<AF, NoMeta> {
            net: self.net,
            len: self.len,
            meta: None,
        }
    }
}

impl<T, AF> std::fmt::Display for InternalPrefixRecord<AF, T>
where
    T: Meta + MergeUpdate,
    AF: AddressFamily + PrimInt + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{} {}",
            AddressFamily::fmt_net(self.net),
            self.len,
            self.meta.as_ref().unwrap().summary()
        )
    }
}

impl<AF, T> Ord for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (self.net >> (AF::BITS - self.len) as usize)
            .cmp(&(other.net >> ((AF::BITS - other.len) % 32) as usize))
    }
}

impl<AF, T> PartialEq for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    fn eq(&self, other: &Self) -> bool {
        self.net >> (AF::BITS - self.len) as usize
            == other.net >> ((AF::BITS - other.len) % 32) as usize
    }
}

impl<AF, T> PartialOrd for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            (self.net >> (AF::BITS - self.len) as usize)
                .cmp(&(other.net >> ((AF::BITS - other.len) % 32) as usize)),
        )
    }
}

impl<AF, T> Eq for InternalPrefixRecord<AF, T>
where
    T: Meta,
    AF: AddressFamily + PrimInt + Debug,
{
}

impl<T, AF> Debug for InternalPrefixRecord<AF, T>
where
    AF: AddressFamily + PrimInt + Debug,
    T: Meta,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "{}/{} with {:?}",
            AddressFamily::fmt_net(self.net),
            self.len,
            self.meta
        ))
    }
}

//------------ TrieLevelStats --------------------------------------------------------

pub struct TrieLevelStats {
    pub level: u8,
    pub nodes_num: u32,
    pub prefixes_num: u32,
}

impl fmt::Debug for TrieLevelStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{\"level\":{},\"nodes_num\":{},\"prefixes_num\":{}}}",
            self.level, self.nodes_num, self.prefixes_num
        )
    }
}
