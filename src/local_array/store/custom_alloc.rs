use std::{
    fmt::Debug,
    mem::MaybeUninit,
    ops::Add,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_epoch::{self as epoch, Atomic};

use log::trace;

use epoch::{Guard, Owned};
use std::marker::PhantomData;

use crate::local_array::tree::*;
use crate::{
    // local_array::storage_backend::PrefixIter,
    local_array::storage_backend::StorageBackend,
};

use crate::prefix_record::InternalPrefixRecord;
use crate::{impl_search_level, impl_search_level_mut, impl_write_level};

use crate::AddressFamily;
use routecore::record::MergeUpdate;
use routecore::record::Meta;

use super::storage_backend::SizedNodeRefOption;

pub fn prefix_store_bits_4(len: u8, level: u8) -> Option<&'static u8> {
    // (vert x hor) = len x level -> number of bits
    [
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 0
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 1 - never exists
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
        [4, 8, 12, 16, 20, 24, 28, 31, 0, 0],  // 31
        [4, 8 , 12, 16, 20, 24, 28, 32, 0, 0], // 32
    ][len as usize]
        .get(level as usize)
}

pub fn prefix_store_bits_6(len: u8, level: u8) -> Option<&'static u8> {
    // (vert x hor) = len x level -> number of bits
    [
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 0
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 1 - never exists
        [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 2
        [3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 3
        [4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 4
        [5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 5
        [6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 6
        [7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 7
        [8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 8
        [9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // len 9
        [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // len 10
        [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // len 11
        [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // len 12
        [12, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 13
        [12, 14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 14
        [12, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 15
        [12, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 16
        [12, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 17
        [12, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 18
        [12, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 19
        [12, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 20
        [12, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 21
        [12, 22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 22
        [12, 23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 23
        [12, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 24
        [12, 24, 25, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 25
        [12, 24, 26, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 26
        [12, 24, 27, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 27
        [12, 24, 28, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 28
        [12, 24, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 29
        [12, 24, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 30
        [12, 24, 31, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 31
        [12, 24, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 32
        [12, 24, 33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 33
        [12, 24, 34, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 34
        [12, 24, 35, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 35
        [12, 24, 36, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 36
        [12, 24, 36, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 37
        [12, 24, 36, 38, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 38
        [12, 24, 36, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 39
        [12, 24, 36, 40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 40
        [12, 24, 36, 41, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 41
        [12, 24, 36, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 42
        [12, 24, 36, 43, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 43
        [12, 24, 36, 44, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 44
        [12, 24, 36, 45, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 45
        [12, 24, 36, 46, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 46
        [12, 24, 36, 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 47
        [12, 24, 36, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 48
        [4, 8, 12, 24, 28, 48, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 49
        [4, 8, 12, 24, 28, 48, 50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 50
        [4, 8, 12, 24, 28, 48, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 51
        [4, 8, 12, 24, 28, 48, 52, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 52
        [4, 8, 12, 24, 28, 48, 52, 53, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 53
        [4, 8, 12, 24, 28, 48, 52, 54, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 54
        [4, 8, 12, 24, 28, 48, 52, 55, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 55
        [4, 8, 12, 24, 28, 48, 52, 56, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 56
        [4, 8, 12, 24, 28, 48, 52, 56, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 57
        [4, 8, 12, 24, 28, 48, 52, 56, 58, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 58
        [4, 8, 12, 24, 28, 48, 52, 56, 59, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 59
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 60
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 61, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 61
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 62, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 62
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], // 63
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 64
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 65
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 66, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 66
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 67
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    // 68
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 69, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 69
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 70
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 71, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 71
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 72, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 72
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 73, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 73
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   // 74
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 75, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 75
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 76, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 76
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 77, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // 77
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 78
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 79, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 79
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 80
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 81
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // 82
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 83, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],        // 83
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],        // 84
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 85, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 85
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 86, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 86
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 87, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 87
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       // 88
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 89, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 89
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 90, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 90
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 91, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 91
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // 92
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 93, 0, 0, 0, 0, 0, 0, 0, 0],     // 93
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 94, 0, 0, 0, 0, 0, 0, 0, 0],     // 94
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 95, 0, 0, 0, 0, 0, 0, 0, 0],     // 95
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 0, 0, 0, 0, 0, 0, 0, 0],     // 96
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 97, 0, 0, 0, 0, 0, 0, 0],    // 97
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 98, 0, 0, 0, 0, 0, 0, 0],    // 98
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 99, 0, 0, 0, 0, 0, 0, 0],        // 99
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 0, 0, 0, 0, 0, 0, 0],       // 100
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 101, 0, 0, 0, 0, 0, 0],     // 101
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 102, 0, 0, 0, 0, 0, 0],     // 102
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 103, 0, 0, 0, 0, 0, 0],     // 103
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 0, 0, 0, 0, 0, 0],     // 104
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 105, 0, 0, 0, 0, 0],   // 105
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 106, 0, 0, 0, 0, 0],       // 106
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 107, 0, 0, 0, 0, 0],       // 107
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 0, 0, 0, 0, 0],       // 108
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 109, 0, 0, 0, 0],     // 109
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 110, 0, 0, 0, 0],     // 110
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 111, 0, 0, 0, 0],           // 111
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 0, 0, 0, 0],           // 112
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 113, 0, 0, 0],         // 113
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 114, 0, 0, 0],         // 114
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 115, 0, 0, 0],         // 115
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 0, 0, 0],         // 116
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 117, 0, 0],       // 117
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 118, 0, 0],       // 118
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 119, 0, 0],       // 119
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 0, 0],       // 120
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 121, 0],     // 121
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 122, 0],     // 122
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 123, 0],     // 123
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 0],     // 124
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 125],   // 125
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 126],   // 126
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 127],   // 127
        [4, 8, 12, 24, 28, 48, 52, 56, 60, 64, 68, 74, 78, 82, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 128],   // 128
    ][len as usize]
        .get(level as usize)
}

// ----------- Node related structs -----------------------------------------

#[derive(Debug)]
pub struct NodeSet<AF: AddressFamily, S: Stride>(
    pub Atomic<[MaybeUninit<StoredNode<AF, S>>]>,
);

#[derive(Debug)]
pub enum StoredNode<AF, S>
where
    Self: Sized,
    S: Stride,
    AF: AddressFamily,
{
    NodeWithRef((StrideNodeId<AF>, TreeBitMapNode<AF, S>, NodeSet<AF, S>)),
    Empty,
}

impl<AF: AddressFamily, S: Stride> Default for StoredNode<AF, S> {
    fn default() -> Self {
        StoredNode::Empty
    }
}

impl<AF: AddressFamily, S: Stride> NodeSet<AF, S> {
    pub fn init(size: usize) -> Self {
        trace!("creating space for {} nodes", &size);
        let mut l = Owned::<[MaybeUninit<StoredNode<AF, S>>]>::init(size);
        for i in 0..size {
            l[i] = MaybeUninit::new(StoredNode::Empty);
        }
        NodeSet(l.into())
    }
}

// ----------- Prefix related structs ---------------------------------------

// Unlike StoredNode, we don't need an Empty variant, since we're using
// serial == 0 as the empty value. We're not using an Option here, to
// avoid going outside our atomic procedure.
pub struct StoredPrefix<AF: AddressFamily, Meta: routecore::record::Meta>(
    AtomicUsize,                                // 0 the serial
    pub Option<InternalPrefixRecord<AF, Meta>>, // 1 the record
    PrefixSet<AF, Meta>,                        // 2 the next set of nodes
);

impl<AF: AddressFamily, Meta: routecore::record::Meta>
    StoredPrefix<AF, Meta>
{
    pub(crate) fn empty(size: usize) -> Self {
        StoredPrefix(AtomicUsize::new(0), None, PrefixSet(Atomic::null()))
    }
    // fn len_to_store_bits(len: u8, level: u8) -> Option<&'static u8> {
    //     todo!()
    // }
    // fn get_store_mut(
    //     &mut self,
    //     id: StrideNodeId<AF>,
    // ) -> &mut NodeSet<AF, Stride3> {
    //     todo!()
    // }
    // fn get_store(&self, id: PrefixId<AF>) -> &NodeSet<AF, Stride3> {
    //     todo!()
    // }
    pub(crate) fn is_empty(&self) -> bool {
        if self.0.load(Ordering::Relaxed) == 0 {
            true
        } else {
            false
        }
    }
    pub(crate) fn get_serial_mut(&mut self) -> &mut AtomicUsize {
        &mut self.0
    }
    pub(crate) fn get_serial(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
    fn get_prefix_id(&self) -> PrefixId<AF> {
        if let Some(pfx_rec) = &self.1 {
            PrefixId::new(pfx_rec.net, pfx_rec.len)
        } else {
            panic!("Empty prefix encountered and that's fatal.");
        }
    }
    pub(crate) fn get_next_bucket(&self) -> &PrefixSet<AF, Meta> {
        &self.2
    }
}

pub struct PrefixIter<'a, AF: AddressFamily, M: routecore::record::Meta> {
    pub cur_bucket: &'a PrefixSet<AF, M>, // the bucket we're iterating over
    pub cur_len: u8, // the current prefix length we're iterating over
    pub cur_level : u8, // the level we're iterating over.
    pub cur_max_index: u8, // the maximum index of the level we're iterating over.
    pub cursor: u8, // current index in the level
    pub guard: &'a Guard,
    pub _af: PhantomData<AF>,
}

impl<'a, AF: AddressFamily, M: routecore::record::Meta> Iterator for PrefixIter<'a, AF, M> {
    type Item = &'a InternalPrefixRecord<AF, M>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.cursor += 1;

            if self.cursor == self.cur_max_index {
                // This level is done, go to the next level
                // get the next bucket
                self.cur_bucket = self.cur_bucket.get_by_index(self.cursor as usize, self.guard).get_next_bucket();
                self.cur_level += 1;
                self.cursor = 0; // reset the index for the next level.
                self.cur_max_index = 1 << *prefix_store_bits_4(self.cur_len, self.cur_level).unwrap();

                if self.cur_max_index == 0 {
                    // This length is done too, go to the next length
                    self.cur_len += 1;

                    if self.cur_len == AF::BITS as u8 {
                        // This is the end, my friend
                        return None;
                    }

                    self.cur_level = 0;
                    self.cur_max_index = 1 << *prefix_store_bits_6(self.cur_len, self.cur_level).unwrap();
                }

            }

            match self.cur_bucket.get_by_index(self.cursor as usize, self.guard).1.as_ref() {
                Some(prefix) => {
                    return Some(prefix);
                },
                None => {
                    // This slot is empty, go to the next one.
                    continue;
                }
            }
        }
    }
}

// ----------- FamilyBuckets Trait ------------------------------------------
//
// Implementations of this trait are done by a proc-macro called
// `stride_sizes`from the `rotonda-macros` crate.

#[derive(Debug)]
pub(crate) struct LenToBits([[u8; 10]; 33]);

pub trait NodeBuckets<AF: AddressFamily> {
    fn init() -> Self;
    fn len_to_store_bits(len: u8, level: u8) -> Option<&'static u8>;
    fn get_stride_sizes(&self) -> &[u8];
    fn get_stride_for_id(&self, id: StrideNodeId<AF>) -> u8;
    fn get_store3_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride3>;
    fn get_store4_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride4>;
    fn get_store5_mut(
        &mut self,
        id: StrideNodeId<AF>,
    ) -> &mut NodeSet<AF, Stride5>;
    fn get_store3(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride3>;
    fn get_store4(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride4>;
    fn get_store5(&self, id: StrideNodeId<AF>) -> &NodeSet<AF, Stride5>;
    fn get_strides_len() -> u8;
    fn get_first_stride_size() -> u8;
}

pub trait PrefixBuckets<AF: AddressFamily, M: Meta> {
    fn init() -> Self;
    // fn len_to_store_bits(len: u8, level: u8) -> Option<&'static u8>;
    // fn get<'a>(
    //     &'a self,
    //     id: PrefixId<AF>,
    // ) -> Option<&'a InternalPrefixRecord<AF, M>>;
    fn len(&self) -> usize;
    fn iter<'a>(&'a self, guard: &'a Guard) -> PrefixIter<'a, AF, M>;
    fn remove(
        &mut self,
        id: PrefixId<AF>,
    ) -> Option<InternalPrefixRecord<AF, M>>;
    fn get_root_prefix_set(&self) -> &'_ PrefixSet<AF, M>;
    fn get_root_prefix_set_mut(&mut self) -> &mut PrefixSet<AF, M>;
}

//------------ PrefixSet ----------------------------------------------------

// The PrefixSet is the type that powers pfx_vec, the ARRAY that holds all
// the child prefixes in a node. Since we are storing these prefixes in the
// global store in a HashMap that is keyed on the tuple (addr_bits, len,
// serial number) we can get away with storing ONLY THE SERIAL NUMBER in the
// pfx_vec: The addr_bits and len are implied in the position in the array a
// serial numher has. A PrefixSet doesn't know anything about the node it is
// contained in, so it needs a base address to be able to calculate the
// complete prefix of a child prefix.

#[derive(Debug)]
pub struct PrefixSet<AF: AddressFamily, M: Meta>(
    pub Atomic<[MaybeUninit<StoredPrefix<AF, M>>]>,
);

impl<AF: AddressFamily, M: Meta> std::fmt::Display for PrefixSet<AF, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<AF: AddressFamily, M: Meta> PrefixSet<AF, M> {
    // Collect all PrefixIds into a vec. Since the net and len of the
    // PrefixIds are implied by the position in the pfx_vec we can
    // calculate them with if we know the base address of the node
    // this PrefixSet lives in.
    pub(crate) fn to_vec<'a>(
        &self,
        base_prefix: StrideNodeId<AF>,
        guard: &'a Guard,
    ) -> Vec<&'a InternalPrefixRecord<AF, M>> {
        let prefix_ids =
            unsafe { self.0.load(Ordering::Relaxed, guard).deref() };
        let mut vec = vec![];
        let mut i: usize = 0;
        let mut nibble_len = 1;
        while i < prefix_ids.len() {
            for nibble in 0..1 << nibble_len {
                let this_prefix = unsafe { prefix_ids[i].assume_init_ref() };
                match this_prefix.0
                .load(Ordering::Relaxed) {
                    0 => (),
                    serial => {
                        vec.push(this_prefix.1.as_ref().unwrap());
                    }
                    // serial => vec.push(
                    //     PrefixId::<AF>::new(
                    //         base_prefix
                    //             .get_id()
                    //             .0
                    //             .add_nibble(
                    //                 base_prefix.get_id().1,
                    //                 nibble,
                    //                 nibble_len,
                    //             )
                    //             .0,
                    //         base_prefix.get_id().1 + nibble_len,
                    //     )
                    //     .set_serial(serial),
                    // ),
                }
                i += 1;
            }
            nibble_len += 1;
        }
        vec
        // let mut prefix_ids = Vec::new();
        // let pfxbitarr = self.pfxbitarr.load();
    }

    // pub(crate) fn empty(len: u8) -> Self {
    //     // let arr = array_init::array_init(|_| AtomicUsize::new(0));
    //     let mut v: Vec<AtomicUsize> = Vec::new();
    //     for _ in 0..len {
    //         v.push(AtomicUsize::new(0));
    //     }
    //     PrefixSet(v.into_boxed_slice(), len)
    // }

    pub fn init(size: usize) -> Self {
        trace!("creating space for {} prefixes in prefix_set", &size);
        let mut l = Owned::<[MaybeUninit<StoredPrefix<AF, M>>]>::init(size);
        for i in 0..size {
            l[i] = MaybeUninit::new(StoredPrefix::empty(size));
        }
        PrefixSet(l.into())
    }

    pub(crate) fn get_serial_at<'a>(
        &'a mut self,
        index: usize,
        guard: &'a Guard,
    ) -> &mut AtomicUsize {
        unsafe {
            self.0.load(Ordering::Relaxed, guard).deref_mut()[index as usize]
                .assume_init_mut()
        }
        .get_serial_mut()
    }

    pub(crate) fn get_by_index<'a>(
        &'a self,
        index: usize,
        guard: &'a Guard,
    ) -> &'a StoredPrefix<AF, M> {
        unsafe {
            self.0.load(Ordering::Relaxed, guard).deref()[index as usize]
                .assume_init_ref()
        }
    }
}

// impl<AF: AddressFamily, M: Meta> std::ops::Index<usize> for PrefixSet<AF, M> {
//     type Output = StoredPrefix<AF, M>;

//     fn index(&self, idx: usize) -> &StoredPrefix<AF, M> {
//         let guard = &epoch::pin();
//         unsafe {
//             self.0.load(Ordering::Relaxed, guard).as_ref().unwrap()
//                 [idx as usize]
//                 .assume_init_ref()
//         }
//     }
// }

// impl<AF: AddressFamily, M: Meta> std::ops::IndexMut<usize>
//     for PrefixSet<AF, M>
// {
//     fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
//         let guard = &epoch::pin();
//         unsafe {
//             self.0.load(Ordering::Relaxed, guard).deref_mut()[idx as usize]
//                 .assume_init_mut()
//         }
//     }
// }

// ----------- CustomAllocStorage Implementation ----------------------------
//
// CustomAllocStorage is a storage backend that uses a custom allocator, that
// consitss of arrays that point to other arrays on collision.
#[derive(Debug)]
pub struct CustomAllocStorage<
    AF: AddressFamily,
    Meta: routecore::record::Meta,
    NB: NodeBuckets<AF>,
    PB: PrefixBuckets<AF, Meta>,
> {
    pub(crate) buckets: NB,
    pub prefixes: PB,
    pub default_route_prefix_serial: AtomicUsize,
    _m: PhantomData<Meta>,
    _af: PhantomData<AF>,
}

impl<
        AF: AddressFamily,
        Meta: routecore::record::Meta + MergeUpdate,
        NB: NodeBuckets<AF>,
        PB: PrefixBuckets<AF, Meta>,
    > StorageBackend for CustomAllocStorage<AF, Meta, NB, PB>
{
    type AF = AF;
    type Meta = Meta;

    fn init(root_node: SizedStrideNode<Self::AF>) -> Self {
        trace!("initialize storage backend");

        let mut store = CustomAllocStorage {
            buckets: NB::init(),
            prefixes: PB::init(),
            // len_to_stride_size,
            default_route_prefix_serial: AtomicUsize::new(0),
            _af: PhantomData,
            _m: PhantomData,
        };

        store.store_node(
            StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0),
            root_node,
        );
        store
    }

    fn acquire_new_node_id(
        &self,
        // sort: <<Self as StorageBackend>::NodeType as SortableNodeId>::Sort,
        //
        (prefix_net, sub_prefix_len): (Self::AF, u8),
    ) -> StrideNodeId<Self::AF> {
        StrideNodeId::new_with_cleaned_id(prefix_net, sub_prefix_len)
    }

    // Create a new node in the store with paylaod `next_node`.
    //
    // Next node will be ignored if a node with the same `id` already exists.
    #[allow(clippy::type_complexity)]
    fn store_node(
        &mut self,
        id: StrideNodeId<Self::AF>,
        next_node: SizedStrideNode<Self::AF>,
    ) -> Option<StrideNodeId<Self::AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn Fn(
                &SearchLevel<AF, S>,
                &mut NodeSet<AF, S>,
                TreeBitMapNode<AF, S>,
                u8,
            ) -> Option<StrideNodeId<AF>>,
        }

        let search_level_3 = impl_write_level![Stride3; id;];
        let search_level_4 = impl_write_level![Stride4; id;];
        let search_level_5 = impl_write_level![Stride5; id;];

        trace!("insert node {}: {:?}", id, next_node);
        match next_node {
            SizedStrideNode::Stride3(new_node) => (search_level_3.f)(
                &search_level_3,
                self.buckets.get_store3_mut(id),
                new_node,
                0,
            ),
            SizedStrideNode::Stride4(new_node) => (search_level_4.f)(
                &search_level_4,
                self.buckets.get_store4_mut(id),
                new_node,
                0,
            ),
            SizedStrideNode::Stride5(new_node) => (search_level_5.f)(
                &search_level_5,
                self.buckets.get_store5_mut(id),
                new_node,
                0,
            ),
        }
    }

    // fn store_node_in_store(
    //     _store: &mut StrideWriteStore<Self::AF>,
    //     _id: StrideNodeId<Self::AF>,
    //     _next_node: SizedStrideNode<Self::AF>,
    // ) -> Option<StrideNodeId<Self::AF>> {
    //     unimplemented!()
    // }

    fn update_node(
        &mut self,
        id: StrideNodeId<AF>,
        updated_node: SizedStrideRefMut<AF>,
    ) {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn Fn(
                &SearchLevel<AF, S>,
                &mut NodeSet<AF, S>,
                TreeBitMapNode<AF, S>,
                u8,
            ) -> Option<StrideNodeId<AF>>,
        }

        let search_level_3 = impl_write_level![Stride3; id;];
        let search_level_4 = impl_write_level![Stride4; id;];
        let search_level_5 = impl_write_level![Stride5; id;];

        match updated_node {
            SizedStrideRefMut::Stride3(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3_mut(id),
                    new_node,
                    // Self::len_to_store_bits(id.get_id().1),
                    0,
                )
            }
            SizedStrideRefMut::Stride4(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4_mut(id),
                    new_node,
                    0,
                )
            }
            SizedStrideRefMut::Stride5(new_node) => {
                let new_node = std::mem::take(new_node);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5_mut(id),
                    new_node,
                    0,
                )
            }
        };
    }

    // fn update_node_in_store(
    //     &self,
    //     _store: &mut StrideWriteStore<Self::AF>,
    //     _current_node_id: StrideNodeId<Self::AF>,
    //     _updated_node: SizedStrideNode<Self::AF>,
    // ) {
    //     todo!()
    // }

    fn retrieve_node(
        &self,
        _id: StrideNodeId<AF>,
    ) -> SizedNodeRefOption<'_, Self::AF> {
        unimplemented!()
    }

    fn retrieve_node_with_guard<'a>(
        &'a self,
        id: StrideNodeId<Self::AF>,
        guard: &'a Guard,
    ) -> Option<SizedStrideRef<'a, Self::AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                u8,
                &'a Guard,
            )
                -> Option<SizedStrideRef<'a, AF>>,
        }

        let search_level_3 = impl_search_level![Stride3; id;];
        let search_level_4 = impl_search_level![Stride4; id;];
        let search_level_5 = impl_search_level![Stride5; id;];

        match self.get_stride_for_id(id) {
            3 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3(id),
                    0,
                    guard,
                )
            }

            4 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4(id),
                    0,
                    guard,
                )
            }
            _ => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5(id),
                    0,
                    guard,
                )
            }
        }
    }

    fn retrieve_node_mut_with_guard<'a>(
        &'a self,
        id: StrideNodeId<Self::AF>,
        guard: &'a Guard,
    ) -> Option<SizedStrideRefMut<'a, Self::AF>> {
        struct SearchLevel<'s, AF: AddressFamily, S: Stride> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, S>,
                &NodeSet<AF, S>,
                // [u8; 10],
                u8,
                &'a Guard,
            )
                -> Option<SizedStrideRefMut<'a, AF>>,
        }

        let search_level_3 = impl_search_level_mut![Stride3; id;];
        let search_level_4 = impl_search_level_mut![Stride4; id;];
        let search_level_5 = impl_search_level_mut![Stride5; id;];

        match self.buckets.get_stride_for_id(id) {
            3 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_3.f)(
                    &search_level_3,
                    self.buckets.get_store3(id),
                    0,
                    guard,
                )
            }

            4 => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_4.f)(
                    &search_level_4,
                    self.buckets.get_store4(id),
                    0,
                    guard,
                )
            }
            _ => {
                trace!("retrieve node {} from l{}", id, id.get_id().1);
                (search_level_5.f)(
                    &search_level_5,
                    self.buckets.get_store5(id),
                    0,
                    guard,
                )
            }
        }
    }

    fn store_node_with_guard(
        &self,
        _current_node: SizedNodeRefOption<Self::AF>,
        _next_node: SizedStrideNode<AF>,
        _guard: &epoch::Guard,
    ) -> Option<StrideNodeId<Self::AF>> {
        unimplemented!()
    }

    fn get_root_node_id(&self, _stride_size: u8) -> StrideNodeId<Self::AF> {
        StrideNodeId::dangerously_new_with_id_as_is(AF::zero(), 0)
    }

    fn get_nodes_len(&self) -> usize {
        0
    }

    // Prefixes related methods

    fn load_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial.load(Ordering::Acquire)
    }

    fn increment_default_route_prefix_serial(&self) -> usize {
        self.default_route_prefix_serial
            .fetch_add(1, Ordering::Acquire)
    }

    fn acquire_new_prefix_id(
        &self,
        prefix: &InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> PrefixId<Self::AF> {
        PrefixId::<AF>::new(prefix.net, prefix.len).set_serial(1)
    }

    fn store_prefix(
        &self,
        id: PrefixId<Self::AF>,
        pfx_rec: InternalPrefixRecord<Self::AF, Self::Meta>,
    ) -> Option<PrefixId<Self::AF>> {
        struct SearchLevel<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                InternalPrefixRecord<AF, M>,
                u8,
            ) -> Option<PrefixId<AF>>,
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, Meta>,
                 prefix_set: &PrefixSet<AF, Meta>,
                 new_prefix: InternalPrefixRecord<AF, Meta>,
                 mut level: u8| {
                let last_level = if level > 0 {
                    *<NB as NodeBuckets<AF>>::len_to_store_bits(
                        id.get_len(),
                        level - 1,
                    )
                    .unwrap()
                } else {
                    0
                };
                let this_level = *<NB as NodeBuckets<AF>>::len_to_store_bits(
                    id.get_len(),
                    level,
                )
                .unwrap();
                let index = ((id.get_net().dangerously_truncate_to_u32()
                    << last_level)
                    >> (AF::BITS - (this_level - last_level)))
                    as usize;
                trace!("{:032b}", id.get_net().dangerously_truncate_to_u32());
                trace!("this_level {}", this_level);
                trace!("last_level {}", last_level);
                trace!("id {:?}", id);
                trace!("calculated index {}", index);
                trace!("level {}", level);
                trace!(
                    "bits_division {}",
                    <NB as NodeBuckets<AF>>::len_to_store_bits(
                        id.get_len(),
                        level
                    )
                    .unwrap()
                );
                let guard = &epoch::pin();
                let mut prefixes =
                    prefix_set.0.load(Ordering::Relaxed, guard);
                // trace!("nodes {:?}", unsafe { unwrapped_nodes.deref_mut().len() });
                let prefix_ref = unsafe { &mut prefixes.deref_mut()[index] };
                match unsafe { prefix_ref.assume_init_mut() } {
                    // No node exists, so we crate one here.
                    StoredPrefix(_serial, None, _next_set) => {
                        trace!("Empty node found, creating new prefix {} len{} lvl{}", id.get_net(), id.get_len(), level + 1);
                        let next_level =
                            <NB as NodeBuckets<AF>>::len_to_store_bits(
                                id.get_len(),
                                level + 1,
                            )
                            .unwrap();
                        trace!("next level {}", next_level);
                        trace!(
                            "creating {} prefixes",
                            1 << (next_level - this_level)
                        );
                        std::mem::swap(
                            prefix_ref,
                            &mut MaybeUninit::new(StoredPrefix(
                                AtomicUsize::new(0),
                                Some(new_prefix),
                                PrefixSet::init(
                                    (1 << (next_level - this_level)) as usize,
                                ),
                            )),
                        );
                        // ABA Baby!
                        match prefix_set.0.compare_exchange(
                            prefixes,
                            prefixes,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            guard,
                        ) {
                            Ok(_) => Some(id),
                            Err(_) => {
                                // TODO: This needs some kind of backoff,
                                // I guess.
                                loop {
                                    trace!("contention while creating prefix {:?}", id);
                                    match prefix_set.0.compare_exchange(
                                        prefixes,
                                        prefixes,
                                        Ordering::SeqCst,
                                        Ordering::SeqCst,
                                        guard,
                                    ) {
                                        Ok(_) => {
                                            return Some(id);
                                        }
                                        Err(_) => {}
                                    };
                                }
                            }
                        };
                        Some(id)
                    }
                    // A node exists, since `store_node` only creates new
                    // nodes, we should not get here with the SAME
                    // esiting node as already in place.
                    StoredPrefix(_serial, Some(prefix), next_set) => {
                        trace!("node here exists {:?}", prefix);
                        trace!("node_id {:032b}", prefix.net);
                        trace!("id {:?}", id);
                        trace!("     id {:032b}", id.get_net());
                        if id == PrefixId::new(prefix.net, prefix.len) {
                            trace!("found node {:?}, STOP", id);
                            // Node already exists, nothing to do
                            panic!(
                                "prefix already exists, should not happen"
                            );
                            // return Some($id);
                        };
                        level += 1;
                        trace!("Collision with node_id {}, move to next level: {:?} len{} next_lvl{} index {}", prefix, id, id.get_len(), level, index);
                        match <NB as NodeBuckets<AF>>::len_to_store_bits(
                            id.get_len(),
                            level,
                        ) {
                            // on to the next level!
                            Some(next_bit_shift) if next_bit_shift > &0 => {
                                (search_level.f)(
                                    search_level,
                                    next_set,
                                    new_prefix,
                                    level,
                                )
                            }
                            // There's no next level!
                            _ => panic!(
                                "out of storage levels, current level is {}",
                                level
                            ),
                        }
                    }
                }
            },
        };

        trace!("insert prefix {:?}: {:?}", id, pfx_rec);
        (search_level.f)(
            &search_level,
            self.prefixes.get_root_prefix_set(),
            pfx_rec,
            0,
        )
        // self.prefixes.insert(pfx_rec.into(), pfx_rec);
    }

    fn retrieve_prefix(
        &self,
        id: PrefixId<Self::AF>,
    ) -> Option<InternalPrefixRecord<Self::AF, Self::Meta>> {
        let guard = epoch::pin();
        struct SearchLevel<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                u8,
                &'a Guard,
                // InternalPrefixRecord<AF, M>,
            )
                -> Option<InternalPrefixRecord<AF, M>>,
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, Meta>,
                 prefix_set: &PrefixSet<AF, Meta>,
                 //  new_prefix: InternalPrefixRecord<AF, Meta>,
                 mut level: u8,
                 guard: &Guard| {
                let last_level = if level > 0 {
                    *<NB as NodeBuckets<AF>>::len_to_store_bits(
                        id.get_len(),
                        level - 1,
                    )
                    .unwrap()
                } else {
                    0
                };
                let this_level = *<NB as NodeBuckets<AF>>::len_to_store_bits(
                    id.get_len(),
                    level,
                )
                .unwrap();
                let index = ((id.get_net().dangerously_truncate_to_u32()
                    << last_level)
                    >> (AF::BITS - (this_level - last_level)))
                    as usize;
                trace!("{:032b}", id.get_net().dangerously_truncate_to_u32());
                trace!("this_level {}", this_level);
                trace!("last_level {}", last_level);
                trace!("id {:?}", id);
                trace!("calculated index {}", index);
                trace!("level {}", level);
                trace!(
                    "bits_division {}",
                    <NB as NodeBuckets<AF>>::len_to_store_bits(
                        id.get_len(),
                        level
                    )
                    .unwrap()
                );
                let mut prefixes =
                    prefix_set.0.load(Ordering::Relaxed, guard);
                // trace!("nodes {:?}", unsafe { unwrapped_nodes.deref_mut().len() });
                let prefix_ref = unsafe { &mut prefixes.deref_mut()[index] };
                match unsafe { prefix_ref.assume_init_ref() } {
                    StoredPrefix(_serial, Some(pfx_rec), next_set) => {
                        if id == PrefixId::from(pfx_rec) {
                            trace!("found requested prefix {:?}", id);
                            return Some(pfx_rec.clone());
                        };
                        level += 1;
                        (search_level.f)(
                            &search_level,
                            &next_set,
                            level,
                            guard,
                        )
                    }
                    StoredPrefix(_serial, None, _next_set) => None,
                }
            },
        };

        (search_level.f)(
            &search_level,
            self.prefixes.get_root_prefix_set(),
            0,
            &guard,
        )
    }

    fn retrieve_prefix_with_guard<'a>(
        &'a self,
        id: PrefixId<Self::AF>,
        guard: &'a Guard,
    ) -> Option<&InternalPrefixRecord<Self::AF, Self::Meta>> {
        struct SearchLevel<'s, AF: AddressFamily, M: routecore::record::Meta> {
            f: &'s dyn for<'a> Fn(
                &SearchLevel<AF, M>,
                &PrefixSet<AF, M>,
                u8,
                &'a Guard,
                // InternalPrefixRecord<AF, M>,
            )
                -> Option<&'a InternalPrefixRecord<AF, M>>,
        }

        let search_level = SearchLevel {
            f: &|search_level: &SearchLevel<AF, Meta>,
                 prefix_set: &PrefixSet<AF, Meta>,
                 //  new_prefix: InternalPrefixRecord<AF, Meta>,
                 mut level: u8,
                 guard: &Guard| {
                let last_level = if level > 0 {
                    *<NB as NodeBuckets<AF>>::len_to_store_bits(
                        id.get_len(),
                        level - 1,
                    )
                    .unwrap()
                } else {
                    0
                };
                let this_level = *<NB as NodeBuckets<AF>>::len_to_store_bits(
                    id.get_len(),
                    level,
                )
                .unwrap();
                let index = ((id.get_net().dangerously_truncate_to_u32()
                    << last_level)
                    >> (AF::BITS - (this_level - last_level)))
                    as usize;
                trace!("{:032b}", id.get_net().dangerously_truncate_to_u32());
                trace!("this_level {}", this_level);
                trace!("last_level {}", last_level);
                trace!("id {:?}", id);
                trace!("calculated index {}", index);
                trace!("level {}", level);
                trace!(
                    "bits_division {}",
                    <NB as NodeBuckets<AF>>::len_to_store_bits(
                        id.get_len(),
                        level
                    )
                    .unwrap()
                );
                let mut prefixes =
                    prefix_set.0.load(Ordering::Relaxed, guard);
                // trace!("nodes {:?}", unsafe { unwrapped_nodes.deref_mut().len() });
                let prefix_ref = unsafe { &mut prefixes.deref_mut()[index] };
                match unsafe { prefix_ref.assume_init_ref() } {
                    StoredPrefix(_serial, Some(pfx_rec), next_set) => {
                        if id == PrefixId::new(pfx_rec.net, pfx_rec.len) {
                            trace!("found requested prefix {:?}", id);
                            return Some(&pfx_rec);
                        };
                        level += 1;
                        (search_level.f)(
                            &search_level,
                            &next_set,
                            level,
                            guard,
                        )
                    }
                    StoredPrefix(_serial, None, _next_set) => None,
                }
            },
        };

        (search_level.f)(
            &search_level,
            self.prefixes.get_root_prefix_set(),
            0,
            &guard,
        )
    }

    fn remove_prefix(
        &mut self,
        index: PrefixId<Self::AF>,
    ) -> Option<InternalPrefixRecord<Self::AF, Self::Meta>> {
        match index.is_empty() {
            false => self.prefixes.remove(index), //.map(|p| p.1),
            true => None,
        }
    }

    // fn get_prefixes(&'_ self) -> &'_ PrefixBuckets<Self::AF, Self::Meta> {
    //     &self.prefixes
    // }

    // fn get_prefixes_clear(&self) -> &PrefixHashMap<Self::AF, Self::Meta> {
    //     &self.prefixes
    // }

    fn get_prefixes_len(&self) -> usize {
        self.prefixes.len()
    }

    // Stride related methods

    fn get_stride_for_id(&self, id: StrideNodeId<Self::AF>) -> u8 {
        self.buckets.get_stride_for_id(id)
    }

    // fn get_stride_for_id_with_read_store(
    //     &self,
    //     id: StrideNodeId<Self::AF>,
    // ) -> (StrideNodeId<Self::AF>, StrideReadStore<Self::AF>) {
    //     todo!()
    // }

    // fn get_stride_for_id_with_write_store(
    //     &self,
    //     id: StrideNodeId<Self::AF>,
    // ) -> (StrideNodeId<Self::AF>, StrideWriteStore<Self::AF>) {
    //     todo!()
    // }

    fn get_stride_sizes(&self) -> &[u8] {
        self.buckets.get_stride_sizes()
    }

    fn get_strides_len() -> u8 {
        NB::get_strides_len()
    }

    fn get_first_stride_size() -> u8 {
        NB::get_first_stride_size()
    }
}
