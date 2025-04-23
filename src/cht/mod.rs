mod oncebox;

pub(crate) use oncebox::OnceBoxSlice;

use crate::rib::STRIDE_SIZE;

pub(crate) trait Value {
    fn init_with_p2_children(size: usize) -> Self;
}

#[derive(Debug)]
pub(crate) struct Cht<
    V,
    const ROOT_SIZE: usize,
    const STRIDES_PER_BUCKET: usize,
>([V; ROOT_SIZE]);

impl<V: Value, const ROOT_SIZE: usize, const STRIDES_PER_BUCKET: usize>
    Cht<V, ROOT_SIZE, STRIDES_PER_BUCKET>
{
    pub(crate) fn init() -> Self {
        Self(std::array::from_fn::<_, ROOT_SIZE, _>(|_| {
            V::init_with_p2_children(STRIDE_SIZE as usize)
        }))
    }

    // There cannot be a root node for a prefix length that has NO slots,
    // STRIDES_PER_BICKET (a instance wide const) should always be bigger
    // than 0.
    #[allow(clippy::indexing_slicing)]
    pub(crate) fn root_for_len(&self, len: u8) -> &V {
        &self.0[len as usize / STRIDES_PER_BUCKET]
    }
}

// This output of this function is exactly same as this (for all values of len
// and lvl we care for at least):
//
// let res = 4 * (lvl + 1);
// if res < len {
//     4
// } else if res >= len + 4 {
//     0
// } else if len % 4 == 0 {
//     4
// } else {
//     len % 4
// }
//
// The gist of this function is that, we want exactly the numnber of slots in
// our NodeSet that we can fill. This means:
// - for any len smaller than STRIDE_SIZE (4), we only have one level, and
//   that level takes `len` slots. There are no next levels in in these lens.
// - len of STRIDE_SIZE and bigger have as many levels as can fit full
//   STRIDE_SIZES, so with len 4, that is still one level (lvl = 0), for
//   len 5 that is two levels, one of size 4 (lvl = 0), and one of size 1
//   (lvl = 1). From len 9 there's three levels and so on.
// - The first len, level combination beyond the max. size of lvl should
//   return a 0, so the looper knows that it has to go to the next len.
//
// This is the output of the first values of len, lvl
//
// len, lvl : input parameters
// ts       : total size of id
// ns       : number of child slots for the NodeSet for this len, lvl
//
// len  lvl  ts ns
// 00   00   00 0
// 01   00   01 1
// 01   01   00 0
// 02   00   02 2
// 02   01   00 0
// 03   00   03 3
// 03   01   00 0
// 04   00   04 4
// 04   01   00 0
// 05   00   04 4
// 05   01   05 1
// 05   02   00 0
// 06   00   04 4
// 06   01   06 2
// 06   02   00 0
// 07   00   04 4
// 07   01   07 3
// 07   02   00 0
// 08   00   04 4
// 08   01   08 4
// 08   02   00 0
// 09   00   04 4
// 09   01   08 4
// 09   02   09 1
// 09   03   00 0
// 10   00   04 4
// ...
pub fn nodeset_size(len: u8, lvl: u8) -> u8 {
    // The multiplication here will only ever overflow if the (len, lvl) input
    // is out of bounds for IPv4 or IPv6 prefixes. Therefore we are including
    // a debug_assert here to panic in debug mode if this happens.  In release
    // compiles this may NOT be noticable, because the undefined behaviour
    // is most probably the desired behaviour (saturating). But it's UB for a
    // reason, so we should not rely on it, and verify that we are not hitting
    // that behaviour.
    debug_assert!(4_u8.checked_mul(lvl + 1).is_some());
    4_u8.saturating_sub((4 * (lvl + 1)).saturating_sub(len))
}

// This test tests both that the outocome of our optimized nodeset_size
// function is the same as our 'naive' approach, and that we do not rely on
// undefined behaviour because of overflowing multiplication or addition.
#[test]
fn test_nodeset_size_valid_range() {
    for len in 0..128 {
        for lvl in 0..(len / 4) {
            let res = 4 * (lvl + 1);
            let nss = if res < len {
                4
            } else if res >= len + 4 {
                0
            } else if len % 4 == 0 {
                4
            } else {
                len % 4
            };
            debug_assert_eq!(nss, nodeset_size(len, lvl));
        }
    }
}

// The value of the set of the parent of this one. used to calculate the shift
// offset in the hash for the CHT, so this is basically the `nodeset_size`
// shifted one (len, lvl) combination downwards.
//
// len lvl prev
// 00  00  00
// 01  00  00
// 01  01  01
// 02  00  00
// 02  01  02
// 03  00  00
// 03  01  03
// 04  00  00
// 04  01  04
// 05  00  00
// 05  01  04
// 05  02  05
// 06  00  00
// 06  01  04
// 06  02  06
// 07  00  00
// 07  01  04
// 07  02  07
// 08  00  00
// 08  01  04
// 08  02  08
// 09  00  00
// 09  01  04
// 09  02  08
// 09  03  09
pub fn prev_node_size(len: u8, lvl: u8) -> u8 {
    // The multiplication here will only ever overflow if the (len, lvl) input
    // is out of bounds for IPv4 or IPv6 prefixes. Therefore we are including
    // a debug_assert here to panic in debug mode if this happens.  In release
    // compiles this may NOT be noticable, because the undefined behaviour
    // is most probably the desired behaviour (saturating). But it's UB for a
    // reason, so we should not rely on it, and verify that we are not hitting
    // that behaviour.
    debug_assert!(4_u8.checked_mul(lvl).is_some());
    (lvl * 4) - lvl.saturating_sub(len >> 2) * ((lvl * 4).saturating_sub(len))
}

// In this test we're only testing to no rely on undefined behaviour for all
// inputs in the valid range
#[test]
fn test_prev_node_size_valid_range() {
    for len in 0..128 {
        for lvl in 0..(len / 4) {
            prev_node_size(len, lvl);
        }
    }
}
