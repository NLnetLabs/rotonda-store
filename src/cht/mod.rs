mod oncebox;

use num_traits::Saturating;
pub(crate) use oncebox::OnceBoxSlice;

use crate::rib::STRIDE_SIZE;

pub(crate) trait Value {
    fn init_with_p2_children(size: usize) -> Self;
    fn init_leaf() -> Self;
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

    pub(crate) fn root_for_len(&self, len: u8) -> &V {
        &self.0[len as usize / STRIDES_PER_BUCKET]
    }
}

pub(crate) fn bits_for_len(len: u8, lvl: u8) -> u8 {
    let res = STRIDE_SIZE * (lvl + 1);
    if res < len {
        res
    } else if res >= len + STRIDE_SIZE {
        0
    } else {
        len
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
    4_u8.saturating_sub((4 * (lvl + 1)).saturating_sub(len))
}
