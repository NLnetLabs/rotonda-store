mod oncebox;

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
