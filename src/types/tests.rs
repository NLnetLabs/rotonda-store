#[cfg(test)]
use std::error::Error;

//------------ AddressFamily bit flippers -----------------------------------

#[test]
fn test_af_1() -> Result<(), Box<dyn Error>> {
    use crate::tree_bitmap::StrideNodeId;
    use crate::types::BitSpan;
    use crate::AddressFamily;
    use crate::IPv4;

    let bit_addr: IPv4 = 0b1111_1111_1111_1111_1111_1111_1111_1111.into();
    let base_prefix =
        StrideNodeId::dangerously_new_with_id_as_is(bit_addr, 32);

    assert_eq!(base_prefix.bits(), bit_addr);
    assert_eq!(base_prefix.truncate_to_len().bits(), base_prefix.bits());
    assert_eq!(
        StrideNodeId::dangerously_new_with_id_as_is(
            base_prefix.bits().truncate_to_len(28),
            28
        )
        .add_bit_span(BitSpan {
            bits: 0b0101,
            len: 4
        })
        .bits(),
        0b1111_1111_1111_1111_1111_1111_1111_0101
    );

    Ok(())
}

#[test]
fn test_af_2() -> Result<(), Box<dyn Error>> {
    use crate::IPv4;
    use crate::{tree_bitmap::StrideNodeId, types::BitSpan};

    let bit_addr: IPv4 = 0b1111_1111_1111_1111_1111_1111_1111_1111.into();
    let nu_prefix = StrideNodeId::dangerously_new_with_id_as_is(bit_addr, 8);

    assert_eq!(nu_prefix.bits(), bit_addr);
    assert_eq!(
        nu_prefix.truncate_to_len().bits(),
        0b1111_1111_0000_0000_0000_0000_0000_0000
    );

    assert_eq!(
        nu_prefix
            .add_bit_span(BitSpan {
                bits: 0b1010,
                len: 4
            })
            .bits(),
        0b1111_1111_1010_0000_0000_0000_0000_0000
    );
    assert_eq!(
        nu_prefix
            .truncate_to_len()
            .add_bit_span(BitSpan {
                bits: 0b1010,
                len: 4
            })
            .bits(),
        0b1111_1111_1010_0000_0000_0000_0000_0000
    );

    Ok(())
}
