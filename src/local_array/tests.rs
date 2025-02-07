#[cfg(test)]
use std::error::Error;

//------------ AddressFamily bit flippers -----------------------------------

#[test]
fn test_af_1() -> Result<(), Box<dyn Error>> {
    use crate::local_array::bit_span::BitSpan;
    use crate::prelude::multi::StrideNodeId;
    use crate::AddressFamily;
    use crate::IPv4;

    let bit_addr: IPv4 = 0b1111_1111_1111_1111_1111_1111_1111_1111;
    let base_prefix =
        StrideNodeId::dangerously_new_with_id_as_is(bit_addr, 32);

    assert_eq!(base_prefix.get_id().0, bit_addr);
    assert_eq!(
        base_prefix.truncate_to_len().get_id().0,
        base_prefix.get_id().0
    );
    assert_eq!(
        StrideNodeId::dangerously_new_with_id_as_is(
            base_prefix.get_id().0.truncate_to_len(28),
            28
        )
        .add_bit_span(BitSpan {
            bits: 0b0101,
            len: 4
        })
        .get_id()
        .0,
        0b1111_1111_1111_1111_1111_1111_1111_0101
    );

    Ok(())
}

#[test]
fn test_af_2() -> Result<(), Box<dyn Error>> {
    use crate::local_array::bit_span::BitSpan;
    use crate::prelude::multi::StrideNodeId;
    use crate::IPv4;

    let bit_addr: IPv4 = 0b1111_1111_1111_1111_1111_1111_1111_1111;
    let nu_prefix = StrideNodeId::dangerously_new_with_id_as_is(bit_addr, 8);

    assert_eq!(nu_prefix.get_id().0, bit_addr);
    assert_eq!(
        nu_prefix.truncate_to_len().get_id().0,
        0b1111_1111_0000_0000_0000_0000_0000_0000
    );

    assert_eq!(
        nu_prefix
            .add_bit_span(BitSpan {
                bits: 0b1010,
                len: 4
            })
            .get_id()
            .0,
        0b1111_1111_1010_0000_0000_0000_0000_0000
    );
    assert_eq!(
        nu_prefix
            .truncate_to_len()
            .add_bit_span(BitSpan {
                bits: 0b1010,
                len: 4
            })
            .get_id()
            .0,
        0b1111_1111_1010_0000_0000_0000_0000_0000
    );

    Ok(())
}
