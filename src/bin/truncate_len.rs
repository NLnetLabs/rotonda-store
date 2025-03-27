use zerocopy::{NetworkEndian, U32};

// max len 128
// fn truncate_to_len(bits: U32<NetworkEndian>, len: u8) -> u32 {
//     match len {
//         0 => U32::from(0),
//         1..=31 => (bits >> (32 - len as u32)) << (32 - len as u32),
//         32 => bits,
//         len => panic!("Can't truncate to more than 128 bits: {}", len),
//     }
// }

fn truncate_to_len(bits: U32<NetworkEndian>, len: u8) -> U32<NetworkEndian> {
    match len {
        0 => U32::new(0),
        1..=31 => {
            (bits >> U32::from(32 - len as u32)) << U32::from(32 - len as u32)
        }
        32 => bits,
        len => panic!("Can't truncate to more than 128 bits: {}", len),
    }
}

fn branchless_trunc(bits: U32<NetworkEndian>, len: u8) -> u32 {
    (bits
        & ((1_u32.rotate_right(len as u32)
            ^ 1_u32.saturating_sub(len as u32))
        .wrapping_sub(1)
            ^ u32::MAX))
        .into()
}

fn main() {
    let ex_bits =
        U32::<NetworkEndian>::from(0b10_1110_0100_0000_0000_0000_0000_0000);
    for l in 0..=32 {
        let tl1 = truncate_to_len(ex_bits, l);
        let tl2 = branchless_trunc(ex_bits, l);
        println!(
            "{:032b} {:032b}",
            branchless_trunc(ex_bits, l),
            truncate_to_len(ex_bits, l)
        );
        assert_eq!(tl1, tl2);
    }
}
