use zerocopy::{NetworkEndian, U32};

// max len 128!

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
    for b in 0..=u32::MAX {
        if b % (1024 * 256) == 0 {
            print!(".");
        }
        for l in 0..=32 {
            let tl1 = truncate_to_len(U32::<NetworkEndian>::new(b), l);
            let tl2 = branchless_trunc(U32::<NetworkEndian>::new(b), l);
            // println!("{:032b} {:032b}", tl1, tl2);
            assert_eq!(tl1, tl2);
        }
    }
}
