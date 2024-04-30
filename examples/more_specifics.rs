use rotonda_store::meta_examples::PrefixAs;
use rotonda_store::prelude::*;
use rotonda_store::prelude::multi::*;

use rotonda_store::AddressFamily;
use inetnum::addr::Prefix;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // type StoreType = InMemStorage<u32, PrefixAs>;
    let tree_bitmap =
        MultiThreadedStore::<PrefixAs>::new()?;
    let pfxs = vec![
        Prefix::new_relaxed(
            0b0000_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            0,
        ),
        Prefix::new_relaxed(
            0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
            32,
        ),
        Prefix::new(
            0b0000_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b0001_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b0010_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b0011_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b0100_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b0101_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b0110_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b0111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1000_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1001_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1010_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1011_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1100_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1101_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1110_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            9,
        ),
        Prefix::new(
            0b1111_0000_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            9,
        ),
        Prefix::new(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            12,
        ),
        Prefix::new_relaxed(
            0b1111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            9,
        ),
        Prefix::new_relaxed(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            9,
        ),
        Prefix::new_relaxed(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            10,
        ),
        Prefix::new_relaxed(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            11,
        ),
        Prefix::new_relaxed(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            12,
        ),
        Prefix::new_relaxed(
            0b0111_0111_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            12,
        ),
        Prefix::new_relaxed(
            0b0111_0111_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            13,
        ),
        Prefix::new_relaxed(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            13,
        ),
        Prefix::new_relaxed(
            0b0111_0111_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            14,
        ),
        Prefix::new_relaxed(
            0b0111_0111_0100_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            14,
        ),
        Prefix::new_relaxed(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            14,
        ),
        Prefix::new_relaxed(
            0b0111_0111_1100_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            14,
        ),
        Prefix::new_relaxed(
            0b1110_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(std::net::Ipv4Addr::new(192, 0, 0, 0).into(), 23),
        Prefix::new_relaxed(std::net::Ipv4Addr::new(192, 0, 0, 0).into(), 16),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(192, 0, 10, 0).into(),
            23,
        ),
        Prefix::new_relaxed(std::net::Ipv4Addr::new(192, 0, 9, 0).into(), 24),
        Prefix::new_relaxed(std::net::Ipv4Addr::new(193, 0, 0, 0).into(), 23),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(193, 0, 10, 0).into(),
            23,
        ),
        Prefix::new_relaxed(std::net::Ipv4Addr::new(209, 0, 0, 0).into(), 16),
        Prefix::new_relaxed(std::net::Ipv4Addr::new(193, 0, 9, 0).into(), 24),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(193, 0, 10, 0).into(),
            24,
        ),
        Prefix::new_relaxed(
            0b0011_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b1000_0011_1000_1111_0000_0000_0000_0000_u32.into_ipaddr(),
            11,
        ),
        Prefix::new_relaxed(
            0b1000_0010_0101_0111_1111_1000_0000_0000_u32.into_ipaddr(),
            13,
        ),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 24),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 25),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 128).into(), 25),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 26),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 64).into(), 26),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 128).into(), 26),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 192).into(), 26),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 193).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 3).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 4).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 5).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 16).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 12).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 63).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 127).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 128).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 129).into(), 32),
        Prefix::new_relaxed(
            0b1111_1111_0000_0001_0000_0000_0000_0000_u32.into_ipaddr(),
            12,
        ),
        Prefix::new_relaxed(
            0b1111_1111_0011_0111_0000_0000_0000_0000_u32.into_ipaddr(),
            17,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(100, 0, 12, 0).into(),
            24,
        ),
        Prefix::new(
            0b0000_0001_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            24,
        ),
        Prefix::new_relaxed(std::net::Ipv4Addr::new(1, 0, 128, 0).into(), 24),
    ];

    for pfx in pfxs.into_iter() {
        // println!("insert {:?}", pfx);
        let p: Prefix = pfx.unwrap();
        tree_bitmap.insert(&p, Record::new(0,0, RouteStatus::Active, PrefixAs(666)), None)?;
    }
    println!("------ end of inserts\n");
    // println!(
    //     "{:#?}",
    //     tree_bitmap
    //         .prefixes_iter()
    //         .enumerate()
    //         .collect::<Vec<(usize, _)>>()
    // );
    // println!(
    //     "{:#?}",
    //     tree_bitmap
    //         .prefixes_iter()
    //         .enumerate()
    //         .collect::<Vec<(usize, _)>>()
    // );

    // println!("pfxbitarr: {:032b}", tree_bitmap.0.pfxbitarr);

    for spfx in &[
        // Prefix::new(0b0000_0000_0000_0000_0000_0000_0000_0000_u32, 0),
        // Prefix::new(0b0000_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b0001_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b0010_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b0011_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b0100_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b0101_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b0110_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b0111_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b1000_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b1001_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b1010_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b1100_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b1101_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b1110_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b0111_0000_0000_0000_0000_0000_0000_0000_u32, 4),
        // Prefix::new(0b1111_0000_0000_0000_0000_0000_0000_0000_u32, 9),
        // Prefix::new_relaxed(std::net::Ipv4Addr::new(255, 1, 0, 0).into(), 24),
        // Prefix::new_relaxed(std::net::Ipv4Addr::new(224, 0, 0, 0).into(), 4),
        // Prefix::new_relaxed(std::net::Ipv4Addr::new(224, 0, 0, 0).into(), 8),
        // Prefix::new_relaxed(0b0100_0001_0000_0000_0000_0000_1111_1111_u32, 32),
        // Prefix::new_relaxed(std::net::Ipv4Addr::new(1, 0, 100, 10).into(), 16),
        // Prefix::new_relaxed(std::net::Ipv4Addr::new(131, 143, 0, 0).into(), 11),

        // Prefix::new_relaxed(std::net::Ipv4Addr::new(12, 0, 0, 34).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 24),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 23),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 25),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 26),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 192).into(), 26),
        // Prefix::new(std::net::Ipv4Addr::new(193, 0, 3, 0).into(), 23),
        // Prefix::new(std::net::Ipv4Addr::new(193, 0, 10, 0).into(), 23),
        // Prefix::new(0b0111_0111_1000_0000_0000_0000_0000_0000_u32, 14),
        // Prefix::new(std::net::Ipv4Addr::new(193, 0, 10, 0).into(), 24),
        // Prefix::new(std::net::Ipv4Addr::new(100, 0, 12, 0).into(), 24),
        // Prefix::new(std::net::Ipv4Addr::new(255, 255, 255, 255).into(), 32),
        // Prefix::new(std::net::Ipv4Addr::new(1, 0, 0, 0).into(), 24),
        // Prefix::new(std::net::Ipv4Addr::new(1, 0, 128, 0).into(), 24),
    ] {
        println!("search for: {:?}", spfx);
        let guard = &epoch::pin();
        let s_spfx = tree_bitmap.match_prefix(
            &spfx.unwrap(),
            &MatchOptions {
                match_type: MatchType::ExactMatch,
                include_all_records: false,
                include_less_specifics: true,
                include_more_specifics: true,
            },
            guard
        );
        println!("em/m-s: {:#?}", s_spfx);
        println!("-----------");
    }
    Ok(())
}
