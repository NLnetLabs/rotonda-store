use std::{sync::Arc, thread};

use rotonda_store::prelude::*;
use rotonda_store::prelude::multi::*;
use rotonda_store::meta_examples::NoMeta;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tree_bitmap = Arc::new(MultiThreadedStore::<NoMeta>::new()?);

    let _: Vec<_> = (0..16)
        .map(|i: i32| {
            let tree_bitmap = tree_bitmap.clone();

            thread::Builder::new().name(i.to_string()).spawn(move || {
                let pfxs = get_pfx();

                for pfx in pfxs.into_iter() {
                    println!("insert {}", pfx.unwrap());

                    match tree_bitmap
                        .insert(
                            &pfx.unwrap(), 
                            Record::new(0, 0, RouteStatus::Active, NoMeta::Empty),
                            None
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            println!("{}", e);
                        }
                    };
                }
            }).unwrap()
        })
        .map(|t| t.join())
        .collect();

    let guard = &epoch::pin();

    println!("------ end of inserts\n");

    for spfx in &get_pfx() {
        println!("search for: {}", spfx.unwrap());
        let s_spfx = tree_bitmap.match_prefix(
            &spfx.unwrap(),
            &MatchOptions {
                match_type: rotonda_store::MatchType::ExactMatch,
                include_all_records: false,
                include_less_specifics: true,
                include_more_specifics: true,
            },
            guard,
        );
        println!("query result");
        println!("{}", s_spfx);
        println!("{}", s_spfx.more_specifics.unwrap());
        println!("-----------");
    }
    Ok(())
}

fn get_pfx() -> Vec<Result<Prefix, rotonda_store::addr::PrefixError>> {
    vec![
        Prefix::new_relaxed(
            0b0000_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            0,
        ),
        Prefix::new_relaxed(
            0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
            32,
        ),
        Prefix::new_relaxed(
            0b0000_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b0001_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b0010_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b0011_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b0100_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b0101_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b0110_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b0111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b1000_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b1001_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b1010_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b1011_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b1100_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b1101_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b1110_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b1111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new_relaxed(
            0b1111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            9,
        ),
        Prefix::new_relaxed(
            0b1111_0000_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            9,
        ),
        Prefix::new_relaxed(
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
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
            24,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
            25,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 128).into(),
            25,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
            26,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 64).into(),
            26,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 128).into(),
            26,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 192).into(),
            26,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 3).into(),
            32,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 4).into(),
            32,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 5).into(),
            32,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 16).into(),
            32,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 12).into(),
            32,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 63).into(),
            32,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 127).into(),
            32,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 128).into(),
            32,
        ),
        Prefix::new_relaxed(
            std::net::Ipv4Addr::new(130, 55, 240, 129).into(),
            32,
        ),
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
        Prefix::new_relaxed(
            0b0000_0001_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            24,
        ),
        Prefix::new_relaxed(std::net::Ipv4Addr::new(1, 0, 128, 0).into(), 24),
    ]
}
