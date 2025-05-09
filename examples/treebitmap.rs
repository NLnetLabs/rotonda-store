use inetnum::addr::Prefix;
use rotonda_store::match_options::IncludeHistory;
use rotonda_store::match_options::MatchOptions;
use rotonda_store::match_options::MatchType;
use rotonda_store::prefix_record::Record;
use rotonda_store::prefix_record::RouteStatus;
use rotonda_store::rib::config::MemoryOnlyConfig;
use rotonda_store::rib::StarCastRib;
use rotonda_store::test_types::NoMeta;
use rotonda_store::IntoIpAddr;

type Prefix4<'a> = Prefix;

type Type = Result<(), Box<dyn std::error::Error>>;

fn main() -> Type {
    let tree_bitmap = StarCastRib::<_, MemoryOnlyConfig>::try_default()?;
    let pfxs = vec![
        Prefix::new(
            0b0000_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            0,
        ),
        Prefix::new(
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
        Prefix::new(
            0b1111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            9,
        ),
        Prefix::new(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            9,
        ),
        Prefix::new(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            10,
        ),
        Prefix::new(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            11,
        ),
        Prefix::new(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            12,
        ),
        Prefix::new(
            0b0111_0111_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            12,
        ),
        Prefix::new(
            0b0111_0111_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            13,
        ),
        Prefix::new(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            13,
        ),
        Prefix::new(
            0b0111_0111_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            14,
        ),
        Prefix::new(
            0b0111_0111_0100_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            14,
        ),
        Prefix::new(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            14,
        ),
        Prefix::new(
            0b0111_0111_1100_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            14,
        ),
        Prefix::new(
            0b1110_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(std::net::Ipv4Addr::new(192, 0, 0, 0).into(), 23),
        Prefix::new(std::net::Ipv4Addr::new(192, 0, 0, 0).into(), 16),
        Prefix::new(std::net::Ipv4Addr::new(192, 0, 10, 0).into(), 23),
        Prefix::new(std::net::Ipv4Addr::new(192, 0, 9, 0).into(), 24),
        Prefix::new(std::net::Ipv4Addr::new(193, 0, 0, 0).into(), 23),
        Prefix::new(std::net::Ipv4Addr::new(193, 0, 10, 0).into(), 23),
        Prefix::new(std::net::Ipv4Addr::new(209, 0, 0, 0).into(), 16),
        Prefix::new(std::net::Ipv4Addr::new(193, 0, 9, 0).into(), 24),
        Prefix::new(std::net::Ipv4Addr::new(193, 0, 10, 0).into(), 24),
        Prefix::new(
            0b0011_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1000_0011_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            11,
        ),
        Prefix::new(
            0b1000_0010_0101_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            13,
        ),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 24),
        Prefix::new(
            0b1111_1111_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            12,
        ),
        Prefix::new(
            0b1111_1111_0011_0111_0000_0000_0000_0000_u32.into_ipaddr(),
            17,
        ),
        Prefix::new(std::net::Ipv4Addr::new(100, 0, 12, 0).into(), 24),
        Prefix::new(
            0b0000_0001_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            24,
        ),
        Prefix::new(std::net::Ipv4Addr::new(1, 0, 128, 0).into(), 24),
    ];

    for pfx in pfxs.into_iter() {
        // println!("insert {:?}", pfx);
        tree_bitmap.insert(
            &pfx.unwrap(),
            Record::new(0, 0, RouteStatus::Active, NoMeta::Empty),
            None,
        )?;
    }
    println!("------ end of inserts\n");
    // println!("{:#?}", tree_bitmap.store.prefixes);

    // println!("pfxbitarr: {:032b}", tree_bitmap.0.pfxbitarr);

    for spfx in &[
        Prefix::new(
            0b0000_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            0,
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
            0b0111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            4,
        ),
        Prefix::new(
            0b1111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            9,
        ),
        Prefix4::new(std::net::Ipv4Addr::new(255, 1, 0, 0).into(), 24),
        Prefix4::new(std::net::Ipv4Addr::new(224, 0, 0, 0).into(), 4),
        Prefix4::new(std::net::Ipv4Addr::new(224, 0, 0, 0).into(), 8),
        Prefix4::new(
            0b0100_0001_0000_0000_0000_0000_1111_1111_u32.into_ipaddr(),
            32,
        ),
        Prefix4::new(std::net::Ipv4Addr::new(1, 0, 0, 0).into(), 16),
        Prefix4::new(std::net::Ipv4Addr::new(131, 128, 0, 0).into(), 11),
        Prefix4::new(std::net::Ipv4Addr::new(12, 0, 0, 34).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 24),
        Prefix::new(std::net::Ipv4Addr::new(193, 0, 2, 0).into(), 23),
        Prefix::new(std::net::Ipv4Addr::new(193, 0, 10, 0).into(), 23),
        Prefix::new(
            0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
            14,
        ),
        Prefix::new(std::net::Ipv4Addr::new(193, 0, 10, 0).into(), 24),
        Prefix::new(std::net::Ipv4Addr::new(100, 0, 12, 0).into(), 24),
        Prefix::new(std::net::Ipv4Addr::new(255, 255, 255, 255).into(), 32),
        Prefix::new(std::net::Ipv4Addr::new(1, 0, 0, 0).into(), 24),
        Prefix::new(std::net::Ipv4Addr::new(1, 0, 128, 0).into(), 24),
    ] {
        println!("search for: {:?}", spfx);
        // let locks = tree_bitmap.acquire_prefixes_rwlock_read();
        let guard = &rotonda_store::epoch::pin();
        let s_spfx = tree_bitmap.match_prefix(
            &spfx.unwrap(),
            &MatchOptions {
                match_type: MatchType::LongestMatch,
                include_withdrawn: false,
                include_less_specifics: true,
                include_more_specifics: false,
                mui: None,
                include_history: IncludeHistory::None,
            },
            guard,
        );
        println!("lmp: {:?}", s_spfx);
        println!("-----------");
    }
    Ok(())
}
