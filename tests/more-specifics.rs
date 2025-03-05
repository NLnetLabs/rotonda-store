// type Prefix4<'a> = Prefix<u32, PrefixAs>;
use inetnum::addr::Prefix;
use rotonda_store::{
    epoch, meta_examples::PrefixAs, rib::starcast_af::Config, IncludeHistory,
    MatchOptions, MatchType, Record, RouteStatus, StarCastRib,
};

use std::error::Error;

mod common {
    use std::io::Write;

    pub fn init() {
        let _ = env_logger::builder()
            .format(|buf, record| writeln!(buf, "{}", record.args()))
            .is_test(true)
            .try_init();
    }
}

rotonda_store::all_strategies![
    test_ms_1;
    test_more_specifics;
    PrefixAs
];

// #[test]
fn test_more_specifics<C: Config>(
    tree_bitmap: StarCastRib<PrefixAs, C>,
) -> Result<(), Box<dyn Error>> {
    crate::common::init();

    // let tree_bitmap = MultiThreadedStore::<PrefixAs>::try_default()?;
    let pfxs = vec![
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 24), // 0
        //
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 25), // 1
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 128).into(), 25), // 2
        //
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 26), // 3
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 64).into(), 26), // 4
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 128).into(), 26), // 5
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 192).into(), 26), // 6
        //
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 27), // 7
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 32).into(), 27), // 8
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 64).into(), 27), // 9
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 128).into(), 27), // 10
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 160).into(), 27), // 11
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 192).into(), 27), // 12
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 224).into(), 27), // 13
        //
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 32), // 14
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 31).into(), 32), // 15
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 32).into(), 32), // 16
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 63).into(), 32), // 17
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 64).into(), 32), // 18
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 127).into(), 32), // 19
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 128).into(), 32), // 20
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 159).into(), 32), // 21
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 160).into(), 32), // 22
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 191).into(), 32), // 23
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 192).into(), 32), // 24
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 223).into(), 32), // 25
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 224).into(), 32), // 26
        Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 255).into(), 32), // 27
    ];
    for pfx in pfxs.iter().flatten() {
        tree_bitmap.insert(
            pfx,
            Record::new(
                0,
                0,
                RouteStatus::Active,
                PrefixAs::new_from_u32(666),
            ),
            None,
        )?;
    }
    println!("------ end of inserts\n");

    // let locks = tree_bitmap.acquire_prefixes_rwlock_read();
    let guard = &epoch::pin();
    for (i, spfx) in &[
        (
            0,
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    23,
                ),
                None,
                // These are the indexes to pfxs.2 vec.
                // These are all supposed to show up in the result.
                vec![
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                ],
            ),
        ),
        (
            1,
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    24,
                ),
                Some(Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    24,
                )?),
                // These are the indexes to pfxs.2 vec.
                // These are all supposed to show up in the result.
                vec![
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                ],
            ),
        ),
        (
            2,
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    25,
                ),
                Some(Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    25,
                )?),
                vec![3, 4, 7, 8, 9, 14, 15, 16, 17, 18, 19],
            ),
        ),
        (
            3,
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    26,
                ),
                Some(Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    26,
                )?),
                vec![7, 8, 14, 15, 16, 17],
            ),
        ),
        (
            4,
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 192).into(),
                    26,
                ),
                Some(Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 192).into(),
                    26,
                )?),
                vec![12, 13, 24, 25, 26, 27],
            ),
        ),
        (
            5,
            (
                &Prefix::new(std::net::Ipv4Addr::new(0, 0, 0, 0).into(), 0),
                None,
                // These are the indexes to pfxs.2 vec.
                // These are all supposed to show up in the result.
                vec![
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                ],
            ),
        ),
    ] {
        println!("round {}", i);
        println!("search for: {}", (*spfx.0)?);
        let found_result = tree_bitmap.match_prefix(
            &spfx.0.unwrap(),
            &MatchOptions {
                match_type: MatchType::ExactMatch,
                include_withdrawn: false,
                include_less_specifics: false,
                include_more_specifics: true,
                mui: None,
                include_history: IncludeHistory::None,
            },
            guard,
        );
        // println!("em/m-s: {:#?}", found_result);
        //
        println!("search prefix: {}", spfx.0.unwrap());
        if let Some(pfx) = found_result.clone().prefix {
            println!("found prefix: {}", pfx);
        } else {
            println!("no found prefix");
        }

        for (i, p) in found_result
            .clone()
            .more_specifics
            .unwrap()
            .v4
            .iter()
            .enumerate()
            .map(|(i, p)| (i, p.prefix))
        {
            println!("ms {}: {}", i, p);
        }

        println!("--");
        println!("all prefixes");

        for (i, p) in tree_bitmap
            .prefixes_iter_v4(guard)
            .enumerate()
            .map(|(i, p)| (i, p.prefix))
        {
            println!("ms {}: {}", i, p);
        }

        println!("25 {}", pfxs[25].unwrap());
        assert!(tree_bitmap.contains(&pfxs[25].unwrap(), None));
        assert!(tree_bitmap.contains(&pfxs[26].unwrap(), None));
        assert!(tree_bitmap.contains(&pfxs[27].unwrap(), None));
        // let mut ms2 = tree_bitmap.more_specifics_keys_from(&spfx.0.unwrap());
        // println!("ms2 {:#?}", ms2);
        // println!("ms2 len {}", ms2.len());
        // ms2.dedup();
        // println!("ms2 deduped {}", ms2.len());
        let more_specifics = found_result
            .more_specifics
            .unwrap()
            .iter()
            .filter(|p| p.prefix != spfx.0.unwrap())
            .collect::<Vec<_>>();

        println!(
            ">> {:?}",
            more_specifics
                .iter()
                .find(|ms| ms.prefix == spfx.0.unwrap())
        );
        assert_eq!(found_result.prefix, spfx.1);

        println!("round {}", i);
        println!("{:?}", tree_bitmap.persist_strategy());
        assert_eq!(&more_specifics.len(), &spfx.2.len());

        for i in spfx.2.iter() {
            print!("{} ", i);

            let result_pfx = more_specifics
                .iter()
                .find(|pfx| pfx.prefix == pfxs[*i].unwrap());
            assert!(result_pfx.is_some());
        }
        println!("-----------");
    }
    Ok(())
}

rotonda_store::all_strategies![
    test_b_1;
    test_brunos_more_specifics;
    PrefixAs
];

fn test_brunos_more_specifics<C: Config>(
    tree_bitmap: StarCastRib<PrefixAs, C>,
) -> Result<(), Box<dyn Error>> {
    tree_bitmap.insert(
        &Prefix::new(std::net::Ipv4Addr::new(168, 181, 224, 0).into(), 22)
            .unwrap(),
        Record::new(0, 0, RouteStatus::Active, PrefixAs::new_from_u32(666)),
        None,
    )?;
    tree_bitmap.insert(
        &Prefix::new(std::net::Ipv4Addr::new(168, 181, 120, 0).into(), 24)?,
        Record::new(0, 0, RouteStatus::Active, PrefixAs::new_from_u32(666)),
        None,
    )?;
    tree_bitmap.insert(
        &Prefix::new(std::net::Ipv4Addr::new(168, 181, 121, 0).into(), 24)
            .unwrap(),
        Record::new(0, 0, RouteStatus::Active, PrefixAs::new_from_u32(666)),
        None,
    )?;

    let guard = &epoch::pin();
    let found_result = tree_bitmap.match_prefix(
        &Prefix::new(std::net::Ipv4Addr::new(168, 181, 224, 0).into(), 22)
            .unwrap(),
        &MatchOptions {
            match_type: MatchType::ExactMatch,
            include_withdrawn: false,
            include_less_specifics: false,
            include_more_specifics: true,
            mui: None,
            include_history: IncludeHistory::None,
        },
        guard,
    );

    assert!(found_result.more_specifics.unwrap().is_empty());
    Ok(())
}
