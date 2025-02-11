// type Prefix4<'a> = Prefix<u32, PrefixAs>;

use std::error::Error;

use inetnum::addr::Prefix;

use rotonda_store::{
    meta_examples::PrefixAs, prelude::multi::RouteStatus, rib::StoreConfig,
    IncludeHistory, MatchOptions, MatchType, MultiThreadedStore,
    PublicRecord as Record,
};
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
    test_ms_w_ls_1;
    test_more_specifics_without_less_specifics;
    PrefixAs
];

// #[test]
fn test_more_specifics_without_less_specifics(
    tree_bitmap: MultiThreadedStore<PrefixAs>,
) -> Result<(), Box<dyn Error>> {
    crate::common::init();

    // let tree_bitmap = MultiThreadedStore::<PrefixAs>::try_default()?;
    let pfxs = vec![
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 64, 0).into(), 18)?, // 0
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 109, 0).into(), 24)?, // 1
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 153, 0).into(), 24)?, // 2
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 21)?,  // 3
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 176, 0).into(), 20)?, // 4
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 8)?,   // 5
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 184, 0).into(), 23)?, // 6
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 71, 0).into(), 24)?, // 7
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 9)?,   // 8
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 117, 0).into(), 24)?, // 9
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 99, 0).into(), 24)?, // 10
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 224, 0).into(), 24)?, // 11
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 128, 0).into(), 18)?, // 12
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 120, 0).into(), 24)?, // 13
    ];

    for pfx in pfxs.iter() {
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
    let guard = &rotonda_store::epoch::pin();
    for (r, spfx) in &[
        (
            0,
            (
                &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 9),
                &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 9), // 0
                vec![0, 1, 2, 3, 4, 6, 7, 9, 10, 11, 12, 13],
            ),
        ),
        (
            1,
            (
                &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 8),
                &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 8), // 0
                vec![0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13],
            ),
        ),
    ] {
        println!("start round {}", r);
        println!("search for: {}", spfx.0.unwrap());
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
        println!("em/m-s: {:#?}", found_result);

        let more_specifics = found_result
            .more_specifics
            .unwrap()
            .iter()
            .filter(|p| p.prefix != spfx.0.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(found_result.prefix.unwrap(), spfx.1.unwrap());
        assert_eq!(&more_specifics.len(), &spfx.2.len());

        for i in spfx.2.iter() {
            print!("{} ", i);

            let result_pfx =
                more_specifics.iter().find(|pfx| pfx.prefix == pfxs[*i]);
            assert!(result_pfx.is_some());
        }
        println!("end round {}", r);
        println!("-----------");
    }
    Ok(())
}

#[test]
fn test_more_specifics_with_less_specifics() -> Result<(), Box<dyn Error>> {
    crate::common::init();

    let tree_bitmap = MultiThreadedStore::<PrefixAs>::try_default()?;
    let pfxs = vec![
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 64, 0).into(), 18), // 0
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 109, 0).into(), 24), // 1
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 153, 0).into(), 24), // 2
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 21),  // 3
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 176, 0).into(), 20), // 4
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 8),   // 5
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 184, 0).into(), 23), // 6
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 71, 0).into(), 24), // 7
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 9),   // 8
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 117, 0).into(), 24), // 9
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 99, 0).into(), 24), // 10
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 224, 0).into(), 24), // 11
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 128, 0).into(), 18), // 12
        Prefix::new(std::net::Ipv4Addr::new(17, 0, 120, 0).into(), 24), // 13
    ];

    let ltime = 0;
    let status = RouteStatus::Active;
    for pfx in pfxs.iter() {
        tree_bitmap.insert(
            &pfx.unwrap(),
            Record::new(0, ltime, status, PrefixAs::new_from_u32(666)),
            None,
        )?;
    }
    println!("------ end of inserts\n");
    let guard = &rotonda_store::epoch::pin();

    for spfx in &[
        (
            &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 9),
            Some(&Prefix::new(
                std::net::Ipv4Addr::new(17, 0, 0, 0).into(),
                9,
            )), // 0
            vec![0, 1, 2, 3, 4, 6, 7, 9, 10, 11, 12, 13],
        ),
        (
            &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 8),
            Some(&Prefix::new(
                std::net::Ipv4Addr::new(17, 0, 0, 0).into(),
                8,
            )), // 0
            vec![0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13],
        ),
    ] {
        println!("search for: {:#}", (*spfx.0)?);
        let found_result = tree_bitmap.match_prefix(
            &spfx.0.unwrap(),
            &MatchOptions {
                match_type: MatchType::LongestMatch,
                include_withdrawn: false,
                include_less_specifics: false,
                include_more_specifics: true,
                mui: None,
                include_history: IncludeHistory::None,
            },
            guard,
        );
        println!("em/m-s: {}", found_result);

        let more_specifics = found_result
            .more_specifics
            .unwrap()
            .iter()
            .filter(|p| p.prefix != spfx.0.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(found_result.prefix.unwrap(), spfx.1.unwrap().unwrap());
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
