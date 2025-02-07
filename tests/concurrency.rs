use std::{
    str::FromStr,
    sync::{atomic::Ordering, Arc},
};

use inetnum::{addr::Prefix, asn::Asn};
use rotonda_store::{
    meta_examples::NoMeta, prelude::multi::RouteStatus, rib::StoreConfig,
    test_types::BeBytesAsn, IncludeHistory, MatchOptions, MultiThreadedStore,
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
    test_cc_updates_1;
    test_concurrent_updates_1;
    BeBytesAsn
];

fn test_concurrent_updates_1(
    tree_bitmap: MultiThreadedStore<BeBytesAsn>,
) -> Result<(), Box<dyn std::error::Error>> {
    crate::common::init();

    let pfx_vec_1 = vec![
        Prefix::from_str("185.34.0.0/16")?,
        Prefix::from_str("185.34.10.0/24")?,
        Prefix::from_str("185.34.11.0/24")?,
        Prefix::from_str("183.0.0.0/8")?,
    ];

    let pfx_vec_2 = vec![
        Prefix::from_str("185.34.0.0/16")?,
        Prefix::from_str("185.34.10.0/24")?,
        Prefix::from_str("185.34.12.0/24")?,
        Prefix::from_str("186.0.0.0/8")?,
    ];

    let pfx_vec_3 = vec![
        Prefix::from_str("185.36.0.0/16")?,
        Prefix::from_str("185.34.10.0/24")?,
        Prefix::from_str("185.34.12.0/24")?,
        Prefix::from_str("187.0.0.0/8")?,
    ];

    struct MuiData {
        mui: u32,
        asn: Asn,
        pfxs: Vec<Prefix>,
    }

    // let store_config = StoreConfig {
    //     persist_strategy: rotonda_store::rib::PersistStrategy::PersistOnly,
    //     persist_path: "/tmp/rotonda/".into(),
    // };

    // let store = std::sync::Arc::new(
    //     MultiThreadedStore::<BeBytesAsn>::new_with_config(store_config)?,
    // );
    let guard = &rotonda_store::epoch::pin();

    let mui_data_1 = MuiData {
        mui: 1,
        asn: 65501.into(),
        pfxs: pfx_vec_1.clone(),
    };

    let mui_data_2 = MuiData {
        mui: 2,
        asn: 65502.into(),
        pfxs: pfx_vec_2.clone(),
    };

    let mui_data_3 = MuiData {
        mui: 3,
        asn: 65503.into(),
        pfxs: pfx_vec_3.clone(),
    };

    let cur_ltime = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let tree_bitmap = std::sync::Arc::new(tree_bitmap);
    let _: Vec<_> = vec![mui_data_1, mui_data_2, mui_data_3]
        .into_iter()
        .map(|data: MuiData| {
            let cur_ltime = cur_ltime.clone();
            let tbm = tree_bitmap.clone();

            std::thread::Builder::new()
                .name(data.mui.to_string())
                .spawn(move || {
                    print!("\nstart {} ---", data.mui);
                    for pfx in data.pfxs {
                        let _ = cur_ltime.fetch_add(1, Ordering::Release);

                        match tbm.insert(
                            &pfx,
                            Record::new(
                                data.mui,
                                cur_ltime.load(Ordering::Acquire),
                                RouteStatus::Active,
                                data.asn.into(),
                            ),
                            None,
                        ) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("{}", e);
                            }
                        };
                    }

                    println!("--thread {} done.", data.mui);
                })
                .unwrap()
        })
        .map(|t| t.join())
        .collect();

    println!("COUNT {:?}", tree_bitmap.prefixes_count());

    let all_pfxs_iter = tree_bitmap.prefixes_iter(guard).collect::<Vec<_>>();
    println!("all_pfxs_iter {:#?}", all_pfxs_iter);

    let pfx = Prefix::from_str("185.34.0.0/16").unwrap();

    assert!(tree_bitmap.contains(&pfx, None));
    assert!(tree_bitmap.contains(&pfx, Some(1)));
    assert!(tree_bitmap.contains(&pfx, Some(2)));

    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));

    let pfx = Prefix::from_str("185.34.10.0/24").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 3 && m.meta == 65503.into()));

    let pfx = Prefix::from_str("185.34.11.0/24").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 2 || m.meta == 65502.into())));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 3 || m.meta == 65503.into())));

    let pfx = Prefix::from_str("185.34.11.0/24").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 2 || m.meta == 65502.into())));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 3 && m.meta == 65503.into())));

    let pfx = Prefix::from_str("185.34.12.0/24").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 3 && m.meta == 65503.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 1 || m.meta == 65501.into())));

    let pfx = Prefix::from_str("183.0.0.0/8")?;
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 1 || m.meta == 65501.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 2 && m.meta == 65502.into())));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 3 && m.meta == 65503.into())));

    let pfx = Prefix::from_str("186.0.0.0/8")?;
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 1 || m.meta == 65501.into())));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 3 && m.meta == 65503.into())));

    let pfx = Prefix::from_str("187.0.0.0/8")?;
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 3 && m.meta == 65503.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 2 && m.meta == 65502.into())));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .all(|m| !(m.multi_uniq_id == 1 || m.meta == 65501.into())));

    // Create Withdrawals

    let _: Vec<_> = pfx_vec_2
        .clone()
        .into_iter()
        .map(|pfx: Prefix| {
            let tree_bitmap = tree_bitmap.clone();
            let cur_ltime = cur_ltime.clone();

            std::thread::Builder::new()
                .name("2".to_string())
                .spawn(move || {
                    print!("\nstart withdraw {} ---", 2);

                    let _ = cur_ltime.fetch_add(1, Ordering::Release);
                    tree_bitmap
                        .mark_mui_as_withdrawn_for_prefix(&pfx, 2, 10)
                        .unwrap();

                    println!("--thread withdraw 2 done.");
                })
                .unwrap()
        })
        .map(|t| t.join())
        .collect();

    println!(
        "prefixes_iter {:#?}",
        tree_bitmap
            .as_ref()
            .prefixes_iter(guard)
            .collect::<Vec<_>>()
    );

    let match_options = MatchOptions {
        match_type: rotonda_store::MatchType::ExactMatch,
        include_withdrawn: true,
        include_less_specifics: false,
        include_more_specifics: false,
        mui: None,
        include_history: IncludeHistory::None,
    };

    for pfx in pfx_vec_2 {
        let guard = rotonda_store::epoch::pin();
        let res = tree_bitmap.match_prefix(&pfx, &match_options, &guard);
        assert_eq!(res.prefix, Some(pfx));
        println!("strategy {:?}", tree_bitmap.persist_strategy());
        println!("PFX {}", res);
        assert_eq!(
            res.prefix_meta
                .iter()
                .find(|m| m.multi_uniq_id == 2)
                .unwrap()
                .status,
            RouteStatus::Withdrawn
        );
    }
    Ok(())
}

// rotonda_store::all_strategies_arced![
//     test_cc_updates_2;
//     test_concurrent_updates_2;
//     BeBytesAsn
// ];

#[test]
fn test_concurrent_updates_2(// tree_bitmap: Arc<MultiThreadedStore<BeBytesAsn>>,
) -> Result<(), Box<dyn std::error::Error>> {
    crate::common::init();

    let pfx_vec_1 = vec![
        Prefix::from_str("185.33.0.0/16")?,
        Prefix::from_str("185.33.0.0/16")?,
        Prefix::from_str("185.33.0.0/16")?,
        Prefix::from_str("185.33.0.0/16")?,
    ];

    let pfx_vec_2 = vec![
        Prefix::from_str("185.34.0.0/16")?,
        Prefix::from_str("185.34.0.0/16")?,
        Prefix::from_str("185.34.14.0/24")?,
        Prefix::from_str("187.0.0.0/8")?,
    ];

    let pfx_vec_3 = vec![
        Prefix::from_str("185.35.0.0/16")?,
        Prefix::from_str("185.34.15.0/24")?,
        Prefix::from_str("185.34.15.0/24")?,
        Prefix::from_str("188.0.0.0/8")?,
    ];

    const MUI_DATA: [u32; 4] = [65501, 65502, 65503, 65504];

    let cur_ltime = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let store_config = StoreConfig {
        persist_strategy: rotonda_store::rib::PersistStrategy::MemoryOnly,
        persist_path: "/tmp/rotonda/".into(),
    };
    let tree_bitmap = std::sync::Arc::new(
        MultiThreadedStore::<BeBytesAsn>::new_with_config(store_config)?,
    );
    let guard = &rotonda_store::epoch::pin();

    let _: Vec<_> =
        vec![pfx_vec_1.clone(), pfx_vec_2.clone(), pfx_vec_3.clone()]
            .into_iter()
            .enumerate()
            .map(|(n, pfxs)| {
                let tbm = std::sync::Arc::clone(&tree_bitmap);
                let cur_ltime = cur_ltime.clone();

                std::thread::Builder::new()
                    .name(n.to_string())
                    .spawn(move || {
                        print!("\nstart prefix batch {} ---", n);
                        for (i, pfx) in pfxs.iter().enumerate() {
                            let _ = cur_ltime.fetch_add(1, Ordering::Release);

                            match tbm.insert(
                                pfx,
                                Record::new(
                                    i as u32 + 1,
                                    cur_ltime.load(Ordering::Acquire),
                                    RouteStatus::Active,
                                    Asn::from(MUI_DATA[i]).into(),
                                ),
                                None,
                            ) {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("Err: {}", e);
                                }
                            };
                        }

                        println!("--thread prefix batch {} done.", n);
                    })
                    .unwrap()
            })
            .map(|t| t.join())
            .collect();

    println!(
        "prefixes_iter#1 :{:#?}",
        tree_bitmap.prefixes_iter(guard).collect::<Vec<_>>()
    );

    let all_pfxs_iter = tree_bitmap.prefixes_iter(guard).collect::<Vec<_>>();

    let pfx = Prefix::from_str("185.33.0.0/16").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 3 && m.meta == 65503.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 4 && m.meta == 65504.into()));

    let pfx = Prefix::from_str("185.34.0.0/16").unwrap();
    assert_eq!(
        all_pfxs_iter
            .iter()
            .find(|p| p.prefix == pfx)
            .unwrap()
            .meta
            .len(),
        2
    );
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));

    let pfx = Prefix::from_str("185.34.14.0/24").unwrap();
    assert_eq!(
        all_pfxs_iter
            .iter()
            .find(|p| p.prefix == pfx)
            .unwrap()
            .meta
            .len(),
        1
    );
    assert_eq!(
        all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta[0].meta,
        Asn::from_u32(65503).into()
    );

    let pfx = Prefix::from_str("187.0.0.0/8").unwrap();
    assert_eq!(
        all_pfxs_iter
            .iter()
            .find(|p| p.prefix == pfx)
            .unwrap()
            .meta
            .len(),
        1
    );
    assert_eq!(
        all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta[0].meta,
        Asn::from_u32(65504).into()
    );

    let pfx = Prefix::from_str("185.35.0.0/16").unwrap();
    assert_eq!(
        all_pfxs_iter
            .iter()
            .find(|p| p.prefix == pfx)
            .unwrap()
            .meta
            .len(),
        1
    );
    assert_eq!(
        all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta[0].meta,
        Asn::from_u32(65501).into()
    );

    let pfx = Prefix::from_str("185.34.15.0/24").unwrap();
    assert_eq!(
        all_pfxs_iter
            .iter()
            .find(|p| p.prefix == pfx)
            .unwrap()
            .meta
            .len(),
        2
    );
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));
    assert!(all_pfxs_iter
        .iter()
        .find(|p| p.prefix == pfx)
        .unwrap()
        .meta
        .iter()
        .any(|m| m.multi_uniq_id == 3 && m.meta == 65503.into()));

    let pfx = Prefix::from_str("188.0.0.0/8").unwrap();
    assert_eq!(
        all_pfxs_iter
            .iter()
            .find(|p| p.prefix == pfx)
            .unwrap()
            .meta
            .len(),
        1
    );
    assert_eq!(
        all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta[0].meta,
        Asn::from_u32(65504).into()
    );

    // Create Withdrawals
    let wd_pfxs = [pfx_vec_1[1], pfx_vec_2[1], pfx_vec_3[1]];

    let _: Vec<_> = wd_pfxs
        .into_iter()
        .map(|pfx: Prefix| {
            let tbm = std::sync::Arc::clone(&tree_bitmap);
            let cur_ltime = cur_ltime.clone();

            std::thread::Builder::new()
                .name("2".to_string())
                .spawn(move || {
                    print!("\nstart withdraw {} ---", 2);

                    let _ = cur_ltime.fetch_add(1, Ordering::Release);
                    tbm.mark_mui_as_withdrawn_for_prefix(&pfx, 2, 15)
                        .unwrap();

                    println!("--thread withdraw 2 done.");
                })
                .unwrap()
        })
        .map(|t| t.join())
        .collect();

    let match_options = MatchOptions {
        match_type: rotonda_store::MatchType::ExactMatch,
        include_withdrawn: true,
        include_less_specifics: false,
        include_more_specifics: false,
        mui: None,
        include_history: IncludeHistory::None,
    };

    for pfx in wd_pfxs {
        let guard = rotonda_store::epoch::pin();
        let res = tree_bitmap.match_prefix(&pfx, &match_options, &guard);
        assert_eq!(res.prefix, Some(pfx));
        println!("RES {:#?}", res);
        assert_eq!(
            res.prefix_meta
                .iter()
                .find(|m| m.multi_uniq_id == 2)
                .unwrap()
                .status,
            RouteStatus::Withdrawn
        );
    }

    println!("get all prefixes");
    let match_options = MatchOptions {
        match_type: rotonda_store::MatchType::EmptyMatch,
        include_withdrawn: false,
        include_less_specifics: false,
        include_more_specifics: true,
        mui: None,
        include_history: IncludeHistory::None,
    };

    println!("strategy {:?}", tree_bitmap.persist_strategy());
    // should cover all the prefixes
    // let pfx0 = Prefix::from_str("184.0.0.0/6").unwrap();
    let pfx128 = Prefix::from_str("128.0.0.0/1").unwrap();
    let guard = rotonda_store::epoch::pin();
    // let res0 = tree_bitmap.match_prefix(&pfx0, &match_options, &guard);

    // println!("000 {:#?}", res0);

    assert!(tree_bitmap
        .contains(&Prefix::from_str("185.34.14.0/24").unwrap(), None));

    tree_bitmap
        .insert(
            &Prefix::from_str("32.0.0.0/4").unwrap(),
            Record::new(
                1,
                cur_ltime.load(Ordering::Acquire),
                RouteStatus::Active,
                Asn::from(653400).into(),
            ),
            None,
        )
        .unwrap();

    assert!(
        tree_bitmap.contains(&Prefix::from_str("32.0.0.0/4").unwrap(), None)
    );

    let mp02 = tree_bitmap
        .match_prefix(
            &Prefix::from_str("0.0.0.0/2").unwrap(),
            &match_options,
            &guard,
        )
        .more_specifics
        .unwrap();
    println!("0/2 {}", mp02);
    assert_eq!(mp02.len(), 1);

    let res128 = tree_bitmap.match_prefix(&pfx128, &match_options, &guard);
    println!("128 {:#?}", res128);
    // let guard = rotonda_store::epoch::pin();
    // println!(
    //     "more_specifics_iter_from {:#?}",
    //     tree_bitmap.more_specifics_keys_from(&pfx128)
    // );

    let active_len = all_pfxs_iter
        .iter()
        .filter(|p| p.meta.iter().all(|m| m.status == RouteStatus::Active))
        .collect::<Vec<_>>()
        .len();
    assert_eq!(active_len, all_pfxs_iter.len());
    // let len_2 = res0.more_specifics.unwrap().v4.len()
    assert_eq!(active_len, res128.more_specifics.unwrap().v4.len());

    Ok(())
}

#[test]
fn more_specifics_short_lengths() -> Result<(), Box<dyn std::error::Error>> {
    crate::common::init();

    println!("PersistOnly strategy starting...");
    let store_config = StoreConfig {
        persist_strategy: rotonda_store::rib::PersistStrategy::PersistOnly,
        persist_path: "/tmp/rotonda/".into(),
    };
    let tree_bitmap = std::sync::Arc::new(
        MultiThreadedStore::<NoMeta>::new_with_config(store_config)?,
    );
    let match_options = MatchOptions {
        match_type: rotonda_store::MatchType::EmptyMatch,
        include_withdrawn: false,
        include_less_specifics: false,
        include_more_specifics: true,
        mui: None,
        include_history: IncludeHistory::None,
    };

    let pfx1 = Prefix::from_str("185.34.0.0/16")?;
    let pfx2 = Prefix::from_str("185.34.3.0/24")?;
    let pfx3 = Prefix::from_str("185.34.4.0/24")?;

    tree_bitmap
        .insert(
            &pfx1,
            Record::new(1, 0, RouteStatus::Active, NoMeta::Empty),
            None,
        )
        .unwrap();

    tree_bitmap
        .insert(
            &pfx2,
            Record::new(1, 0, RouteStatus::Active, NoMeta::Empty),
            None,
        )
        .unwrap();

    tree_bitmap
        .insert(
            &pfx3,
            Record::new(1, 0, RouteStatus::Active, NoMeta::Empty),
            None,
        )
        .unwrap();

    let guard = rotonda_store::epoch::pin();

    assert!(tree_bitmap.contains(&pfx1, None));
    assert!(tree_bitmap.contains(&pfx2, None));
    assert!(tree_bitmap.contains(&pfx3, None));

    println!("-------------------");
    // let search_pfx = Prefix::from_str("0.0.0.0/0")?;
    // let mp = tree_bitmap
    //     .more_specifics_iter_from(&search_pfx, None, false, &guard)
    //     .collect::<Vec<_>>();

    // println!("more specifics : {:#?}", mp);

    // assert_eq!(mp.len(), 2);

    let search_pfx = Prefix::from_str("128.0.0.0/1")?;

    let m = tree_bitmap.match_prefix(&search_pfx, &match_options, &guard);

    // let mp = tree_bitmap
    //     .more_specifics_iter_from(&search_pfx, None, false, &guard)
    //     .collect::<Vec<_>>();

    println!(
        "more specifics#0: {}",
        m.more_specifics.as_ref().unwrap()[0]
    );
    println!(
        "more specifics#1: {}",
        m.more_specifics.as_ref().unwrap()[1]
    );
    println!(
        "more specifics#2: {}",
        m.more_specifics.as_ref().unwrap()[2]
    );

    assert_eq!(m.more_specifics.map(|mp| mp.len()), Some(3));

    Ok(())
}
