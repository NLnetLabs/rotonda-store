use std::{str::FromStr, sync::atomic::Ordering};

use inetnum::{addr::Prefix, asn::Asn};
use rotonda_store::{prelude::multi::{Record, RouteStatus}, MatchOptions, MultiThreadedStore};

mod common {
    use std::io::Write;

    pub fn init() {
        let _ = env_logger::builder()
            .format(|buf, record| writeln!(buf, "{}", record.args()))
            .is_test(true)
            .try_init();
    }
}

#[test]
fn test_concurrent_updates_1() -> Result<(), Box<dyn std::error::Error>> {
    crate::common::init();

    let pfx_vec_1 = vec![
        Prefix::from_str("185.34.0.0/16")?,
        Prefix::from_str("185.34.10.0/24")?,
        Prefix::from_str("185.34.11.0/24")?,
        Prefix::from_str("183.0.0.0/8")?
    ];

    let pfx_vec_2 = vec![
        Prefix::from_str("185.34.0.0/16")?,
        Prefix::from_str("185.34.10.0/24")?,
        Prefix::from_str("185.34.12.0/24")?,
        Prefix::from_str("186.0.0.0/8")?
    ];

    let pfx_vec_3 = vec![
        Prefix::from_str("185.36.0.0/16")?,
        Prefix::from_str("185.34.10.0/24")?,
        Prefix::from_str("185.34.12.0/24")?,
        Prefix::from_str("187.0.0.0/8")?
    ];

    struct MuiData {
        mui: u32,
        asn: Asn,
        pfxs: Vec<Prefix>
    }

    let tree_bitmap = std::sync::Arc::new(MultiThreadedStore::<Asn>::new()?);

    let mui_data_1 = MuiData {
        mui: 1,
        asn: 65501.into(),
        pfxs: pfx_vec_1.clone()
    };

    let mui_data_2 = MuiData {
        mui: 2,
        asn: 65502.into(),
        pfxs: pfx_vec_2.clone()
    };

    let mui_data_3 = MuiData {
        mui: 3,
        asn: 65503.into(),
        pfxs: pfx_vec_3.clone()
    };

    let cur_ltime = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let _: Vec<_> = vec![mui_data_1, mui_data_2, mui_data_3].into_iter()
        .map(|data: MuiData| {
            let tree_bitmap = tree_bitmap.clone();
            let cur_ltime = cur_ltime.clone();

            std::thread::Builder::new()
                .name(data.mui.to_string())
                .spawn(move || {
                    print!("\nstart {} ---", data.mui);
                    for pfx in data.pfxs {
                        let _ = cur_ltime.fetch_add(1, Ordering::Release);
              
                        match tree_bitmap.insert(
                            &pfx,
                            Record::new(data.mui, cur_ltime.load(Ordering::Acquire), RouteStatus::Active, data.asn),
                            None
                        ) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("{}", e);
                            }
                        };
                    }

                    println!("--thread {} done.", data.mui);
                }).unwrap()
        })
        .map(|t| t.join())
        .collect();

    let guard = rotonda_store::epoch::pin();
    println!("{:#?}", tree_bitmap.prefixes_iter(&guard).collect::<Vec<_>>());

    let all_pfxs_iter = tree_bitmap.prefixes_iter(&guard).collect::<Vec<_>>();

    let pfx = Prefix::from_str("185.34.0.0/16").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));

    let pfx = Prefix::from_str("185.34.10.0/24").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 3 && m.meta == 65503.into()));

    let pfx = Prefix::from_str("185.34.11.0/24").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 2 || m.meta == 65502.into())));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 3 || m.meta == 65503.into())));

    let pfx = Prefix::from_str("185.34.11.0/24").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 2 || m.meta == 65502.into())));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 3 && m.meta == 65503.into())));

    let pfx = Prefix::from_str("185.34.12.0/24").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 3 && m.meta == 65503.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 1 || m.meta == 65501.into())));

    let pfx = Prefix::from_str("183.0.0.0/8")?;
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 1 || m.meta == 65501.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 2 && m.meta == 65502.into())));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 3 && m.meta == 65503.into())));
    
    let pfx = Prefix::from_str("186.0.0.0/8")?;
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 1 || m.meta == 65501.into())));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 3 && m.meta == 65503.into())));
    
    let pfx = Prefix::from_str("187.0.0.0/8")?;
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 3 && m.meta == 65503.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 2 && m.meta == 65502.into())));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().all(|m| !(m.multi_uniq_id == 1 || m.meta == 65501.into())));

    
    // Create Withdrawals
    
    let _: Vec<_> = pfx_vec_2.clone().into_iter()
    .map(|pfx: Prefix| {
        let tree_bitmap = tree_bitmap.clone();
        let cur_ltime = cur_ltime.clone();

        std::thread::Builder::new()
            .name("2".to_string())
            .spawn(move || {
                print!("\nstart withdraw {} ---", 2);

                let _ = cur_ltime.fetch_add(1, Ordering::Release);
                tree_bitmap.mark_mui_as_withdrawn_for_prefix(&pfx, 2).unwrap();

                println!("--thread withdraw 2 done.");
            }).unwrap()
    })
    .map(|t| t.join())
    .collect();

    println!("{:#?}", tree_bitmap.prefixes_iter(&guard).collect::<Vec<_>>());

    let match_options = MatchOptions {
        match_type: rotonda_store::MatchType::ExactMatch,
        include_withdrawn: true,
        include_less_specifics: false,
        include_more_specifics: false,
        mui: None,
    };

    for pfx in pfx_vec_2 {
        let guard = rotonda_store::epoch::pin();
        let res = tree_bitmap.match_prefix(&pfx, &match_options, &guard);
        assert_eq!(res.prefix, Some(pfx));
        assert_eq!(res.prefix_meta.iter().find(|m| m.multi_uniq_id == 2).unwrap().status, RouteStatus::Withdrawn);
    }
    Ok(())
}

#[test]
fn test_concurrent_updates_2() -> Result<(), Box<dyn std::error::Error>> {
    crate::common::init();

    let pfx_vec_1 = vec![
        Prefix::from_str("185.33.0.0/16")?,
        Prefix::from_str("185.33.0.0/16")?,
        Prefix::from_str("185.33.0.0/16")?,
        Prefix::from_str("185.33.0.0/16")?
    ];

    let pfx_vec_2 = vec![
        Prefix::from_str("185.34.0.0/16")?,
        Prefix::from_str("185.34.0.0/16")?,
        Prefix::from_str("185.34.14.0/24")?,
        Prefix::from_str("187.0.0.0/8")?
    ];

    let pfx_vec_3 = vec![
        Prefix::from_str("185.35.0.0/16")?,
        Prefix::from_str("185.34.15.0/24")?,
        Prefix::from_str("185.34.15.0/24")?,
        Prefix::from_str("188.0.0.0/8")?
    ];

    #[derive(Debug)]
    struct MuiData {
        asn: u32,
    }

    let tree_bitmap = std::sync::Arc::new(MultiThreadedStore::<Asn>::new()?);

    const MUI_DATA: [MuiData; 4] = [
        MuiData {
            asn: 65501
        },
        MuiData {
            asn: 65502
        },
        MuiData {
            asn: 65503
        },
        MuiData {
            asn: 65504
        }
    ];

    let cur_ltime = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let _: Vec<_> = vec![pfx_vec_1.clone(), pfx_vec_2.clone(), pfx_vec_3.clone()].into_iter().enumerate()
        .map(|(n, pfxs)| {
            let tree_bitmap = tree_bitmap.clone();
            let cur_ltime = cur_ltime.clone();

            std::thread::Builder::new()
                .name(n.to_string())
                .spawn(move || {
                    print!("\nstart prefix batch {} ---", n);
                    for (i, pfx) in pfxs.iter().enumerate() {
                        let _ = cur_ltime.fetch_add(1, Ordering::Release);
              
                        match tree_bitmap.insert(
                            pfx,
                            Record::new(i as u32 + 1, cur_ltime.load(Ordering::Acquire), RouteStatus::Active, MUI_DATA[i].asn.into()),
                            None
                        ) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("{}", e);
                            }
                        };
                    }

                    println!("--thread prefix batch {} done.", n);
                }).unwrap()
        })
        .map(|t| t.join())
        .collect();

    let guard = rotonda_store::epoch::pin();
    println!("{:#?}", tree_bitmap.prefixes_iter(&guard).collect::<Vec<_>>());

    let all_pfxs_iter = tree_bitmap.prefixes_iter(&guard).collect::<Vec<_>>();

    let pfx = Prefix::from_str("185.33.0.0/16").unwrap();
    assert!(all_pfxs_iter.iter().any(|p| p.prefix == pfx));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 3 && m.meta == 65503.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 4 && m.meta == 65504.into()));

    let pfx = Prefix::from_str("185.34.0.0/16").unwrap();
    assert_eq!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.len(), 2);
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 1 && m.meta == 65501.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));

    let pfx = Prefix::from_str("185.34.14.0/24").unwrap();
    assert_eq!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.len(), 1);
    assert_eq!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta[0].meta, Asn::from_u32(65503));

    let pfx = Prefix::from_str("187.0.0.0/8").unwrap();
    assert_eq!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.len(), 1);
    assert_eq!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta[0].meta, Asn::from_u32(65504));

    let pfx = Prefix::from_str("185.35.0.0/16").unwrap();
    assert_eq!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.len(), 1);
    assert_eq!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta[0].meta, Asn::from_u32(65501));

    let pfx = Prefix::from_str("185.34.15.0/24").unwrap();
    assert_eq!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.len(), 2);
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 2 && m.meta == 65502.into()));
    assert!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.iter().any(|m| m.multi_uniq_id == 3 && m.meta == 65503.into()));
    
    let pfx = Prefix::from_str("188.0.0.0/8").unwrap();
    assert_eq!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta.len(), 1);
    assert_eq!(all_pfxs_iter.iter().find(|p| p.prefix == pfx).unwrap().meta[0].meta, Asn::from_u32(65504));

    // Create Withdrawals
    let wd_pfxs = [pfx_vec_1[1], pfx_vec_2[1], pfx_vec_3[1]];
    
    let _: Vec<_> = wd_pfxs.into_iter()
    .map(|pfx: Prefix| {
        let tree_bitmap = tree_bitmap.clone();
        let cur_ltime = cur_ltime.clone();

        std::thread::Builder::new()
            .name("2".to_string())
            .spawn(move || {
                print!("\nstart withdraw {} ---", 2);

                let _ = cur_ltime.fetch_add(1, Ordering::Release);
                tree_bitmap.mark_mui_as_withdrawn_for_prefix(&pfx, 2).unwrap();

                println!("--thread withdraw 2 done.");
            }).unwrap()
    })
    .map(|t| t.join())
    .collect();

    let match_options = MatchOptions {
        match_type: rotonda_store::MatchType::ExactMatch,
        include_withdrawn: true,
        include_less_specifics: false,
        include_more_specifics: false,
        mui: None,
    };

    for pfx in wd_pfxs {
        let guard = rotonda_store::epoch::pin();
        let res = tree_bitmap.match_prefix(&pfx, &match_options, &guard);
        assert_eq!(res.prefix, Some(pfx));
        assert_eq!(res.prefix_meta.iter().find(|m| m.multi_uniq_id == 2).unwrap().status, RouteStatus::Withdrawn);
    }

    
    let match_options = MatchOptions {
        match_type: rotonda_store::MatchType::ExactMatch,
        include_withdrawn: false,
        include_less_specifics: false,
        include_more_specifics: true,
        mui: None,
    };

    let pfx = Prefix::from_str("0.0.0.0/0").unwrap();
    let guard = rotonda_store::epoch::pin();
    let res = tree_bitmap.match_prefix(&pfx, &match_options, &guard);
    
    println!("{:#?}", res);

    let active_len = all_pfxs_iter.iter().filter(|p| p.meta.iter().all(|m| m.status == RouteStatus::Active)).collect::<Vec<_>>().len();
    assert_eq!(active_len, all_pfxs_iter.len());
    let len_2 = res.more_specifics.unwrap().v4.len();
    assert_eq!(active_len, len_2);

    Ok(())
}