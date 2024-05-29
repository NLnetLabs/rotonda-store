use std::{str::FromStr, sync::atomic::Ordering};

use inetnum::{addr::Prefix, asn::Asn};
use rotonda_store::{prelude::multi::{Record, RouteStatus}, MultiThreadedStore};

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

    struct MuiData {
        mui: u32,
        asn: Asn,
        pfxs: Vec<Prefix>
    }

    let tree_bitmap = std::sync::Arc::new(MultiThreadedStore::<Asn>::new()?);

    let mui_data_1 = MuiData {
        mui: 1,
        asn: 65501.into(),
        pfxs: vec![
            Prefix::from_str("185.34.0.0/16")?,
            Prefix::from_str("185.34.10.0/24")?,
            Prefix::from_str("185.34.11.0/24")?,
            Prefix::from_str("183.0.0.0/8")?
        ]
    };

    let mui_data_2 = MuiData {
        mui: 2,
        asn: 65502.into(),
        pfxs: vec![
            Prefix::from_str("185.34.0.0/16")?,
            Prefix::from_str("185.34.10.0/24")?,
            Prefix::from_str("185.34.12.0/24")?,
            Prefix::from_str("186.0.0.0/8")?
        ]
    };

    let mui_data_3 = MuiData {
        mui: 3,
        asn: 65503.into(),
        pfxs: vec![
            Prefix::from_str("185.36.0.0/16")?,
            Prefix::from_str("185.34.10.0/24")?,
            Prefix::from_str("185.34.12.0/24")?,
            Prefix::from_str("187.0.0.0/8")?
        ]
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

    Ok(())
}