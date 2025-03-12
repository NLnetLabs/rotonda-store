use log::trace;
use rotonda_store::match_options::{IncludeHistory, MatchOptions, MatchType};
use rotonda_store::prefix_record::{Record, RouteStatus};
use rotonda_store::rib::config::MemoryOnlyConfig;
use rotonda_store::rib::StarCastRib;
use rotonda_store::IntoIpAddr;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rand::Rng;

use rotonda_store::test_types::PrefixAs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    trace!("Starting multi-threaded yolo testing....");
    let tree_bitmap =
        Arc::new(StarCastRib::<PrefixAs, MemoryOnlyConfig>::try_default()?);

    let pfx = inetnum::addr::Prefix::new_relaxed(
        0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
        32,
    );

    let threads =
        (0..16).enumerate().map(|(i, _)| {
            let tree_bitmap = tree_bitmap.clone();

            std::thread::Builder::new()
            .name(i.to_string())
            .spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send>> {
                    let mut rng = rand::rng();

                    println!("park thread {}", i);
                    thread::park();

                    print!("\nstart {} ---", i);

                    let mut x = 0;
                    loop {
                        let guard = &crossbeam_epoch::pin();
                        while x < 10_000 {
                            let asn = PrefixAs::new_from_u32(rng.random());
                            match tree_bitmap.insert(
                                &pfx.unwrap(),
                                Record::new(0, 0, RouteStatus::Active, asn),
                                None,
                            ) {
                                Ok(metrics) => {
                                    if metrics.prefix_new {
                                        println!(
                                            "thread {} won: {} with value {}",
                                            i, metrics.prefix_new, asn
                                        );
                                    }
                                }
                                Err(e) => {
                                    println!("{}", e);
                                }
                            };
                            let _s_spfx = tree_bitmap.match_prefix(
                        &pfx.unwrap(),
                        &MatchOptions {
                            match_type: MatchType::ExactMatch,
                            include_withdrawn: true,
                            include_less_specifics: true,
                            include_more_specifics: true,
                            mui: None,
                            include_history: IncludeHistory::None,
                        },
                        guard,
                    ).prefix_meta;
                            x += 1;
                        }

                        println!("thread to sleep  {}", i);

                        guard.flush();
                        thread::sleep(Duration::from_secs(3));
                        println!("wake thread {}", i);
                        println!(
                            "prefix count {:?}",
                            tree_bitmap.prefixes_count()
                        );
                        x = 0;
                    }
                },
            )
            .unwrap()
        });

    threads.for_each(|t| {
        t.thread().unpark();
    });

    std::thread::park();
    Ok(())
}
