use log::trace;
use rotonda_store::prelude::Upsert;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rand::Rng;

use rotonda_store::{addr::Prefix, AddressFamily, MultiThreadedStore};

use rotonda_store::{MatchOptions, PrefixAs};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    trace!("Starting multi-threaded yolo testing....");
    let tree_bitmap = Arc::new(MultiThreadedStore::<PrefixAs>::new()?);

    let pfx = Prefix::new_relaxed(
        0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
        32,
    );

    let threads = (0..16).enumerate().map(|(i, _)| {
        let tree_bitmap = tree_bitmap.clone();

        std::thread::Builder::new()
            .name(i.to_string())
            .spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send>> {
                    let mut rng = rand::thread_rng();

                    println!("park thread {}", i);
                    thread::park();

                    print!("\nstart {} ---", i);

                    let mut x = 0;
                    loop {
                        let guard = &crossbeam_epoch::pin();
                        while x < 10_000 {
                            let asn = PrefixAs(rng.gen());
                            match tree_bitmap.insert(&pfx.unwrap(), asn) {
                                Ok(metrics) => {
                                    if let Upsert::Insert = metrics.0 {
                                        println!(
                                            "thread {} won: {} with value {}",
                                            i, metrics.0, asn
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
                            match_type: rotonda_store::MatchType::ExactMatch,
                            include_all_records: true,
                            include_less_specifics: true,
                            include_more_specifics: true,
                        },
                        guard,
                    ).prefix_meta;
                            x += 1;
                        }

                        println!("thread to sleep  {}", i);
                        guard.flush();
                        thread::sleep(Duration::from_secs(3));
                        println!("wake thread {}", i);
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
