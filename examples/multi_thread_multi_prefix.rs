use inetnum::addr::Prefix;
use log::trace;

use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rand::Rng;

use rotonda_store::meta_examples::PrefixAs;
use rotonda_store::{IncludeHistory, IntoIpAddr, MatchOptions, MemoryOnlyConfig, StarCastRib, Record, RouteStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    trace!("Starting multi-threaded yolo testing....");
    let tree_bitmap =
        Arc::new(StarCastRib::<PrefixAs, MemoryOnlyConfig>::try_default()?);
    // let pfx = Prefix::new_relaxed(
    //     0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
    //     32,
    // );

    let pfx_arc: Arc<AtomicU32> = Arc::new(0_u32.into());

    let threads = (0..100).enumerate().map(|(i, _)| {
        let tree_bitmap = tree_bitmap.clone();
        let pfx_int = Arc::clone(&pfx_arc);

        std::thread::Builder::new()
            .name(i.to_string())
            .spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send>> {
                    let mut rng = rand::rng(); 
                    
                    // println!("park thread {}", i);
                    thread::park();

                    // print!("\nstart {} ---", i);

                    let mut x = 0;
                    loop {
                        let pfx = Prefix::new_relaxed(pfx_int.clone().load(std::sync::atomic::Ordering::Relaxed).into_ipaddr(), 32).unwrap();
                        let guard = &crossbeam_epoch::pin();
                        while x < 100 {
                            let asn = PrefixAs::new_from_u32(rng.random());
                            match tree_bitmap.insert(
                                &pfx,
                                Record::new(0, 0, RouteStatus::Active, asn),
                                None
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
                                &pfx,
                                &MatchOptions {
                                    match_type: rotonda_store::MatchType::ExactMatch,
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

                        println!("thread {} will park itself", i);

                        guard.flush();
                        thread::park();
                        // thread::sleep(Duration::from_secs(3));
                        println!("wake thread {}", i);
                        println!("prefix count {:?}", tree_bitmap.prefixes_count());
                        x = 0;

                    }
                },
            )
            .unwrap()
    });

    // threads.clone().for_each(|t| {
    //     t.thread().unpark();
    // });

    loop {
        pfx_arc
            .clone()
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        println!(
            "increased pfx to {}",
            pfx_arc.clone().load(std::sync::atomic::Ordering::Relaxed)
        );
        println!("prefix count: {}", tree_bitmap.prefixes_count());

        threads.clone().for_each(|t| {
            t.thread().unpark();
        });

        thread::sleep(Duration::from_secs(10));
    }

    // Ok(())
}
