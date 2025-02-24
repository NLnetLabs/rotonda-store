use log::trace;
use rotonda_store::rib::MemoryOnlyConfig;
use std::time::Duration;
use std::{sync::Arc, thread};

use rotonda_store::prelude::multi::*;
use rotonda_store::prelude::*;

use rotonda_store::meta_examples::PrefixAs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    trace!("Starting multi-threaded yolo testing....");
    let tree_bitmap = Arc::new(MultiThreadedStore::<
        PrefixAs,
        MemoryOnlyConfig,
    >::try_default()?);
    let f = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let pfx = Prefix::new_relaxed(
        0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
        32,
    );

    let threads = (0..256).enumerate().map(|(i, _)| {
        let tree_bitmap = tree_bitmap.clone();
        let start_flag = Arc::clone(&f);

        thread::Builder::new()
            .name(i.to_string())
            .spawn(move || {
                while !start_flag.load(std::sync::atomic::Ordering::SeqCst) {
                    trace!("park thread {}", i);
                    thread::park();
                }

                match tree_bitmap.insert(
                    &pfx.unwrap(),
                    Record::new(
                        0,
                        0,
                        RouteStatus::Active,
                        PrefixAs::new((i as u32).into()),
                    ),
                    None,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{}", e);
                    }
                };
            })
            .unwrap()
    });

    thread::sleep(Duration::from_millis(1000));

    f.store(true, std::sync::atomic::Ordering::Release);
    threads.for_each(|t| {
        t.thread().unpark();
    });

    println!("------ end of inserts\n");

    let guard = &epoch::pin();

    let s_spfx = tree_bitmap.match_prefix(
        &pfx.unwrap(),
        &MatchOptions {
            match_type: rotonda_store::MatchType::ExactMatch,
            include_withdrawn: true,
            include_less_specifics: true,
            include_more_specifics: true,
            mui: None,
            include_history: IncludeHistory::None,
        },
        guard,
    );
    println!("query result");
    println!("{}", s_spfx);
    println!("{}", s_spfx.more_specifics.unwrap());
    println!("-----------");

    Ok(())
}
