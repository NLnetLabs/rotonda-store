use log::trace;
use std::time::Duration;
use std::{sync::Arc, thread};

use rotonda_store::{
    addr::Prefix, epoch, AddressFamily, MatchOptions, MultiThreadedStore,
};

use rotonda_store::PrefixAs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    trace!("Starting multi-threaded yolo testing....");
    let tree_bitmap = Arc::new(MultiThreadedStore::<PrefixAs>::new()?);
    let f = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let pfx = Prefix::new_relaxed(
        0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
        32,
    );

    let threads =
        (0..16).enumerate().map(|(i, _)| {
            let tree_bitmap = tree_bitmap.clone();
            // let start_flag = Arc::clone(&f);

            std::thread::Builder::new().name(i.to_string()).spawn(
            move || -> Result<(), Box<dyn std::error::Error + Send>> {
                // while !start_flag.load(std::sync::atomic::Ordering::Acquire) {
                    println!("park thread {}", i);
                    thread::park();
                // }

                print!("\nstart {} ---", i);
                let mut x = 0;
                loop {
                    x += 1;
                    // print!("{}-", i);
                    match tree_bitmap
                        .insert(&pfx.unwrap(), PrefixAs(i as u32))
                    {
                        Ok(metrics) => {
                            if metrics.1 > 0  {
                                println!("{} {} {:?} retry count {},", std::thread::current().name().unwrap(), metrics.0, pfx, metrics.1);
                            }
                        }
                        Err(e) => {
                            println!("{}", e);
                        }
                    };
                    if x % 1_000_000 == 0 { println!("{}", x); }
                }
                // println!("--thread {} done.", i);
            },
        ).unwrap()
        });

    // thread::sleep(Duration::from_secs(60));

    f.store(true, std::sync::atomic::Ordering::Release);
    threads.for_each(|t| {
        t.thread().unpark();
    });

    thread::sleep(Duration::from_secs(60));

    println!("------ end of inserts\n");

    let guard = &epoch::pin();

    let s_spfx = tree_bitmap.match_prefix(
        &pfx.unwrap(),
        &MatchOptions {
            match_type: rotonda_store::MatchType::ExactMatch,
            include_all_records: true,
            include_less_specifics: true,
            include_more_specifics: true,
        },
        guard,
    );
    println!("query result");
    println!("{}", s_spfx);
    println!("{}", s_spfx.more_specifics.unwrap());
   
    println!("-----------");

    Ok(())
}
