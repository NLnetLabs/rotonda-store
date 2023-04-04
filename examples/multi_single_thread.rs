use log::trace;
use std::time::Duration;
use std::thread;

use rand::Rng;

use rotonda_store::{
    addr::Prefix, AddressFamily, MultiThreadedStore,
};

use rotonda_store::PrefixAs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    trace!("Starting multi-threaded yolo testing....");
    let tree_bitmap = MultiThreadedStore::<PrefixAs>::new()?;
    // let f = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let pfx = Prefix::new_relaxed(
        0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
        32,
    );

    // let threads =
    // (0..16).enumerate().map(|(i, _)| {
    // let tree_bitmap = tree_bitmap.clone();
    // let start_flag = Arc::clone(&f);

    let thread = std::thread::Builder::new()
        .name(1_u8.to_string())
        .spawn(move || -> Result<(), Box<dyn std::error::Error + Send>> {
            // while !start_flag.load(std::sync::atomic::Ordering::Acquire) {
            let mut rng= rand::thread_rng();

            println!("park thread {}", 1);
            thread::park();
            // }

            print!("\nstart {} ---", 1);
            // let mut x = 0;
            loop {
                // x += 1;
                // print!("{}-", i);
                let asn: u32 = rng.gen();
                match tree_bitmap.insert(&pfx.unwrap(), PrefixAs(asn)) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{}", e);
                    }
                };
            }
            // println!("--thread {} done.", 1);
        })
        .unwrap();
    // });

    // thread::sleep(Duration::from_secs(60));

    // f.store(true, std::sync::atomic::Ordering::Release);
    // thread.for_each(|t| {
    thread.thread().unpark();
    // });

    thread::sleep(Duration::from_secs(120));

    println!("------ end of inserts\n");

    // let guard = unsafe { epoch::unprotected() };

    // let s_spfx = tree_bitmap.match_prefix(
    //     &pfx.unwrap(),
    //     &MatchOptions {
    //         match_type: rotonda_store::MatchType::ExactMatch,
    //         include_all_records: true,
    //         include_less_specifics: true,
    //         include_more_specifics: true,
    //     },
    //     guard,
    // );
    // println!("query result");
    // println!("{}", s_spfx);
    // println!("{}", s_spfx.more_specifics.unwrap());

    println!("-----------");

    Ok(())
}
