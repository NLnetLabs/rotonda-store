use inetnum::addr::Prefix;
use log::trace;
use rotonda_store::prefix_record::{Record, RouteStatus};
use rotonda_store::rib::config::MemoryOnlyConfig;
use rotonda_store::rib::StarCastRib;
use rotonda_store::IntoIpAddr;
use std::thread;
use std::time::Duration;

use rand::Rng;

use rotonda_store::test_types::PrefixAs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    trace!("Starting one-threaded yolo testing....");
    let tree_bitmap =
        StarCastRib::<PrefixAs, MemoryOnlyConfig>::try_default()?;

    let mut pfx_int = 0_u32;

    let thread = std::thread::Builder::new()
        .name(1_u8.to_string())
        .spawn(move || -> Result<(), Box<dyn std::error::Error + Send>> {
            let mut rng = rand::rng();

            println!("park thread {}", 1);
            thread::park();

            print!("\nstart {} ---", 1);

            while pfx_int <= 24 {
                pfx_int += 1;
                let pfx = Prefix::new_relaxed(pfx_int.into_ipaddr(), 32);

                print!("{}-", pfx_int);
                let asn: u32 = rng.random();
                match tree_bitmap.insert(
                    &pfx.unwrap(),
                    Record::new(
                        1,
                        0,
                        RouteStatus::Active,
                        PrefixAs::new_from_u32(asn),
                    ),
                    None,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{}", e);
                    }
                };
            }

            println!("--thread {} done.", 1);

            Ok(())
        })
        .unwrap();

    thread.thread().unpark();
    thread::sleep(Duration::from_secs(10));

    Ok(())
}
