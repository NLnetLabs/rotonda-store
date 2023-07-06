use log::trace;
use std::time::Duration;
use std::thread;

use rand::Rng;

use rotonda_store::prelude::*;

use rotonda_store::meta_examples::PrefixAs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    trace!("Starting one-threaded yolo testing....");
    let v4 = vec![8];
    let v6 = vec![8];
    let mut tree_bitmap = rotonda_store::SingleThreadedStore::<PrefixAs>::new(v4, v6);

    let mut pfx_int = 0_u32;

    let thread = std::thread::Builder::new()
        .name(1_u8.to_string())
        .spawn(move || -> Result<(), Box<dyn std::error::Error + Send>> {
            let mut rng= rand::thread_rng();

            println!("park thread {}", 1);
            thread::park();

            print!("\nstart {} ---", 1);

            while pfx_int <= 24 {
                pfx_int += 1;
                let pfx = Prefix::new_relaxed(
                    pfx_int.into_ipaddr(),
                    32,
                );

                print!("{}-", pfx_int);
                let asn: u32 = rng.gen();
                match tree_bitmap.insert(&pfx.unwrap(), PrefixAs(asn), None) {
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
