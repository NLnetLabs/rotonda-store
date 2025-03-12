use inetnum::addr::Prefix;
use log::trace;

use rotonda_store::epoch;
use rotonda_store::match_options::{IncludeHistory, MatchOptions, MatchType};
use rotonda_store::prefix_record::{Record, RouteStatus};
use rotonda_store::rib::config::MemoryOnlyConfig;
use rotonda_store::rib::StarCastRib;
use rotonda_store::test_types::PrefixAs;
use rotonda_store::IntoIpAddr;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    trace!("Starting multi-threaded yolo testing....");
    let tree_bitmap =
        StarCastRib::<PrefixAs, MemoryOnlyConfig>::try_default()?;
    // let f = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let pfx = Prefix::new_relaxed(
        0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
        32,
    );

    print!("\nstart {} ---", 1);
    let mut x = 0;
    loop {
        x += 1;
        // print!("{}-", i);
        match tree_bitmap.insert(
            &pfx.unwrap(),
            Record::new(
                0,
                0,
                RouteStatus::Active,
                PrefixAs::new((x % 1000).into()),
            ),
            None,
        ) {
            Ok(_) => {}
            Err(e) => {
                println!("{}", e);
            }
        };
        if (x % 1_000_000) == 0 {
            println!("inserts: {}", x);
        }
        if x == 100_000_000 {
            break;
        }
    }
    println!("--thread {} done.", 1);

    println!("------ end of inserts\n");

    let guard = unsafe { epoch::unprotected() };

    let s_spfx = tree_bitmap.match_prefix(
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
    );
    println!("query result");
    println!("{}", s_spfx);
    println!("{}", s_spfx.more_specifics.unwrap());

    println!("-----------");

    Ok(())
}
