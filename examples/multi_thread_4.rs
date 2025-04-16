use inetnum::addr::Prefix;
use inetnum::asn::Asn;
use log::trace;
use rotonda_store::match_options::{IncludeHistory, MatchOptions, MatchType};
use rotonda_store::prefix_record::{Meta, Record, RouteStatus};
use rotonda_store::rib::config::MemoryOnlyConfig;
use rotonda_store::rib::StarCastRib;
use rotonda_store::IntoIpAddr;
use std::time::Duration;
use std::{sync::Arc, thread};

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct BytesPrefixAs(pub [u8; 4]);

impl AsRef<[u8]> for BytesPrefixAs {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<Vec<u8>> for BytesPrefixAs {
    fn from(value: Vec<u8>) -> Self {
        Self(*value.first_chunk::<4>().unwrap())
    }
}

impl Meta for BytesPrefixAs {
    type Orderable<'a> = Asn;
    type TBI = ();

    fn as_orderable(&self, _tbi: Self::TBI) -> Asn {
        u32::from_be_bytes(self.0).into()
    }
}

impl std::fmt::Display for BytesPrefixAs {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AS{:?}", self.0)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    trace!("Starting multi-threaded yolo testing....");
    let tree_bitmap = Arc::new(
        StarCastRib::<BytesPrefixAs, MemoryOnlyConfig>::try_default()?,
    );
    let f = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let pfx = Prefix::new_relaxed(
        0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
        32,
    );

    let threads = (0..1).enumerate().map(|(i, _)| {
        let tree_bitmap = tree_bitmap.clone();
        // let start_flag = Arc::clone(&f);

        std::thread::Builder::new()
            .name(i.to_string())
            .spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send>> {
                    println!("park thread {}", i);
                    thread::park();

                    print!("\nstart {} ---", i);
                    let mut x: u32 = 0;
                    loop {
                        x += 1;

                        let pfx = Prefix::new_relaxed(x.into_ipaddr(), 32);
                        // print!("{}-", i);
                        match tree_bitmap.insert(
                            &pfx.unwrap(),
                            Record::new(
                                0,
                                0,
                                RouteStatus::Active,
                                BytesPrefixAs((i as u32).to_be_bytes()),
                            ),
                            None,
                        ) {
                            Ok(metrics) => {
                                if metrics.cas_count > 0 {
                                    eprintln!(
                                        "{} {} {:?}
                                        retry count: {},",
                                        std::thread::current()
                                            .name()
                                            .unwrap(),
                                        metrics.prefix_new,
                                        pfx,
                                        metrics.cas_count
                                    );
                                }
                            }
                            Err(e) => {
                                println!("{}", e);
                            }
                        };

                        if x % 1_000_000 == 0 {
                            println!(
                                "{:?} {} (prefixes count: {:?},
                                nodes count: {}",
                                std::thread::current().name(),
                                x,
                                tree_bitmap.prefixes_count(),
                                tree_bitmap.nodes_count()
                            );
                        }
                    }
                },
            )
            .unwrap()
    });

    f.store(true, std::sync::atomic::Ordering::Release);
    threads.for_each(|t| {
        t.thread().unpark();
    });

    thread::sleep(Duration::from_secs(60));

    println!("------ end of inserts\n");

    let guard = &rotonda_store::epoch::pin();

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
    )?;
    println!("query result");
    println!("{}", s_spfx);
    println!("{}", s_spfx.more_specifics.unwrap());

    println!("-----------");

    Ok(())
}
