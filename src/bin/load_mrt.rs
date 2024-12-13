use std::collections::BTreeSet;
use std::fmt;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use inetnum::addr::Prefix;
use memmap2::Mmap;
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use rotonda_store::custom_alloc::PersistStrategy;
use rotonda_store::custom_alloc::StoreConfig;
use rotonda_store::custom_alloc::UpsertReport;
use rotonda_store::prelude::multi::PrefixStoreError;
use rotonda_store::prelude::multi::{MultiThreadedStore, RouteStatus};
use rotonda_store::PublicRecord;
use routecore::mrt::MrtFile;

use rand::seq::SliceRandom;
use routecore::mrt::RibEntryIterator;
use routecore::mrt::TableDumpIterator;

#[derive(Clone, Debug)]
struct PaBytes(Vec<u8>);

impl std::fmt::Display for PaBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl AsRef<[u8]> for PaBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<Vec<u8>> for PaBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl rotonda_store::Meta for PaBytes {
    type Orderable<'a> = u32;

    type TBI = u32;

    fn as_orderable(&self, _tbi: Self::TBI) -> Self::Orderable<'_> {
        todo!()
    }
}

#[derive(Copy, Clone, Default)]
struct UpsertCounters {
    unique_prefixes: usize,
    unique_routes: usize,
    persisted_routes: usize,
    total_routes: usize,
}

impl std::ops::AddAssign for UpsertCounters {
    fn add_assign(&mut self, rhs: Self) {
        self.unique_prefixes += rhs.unique_prefixes;
        self.unique_routes += rhs.unique_routes;
        self.persisted_routes += rhs.persisted_routes;
        self.total_routes += rhs.total_routes;
    }
}

impl std::ops::Add for UpsertCounters {
    type Output = UpsertCounters;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            unique_prefixes: self.unique_prefixes + rhs.unique_prefixes,
            unique_routes: self.unique_routes + rhs.unique_routes,
            persisted_routes: self.persisted_routes + rhs.persisted_routes,
            total_routes: self.total_routes + rhs.total_routes,
        }
    }
}

impl std::fmt::Display for UpsertCounters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "inserted unique prefixes:\t{}", self.unique_prefixes)?;
        writeln!(f, "inserted unique routes:\t\t{}", self.unique_routes)?;
        writeln!(f, "persisted routes:\t\t{}", self.persisted_routes)?;
        writeln!(f, "total routes:\t\t\t{}", self.total_routes)?;
        writeln!(
            f,
            "calculated persisted routes:\t{}",
            self.total_routes - self.unique_routes
        )
    }
}

fn counter_update(
    counters: &mut UpsertCounters,
) -> impl FnMut(UpsertReport) -> UpsertCounters + '_ {
    move |r| match (r.prefix_new, r.mui_new) {
        // new prefix, new mui
        (true, true) => {
            counters.unique_prefixes += 1;
            counters.unique_routes += 1;
            counters.total_routes += 1;
            *counters
        }
        // old prefix, new mui
        (false, true) => {
            counters.unique_routes += 1;
            counters.total_routes += 1;
            *counters
        }
        // old prefix, old mui
        (false, false) => {
            counters.total_routes += 1;
            *counters
        }
        // new prefix, old mui
        (true, false) => {
            panic!("THIS DOESN'T MEAN ANYTHING!");
        }
    }
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Enable concurrent route inserts
    #[arg(short, long, default_value_t = false)]
    mt: bool,

    /// Prime store by sequentially inserting prefixes first
    #[arg(short, long, default_value_t = false)]
    prime: bool,

    /// Enable concurrent priming inserts
    #[arg(long, default_value_t = false)]
    mt_prime: bool,

    /// Shuffle prefixes before priming the store. Enables priming.
    #[arg(short, long, default_value_t = false)]
    shuffle: bool,

    /// Use the same store for all MRT_FILES
    #[arg(long, default_value_t = false)]
    single_store: bool,

    /// MRT files to process.
    #[arg(required = true)]
    mrt_files: Vec<PathBuf>,

    /// Don't insert in store, only parse MRT_FILES
    #[arg(long, default_value_t = false)]
    parse_only: bool,

    /// Verify the persisted entries
    #[arg(long, default_value_t = false)]
    verify: bool,
}

fn insert<T: rotonda_store::Meta>(
    store: &MultiThreadedStore<T>,
    prefix: &Prefix,
    mui: u32,
    ltime: u64,
    route_status: RouteStatus,
    value: T,
) -> Result<UpsertReport, PrefixStoreError> {
    let record = PublicRecord::new(mui, ltime, route_status, value);
    store
        .insert(prefix, record, None)
        .inspect_err(|e| eprintln!("Error in test_store: {e}"))
}

fn par_load_prefixes(
    mrt_file: &MrtFile,
    shuffle: bool,
) -> Vec<(Prefix, u16)> {
    let t0 = std::time::Instant::now();
    let mut prefixes = mrt_file
        .tables()
        .unwrap()
        .par_bridge()
        .map(|(_fam, reh)| {
            let iter = routecore::mrt::SingleEntryIterator::new(reh);
            iter.map(|(prefix, peer_idx, _)| (prefix, peer_idx))
        })
        .flatten_iter()
        .collect::<Vec<_>>();

    eprintln!(
        "loaded file with {} prefixes in {}ms",
        prefixes.len(),
        t0.elapsed().as_millis()
    );

    if shuffle {
        let t_s = Instant::now();
        eprint!("shuffling prefixes... ");
        prefixes.shuffle(&mut rand::thread_rng());
        eprintln!("done! took {}ms", t_s.elapsed().as_millis());
    }

    prefixes
}

fn mt_parse_and_insert_table(
    tables: TableDumpIterator<&[u8]>,
    store: Option<&MultiThreadedStore<PaBytes>>,
    ltime: u64,
) -> (UpsertCounters, Vec<Prefix>) {
    let persist_strategy =
        store.map_or(PersistStrategy::MemoryOnly, |p| p.persist_strategy());
    let counters = tables
        .par_bridge()
        .map(|(_fam, reh)| {
            let mut local_counters = UpsertCounters::default();
            let iter = routecore::mrt::SingleEntryIterator::new(reh);
            let persisted_prefixes = &mut vec![];
            // let mut cnt = 0;
            for (prefix, peer_idx, pa_bytes) in iter {
                // cnt += 1;
                // let (prefix, peer_idx, pa_bytes) = e;
                let mui = peer_idx.into();
                let val = PaBytes(pa_bytes);
                let mut persisted_routes = 0;

                if let Some(store) = store {
                    let counters = insert(
                        store,
                        &prefix,
                        mui,
                        ltime,
                        RouteStatus::Active,
                        val,
                    )
                    .map(|r| match (r.prefix_new, r.mui_new) {
                        // new prefix, new mui
                        (true, true) => {
                            match persist_strategy {
                                PersistStrategy::WriteAhead
                                | PersistStrategy::PersistOnly => {
                                    persisted_prefixes.push(prefix);
                                    persisted_routes = 1;
                                }
                                _ => {}
                            };
                            UpsertCounters {
                                unique_prefixes: 1,
                                unique_routes: 1,
                                persisted_routes,
                                total_routes: 1,
                            }
                        }
                        // old prefix, new mui
                        (false, true) => {
                            match persist_strategy {
                                PersistStrategy::WriteAhead
                                | PersistStrategy::PersistOnly => {
                                    persisted_prefixes.push(prefix);
                                    persisted_routes = 1;
                                }
                                _ => {}
                            };

                            UpsertCounters {
                                unique_prefixes: 0,
                                unique_routes: 1,
                                persisted_routes,
                                total_routes: 1,
                            }
                        }
                        // old prefix, old mui
                        (false, false) => {
                            if persist_strategy != PersistStrategy::MemoryOnly
                            {
                                persisted_prefixes.push(prefix);
                                persisted_routes = 1;
                            }
                            UpsertCounters {
                                unique_prefixes: 0,
                                unique_routes: 0,
                                persisted_routes,
                                total_routes: 1,
                            }
                        }
                        // new prefix, old mui
                        (true, false) => {
                            panic!("THIS DOESN'T MEAN ANYTHING!");
                        }
                    })
                    .unwrap();

                    local_counters += counters;
                }
            }
            (local_counters, persisted_prefixes.clone())
        })
        .fold(
            || (UpsertCounters::default(), vec![]),
            |mut acc, c| {
                acc.1.extend(c.1);
                (acc.0 + c.0, acc.1)
            },
        )
        .reduce(
            || (UpsertCounters::default(), vec![]),
            |mut acc, c| {
                acc.1.extend(c.1);
                (acc.0 + c.0, acc.1)
            },
        );

    println!("{}", counters.0);

    counters
}

fn st_parse_and_insert_table(
    entries: RibEntryIterator<&[u8]>,
    store: Option<&MultiThreadedStore<PaBytes>>,
    ltime: u64,
) -> UpsertCounters {
    let mut counters = UpsertCounters::default();
    let mut cnt = 0;
    let t0 = std::time::Instant::now();

    for (_, peer_idx, _, prefix, pamap) in entries {
        cnt += 1;
        let mui = peer_idx.into();
        let val = PaBytes(pamap);

        if let Some(store) = store {
            insert(store, &prefix, mui, ltime, RouteStatus::Active, val)
                .map(counter_update(&mut counters))
                .unwrap();
        }
    }

    println!(
        "parsed & inserted {} prefixes in {}ms",
        cnt,
        t0.elapsed().as_millis()
    );
    println!("{}", counters);

    counters
}

fn mt_prime_store(
    prefixes: &Vec<(Prefix, u16)>,
    store: &MultiThreadedStore<PaBytes>,
) -> UpsertCounters {
    let t0 = std::time::Instant::now();

    let counters = prefixes
        .par_iter()
        .fold(UpsertCounters::default, |mut acc, p| {
            insert(
                store,
                &p.0,
                p.1 as u32,
                0,
                RouteStatus::InActive,
                PaBytes(vec![]),
            )
            .map(counter_update(&mut acc))
            .unwrap()
        })
        .reduce(UpsertCounters::default, |c1, c2| c1 + c2);

    println!(
        "primed {} prefixes in {}ms",
        prefixes.len(),
        t0.elapsed().as_millis()
    );

    // println!("{}", counters);

    counters
}

fn st_prime_store(
    prefixes: &Vec<(Prefix, u16)>,
    store: &MultiThreadedStore<PaBytes>,
) -> UpsertCounters {
    let mut counters = UpsertCounters::default();

    for p in prefixes {
        insert(
            store,
            &p.0,
            p.1 as u32,
            0,
            RouteStatus::InActive,
            PaBytes(vec![]),
        )
        .map(counter_update(&mut counters))
        .unwrap();
    }

    counters
}

fn main() {
    let store_config = StoreConfig {
        persist_strategy: PersistStrategy::PersistOnly,
        persist_path: "/tmp/rotonda/".into(),
    };

    let args = Cli::parse();

    let t_total = Instant::now();

    let mut global_counters = UpsertCounters::default();
    let mut mib_total: usize = 0;
    let mut inner_stores = vec![];
    let mut persisted_prefixes = BTreeSet::new();

    // Create all the stores necessary, and if at least one is created, create
    // a reference to the first one.
    let mut store = match &args {
        a if a.single_store && a.parse_only => {
            eprintln!(
                "Can't combine --parse-only and --single-store. 
                Make up your mind."
            );
            return;
        }
        a if a.single_store => {
            inner_stores.push(
                MultiThreadedStore::<PaBytes>::new_with_config(store_config)
                    .unwrap(),
            );
            println!("created a single-store\n");
            Some(&inner_stores[0])
        }
        a if a.parse_only => {
            println!("No store created (parse only)");
            None
        }
        _ => {
            for _ in &args.mrt_files {
                inner_stores.push(
                    MultiThreadedStore::<PaBytes>::try_default().unwrap(),
                );
            }
            println!("Number of created stores: {}", inner_stores.len());
            Some(&inner_stores[0])
        }
    };

    // Loop over all the mrt-files specified as arguments
    for (f_index, mrtfile) in args.mrt_files.iter().enumerate() {
        print!("file #{} ", f_index);

        let file = File::open(mrtfile).unwrap();
        let mmap = unsafe { Mmap::map(&file).unwrap() };
        println!("{} ({}MiB)", mrtfile.to_string_lossy(), mmap.len() >> 20);
        mib_total += mmap.len() >> 20;

        let mrt_file = MrtFile::new(&mmap[..]);

        if !args.single_store && !args.parse_only {
            println!("use store #{}", f_index);
            store = Some(&inner_stores[f_index]);
        }
        // Load the mrt file, maybe shuffle, and maybe prime the store
        match &args {
            a if a.mt_prime && a.prime => {
                eprintln!(
                    "--prime and --mt-prime can't be combined.
                    Make up your mind."
                );
                return;
            }
            a if a.prime => {
                let prefixes = par_load_prefixes(&mrt_file, a.shuffle);
                st_prime_store(&prefixes, store.unwrap());
            }
            a if a.mt_prime => {
                let prefixes = par_load_prefixes(&mrt_file, a.shuffle);
                mt_prime_store(&prefixes, store.unwrap());
            }
            _ => {}
        };

        // Parse the prefixes in the file, and maybe insert them into the
        // Store
        global_counters += match &args {
            a if a.mt => {
                let tables = mrt_file.tables().unwrap();
                let (counters, per_pfxs) =
                    mt_parse_and_insert_table(tables, store, f_index as u64);
                if args.verify {
                    persisted_prefixes.extend(&per_pfxs)
                }
                counters
            }
            _ => {
                let entries = mrt_file.rib_entries().unwrap();
                st_parse_and_insert_table(entries, store, f_index as u64)
            }
        };
    }

    if let Some(store) = store {
        let res = store.flush_to_disk();
        if res.is_err() {
            eprintln!("Persistence Error: {:?}", res);
        }
    }

    // eprintln!(
    //     "processed {} routes in {} files in {:.2}s",
    //     routes_count,
    //     args.mrt_files.len(),
    //     t_total.elapsed().as_millis() as f64 / 1000.0
    // );

    println!("upsert counters");
    println!("---------------");
    println!("{}", global_counters);

    if let Some(store) = store {
        println!("store in-memory counters");
        println!("------------------------");
        println!("prefixes:\t\t\t{:?}\n", store.prefixes_count());

        println!("store persistence counters");
        println!("--------------------------");
        println!(
            "approx. prefixes:\t\t{} + {} = {}",
            store.approx_persisted_items().0,
            store.approx_persisted_items().1,
            store.approx_persisted_items().0
                + store.approx_persisted_items().1
        );
        println!(
            "disk size of persisted store:\t{}MiB\n",
            store.disk_space() / (1024 * 1024)
        );
    }

    println!(
        "{:.0} routes per second\n\
            {:.0} MiB per second",
        global_counters.total_routes as f64
            / t_total.elapsed().as_secs() as f64,
        mib_total as f64 / t_total.elapsed().as_secs() as f64
    );

    if args.verify {
        println!("\nverifying disk persistence...");
        let mut max_len = 0;
        for pfx in persisted_prefixes {
            let values = store.unwrap().get_records_for_prefix(&pfx);
            if values.is_empty() {
                eprintln!("Found empty prefix on disk");
                eprintln!("prefix: {}", pfx);
                return;
            }
            if values.len() > max_len {
                max_len = values.len();
                println!(
                    "len {}: {} -> {:?}",
                    max_len,
                    pfx,
                    store.unwrap().get_records_for_prefix(&pfx)
                );
            }
            values.iter().filter(|v| v.meta.0.is_empty()).for_each(|v| {
                println!("withdraw for {}, mui {}", pfx, v.multi_uniq_id)
            })
        }
    }
}
