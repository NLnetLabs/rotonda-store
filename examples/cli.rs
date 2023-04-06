// #![cfg(feature = "cli")]

use ansi_term::Colour;
use rotonda_store::prelude::*;
use rotonda_store::PrefixAs;
use rotonda_store::{MatchOptions, MatchType, MultiThreadedStore};

use routecore::addr::Prefix;
use rustyline::Editor;
use rustyline::error::ReadlineError;

use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::process;

// #[cfg(feature = "cli")]
// use rustyline::error::ReadlineError;
// #[cfg(feature = "cli")]
// use rustyline::Editor;

fn get_first_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("expected 1 argument, but got none")),
        Some(file_path) => Ok(file_path),
    }
}

fn load_prefixes(
    pfxs: &mut Vec<PrefixRecord<PrefixAs>>,
) -> Result<(), Box<dyn Error>> {
    // Build the CSV reader and iterate over each record.
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;
    let mut rdr = csv::Reader::from_reader(file);
    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result?;

        let ip = record[0].parse::<std::net::IpAddr>()?;

        let len: u8 = record[1].parse().unwrap();
        let asn: u32 = record[2].parse().unwrap();
        let pfx = PrefixRecord::new(
            Prefix::new(ip, len)?,
            PrefixAs(asn),
        );

        // let ip: Vec<_> = record[0]
        //     .split('.')
        //     .map(|o| -> u8 { o.parse().unwrap() })
        //     .collect();
        // let net = std::net::Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]);
        // let len: u8 = record[1].parse().unwrap();
        // let asn: u32 = record[2].parse().unwrap();
        // let pfx = PrefixRecord::new_with_local_meta(
        //     Prefix::new(net.into(), len)?,
        //     PrefixAs(asn),
        // );
        pfxs.push(pfx);
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cli")]
    env_logger::init();

    let mut pfxs: Vec<PrefixRecord<PrefixAs>> = vec![];
    let tree_bitmap = MultiThreadedStore::<PrefixAs>::new()?;

    if let Err(err) = load_prefixes(&mut pfxs) {
        println!("error running example: {}", err);
        process::exit(1);
    }
    println!("finished loading {} prefixes...", pfxs.len());
    let start = std::time::Instant::now();

    for pfx in pfxs.into_iter() {
        tree_bitmap.insert(&pfx.prefix, pfx.meta)?;
    }
    let ready = std::time::Instant::now();
    // println!("{:#?}", tree_bitmap.store.prefixes);
    println!(
        "finished building tree in {} msecs...",
        ready.checked_duration_since(start).unwrap().as_millis()
    );

    // tree_bitmap.print_funky_stats();
    // let locks = tree_bitmap.acquire_prefixes_rwlock_read();
    let guard = &epoch::pin();

    let mut rl = Editor::<()>::new();
    if rl.load_history("/tmp/rotonda-store-history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline("(rotonda-store)> ");
        match readline {
            Ok(line) => {
                let s_pref: Vec<&str> = line.split('/').collect();

                if s_pref.len() < 2 {
                    if let Some(cmd) = line.chars().next() {
                        match cmd.to_string().as_ref() {
                            "p" => match line.chars().as_str() {
                                "p4" => {
                                    tree_bitmap
                                        .prefixes_iter_v4(guard)
                                        .for_each(|pfx| {
                                            println!(
                                                "{} {}",
                                                pfx.prefix, pfx.meta
                                            );
                                        });
                                    println!(
                                        "ipv4 prefixes :\t{}",
                                        tree_bitmap.prefixes_v4_len()
                                    );
                                }
                                "p6" => {
                                    tree_bitmap
                                        .prefixes_iter_v6(guard)
                                        .for_each(|pfx| {
                                            println!(
                                                "{} {}",
                                                pfx.prefix, pfx.meta
                                            );
                                        });
                                    println!(
                                        "ipv6 prefixes :\t{}",
                                        tree_bitmap.prefixes_v6_len()
                                    );
                                }
                                _ => {
                                    println!(
                                        "ipv4 prefixes :\t{}",
                                        tree_bitmap.prefixes_v4_len()
                                    );
                                    println!(
                                        "ipv6 prefixes :\t{}",
                                        tree_bitmap.prefixes_v6_len()
                                    );
                                    tree_bitmap
                                        .prefixes_iter(guard)
                                        .for_each(|pfx| {
                                            println!(
                                                "{} {}",
                                                pfx.prefix, pfx.meta
                                            );
                                        });
                                    println!(
                                        "total prefixes :\t{}",
                                        tree_bitmap.prefixes_len()
                                    );
                                }
                            },
                            "n" => {
                                // if let Some(num) = line.split(' ').collect::<Vec<&str>>().get(1) {
                                //     for n in tree_bitmap
                                //         .nodes()
                                //         .iter()
                                //         .take(num.parse::<usize>()?)
                                //     {
                                //         println!("{}", n);
                                //     }
                                // }

                                println!(
                                    "total nodes :\t{}",
                                    tree_bitmap.nodes_len()
                                );
                                println!(
                                    "ipv4 nodes :\t{}",
                                    tree_bitmap.nodes_v4_len()
                                );
                                println!(
                                    "ipv6 nodes :\t{}",
                                    tree_bitmap.nodes_v6_len()
                                );
                                // println!(
                                //     "{:#?}",
                                //     tree_bitmap
                                //         .nodes_v4_iter()
                                //         .collect::<Vec<_>>()
                                // );
                            }
                            _ => {
                                println!(
                                    "Error: unknown command {:?}",
                                    s_pref
                                );
                            }
                        }
                    } else {
                        println!(
                            "Error: can't parse prefix {:?}. Maybe add a /<LEN> part?",
                            s_pref
                        );
                    }
                    continue;
                }

                let len = s_pref[1].parse::<u8>();
                let len = match len {
                    Ok(len) => len,
                    Err(_) => {
                        println!("Error: can't parse prefix length {:?}. Should be a decimal number 0 - 255", s_pref[1]);
                        continue;
                    }
                };

                let ip: Result<std::net::IpAddr, _> = s_pref[0].parse();
                let pfx;

                match ip {
                    Ok(ip) => {
                        rl.add_history_entry(line.as_str());
                        println!("Searching for prefix: {}/{}", ip, len);

                        pfx = Prefix::new(ip, len);
                        match pfx {
                            Ok(p) => {
                                let query_result = tree_bitmap.match_prefix(
                                    &p,
                                    &MatchOptions {
                                        match_type: MatchType::ExactMatch,
                                        include_all_records: true,
                                        include_less_specifics: true,
                                        include_more_specifics: true,
                                    },
                                    guard,
                                );
                                println!("start query result");
                                println!("{}", query_result);
                                println!("end query result");
                                println!(
                                    "more_specifics: {}",
                                    query_result
                                        .more_specifics
                                        .map_or("None".to_string(), |x| x
                                            .to_string())
                                );
                                println!(
                                    "less_specifics: {}",
                                    query_result
                                        .less_specifics
                                        .map_or("None".to_string(), |x| x
                                            .to_string())
                                );

                                println!("--- numatch");
                                println!("more specifics");
                                println!(
                                    "{}",
                                    tree_bitmap
                                        .more_specifics_from(
                                            &Prefix::new_relaxed(ip, len)?,
                                            guard
                                        )
                                        .more_specifics
                                        .map_or("None".to_string(), |x| x
                                            .to_string())
                                );
                                println!("less specifics");
                                println!(
                                    "{}",
                                    tree_bitmap
                                        .less_specifics_from(
                                            &Prefix::new_relaxed(ip, len)?,
                                            guard
                                        )
                                        .less_specifics
                                        .map_or("None".to_string(), |x| x
                                            .to_string())
                                );
                            }
                            Err(
                                routecore::addr::PrefixError::NonZeroHost,
                            ) => {
                                println!("{}", Colour::Yellow.paint("Warning: Prefix has bits set to the right of the prefix length. Zeroing those out."));
                                println!(
                                    "{}",
                                    tree_bitmap.match_prefix(
                                        &Prefix::new_relaxed(ip, len)?,
                                        &MatchOptions {
                                            match_type:
                                                MatchType::ExactMatch,
                                            include_all_records: true,
                                            include_less_specifics: true,
                                            include_more_specifics: true
                                        },
                                        guard
                                    )
                                );
                                println!("--- numatch");
                                println!("more specifics");
                                println!(
                                    "{}",
                                    tree_bitmap
                                        .more_specifics_from(
                                            &Prefix::new_relaxed(ip, len)?,
                                            guard
                                        )
                                        .more_specifics
                                        .map_or("None".to_string(), |x| x
                                            .to_string())
                                );
                                println!("less specifics");
                                println!(
                                    "{}",
                                    tree_bitmap
                                        .less_specifics_from(
                                            &Prefix::new_relaxed(ip, len)?,
                                            guard
                                        )
                                        .less_specifics
                                        .map_or("None".to_string(), |x| x
                                            .to_string())
                                );
                            }
                            Err(_) => {
                                println!("Error: Can't parse prefix. Pleasy try again.");
                                continue;
                            }
                        }
                    }
                    Err(err) => {
                        println!(
                            "Error: Can't parse address part. {:?}: {}",
                            s_pref[0], err
                        );
                    }
                };
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(_err) => {
                println!("Error: Can't parse the command");
                continue;
            }
        }
    }
    rl.save_history("/tmp/rotonda-store-history.txt").unwrap();
    Ok(())
}
