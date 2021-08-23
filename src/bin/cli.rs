#[cfg(feature = "cli")]
use ansi_term::Colour;

use rotonda_store::common::{NoMeta, Prefix, PrefixAs};
use rotonda_store::{InMemNodeId, InMemStorage, SizedStrideNode, StorageBackend, TreeBitMap};
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::process;

#[cfg(feature = "cli")]
use rustyline::error::ReadlineError;
#[cfg(feature = "cli")]
use rustyline::Editor;

fn get_first_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("expected 1 argument, but got none")),
        Some(file_path) => Ok(file_path),
    }
}

fn load_prefixes(pfxs: &mut Vec<Prefix<u32, PrefixAs>>) -> Result<(), Box<dyn Error>> {
    // Build the CSV reader and iterate over each record.
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;
    let mut rdr = csv::Reader::from_reader(file);
    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result?;
        let ip: Vec<_> = record[0]
            .split('.')
            .map(|o| -> u8 { o.parse().unwrap() })
            .collect();
        let net = std::net::Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]);
        let len: u8 = record[1].parse().unwrap();
        let asn: u32 = record[2].parse().unwrap();
        let pfx = Prefix::<u32, PrefixAs>::new_with_meta(net.into(), len, PrefixAs(asn));
        pfxs.push(pfx);
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    type StoreType = InMemStorage<u32, PrefixAs>;
    let mut pfxs: Vec<Prefix<u32, PrefixAs>> = vec![];
    let mut tree_bitmap: TreeBitMap<StoreType> = TreeBitMap::new(vec![4]);

    if let Err(err) = load_prefixes(&mut pfxs) {
        println!("error running example: {}", err);
        process::exit(1);
    }
    println!("finished loading {} prefixes...", pfxs.len());
    let start = std::time::Instant::now();

    for pfx in pfxs.into_iter() {
        tree_bitmap.insert(pfx)?;
    }
    let ready = std::time::Instant::now();
    // println!("{:#?}", tree_bitmap.store.prefixes);
    println!(
        "finished building tree in {} msecs...",
        ready.checked_duration_since(start).unwrap().as_millis()
    );

    let total_nodes = tree_bitmap.stats.iter().fold(0, |mut acc, c| {
        acc += c.created_nodes.iter().fold(0, |mut sum, l| {
            sum += l.count;
            sum
        });
        acc
    });
    println!("prefix vec size {}", tree_bitmap.store.get_prefixes_len());
    println!("finished building tree...");
    println!("{:?} nodes created", total_nodes);
    println!(
        "size of node: {} bytes",
        std::mem::size_of::<SizedStrideNode<u32, InMemNodeId>>()
    );
    println!(
        "memory used by nodes: {}kb",
        total_nodes * std::mem::size_of::<SizedStrideNode<u32, InMemNodeId>>() / 1024
    );
    println!(
        "size of prefix: {} bytes",
        std::mem::size_of::<Prefix<u32, PrefixAs>>()
    );
    println!(
        "memory used by prefixes: {}kb",
        tree_bitmap.store.get_prefixes_len() * std::mem::size_of::<Prefix<u32, NoMeta>>() / 1024
    );
    println!("stride division  {:?}", tree_bitmap.strides);

    for s in &tree_bitmap.stats {
        println!("{:?}", s);
    }

    println!(
        "level\t[{}|{}] nodes occupied/max nodes percentage_max_nodes_occupied prefixes",
        Colour::Blue.paint("nodes"),
        Colour::Green.paint("prefixes")
    );
    let bars = ["▏", "▎", "▍", "▌", "▋", "▊", "▉"];
    let mut stride_bits = [0, 0];
    const SCALE: u32 = 5500;

    for stride in tree_bitmap.strides.iter().enumerate() {
        // let level = stride.0;
        stride_bits = [stride_bits[1] + 1, stride_bits[1] + stride.1];
        let nodes_num = tree_bitmap
            .stats
            .iter()
            .find(|s| s.stride_len == *stride.1)
            .unwrap()
            .created_nodes[stride.0]
            .count as u32;
        let prefixes_num = tree_bitmap
            .stats
            .iter()
            .find(|s| s.stride_len == *stride.1)
            .unwrap()
            .prefixes_num[stride.0]
            .count as u32;

        let n = (nodes_num / SCALE) as usize;
        let max_pfx: u64 = u64::pow(2, stride_bits[1] as u32);

        print!("{}-{}\t", stride_bits[0], stride_bits[1]);

        for _ in 0..n {
            print!("{}", Colour::Blue.paint("█"));
        }

        print!(
            "{}",
            Colour::Blue.paint(bars[((nodes_num % SCALE) / (SCALE / 7)) as usize]) //  = scale / 7
        );

        print!(
            " {}/{} {:.2}%",
            nodes_num,
            max_pfx,
            (nodes_num as f64 / max_pfx as f64) * 100.0
        );
        print!("\n\t");

        let n = (prefixes_num / SCALE) as usize;
        for _ in 0..n {
            print!("{}", Colour::Green.paint("█"));
        }

        print!(
            "{}",
            Colour::Green.paint(bars[((nodes_num % SCALE) / (SCALE / 7)) as usize]) //  = scale / 7
        );

        println!(" {}", prefixes_num);
    }

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
                    println!(
                        "Error: can't parse prefix {:?}. Maybe add a /<LEN> part?",
                        s_pref
                    );
                    continue;
                }

                let len = s_pref[1].parse::<u8>().unwrap();
                let ip: Result<std::net::Ipv4Addr, _> = s_pref[0].parse();
                let pfx;

                match ip {
                    Ok(ip) => {
                        rl.add_history_entry(line.as_str());
                        println!("Searching for prefix: {}/{}", ip, len);

                        pfx = Prefix::<u32, NoMeta>::new(ip.into(), len);
                        println!("{:?}", tree_bitmap.match_longest_prefix_with_less_specifics(&pfx));
                    }
                    Err(err) => {
                        println!("Error: Can't parse address part. {:?}: {}", s_pref[0], err);
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
            Err(err) => {
                println!("Error: {:?}", err);
                continue;
            }
        }
    }
    rl.save_history("/tmp/rotonda-store-history.txt").unwrap();
    Ok(())
}
