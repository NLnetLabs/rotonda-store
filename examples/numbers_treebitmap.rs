use rotonda_store::prelude::*;

use rotonda_store::PrefixAs;
use rotonda_macros::create_store;
// use routecore::addr::Prefix;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::net::{IpAddr, Ipv4Addr};
use std::process;

#[create_store((
    [4, 4, 4, 4, 4, 4, 4, 4],
    [3, 4, 5, 4]
))]
struct MyStore<PrefixAs>;

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
        // let pfx = Prefix::<u32, PrefixAs>::new_with_meta(net, len, asn);
        let ip: Vec<_> = record[0]
            .split('.')
            .map(|o| -> u8 { o.parse().unwrap() })
            .collect();
        let net = IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]));
        let len: u8 = record[1].parse().unwrap();
        let asn: u32 = record[2].parse().unwrap();
        let pfx = PrefixRecord::<PrefixAs>::new(
            Prefix::new(net, len)?,
            PrefixAs(asn),
        );
        pfxs.push(pfx);
        // trie.insert(&pfx);
        // println!("{:?}", pfx);
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let strides_vec = [vec![4, 4, 4, 4, 4, 4, 4, 4], vec![3, 4, 5, 4]];

    for _strides in strides_vec.iter() {
        let mut pfxs: Vec<PrefixRecord<PrefixAs>> = vec![];
        let tree_bitmap: MyStore<PrefixAs> = MyStore::<PrefixAs>::new()?;

        if let Err(err) = load_prefixes(&mut pfxs) {
            println!("error running example: {}", err);
            process::exit(1);
        }

        for pfx in pfxs.into_iter() {
            tree_bitmap.insert(&pfx.prefix, pfx.meta)?;
        }

        println!("{}", tree_bitmap.stats());
    }
    Ok(())
}
