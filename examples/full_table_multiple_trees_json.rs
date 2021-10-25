use rotonda_store::common::PrefixAs;
use rotonda_store::{MatchOptions, MatchType, MultiThreadedStore};
use routecore::prefix::Prefix;
use routecore::record::{Record, SinglePrefixRoute};
use std::error::Error;
use std::fs::File;
use std::process;

fn main() -> Result<(), Box<dyn Error>> {
    const CSV_FILE_PATH: &str = "./data/uniq_pfx_asn_dfz_rnd.csv";

    fn load_prefixes(pfxs: &mut Vec<SinglePrefixRoute<PrefixAs>>) -> Result<(), Box<dyn Error>> {
        let file = File::open(CSV_FILE_PATH)?;
        let mut rdr = csv::Reader::from_reader(file);
        for result in rdr.records() {
            let record = result?;
            let ip: Vec<_> = record[0]
                .split('.')
                .map(|o| -> u8 { o.parse().unwrap() })
                .collect();
            let net = std::net::Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]);
            let len: u8 = record[1].parse().unwrap();
            let asn: u32 = record[2].parse().unwrap();
            let pfx = SinglePrefixRoute::<PrefixAs>::new_with_local_meta(
                Prefix::new(net.into(), len)?,
                PrefixAs(asn),
            );
            pfxs.push(pfx);
        }
        Ok(())
    }

    println!("[");
    let strides_vec = [
        vec![8],
        vec![4],
        vec![6, 6, 6, 6, 4, 4],
        vec![3, 4, 4, 6, 7, 8],
    ];

    for strides in strides_vec.iter().enumerate() {
        println!("[");
        for n in 1..6 {
            let mut pfxs: Vec<SinglePrefixRoute<PrefixAs>> = vec![];
            let mut tree_bitmap =
                MultiThreadedStore::<PrefixAs>::new(strides.1.to_owned(), Vec::from([]));

            if let Err(err) = load_prefixes(&mut pfxs) {
                println!("error running example: {}", err);
                process::exit(1);
            }
            // println!("finished loading {} prefixes...", pfxs.len());
            let start = std::time::Instant::now();

            let inserts_num = pfxs.len();
            for pfx in pfxs.into_iter() {
                tree_bitmap.insert(&pfx.prefix, pfx.meta.into_owned())?;
            }
            let ready = std::time::Instant::now();
            let dur_insert_nanos = ready.checked_duration_since(start).unwrap().as_nanos();

            let inet_max = 255;
            let len_max = 32;

            let start = std::time::Instant::now();
            for i_net in 0..inet_max {
                for s_len in 0..len_max {
                    for ii_net in 0..inet_max {
                        let pfx =
                            Prefix::new(std::net::Ipv4Addr::new(i_net, ii_net, 0, 0).into(), s_len)?;
                        tree_bitmap.match_prefix(
                            &pfx,
                            &MatchOptions {
                                match_type: MatchType::LongestMatch,
                                include_less_specifics: false,
                                include_more_specifics: false,
                            },
                        );
                    }
                }
            }
            let ready = std::time::Instant::now();
            let dur_search_nanos = ready.checked_duration_since(start).unwrap().as_nanos();
            let searches_num = inet_max as u128 * inet_max as u128 * len_max as u128;

            println!("{{");
            println!("\"type\": \"treebitmap_univec\",");
            println!("\"strides\": {:?},", &tree_bitmap.strides());
            println!("\"run_no\": {},", n);
            println!("\"inserts_num\": {},", inserts_num);
            println!("\"insert_duration_nanos\": {},", dur_insert_nanos);
            println!(
                "\"global_prefix_vec_size\": {},",
                tree_bitmap.prefixes_len()
            );
            println!(
                "\"global_node_vec_size\": {},",
                tree_bitmap.nodes_len()
            );
            println!(
                "\"insert_time_nanos\": {},",
                dur_insert_nanos as f32 / inserts_num as f32
            );
            println!("\"searches_num\": {},", searches_num);
            println!("\"search_duration_nanos\": {},", dur_search_nanos);
            println!(
                "\"search_time_nanos\": {}",
                dur_search_nanos as f32 / searches_num as f32
            );
            println!("}}{}", if n != 5 { "," } else { "" });
        }

        println!(
            "]{}",
            if strides.0 != strides_vec.len() - 1 {
                ","
            } else {
                ""
            }
        );
    }
    println!("]");
    Ok(())
}
