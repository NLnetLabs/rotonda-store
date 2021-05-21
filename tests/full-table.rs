#[cfg(test)]
mod test {

    use rotonda_store::{InMemStorage, common::{NoMeta, Prefix, PrefixAs}};
    use rotonda_store::{TreeBitMap};
    use std::fs::File;
    use std::process;
    use std::{error::Error};

    #[test]
    fn test_full_table_from_csv() -> Result<(), Box<dyn Error>> {

        // These constants are all contingent on the exact csv file,
        // being loaded!
        const CSV_FILE_PATH: &str = "./data/uniq_pfx_asn_dfz_rnd.csv";
        const SEARCHES_NUM: u128 = 2080800;
        const INSERTS_NUM: usize = 893943;
        const GLOBAL_PREFIXES_VEC_SIZE: usize = 886117;
        const FOUND_PREFIXES: u32 = 1322993;

        fn load_prefixes(pfxs: &mut Vec<Prefix<u32, PrefixAs>>) -> Result<(), Box<dyn Error>> {
            let file = File::open(CSV_FILE_PATH)?;
            let mut rdr = csv::Reader::from_reader(file);
            for result in rdr.records() {
                let record = result?;
                let ip: Vec<_> = record[0]
                    .split(".")
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

        let strides_vec = [
            vec![8],
            vec![4],
            vec![6, 6, 6, 6, 4, 4],
            vec![3, 4, 4, 6, 7, 8],
        ];
        type StoreType = InMemStorage<u32, PrefixAs>;
        for strides in strides_vec.iter().enumerate() {
            let mut pfxs: Vec<Prefix<u32, PrefixAs>> = vec![];
            let mut tree_bitmap: TreeBitMap<StoreType> =
                TreeBitMap::new(strides.1.to_owned());

            if let Err(err) = load_prefixes(&mut pfxs) {
                println!("error running example: {}", err);
                process::exit(1);
            }

            let inserts_num = pfxs.len();
            for pfx in pfxs.into_iter() {
                tree_bitmap.insert(pfx)?;
            }

            let inet_max = 255;
            let len_max = 32;

            let mut found_counter = 0_u32;
            (0..inet_max).into_iter().for_each(|i_net| {
                (0..len_max).into_iter().for_each(|s_len| {
                    (0..inet_max).into_iter().for_each(|ii_net| {
                        let pfx = Prefix::<u32, NoMeta>::new(
                            std::net::Ipv4Addr::new(i_net, ii_net, 0, 0).into(),
                            s_len,
                        );
                        if let Some(_pfx) = tree_bitmap.match_longest_prefix_only(&pfx) {
                            assert!(_pfx.len <= pfx.len);
                            assert!(_pfx.net <= pfx.net);
                            found_counter += 1;
                        }
                    });
                });
            });
            println!("found pfx: {}", found_counter);

            let searches_num = inet_max as u128 * inet_max as u128 * len_max as u128;

            assert_eq!(searches_num, SEARCHES_NUM as u128);
            assert_eq!(inserts_num, INSERTS_NUM);
            assert_eq!(tree_bitmap.store.prefixes.len(), GLOBAL_PREFIXES_VEC_SIZE);
            assert_eq!(found_counter, FOUND_PREFIXES);
        }
        Ok(())
    }
}
