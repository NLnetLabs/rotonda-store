
#![cfg(test)]
    
mod full_table {    
    use crate::{
        prelude::*, SingleThreadedStore,
    };

    use std::error::Error;
    use std::fs::File;
    use std::process;

    #[derive(Debug, Clone)]
    pub struct ComplexPrefixAs(pub Vec<u32>);

    impl MergeUpdate for ComplexPrefixAs {
        type UserDataIn = ();
        type UserDataOut = ();

        fn merge_update(
            &mut self,
            update_record: ComplexPrefixAs,
            _: Option<&Self::UserDataIn>,
        ) -> Result<(), Box<dyn std::error::Error>> {
            self.0 = update_record.0;
            Ok(())
        }

        fn clone_merge_update(
            &self,
            update_meta: &Self,
            _: Option<&Self::UserDataIn>,
        ) -> Result<(Self, Self::UserDataOut), Box<dyn std::error::Error>>
        where
            Self: std::marker::Sized,
        {
            let mut new_meta = update_meta.0.clone();
            new_meta.push(self.0[0]);
            Ok((ComplexPrefixAs(new_meta), ()))
        }
    }

    impl std::fmt::Display for ComplexPrefixAs {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "AS{:?}", self.0)
        }
    }

    #[test]
    fn test_full_table_from_csv() -> Result<(), Box<dyn Error>> {
        // These constants are all contingent on the exact csv file,
        // being loaded!
        const CSV_FILE_PATH: &str = "./data/uniq_pfx_asn_dfz_rnd.csv";
        const SEARCHES_NUM: u32 = 2080800;
        const INSERTS_NUM: usize = 893943;
        const GLOBAL_PREFIXES_VEC_SIZE: usize = 886117;
        const FOUND_PREFIXES: u32 = 1322993;

        fn load_prefixes(
            pfxs: &mut Vec<PrefixRecord<ComplexPrefixAs>>,
        ) -> Result<(), Box<dyn Error>> {
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
                let pfx = PrefixRecord::new(
                    Prefix::new(net.into(), len)?,
                    ComplexPrefixAs(vec![asn]),
                );
                pfxs.push(pfx);
            }
            Ok(())
        }

        let strides_vec = [
            // vec![8],
            vec![4],
            // vec![6, 6, 6, 6, 4, 4],
            // vec![3, 4, 4, 6, 7, 8],
        ];
        for _strides in strides_vec.iter().enumerate() {
            let mut pfxs: Vec<PrefixRecord<ComplexPrefixAs>> = vec![];
            let v4_strides = vec![8];
            let v6_strides = vec![8];
            let mut tree_bitmap = SingleThreadedStore::<ComplexPrefixAs>::new(v4_strides, v6_strides);

            if let Err(err) = load_prefixes(&mut pfxs) {
                println!("error running example: {}", err);
                process::exit(1);
            }

            let inserts_num = pfxs.len();
            for pfx in pfxs.into_iter() {
                match tree_bitmap.insert(&pfx.prefix, pfx.meta, None) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{}", e);
                        panic!("STOP TESTING I CAN'T INSERT!");
                    }
                };

                let query = tree_bitmap.match_prefix(&pfx.prefix,
                        &MatchOptions {
                        match_type: MatchType::LongestMatch,
                        include_all_records: false,
                        include_less_specifics: false,
                        include_more_specifics: false,
                    },
                );

                if query.prefix.is_none() { panic!("STOPSTOPSTOPST"); }
                else { 
                    assert_eq!(query.prefix.unwrap(), pfx.prefix);
                }
            }

            println!("done inserting {} prefixes", inserts_num);


            let inet_max = 255;
            let len_max = 32;

            let mut found_counter = 0_u32;
            let mut not_found_counter = 0_u32;
            let mut inet_count = 0;
            let mut len_count = 0;
            (0..inet_max).for_each(|i_net| {
                len_count = 0;
                (0..len_max).for_each(|s_len| {

                    (0..inet_max).for_each(|ii_net| {
                        let pfx = Prefix::new_relaxed(
                            std::net::Ipv4Addr::new(i_net, ii_net, 0, 0)
                                .into(),
                            s_len,
                        );
                        // print!(":{}.{}.0.0/{}:", i_net, ii_net, s_len);
                        let res = tree_bitmap.match_prefix(
                            &pfx.unwrap(),
                            &MatchOptions {
                                match_type: MatchType::LongestMatch,
                                include_all_records: false,
                                include_less_specifics: false,
                                include_more_specifics: false,
                            },
                        );
                        if let Some(_pfx) = res.prefix {
                            // println!("_pfx {:?}", _pfx);
                            // println!("pfx {:?}", pfx);
                            // println!("{:#?}", res);
                            assert!(_pfx.len() <= pfx.unwrap().len());
                            assert!(_pfx.addr() <= pfx.unwrap().addr());
                            found_counter += 1;
                        } else {
                            // println!(
                            //     "not found {:?}",
                            //     if let Ok(e) = pfx {
                            //         e.to_string()
                            //     } else {
                            //         "ok".to_string()
                            //     }
                            // );
                            not_found_counter += 1;
                        }
                    });
                    len_count += 1;
                });
                inet_count += 1;
            });
            println!("found pfx: {}", found_counter);
            println!("not found pfx: {}", not_found_counter);
            println!("inet counter {}", inet_count);
            println!("len counter {}", len_count);

            let searches_num =
                inet_max as u128 * inet_max as u128 * len_max as u128;

            assert_eq!(searches_num, SEARCHES_NUM as u128);
            assert_eq!(inserts_num, INSERTS_NUM);
            assert_eq!(tree_bitmap.prefixes_len(), GLOBAL_PREFIXES_VEC_SIZE);
            assert_eq!(found_counter, FOUND_PREFIXES);
            assert_eq!(not_found_counter, SEARCHES_NUM - FOUND_PREFIXES);
        }
        Ok(())
    }
}