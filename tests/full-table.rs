#![cfg(feature = "csv")]
#[cfg(test)]
mod tests {
    use inetnum::addr::Prefix;
    use inetnum::asn::Asn;
    use rotonda_store::{
        epoch,
        match_options::{IncludeHistory, MatchOptions, MatchType},
        prefix_record::{Meta, PrefixRecord, Record, RouteStatus},
        rib::{config::Config, StarCastRib},
    };

    use std::error::Error;
    use std::fs::File;
    use std::process;

    #[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
    pub struct AsnList(Vec<u8>);

    // pub struct ComplexPrefixAs(pub Vec<u32>);

    impl std::fmt::Display for AsnList {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "AS{:?}", self.0)
        }
    }

    impl Meta for AsnList {
        type Orderable<'a> = Asn;
        type TBI = ();

        fn as_orderable(&self, _tbi: Self::TBI) -> Asn {
            Asn::from(u32::from_be_bytes(*self.0.first_chunk::<4>().unwrap()))
        }
    }

    impl AsRef<[u8]> for AsnList {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    impl From<Vec<u32>> for AsnList {
        fn from(value: Vec<u32>) -> Self {
            AsnList(
                value
                    .into_iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<u8>>(),
            )
        }
    }

    impl From<Vec<u8>> for AsnList {
        fn from(value: Vec<u8>) -> Self {
            Self(value)
        }
    }

    rotonda_store::all_strategies![
        full_table_1;
        test_full_table_from_csv;
        AsnList
    ];

    // #[test]
    fn test_full_table_from_csv<C: Config>(
        tree_bitmap: StarCastRib<AsnList, C>,
    ) -> Result<(), Box<dyn Error>> {
        // These constants are all contingent on the exact csv file,
        // being loaded!

        const CSV_FILE_PATH: &str = "./data/uniq_pfx_asn_dfz_rnd.csv";
        const SEARCHES_NUM: u32 = 2080800;
        const INSERTS_NUM: usize = 893943;
        const GLOBAL_PREFIXES_VEC_SIZE: usize = 886117;
        const FOUND_PREFIXES: u32 = 1322993;

        let guard = &epoch::pin();

        fn load_prefixes(
            pfxs: &mut Vec<PrefixRecord<AsnList>>,
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
                    vec![Record::new(
                        0,
                        0,
                        RouteStatus::Active,
                        vec![asn].into(),
                    )],
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
            let mut pfxs: Vec<PrefixRecord<AsnList>> = vec![];
            // let tree_bitmap = MultiThreadedStore::<AsnList>::try_default()?;
            // .with_user_data("Testing".to_string());

            if let Err(err) = load_prefixes(&mut pfxs) {
                println!("error running example: {}", err);
                process::exit(1);
            }

            let inserts_num = pfxs.len();
            for pfx in pfxs.into_iter() {
                match tree_bitmap.insert(
                    &pfx.prefix,
                    pfx.meta[0].clone(),
                    None,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{}", e);
                        panic!("STOP TESTING I CAN'T INSERT!");
                    }
                };

                let query = tree_bitmap.match_prefix(
                    &pfx.prefix,
                    &MatchOptions {
                        match_type: MatchType::LongestMatch,
                        include_withdrawn: false,
                        include_less_specifics: false,
                        include_more_specifics: false,
                        mui: None,
                        include_history: IncludeHistory::None,
                    },
                    guard,
                );

                if query.as_ref().unwrap().prefix.is_none() {
                    panic!("STOPSTOPSTOPST");
                } else {
                    assert_eq!(
                        query.as_ref().unwrap().prefix.unwrap(),
                        pfx.prefix
                    );
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
                                include_withdrawn: false,
                                include_less_specifics: false,
                                include_more_specifics: false,
                                mui: None,
                                include_history: IncludeHistory::None,
                            },
                            guard,
                        );
                        if let Some(_pfx) = res.as_ref().unwrap().prefix {
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
            assert_eq!(
                tree_bitmap.prefixes_count().total(),
                GLOBAL_PREFIXES_VEC_SIZE
            );
            assert_eq!(found_counter, FOUND_PREFIXES);
            assert_eq!(not_found_counter, SEARCHES_NUM - FOUND_PREFIXES);
        }
        Ok(())
    }
}
