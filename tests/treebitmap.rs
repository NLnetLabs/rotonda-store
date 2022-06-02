#[cfg(test)]
mod tests {
    use rotonda_store::AddressFamily;
    use rotonda_store::{
        prelude::*, MatchOptions, MatchType, MultiThreadedStore, PrefixAs,
    };
    use routecore::addr::Prefix;
    use routecore::record::NoMeta;

    #[test]
    fn test_insert_extremes_ipv4() -> Result<(), Box<dyn std::error::Error>> {
        let trie = &mut MultiThreadedStore::<NoMeta>::new()?;
        let min_pfx = Prefix::new_relaxed(
            std::net::Ipv4Addr::new(0, 0, 0, 0).into(),
            1,
        )
        .unwrap();

        trie.insert(&min_pfx, NoMeta::Empty)?;
        let expect_pfx = Prefix::new_relaxed(
            std::net::Ipv4Addr::new(0, 0, 0, 0).into(),
            1,
        );

        let guard = &epoch::pin();
        let res = trie.match_prefix(
            &expect_pfx?,
            &MatchOptions {
                match_type: MatchType::LongestMatch,
                include_less_specifics: true,
                include_more_specifics: false,
            },
            guard,
        );
        println!("prefix: {:?}", &expect_pfx);
        println!("result: {:#?}", &res);
        assert!(res.prefix.is_some());
        assert_eq!(res.prefix, Some(expect_pfx?));

        let max_pfx = Prefix::new_relaxed(
            std::net::Ipv4Addr::new(255, 255, 255, 255).into(),
            32,
        );

        // drop(locks);
        trie.insert(&max_pfx?, NoMeta::Empty)?;
        let expect_pfx = Prefix::new_relaxed(
            std::net::Ipv4Addr::new(255, 255, 255, 255).into(),
            32,
        );

        let guard = &epoch::pin();
        let res = trie.match_prefix(
            &expect_pfx?,
            &MatchOptions {
                match_type: MatchType::ExactMatch,
                include_less_specifics: true,
                include_more_specifics: false,
            },
            guard,
        );
        assert!(res.prefix.is_some());
        assert_eq!(res.prefix, Some(expect_pfx?));
        Ok(())
    }

    #[test]
    fn test_tree_ipv4() -> Result<(), Box<dyn std::error::Error>> {
        let tree_bitmap = MultiThreadedStore::<PrefixAs>::new()?;
        let pfxs = vec![
            // Prefix::new_relaxed(0b0000_0000_0000_0000_0000_0000_0000_000 0_u32.into_ipaddr(), 0),
            Prefix::new_relaxed(
                0b1111_1111_1111_1111_1111_1111_1111_1111_u32.into_ipaddr(),
                32,
            ),
            Prefix::new_relaxed(
                0b0000_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b0001_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b0010_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b0011_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b0100_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b0101_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b0110_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b0111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b1000_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b1001_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b1010_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b1011_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b1100_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b1101_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b1110_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b1111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b1111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                9,
            ),
            Prefix::new_relaxed(
                0b1111_0000_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                9,
            ),
            Prefix::new_relaxed(
                0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                12,
            ),
            Prefix::new_relaxed(
                0b1111_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                9,
            ),
            Prefix::new_relaxed(
                0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                9,
            ),
            Prefix::new_relaxed(
                0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                10,
            ),
            Prefix::new_relaxed(
                0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                11,
            ),
            Prefix::new_relaxed(
                0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                12,
            ),
            Prefix::new_relaxed(
                0b0111_0111_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                12,
            ),
            Prefix::new_relaxed(
                0b0111_0111_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                13,
            ),
            Prefix::new_relaxed(
                0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                13,
            ),
            Prefix::new_relaxed(
                0b0111_0111_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                14,
            ),
            Prefix::new_relaxed(
                0b0111_0111_0100_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                14,
            ),
            Prefix::new_relaxed(
                0b0111_0111_1000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                14,
            ),
            Prefix::new_relaxed(
                0b0111_0111_1100_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                14,
            ),
            Prefix::new_relaxed(
                0b1110_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(192, 0, 0, 0).into(),
                23,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(192, 0, 0, 0).into(),
                16,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(192, 0, 10, 0).into(),
                23,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(192, 0, 9, 0).into(),
                24,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(193, 0, 0, 0).into(),
                23,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(193, 0, 10, 0).into(),
                23,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(209, 0, 0, 0).into(),
                16,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(193, 0, 9, 0).into(),
                24,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(193, 0, 10, 0).into(),
                24,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(193, 0, 14, 0).into(),
                23,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(193, 0, 14, 0).into(),
                24,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(193, 0, 15, 0).into(),
                24,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(193, 0, 10, 10).into(),
                32,
            ),
            Prefix::new_relaxed(
                0b0011_0000_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                4,
            ),
            Prefix::new_relaxed(
                0b1000_0011_1000_1111_0000_0000_0000_0000_u32.into_ipaddr(),
                11,
            ),
            Prefix::new_relaxed(
                0b1000_0010_0101_0111_1111_1000_0000_0000_u32.into_ipaddr(),
                13,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                24,
            ),
            Prefix::new_relaxed(
                0b1111_1111_0000_0001_0000_0000_0000_0000_u32.into_ipaddr(),
                12,
            ),
            Prefix::new_relaxed(
                0b1111_1111_0011_0111_0000_0000_0000_0000_u32.into_ipaddr(),
                17,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(100, 0, 12, 0).into(),
                24,
            ),
            Prefix::new_relaxed(
                0b0000_0001_0000_0000_0000_0000_0000_0000_u32.into_ipaddr(),
                24,
            ),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(1, 0, 128, 0).into(),
                24,
            ),
        ];

        for pfx in pfxs.into_iter() {
            tree_bitmap.insert(&pfx?, PrefixAs(666))?;
        }

        // let (store_v4, store_v6) = tree_bitmap.acquire_prefixes_rwlock_read();
        // let prefixes_iter = rotonda_store::HashMapPrefixRecordIterator {
        //     v4: Some(store_v4),
        //     v6: store_v6,
        // };

        let guard = &epoch::pin();
        for pfx in tree_bitmap.prefixes_iter(guard) {
            // let pfx_nm = pfx.strip_meta();
            let res = tree_bitmap.match_prefix(
                &pfx.prefix,
                &MatchOptions {
                    match_type: MatchType::LongestMatch,
                    include_less_specifics: false,
                    include_more_specifics: false,
                },
                guard,
            );
            println!("{}", pfx);
            assert_eq!(res.prefix.unwrap(), pfx.prefix);
        }

        let res = tree_bitmap.match_prefix(
            &Prefix::new(std::net::Ipv4Addr::new(192, 0, 1, 0).into(), 24)?,
            &MatchOptions {
                match_type: MatchType::LongestMatch,
                include_less_specifics: true,
                include_more_specifics: false,
            },
            guard,
        );
        println!("prefix {:?}", res.prefix);
        println!("res: {:#?}", &res);

        assert_eq!(
            res.prefix.unwrap(),
            Prefix::new_relaxed(
                std::net::Ipv4Addr::new(192, 0, 0, 0).into(),
                23
            )?
        );

        let less_specifics = res.less_specifics.unwrap();

        assert!(less_specifics.iter().any(|r| {
            *r.0 == Prefix::new(
                std::net::Ipv4Addr::new(192, 0, 0, 0).into(),
                16,
            )
            .unwrap()
        }));
        assert!(less_specifics.iter().any(|r| {
            *r.0 == Prefix::new(
                std::net::Ipv4Addr::new(192, 0, 0, 0).into(),
                4,
            )
            .unwrap()
        }));
        Ok(())
    }

    #[test]
    fn test_ranges_ipv4() -> Result<(), Box<dyn std::error::Error>> {
        for i_net in 0..255 {
            let tree_bitmap = MultiThreadedStore::<NoMeta>::new()?;

            let pfx_vec: Vec<Prefix> = (1..32)
                .collect::<Vec<u8>>()
                .into_iter()
                .map(|i_len| {
                    Prefix::new_relaxed(
                        std::net::Ipv4Addr::new(i_net, 0, 0, 0).into(),
                        i_len,
                    )
                    .unwrap()
                })
                .collect();

            let mut i_len_s = 0;
            for pfx in pfx_vec {
                i_len_s += 1;
                tree_bitmap.insert(&pfx, NoMeta::Empty)?;

                let res_pfx = Prefix::new_relaxed(
                    std::net::Ipv4Addr::new(i_net, 0, 0, 0).into(),
                    i_len_s,
                );

                let guard = &epoch::pin();
                for s_len in i_len_s..32 {
                    let pfx = Prefix::new_relaxed(
                        std::net::Ipv4Addr::new(i_net, 0, 0, 0).into(),
                        s_len,
                    )?;
                    let res = tree_bitmap.match_prefix(
                        &pfx,
                        &MatchOptions {
                            match_type: MatchType::LongestMatch,
                            include_less_specifics: false,
                            include_more_specifics: false,
                        },
                        guard,
                    );
                    println!("{:?}", pfx);

                    assert_eq!(res.prefix.unwrap(), res_pfx?);
                }
            }
        }
        Ok(())
    }
}
