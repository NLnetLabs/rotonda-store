#[cfg(test)]
mod test {
    use rotonda_store::common::{NoMeta, Prefix, PrefixAs};
    use rotonda_store::{InMemStorage, MatchOptions, MatchType, StorageBackend, TreeBitMap};

    #[test]
    fn test_insert_extremes_ipv4() -> Result<(), Box<dyn std::error::Error>> {
        type StoreType = InMemStorage<u32, NoMeta>;
        let trie = &mut TreeBitMap::<StoreType>::new(vec![4]);
        let min_pfx = Prefix::new(std::net::Ipv4Addr::new(0, 0, 0, 0).into(), 1);

        trie.insert(min_pfx)?;
        let expect_pfx = Prefix::new(std::net::Ipv4Addr::new(0, 0, 0, 0).into(), 1);
        let res = trie.match_prefix(
            &expect_pfx,
            &MatchOptions {
                match_type: MatchType::LongestMatch,
                include_less_specifics: true,
                include_more_specifics: false,
            },
        );
        println!("prefix: {:?}", &expect_pfx);
        println!("result: {:#?}", &res);
        assert!(res.prefix.is_some());
        assert_eq!(res.prefix, Some(&expect_pfx));

        let max_pfx = Prefix::new(std::net::Ipv4Addr::new(255, 255, 255, 255).into(), 32);
        trie.insert(max_pfx)?;
        let expect_pfx = Prefix::new(std::net::Ipv4Addr::new(255, 255, 255, 255).into(), 32);
        let res = trie.match_prefix(
            &expect_pfx,
            &MatchOptions {
                match_type: MatchType::ExactMatch,
                include_less_specifics: true,
                include_more_specifics: false,
            },
        );
        assert!(res.prefix.is_some());
        assert_eq!(res.prefix, Some(&expect_pfx));
        Ok(())
    }

    #[test]
    fn test_tree_ipv4() -> Result<(), Box<dyn std::error::Error>> {
        type StoreType = InMemStorage<u32, PrefixAs>;
        let mut tree_bitmap: TreeBitMap<StoreType> = TreeBitMap::new(vec![4]);
        let pfxs = vec![
            // Prefix::<u32, PrefixAs>::new(0b0000_0000_0000_0000_0000_0000_0000_000 0_u32, 0),
            Prefix::<u32, PrefixAs>::new(0b1111_1111_1111_1111_1111_1111_1111_1111_u32, 32),
            Prefix::new(0b0000_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b0001_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b0010_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b0011_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b0100_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b0101_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b0110_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b0111_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b1000_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b1001_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b1010_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b1011_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b1100_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b1101_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b1110_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b1111_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::new(0b1111_0000_0000_0000_0000_0000_0000_0000_u32, 9),
            Prefix::new(0b1111_0000_1000_0000_0000_0000_0000_0000_u32, 9),
            Prefix::new(0b0111_0111_1000_0000_0000_0000_0000_0000_u32, 12),
            Prefix::<u32, PrefixAs>::new(0b1111_0000_0000_0000_0000_0000_0000_0000_u32, 9),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_1000_0000_0000_0000_0000_0000_u32, 9),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_1000_0000_0000_0000_0000_0000_u32, 10),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_1000_0000_0000_0000_0000_0000_u32, 11),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_1000_0000_0000_0000_0000_0000_u32, 12),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_0000_0000_0000_0000_0000_0000_u32, 12),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_0000_0000_0000_0000_0000_0000_u32, 13),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_1000_0000_0000_0000_0000_0000_u32, 13),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_0000_0000_0000_0000_0000_0000_u32, 14),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_0100_0000_0000_0000_0000_0000_u32, 14),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_1000_0000_0000_0000_0000_0000_u32, 14),
            Prefix::<u32, PrefixAs>::new(0b0111_0111_1100_0000_0000_0000_0000_0000_u32, 14),
            Prefix::<u32, PrefixAs>::new(0b1110_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(192, 0, 0, 0).into(), 23),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(192, 0, 0, 0).into(), 16),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(192, 0, 10, 0).into(), 23),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(192, 0, 9, 0).into(), 24),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(193, 0, 0, 0).into(), 23),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(193, 0, 10, 0).into(), 23),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(209, 0, 0, 0).into(), 16),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(193, 0, 9, 0).into(), 24),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(193, 0, 10, 0).into(), 24),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(193, 0, 14, 0).into(), 23),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(193, 0, 14, 0).into(), 24),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(193, 0, 15, 0).into(), 24),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(193, 0, 10, 10).into(), 32),
            Prefix::<u32, PrefixAs>::new(0b0011_0000_0000_0000_0000_0000_0000_0000_u32, 4),
            Prefix::<u32, PrefixAs>::new(0b1000_0011_1000_1111_0000_0000_0000_0000_u32, 11),
            Prefix::<u32, PrefixAs>::new(0b1000_0010_0101_0111_1111_1000_0000_0000_u32, 13),
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 24),
            Prefix::<u32, PrefixAs>::new(0b1111_1111_0000_0001_0000_0000_0000_0000_u32, 12),
            Prefix::<u32, PrefixAs>::new(0b1111_1111_0011_0111_0000_0000_0000_0000_u32, 17),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(100, 0, 12, 0).into(), 24),
            Prefix::new(0b0000_0001_0000_0000_0000_0000_0000_0000_u32, 24),
            Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(1, 0, 128, 0).into(), 24),
        ];

        for pfx in pfxs.into_iter() {
            tree_bitmap.insert(pfx)?;
        }

        for pfx in tree_bitmap.store.prefixes_iter()? {
            let pfx_nm = pfx.strip_meta();
            let res = tree_bitmap.match_prefix(
                &pfx_nm,
                &MatchOptions {
                    match_type: MatchType::LongestMatch,
                    include_less_specifics: false,
                    include_more_specifics: false,
                },
            );
            println!("{:?}", pfx);
            assert_eq!(res.prefix.unwrap(), pfx);
        }

        let res = tree_bitmap.match_prefix(
            &Prefix::<u32, NoMeta>::new(std::net::Ipv4Addr::new(192, 0, 1, 0).into(), 24),
            &MatchOptions {
                match_type: MatchType::LongestMatch,
                include_less_specifics: true,
                include_more_specifics: false,
            },
        );
        println!("res: {:#?}", &res);

        assert_eq!(
            res.prefix.unwrap(),
            &Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(192, 0, 0, 0).into(), 23)
        );

        let less_specifics = res.less_specifics.unwrap();

        assert_eq!(
            less_specifics[1],
            &Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(192, 0, 0, 0).into(), 16)
        );
        assert_eq!(
            less_specifics[0],
            &Prefix::<u32, PrefixAs>::new(std::net::Ipv4Addr::new(192, 0, 0, 0).into(), 4)
        );
        Ok(())
    }

    #[test]
    fn test_ranges_ipv4() -> Result<(), Box<dyn std::error::Error>> {
        type StoreType = InMemStorage<u32, NoMeta>;

        for i_net in 0..255 {
            let mut tree_bitmap: TreeBitMap<StoreType> = TreeBitMap::new(vec![4]);

            let pfx_vec: Vec<Prefix<u32, NoMeta>> = (1..32)
                .collect::<Vec<u8>>()
                .into_iter()
                .map(|i_len| {
                    Prefix::<u32, NoMeta>::new(
                        std::net::Ipv4Addr::new(i_net, 0, 0, 0).into(),
                        i_len,
                    )
                })
                .collect();

            let mut i_len_s = 0;
            for pfx in pfx_vec {
                i_len_s += 1;
                tree_bitmap.insert(pfx)?;

                let res_pfx = Prefix::<u32, NoMeta>::new(
                    std::net::Ipv4Addr::new(i_net, 0, 0, 0).into(),
                    i_len_s,
                );

                for s_len in i_len_s..32 {
                    let pfx = Prefix::<u32, NoMeta>::new(
                        std::net::Ipv4Addr::new(i_net, 0, 0, 0).into(),
                        s_len,
                    );
                    let res = tree_bitmap.match_prefix(
                        &pfx,
                        &MatchOptions {
                            match_type: MatchType::LongestMatch,
                            include_less_specifics: false,
                            include_more_specifics: false,
                        },
                    );
                    println!("{:?}", pfx);

                    assert_eq!(res.prefix.unwrap(), &res_pfx);
                }
            }
        }
        Ok(())
    }
}
