// type Prefix4<'a> = Prefix<u32, PrefixAs>;
mod tests {
    use rotonda_store::PrefixAs;
    use rotonda_store::{MatchOptions, MatchType, MultiThreadedStore};
    use routecore::addr::Prefix;

    use std::error::Error;

    #[test]
    fn test_more_specifics() -> Result<(), Box<dyn Error>> {
        let mut tree_bitmap = MultiThreadedStore::<PrefixAs>::new(
            vec![4, 4, 3, 3, 3, 3, 3, 3, 3, 3],
            vec![4]
        );
        let pfxs = vec![
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 24), // 0
            //
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 25), // 1
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 128).into(),
                25,
            ), // 2
            //
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 26), // 3
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 64).into(), 26), // 4
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 128).into(),
                26,
            ), // 5
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 192).into(),
                26,
            ), // 6
            //
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 27), // 7
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 32).into(), 27), // 8
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 64).into(), 27), // 9
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 128).into(),
                27,
            ), // 10
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 160).into(),
                27,
            ), // 11
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 192).into(),
                27,
            ), // 12
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 224).into(),
                27,
            ), // 13
            //
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 0).into(), 32), // 14
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 31).into(), 32), // 15
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 32).into(), 32), // 16
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 63).into(), 32), // 17
            Prefix::new(std::net::Ipv4Addr::new(130, 55, 240, 64).into(), 32), // 18
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 127).into(),
                32,
            ), // 19
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 128).into(),
                32,
            ), // 20
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 159).into(),
                32,
            ), // 21
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 160).into(),
                32,
            ), // 22
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 191).into(),
                32,
            ), // 23
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 192).into(),
                32,
            ), // 24
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 223).into(),
                32,
            ), // 25
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 224).into(),
                32,
            ), // 26
            Prefix::new(
                std::net::Ipv4Addr::new(130, 55, 240, 255).into(),
                32,
            ), // 27
        ];

        for pfx in pfxs.iter().flatten() {
            tree_bitmap.insert(pfx, PrefixAs(666))?;
        }
        println!("------ end of inserts\n");

        for spfx in &[
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    23,
                ),
                None,
                Vec::<usize>::new(),
            ),
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    24,
                ),
                Some(Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    24,
                )?),
                // These are the indexes to pfxs.2 vec.
                // These are all supposed to show up in the result.
                vec![
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                ],
            ),
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    25,
                ),
                Some(Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    25,
                )?),
                vec![3, 4, 7, 8, 9, 14, 15, 16, 17, 18, 19],
            ),
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    26,
                ),
                Some(Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 0).into(),
                    26,
                )?),
                vec![7, 8, 14, 15, 16, 17],
            ),
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 192).into(),
                    26,
                ),
                Some(Prefix::new(
                    std::net::Ipv4Addr::new(130, 55, 240, 192).into(),
                    26,
                )?),
                vec![12, 13, 24, 25, 26, 27],
            ),
        ] {
            println!("search for: {}", (*spfx.0)?);
            let found_result = tree_bitmap.match_prefix(
                &spfx.0.unwrap(),
                &MatchOptions {
                    match_type: MatchType::ExactMatch,
                    include_less_specifics: false,
                    include_more_specifics: true,
                },
            );
            println!("em/m-s: {:#?}", found_result);

            let more_specifics = found_result.more_specifics.unwrap();
            assert_eq!(found_result.prefix, spfx.1);

            assert_eq!(&more_specifics.len(), &spfx.2.len());

            for i in spfx.2.iter() {
                print!("{} ", i);

                let result_pfx = more_specifics
                    .iter()
                    .find(|pfx| pfx.prefix == pfxs[*i].unwrap());
                assert!(result_pfx.is_some());
            }
            println!("-----------");
        }
        Ok(())
    }
}
