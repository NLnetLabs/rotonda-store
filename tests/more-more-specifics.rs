// type Prefix4<'a> = Prefix<u32, PrefixAs>;
mod test {
    use rotonda_store::{
        MatchOptions, MatchType, MultiThreadedStore, PrefixAs,
    };

    use routecore::addr::Prefix;

    use std::error::Error;

    #[test]
    fn test_more_specifics_without_less_specifics(
    ) -> Result<(), Box<dyn Error>> {
        let mut tree_bitmap =
            MultiThreadedStore::<PrefixAs>::new(vec![8], vec![8]);
        let pfxs = vec![
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 64, 0).into(), 18)?, // 0
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 109, 0).into(), 24)?, // 1
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 153, 0).into(), 24)?, // 2
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 21)?, // 3
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 176, 0).into(), 20)?, // 4
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 8)?, // 5
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 184, 0).into(), 23)?, // 6
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 71, 0).into(), 24)?, // 7
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 9)?, // 8
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 117, 0).into(), 24)?, // 9
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 99, 0).into(), 24)?, // 10
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 224, 0).into(), 24)?, // 11
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 128, 0).into(), 18)?, // 12
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 120, 0).into(), 24)?, // 13
        ];

        for pfx in pfxs.iter() {
            tree_bitmap.insert(pfx, PrefixAs(666))?;
        }
        println!("------ end of inserts\n");

        for spfx in &[
            (
                &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 9),
                &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 9), // 0
                vec![0, 1, 2, 3, 4, 6, 7, 9, 10, 11, 12, 13],
            ),
            (
                &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 8),
                &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 8), // 0
                vec![0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13],
            ),
        ] {
            println!("search for: {:?}", spfx.0);
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

            assert_eq!(found_result.prefix.unwrap(), spfx.1.unwrap());
            assert_eq!(&more_specifics.len(), &spfx.2.len());

            for i in spfx.2.iter() {
                print!("{} ", i);

                let result_pfx =
                    more_specifics.iter().find(|pfx| pfx.prefix == pfxs[*i]);
                assert!(result_pfx.is_some());
            }
            println!("-----------");
        }
        Ok(())
    }

    #[test]
    fn test_more_specifics_with_less_specifics() -> Result<(), Box<dyn Error>>
    {
        let mut tree_bitmap =
            MultiThreadedStore::<PrefixAs>::new(vec![4], vec![8]);
        let pfxs = vec![
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 64, 0).into(), 18), // 0
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 109, 0).into(), 24), // 1
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 153, 0).into(), 24), // 2
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 21), // 3
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 176, 0).into(), 20), // 4
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 8), // 5
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 184, 0).into(), 23), // 6
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 71, 0).into(), 24), // 7
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 9), // 8
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 117, 0).into(), 24), // 9
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 99, 0).into(), 24), // 10
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 224, 0).into(), 24), // 11
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 128, 0).into(), 18), // 12
            Prefix::new(std::net::Ipv4Addr::new(17, 0, 120, 0).into(), 24), // 13
        ];

        for pfx in pfxs.iter() {
            tree_bitmap.insert(&pfx.unwrap(), PrefixAs(666))?;
        }
        println!("------ end of inserts\n");

        for spfx in &[
            (
                &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 9),
                Some(&Prefix::new(
                    std::net::Ipv4Addr::new(17, 0, 0, 0).into(),
                    9,
                )), // 0
                vec![0, 1, 2, 3, 4, 6, 7, 9, 10, 11, 12, 13],
            ),
            (
                &Prefix::new(std::net::Ipv4Addr::new(17, 0, 0, 0).into(), 8),
                Some(&Prefix::new(
                    std::net::Ipv4Addr::new(17, 0, 0, 0).into(),
                    8,
                )), // 0
                vec![0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13],
            ),
        ] {
            println!("search for: {:#}", (*spfx.0)?);
            let found_result = tree_bitmap.match_prefix(
                &spfx.0.unwrap(),
                &MatchOptions {
                    match_type: MatchType::LongestMatch,
                    include_less_specifics: false,
                    include_more_specifics: true,
                },
            );
            println!("em/m-s: {}", found_result);

            let more_specifics = found_result.more_specifics.unwrap();

            assert_eq!(
                found_result.prefix.unwrap(),
                spfx.1.unwrap().unwrap()
            );
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
