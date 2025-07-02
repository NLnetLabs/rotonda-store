mod common {
    use std::io::Write;

    pub fn init() {
        let _ = env_logger::builder()
            .format(|buf, record| writeln!(buf, "{}", record.args()))
            .is_test(true)
            .try_init();
    }
}

#[cfg(test)]
mod tests {
    use rotonda_store::{
        addr::Prefix,
        epoch,
        match_options::{IncludeHistory, MatchOptions, MatchType},
        prefix_cht::map_type::{Mui, MuiPathId},
        prefix_record::{Record, RouteStatus},
        rib::{config::MemoryOnlyConfig, MuiPathIdStarCastRib},
        test_types::NoMeta,
    };

    #[test]
    fn test_insert_multiple_routes() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::common::init();

        let guard = &epoch::pin();
        let a_pfx = Prefix::new_relaxed(
            ("2010:dead:beef::").parse::<std::net::Ipv6Addr>()?.into(),
            48,
        )
        .unwrap();
        let b_pfx = Prefix::new_relaxed(
            ("2011:dead:beef::").parse::<std::net::Ipv6Addr>()?.into(),
            48,
        )
        .unwrap();
        let c_pfx = Prefix::new_relaxed(
            ("2009:dead:beef::").parse::<std::net::Ipv6Addr>()?.into(),
            48,
        )
        .unwrap();
        let rib =
            MuiPathIdStarCastRib::<NoMeta, MemoryOnlyConfig>::try_default()?;

        for path_id in 0_u32..255 {
            let key = MuiPathId::from((0_u32, path_id.to_be_bytes()));
            rib.insert(
                &a_pfx,
                Record::new(key, 0, RouteStatus::Active, NoMeta::Empty),
                None,
            )?;
            rib.insert(
                &b_pfx,
                Record::new(key, 0, RouteStatus::Active, NoMeta::Empty),
                None,
            )?;
            let c_key = Mui::from(100_u32);
            rib.insert(
                &c_pfx,
                Record::new(
                    c_key.into(),
                    0,
                    RouteStatus::Active,
                    NoMeta::Empty,
                ),
                None,
            )?;
        }

        let res = rib.match_prefix(
            &a_pfx,
            &MatchOptions {
                match_type: MatchType::LongestMatch,
                include_withdrawn: false,
                include_less_specifics: true,
                include_more_specifics: false,
                mui: None,
                include_history: IncludeHistory::None,
            },
            guard,
        )?;
        for path_id in 0_u32..255 {
            let key = MuiPathId::from((0_u32, path_id.to_be_bytes()));
            // println!("result1: {:#?}", &res);
            assert_eq!(res.prefix, Some(a_pfx));
            assert_eq!(res.records[path_id as usize].multi_uniq_id, key);
        }

        for path_id in 0_u32..255 {
            let key = MuiPathId::from((0_u32, path_id.to_be_bytes()));
            let res = rib.match_prefix(
                &a_pfx,
                &MatchOptions {
                    match_type: MatchType::LongestMatch,
                    include_withdrawn: false,
                    include_less_specifics: true,
                    include_more_specifics: false,
                    mui: Some(key),
                    include_history: IncludeHistory::None,
                },
                guard,
            )?;
            let key = MuiPathId::from((0_u32, path_id.to_be_bytes()));
            // println!("result2: {:#?}", &res);
            assert_eq!(res.prefix, Some(a_pfx));
            assert_eq!(res.records[0].multi_uniq_id, key);
            assert_eq!(res.records.len(), 1);
        }

        let res = rib.match_prefix(
            &c_pfx,
            &MatchOptions {
                match_type: MatchType::LongestMatch,
                include_withdrawn: false,
                include_less_specifics: true,
                include_more_specifics: false,
                mui: Some(Mui::from(100).into()),
                include_history: IncludeHistory::None,
            },
            guard,
        )?;
        assert_eq!(res.prefix, Some(c_pfx));
        assert_eq!(res.records.len(), 1);
        assert_eq!(res.records[0].multi_uniq_id, Mui::from(100).into());

        for path_id in 0_u32..255 {
            let key = MuiPathId::from((0_u32, path_id.to_be_bytes()));
            let res = rib.match_prefix(
                &c_pfx,
                &MatchOptions {
                    match_type: MatchType::LongestMatch,
                    include_withdrawn: false,
                    include_less_specifics: true,
                    include_more_specifics: false,
                    mui: Some(key),
                    include_history: IncludeHistory::None,
                },
                guard,
            )?;
            assert_eq!(res.prefix, None);
            println!("result3: {:#?}", &res);
            assert_eq!(res.records.len(), 0);
        }
        Ok(())
    }
}
