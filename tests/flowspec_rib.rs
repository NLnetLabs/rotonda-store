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
        prefix_cht::map_type::{KeyExtensions, Mui, MuiRdPathId},
        prefix_record::{Record, RouteStatus},
        rib::{config::PersistOnlyConfig, flowspec::BlobRib},
        test_types::NoMeta,
    };

    #[test]
    fn test_insert_multiple_routes() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::common::init();

        // total length: (nlri; 7 real len + 1 for container (8) and 1
        // for length) 9 + (mui) 4 + (rd) 8 + (pathid) 4 + (ltime) 8 +
        // (routestatus) 1 = 34
        let mui = <u32>::from_le_bytes([3_u8; 4]);
        let a_fs = ([0x01; 7], mui);
        let b_fs = ([0x02; 7], mui);
        let c_fs = ([0x03; 7], 100);
        let path_id = <u32>::from_le_bytes([255, 0, 0, 255]);

        let rib = BlobRib::<NoMeta, 7, PersistOnlyConfig>::try_default()?;

        let a_key =
            MuiRdPathId::from((a_fs.1, [99; 8], path_id.to_be_bytes()));

        rib.insert(
            &a_fs.0,
            Record::new(a_key, 0, RouteStatus::Active, NoMeta::Empty),
            None,
        )?;
        println!("inserted a");

        let b_key =
            MuiRdPathId::from((b_fs.1, [99; 8], path_id.to_be_bytes()));
        rib.insert(
            &b_fs.0,
            Record::new(b_key, 0, RouteStatus::Active, NoMeta::Empty),
            None,
        )?;
        println!("inserted b");

        let c_key =
            MuiRdPathId::from((c_fs.1, [99; 8], path_id.to_be_bytes()));
        rib.insert(
            &c_fs.0,
            Record::new(c_key, 0, RouteStatus::Active, NoMeta::Empty),
            None,
        )?;
        println!("inserted c");

        for fs in [&a_fs, &b_fs, &c_fs] {
            let res = rib.get(&fs.0, None, false)?;
            println!("result1: {:#?}", &res);
            assert_eq!(res[0].multi_uniq_id.mui(), fs.1);
            assert_eq!(res.len(), 1);
        }

        let guard = rotonda_store::epoch::pin();

        println!("start counting...");
        let res = rib
            .nlri_iter(&guard)
            .filter(|r| {
                if let Ok(rr) = r {
                    rr.1.iter().any(|r| r.multi_uniq_id.mui() == mui)
                } else {
                    false
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 2);
        println!("done first iteration");

        let all_recs = rib.records_iter(&guard).collect::<Vec<_>>();
        assert_eq!(all_recs.len(), 3);

        Ok(())
    }
}
