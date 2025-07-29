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
    use log::debug;
    use rotonda_store::{
        prefix_cht::map_type::{KeyExtensions, MuiRdPathId},
        prefix_record::{Record, RouteStatus},
        rib::{
            config::{MemoryOnlyConfig, PersistOnlyConfig},
            flowspec::BlobRib,
            FlowSpecRib,
        },
        test_types::NoMeta,
    };

    #[test]
    fn test_insert_multiple_routes() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::common::init();

        let a_fs = [0x01; 7];
        let b_fs = [0x02; 7];
        let c_fs = [0x03; 7];
        let mui = <u32>::from_le_bytes([3_u8; 4]);
        let path_id = <u32>::from_le_bytes([255, 0, 0, 255]);

        let rib = BlobRib::<NoMeta, 7, PersistOnlyConfig>::try_default()?;

        let a_key = MuiRdPathId::from((mui, [99; 8], path_id.to_be_bytes()));

        rib.insert(
            &a_fs,
            Record::new(a_key, 0, RouteStatus::Active, NoMeta::Empty),
            None,
        )?;
        println!("inserted a");

        let b_key = MuiRdPathId::from((mui, [99; 8], path_id.to_be_bytes()));
        rib.insert(
            &b_fs,
            Record::new(b_key, 0, RouteStatus::Active, NoMeta::Empty),
            None,
        )?;
        println!("inserted b");

        let c_key =
            MuiRdPathId::from((100_u32, [99; 8], path_id.to_be_bytes()));
        rib.insert(
            &c_fs,
            Record::new(c_key, 0, RouteStatus::Active, NoMeta::Empty),
            None,
        )?;
        println!("inserted c");

        for fs in [&a_fs, &b_fs, &c_fs] {
            let res = rib.get(fs, None, false)?;
            // println!("result1: {:#?}", &res);
            assert_eq!(res[0].multi_uniq_id.blob(), Some(fs.as_ref()));
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

        let all_recs = rib.records_iter(&guard).collect::<Vec<_>>();
        assert_eq!(all_recs.len(), 3);

        Ok(())
    }
}
