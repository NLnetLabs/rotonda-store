// type Prefix4<'a> = Prefix<u32, PrefixAs>;
use inetnum::addr::Prefix;
use rotonda_store::{
    epoch,
    prefix_record::{Record, RouteStatus},
    rib::{config::Config, StarCastRib},
    test_types::PrefixAs,
};

use std::error::Error;

mod common {
    use std::io::Write;

    pub fn init() {
        let _ = env_logger::builder()
            .format(|buf, record| writeln!(buf, "{}", record.args()))
            .is_test(true)
            .try_init();
    }
}

rotonda_store::all_strategies![
    test_ms_1;
    test_less_specifics;
    PrefixAs
];

fn test_less_specifics<C: Config>(
    tree_bitmap: StarCastRib<PrefixAs, C>,
) -> Result<(), Box<dyn Error>> {
    crate::common::init();

    let pfxs = [
        Prefix::new(std::net::Ipv4Addr::new(57, 86, 0, 0).into(), 16)?,
        Prefix::new(std::net::Ipv4Addr::new(57, 86, 0, 0).into(), 15)?,
        Prefix::new(std::net::Ipv4Addr::new(57, 84, 0, 0).into(), 14)?,
    ];
    for pfx in pfxs.iter() {
        tree_bitmap.insert(
            pfx,
            Record::new(
                0,
                0,
                RouteStatus::Active,
                PrefixAs::new_from_u32(666),
            ),
            None,
        )?;
    }
    println!("------ end of inserts\n");

    let guard = &epoch::pin();
    for (i, spfx) in &[
        (
            0,
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(57, 86, 0, 0).into(),
                    17,
                ),
                None,
                // These are the indexes to pfxs.2 vec.
                // These are all supposed to show up in the result.
                vec![0, 1, 2],
            ),
        ),
        (
            0,
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(57, 86, 0, 0).into(),
                    16,
                ),
                None,
                vec![1, 2],
            ),
        ),
        (
            0,
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(57, 86, 0, 0).into(),
                    15,
                ),
                None,
                vec![2],
            ),
        ),
        (
            0,
            (
                &Prefix::new(
                    std::net::Ipv4Addr::new(57, 84, 0, 0).into(),
                    14,
                ),
                None,
                vec![],
            ),
        ),
    ] {
        println!("round {}", i);
        println!("search for: {}", (*spfx.0)?);
        println!("search prefix: {}", spfx.0.unwrap());

        let less_iter = tree_bitmap.less_specifics_iter_from(
            &spfx.0.unwrap(),
            spfx.1,
            true,
            guard,
        );

        for (i, p) in less_iter.enumerate() {
            let p = p.unwrap();
            println!("less_iter {} i {}", p, i);
            assert_eq!(p.prefix, pfxs[spfx.2[i]])
        }

        println!("--");
        println!("all prefixes");

        for (i, p) in tree_bitmap
            .prefixes_iter_v4(guard)
            .enumerate()
            .map(|(i, p)| (i, p.as_ref().unwrap().prefix))
        {
            println!("ls {}: {}", i, p);
        }

        println!("-----------");
    }
    Ok(())
}
