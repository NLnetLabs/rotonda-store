use inetnum::addr::Prefix;
use inetnum::asn::Asn;
use rotonda_store::prelude::multi::PrefixStoreError;
use rotonda_store::prelude::multi::Record;
use rotonda_store::prelude::multi::RouteStatus;
use rotonda_store::MatchOptions;
use rotonda_store::Meta;
use rotonda_store::MultiThreadedStore;
use routecore::bgp::aspath::HopPath;
use routecore::bgp::path_attributes::BgpIdentifier;
use routecore::bgp::path_attributes::PaMap;
use routecore::bgp::path_selection::RouteSource;
use routecore::bgp::path_selection::{OrdRoute, Rfc4271, TiebreakerInfo};
use routecore::bgp::types::LocalPref;
use routecore::bgp::types::Origin;
use std::net::Ipv4Addr;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct Ipv4Route(u32, PaMap, TiebreakerInfo);

impl std::fmt::Display for Ipv4Route {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl Meta for Ipv4Route {
    type Orderable<'a> = OrdRoute<'a, Rfc4271>;
    // We are storing the TiebreakerInfo *inside* the metadata, so we don't
    // need to feed it in externally, so this type will be unit for us.
    type TBI = ();

    fn as_orderable(&self, _tbi: Self::TBI) -> Self::Orderable<'_> {
        routecore::bgp::path_selection::OrdRoute::rfc4271(&self.1, self.2)
            .unwrap()
    }
}

impl AsRef<[u8]> for Ipv4Route {
    fn as_ref(&self) -> &[u8] {
        todo!()
    }
}

mod common {
    use std::io::Write;

    pub fn init() {
        let _ = env_logger::builder()
            .format(|buf, record| writeln!(buf, "{}", record.args()))
            .is_test(true)
            .try_init();
    }
}

#[test]
fn test_best_path_1() -> Result<(), Box<dyn std::error::Error>> {
    crate::common::init();

    let tree_bitmap =
        std::sync::Arc::new(std::sync::Arc::new(MultiThreadedStore::<
            Ipv4Route,
        >::new()?));

    let pfx = Prefix::from_str("185.34.0.0/16")?;
    let mut asns = [
        Asn::from(65400),
        Asn::from(65401),
        Asn::from(65402),
        Asn::from(65403),
        Asn::from(65404),
    ]
    .into_iter();
    let mut pa_map = PaMap::empty();

    pa_map.set::<LocalPref>(routecore::bgp::types::LocalPref(50));
    pa_map.set::<HopPath>(HopPath::from(vec![
        Asn::from(65400),
        Asn::from(65401),
        Asn::from(65402),
    ]));
    pa_map.set::<Origin>(routecore::bgp::types::Origin(
        routecore::bgp::types::OriginType::Egp,
    ));

    let mut asns_insert = vec![];

    // Out TiebreakInfo consists of some values that are the same for all of
    // our routes, and some that are specific to the route.
    let tbi_modifier =
        |peer_addr: Ipv4Addr,
         local_asn: Asn,
         bgp_identifier: BgpIdentifier| {
            TiebreakerInfo::new(
                RouteSource::Ebgp,
                None,
                local_asn,
                bgp_identifier,
                std::net::IpAddr::V4(peer_addr),
            )
        };

    for (mui, tbi) in [
        (
            1,
            tbi_modifier(
                std::net::Ipv4Addr::from_str("192.168.12.1")?,
                Asn::from(65400),
                BgpIdentifier::from([0; 4]),
            ),
        ),
        (
            2,
            tbi_modifier(
                std::net::Ipv4Addr::from_str("192.168.12.2")?,
                Asn::from(65400),
                BgpIdentifier::from([0; 4]),
            ),
        ),
        (
            3,
            tbi_modifier(
                std::net::Ipv4Addr::from_str("192.168.12.3")?,
                Asn::from(65400),
                BgpIdentifier::from([0; 4]),
            ),
        ),
        (
            4,
            tbi_modifier(
                std::net::Ipv4Addr::from_str("192.168.12.4")?,
                Asn::from(65400),
                BgpIdentifier::from([0; 4]),
            ),
        ),
        (
            5,
            tbi_modifier(
                std::net::Ipv4Addr::from_str("192.168.12.5")?,
                Asn::from(65400),
                BgpIdentifier::from([0; 4]),
            ),
        ),
    ] {
        asns_insert.push(asns.next().unwrap());
        pa_map.set::<HopPath>(HopPath::from(asns_insert.clone()));
        let rec = Record::new(
            mui,
            0,
            RouteStatus::Active,
            Ipv4Route(mui, pa_map.clone(), tbi),
        );
        tree_bitmap.insert(&pfx, rec, None)?;
    }

    let res = tree_bitmap.match_prefix(
        &pfx,
        &MatchOptions {
            match_type: rotonda_store::MatchType::ExactMatch,
            include_withdrawn: false,
            include_less_specifics: false,
            include_more_specifics: false,
            mui: None,
        },
        &rotonda_store::epoch::pin(),
    );

    println!("{:?}", res.prefix_meta);
    let best_path = tree_bitmap.best_path(&pfx, &rotonda_store::epoch::pin());
    println!(
        "ps outdated? {}",
        tree_bitmap
            .is_ps_outdated(&pfx, &rotonda_store::epoch::pin())
            .unwrap()
    );
    println!("{:?}", best_path);

    // We didn't calculate the best path yet, but the prefix (and its entries)
    // exists, so this should be `Some(Err(BestPathNotFound))` at this point.
    assert_eq!(
        best_path.unwrap().err().unwrap(),
        PrefixStoreError::BestPathNotFound
    );

    tree_bitmap.calculate_and_store_best_and_backup_path(
        &pfx,
        &(),
        &rotonda_store::epoch::pin(),
    )?;

    let best_path = tree_bitmap.best_path(&pfx, &rotonda_store::epoch::pin());
    println!(
        "ps outdated? {}",
        tree_bitmap
            .is_ps_outdated(&pfx, &rotonda_store::epoch::pin())
            .unwrap()
    );
    println!("{:?}", best_path);
    assert_eq!(best_path.unwrap().unwrap().multi_uniq_id, 1);

    Ok(())
}
