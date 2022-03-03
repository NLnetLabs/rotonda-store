use std::{error::Error, fs::File};

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput, BenchmarkId};
use rotonda_store::{bgp::PrefixRecord, PrefixAs, addr::Prefix, MultiThreadedStore, record::Record, MatchOptions, MatchType};

// These constants are all contingent on the exact csv file,
// being loaded!
const CSV_FILE_PATH: &str = "./data/uniq_pfx_asn_dfz_rnd.csv";
const SEARCHES_NUM: u64 = 2080800;
const INSERTS_NUM: u64 = 893943;
const GLOBAL_PREFIXES_VEC_SIZE: usize = 886117;
const FOUND_PREFIXES: u64 = 1322993;

fn load_prefixes(csv_path: &str) -> Result<Vec<PrefixRecord<PrefixAs>>, Box<dyn Error>> {
    let file = File::open(csv_path)?;
    let mut pfxs = Vec::new();

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
        let pfx = PrefixRecord::new_with_local_meta(
            Prefix::new(net.into(), len)?,
            PrefixAs(asn),
        );
        pfxs.push(pfx);
    }

    Ok(pfxs)
}

fn create_tree_from_prefixes(strides: &[u8], pfxs: &[PrefixRecord<PrefixAs>]) -> Result<MultiThreadedStore::<PrefixAs>, Box<dyn Error>> {
    let mut tree_bitmap = MultiThreadedStore::<PrefixAs>::new(
        strides.to_vec(),
        vec![4],
    );

    let inserts_num = pfxs.len() as u64;
    for pfx in pfxs.into_iter() {
        tree_bitmap.insert(&pfx.prefix, *pfx.meta)?;
    }
    assert_eq!(inserts_num, INSERTS_NUM);

    Ok(tree_bitmap)
}

fn lookup_every_ipv4_prefix(tree_bitmap: &MultiThreadedStore::<PrefixAs>) -> Result<(), Box<dyn Error>> {
    let inet_max = 255;
    let len_max = 32;

    let mut found_counter = 0_u64;
    let mut not_found_counter = 0_u64;
    (0..inet_max).into_iter().for_each(|i_net| {
        (0..len_max).into_iter().for_each(|s_len| {
            (0..inet_max).into_iter().for_each(|ii_net| {
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
                        include_less_specifics: false,
                        include_more_specifics: false,
                    },
                );
                if let Some(_pfx) = res.prefix {
                    // println!("_pfx {:?}", _pfx);
                    // println!("pfx {:?}", pfx);
                    // println!("{:#?}", res);
                    assert!(_pfx.len() <= pfx.unwrap().len());
                    assert!(_pfx.addr() <= pfx.unwrap().addr());
                    found_counter += 1;
                } else {
                    not_found_counter += 1;
                }
            });
        });
    });
    // println!("found pfx: {}", found_counter);
    // println!("not found pfx: {}", not_found_counter);

    let searches_num =
        inet_max as u128 * inet_max as u128 * len_max as u128;

    assert_eq!(searches_num, SEARCHES_NUM as u128);
    assert_eq!(tree_bitmap.prefixes_len(), GLOBAL_PREFIXES_VEC_SIZE);
    assert_eq!(found_counter, FOUND_PREFIXES);
    assert_eq!(not_found_counter, SEARCHES_NUM - FOUND_PREFIXES);
    Ok(())
}

fn bench(c: &mut Criterion) {
    let stride_sets = vec![
        vec![4],
        vec![3, 4, 5, 4],
        vec![3, 3, 3, 3, 3, 3, 3, 3, 4, 4],
        vec![5, 5, 4, 3, 3, 3, 3, 3, 3],
        ];

    let pfxs = load_prefixes(CSV_FILE_PATH).expect("Failed to load CSV data");

    let mut group = c.benchmark_group("tree insertion");
    for strides in &stride_sets {
        let iteration_name = format!("{:?}", strides);
        group.sampling_mode(SamplingMode::Auto);
        group.throughput(Throughput::Elements(INSERTS_NUM));
        group.bench_with_input(BenchmarkId::from_parameter(iteration_name), &strides, |b, strides| {
            b.iter(|| create_tree_from_prefixes(strides, &pfxs).expect(&format!("Failed to create tree from prefixes with strides {:?}", strides)))
        });
    }
    group.finish();

    let mut group = c.benchmark_group("tree search");
    for strides in &stride_sets {
        let iteration_name = format!("{:?}", strides);
        let tree = create_tree_from_prefixes(&strides, &pfxs).expect(&format!("Failed to create tree from prefixes with strides {:?}", strides));
        group.sampling_mode(SamplingMode::Auto);
        group.throughput(Throughput::Elements(SEARCHES_NUM));
        group.bench_with_input(BenchmarkId::from_parameter(iteration_name), &tree, |b, tree| {
            b.iter(|| lookup_every_ipv4_prefix(tree).expect(&format!("Failed to search tree with strides {:?}", strides)))
        });
    }
    group.finish();
}

criterion_group!{
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = bench
}
criterion_main!(benches);