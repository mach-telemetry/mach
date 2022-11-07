use super::*;

use clap::*;
use lazy_static::*;
use mach2::id::SourceId;
use mach2::sample::SampleType;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;
use std::fs::File;
use std::io::*;

const RNG_SEED: u64 = 1234567890;

type SourceMap = HashMap<SourceId, Vec<(u64, Vec<SampleType>)>>;

lazy_static! {
    static ref BASE_DATA: SourceMap = read_intel_data();
    pub static ref SAMPLES: Vec<(SourceId, &'static [SampleType], f64)> = generate_samples();
    pub static ref HOT_SOURCES: Vec<SourceId> = {
        let mut source_lengths = HashMap::new();
        for s in SAMPLES.iter() {
            source_lengths
                .entry(s.0)
                .and_modify(|c| *c += 1)
                .or_insert(1);
        }

        //source_lengths.into_iter().filter(|(id, cnt)| *cnt > 15000).map(|(id, cnt)| id).collect()

        let mut source_length_pairs: Vec<_> = source_lengths.into_iter().map(|(id, cnt)| (cnt, id)).collect();

        source_length_pairs.sort_by(|x, y| {
            let x: (usize, u64) = (x.0, *x.1);
            let y: (usize, u64) = (y.0, *y.1);
            y.cmp(&x)
        });

        let result: Vec<SourceId> = source_length_pairs
            .into_iter()
            .map(|(_, source_id)| source_id)
            .collect();
        println!("Longest sources: {:?}", &result[..10]);
        result[..result.len()/10].into()
    };
}

fn generate_samples() -> Vec<(SourceId, &'static [SampleType], f64)> {
    print_source_data_stats(&BASE_DATA);

    let source_count = constants::PARAMETERS.source_count;
    let mut keys: Vec<u64> = BASE_DATA.keys().map(|x| x.0).collect();
    keys.sort();
    println!("Expanding data based on source_count = {}", source_count);
    let mut rng = ChaCha8Rng::seed_from_u64(RNG_SEED);
    let mut tmp_samples = Vec::new();
    let mut stats_map: Vec<(bool, usize)> = Vec::new();
    for id in 0..source_count {
        let ser_id = SourceId(*keys.choose(&mut rng).unwrap());
        let s = BASE_DATA.get(&ser_id).unwrap();
        // count metrics
        let is_metric = match s[0].1[0] {
            SampleType::F64(_) => true,
            _ => false,
        };
        stats_map.push((is_metric, s.len()));
        for item in s.iter() {
            tmp_samples.push((SourceId(id), item.0, item.1.as_slice()));
        }
    }

    println!("Sorting by time");
    tmp_samples.sort_by(|a, b| a.1.cmp(&b.1)); // sort by timestamp

    println!("Setting up final samples");
    let samples: Vec<(SourceId, &[SampleType], f64)> = tmp_samples
        .drain(..)
        .map(|x| {
            let size: f64 = {
                let size: usize = x.2.iter().map(|x| x.size()).sum::<usize>() + 16usize; // include id and timestamp in size
                let x: u32 = size.try_into().unwrap();
                x.try_into().unwrap()
            };
            (x.0, x.2, size)
        })
        .collect();

    println!("Samples stats:");
    println!("Total number of unique samples: {}", samples.len());
    let metrics_count: u64 = stats_map.iter().map(|x| x.0 as u64).sum();
    let average_source_length: u64 = {
        let sum: u64 = stats_map.iter().map(|x| x.1 as u64).sum();
        sum / (stats_map.len() as u64)
    };
    let max_source_length: u64 = stats_map.iter().map(|x| x.1 as u64).max().unwrap();
    let min_source_length: u64 = stats_map.iter().map(|x| x.1 as u64).min().unwrap();
    println!(
        "Number of sources: {}, Number of metrics: {}",
        stats_map.len(),
        metrics_count
    );
    println!("Max source length: {}", max_source_length);
    println!("Average source length: {}", average_source_length);
    println!("Min source length: {}", min_source_length);
    println!("Total samples: {}", samples.len());
    samples
}

fn read_intel_data() -> SourceMap {
    let mut file = File::open(&constants::PARAMETERS.file_path).unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();

    let data: HashMap<SourceId, Vec<(u64, Vec<SampleType>)>> =
        bincode::deserialize(&bytes).unwrap();
    println!("Read data for {} sources", data.len());
    data
}

fn print_source_data_stats(data: &SourceMap) {
    let mut metric_sources = 0;
    let mut non_metric_sources = 0;
    let mut metric_samples = 0;
    let mut non_metric_samples = 0;
    let mut metric_bytes = 0;
    let mut non_metric_bytes = 0;

    let count_source_bytes = |source: &Vec<(u64, Vec<SampleType>)>| {
        source.iter().fold(0, |acc, (_, samples)| {
            acc + samples.iter().fold(0, |acc, s| s.size())
        })
    };

    for (_, source_data) in data {
        let is_metric = match source_data[0].1[0] {
            SampleType::I64(_) => true,
            SampleType::U64(_) => true,
            SampleType::F64(_) => true,
            _ => false,
        };

        if is_metric {
            metric_sources += 1;
            metric_samples += source_data.len();
            metric_bytes += count_source_bytes(source_data);
        } else {
            non_metric_sources += 1;
            non_metric_samples += source_data.len();
            non_metric_bytes += count_source_bytes(source_data);
        }
    }

    println!("\t\tSources,\tSamples,\tBytes");
    println!("Metrics,\t{metric_sources},\t{metric_samples},\t{metric_bytes}");
    println!("Logs + Traces,\t{non_metric_sources},\t{non_metric_samples},\t{non_metric_bytes}");
}
