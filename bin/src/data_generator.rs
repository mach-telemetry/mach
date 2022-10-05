use super::*;

use clap::*;
use lazy_static::*;
use mach::id::SeriesId;
use mach::sample::SampleType;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;
use std::fs::File;
use std::io::*;

const RNG_SEED: u64 = 1234567890;

type SourceMap = HashMap<SeriesId, Vec<(u64, Vec<SampleType>)>>;

lazy_static! {
    static ref BASE_DATA: SourceMap = read_intel_data();
    pub static ref SAMPLES: Vec<(SeriesId, &'static [SampleType], f64)> = generate_samples();
}

fn generate_samples() -> Vec<(SeriesId, &'static [SampleType], f64)> {
    let source_count = constants::PARAMETERS.source_count;
    let mut keys: Vec<u64> = BASE_DATA.keys().map(|x| x.0).collect();
    keys.sort();
    println!("Expanding data based on source_count = {}", source_count);
    let mut rng = ChaCha8Rng::seed_from_u64(RNG_SEED);
    let mut tmp_samples = Vec::new();
    let mut stats_map: Vec<(bool, usize)> = Vec::new();
    for id in 0..source_count {
        let ser_id = SeriesId(*keys.choose(&mut rng).unwrap());
        let s = BASE_DATA.get(&ser_id).unwrap();
        // count metrics
        let is_metric = match s[0].1[0] {
            SampleType::F64(_) => true,
            _ => false,
        };
        stats_map.push((is_metric, s.len()));
        for item in s.iter() {
            tmp_samples.push((SeriesId(id), item.0, item.1.as_slice()));
        }
    }

    println!("Sorting by time");
    tmp_samples.sort_by(|a, b| a.1.cmp(&b.1)); // sort by timestamp

    println!("Setting up final samples");
    let samples: Vec<(SeriesId, &[SampleType], f64)> = tmp_samples
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
    let data: HashMap<SeriesId, Vec<(u64, Vec<SampleType>)>> =
        bincode::deserialize(&bytes).unwrap();
    //let data: SourceMap = data.into_iter().map(|x| {
    //    let series_id = x.0;
    //    let sequence = x.1.into_iter().map(|x| x.1).collect();
    //    (series_id, sequence)
    //}).collect();
    println!("Read data for {} sources", data.len());
    data
}
