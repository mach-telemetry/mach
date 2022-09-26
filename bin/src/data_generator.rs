use super::*;

use clap::*;
use std::fs::File;
use std::io::*;
use mach::id::SeriesId;
use mach::sample::SampleType;
use std::collections::HashMap;
use lazy_static::*;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

type SourceMap = HashMap<SeriesId, Vec<(u64, Vec<SampleType>)>>;

lazy_static! {
    static ref BASE_DATA: SourceMap = read_intel_data();
    pub static ref SAMPLES: Vec<(SeriesId, &'static[SampleType])> = generate_samples();
}

fn generate_samples() -> Vec<(SeriesId, &'static[SampleType])> {
    let source_count = constants::PARAMETERS.source_count;
    let keys: Vec<SeriesId> = BASE_DATA.keys().copied().collect();
    println!("Expanding data based on source_count = {}", source_count);
    let mut rng = ChaCha8Rng::seed_from_u64(1);
    let mut tmp_samples = Vec::new();
    let mut stats_map: Vec<(bool, usize)> = Vec::new();
    for id in 0..source_count {
        let s = BASE_DATA.get(keys.choose(&mut rng).unwrap()).unwrap();
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
    let samples: Vec<(SeriesId, &[SampleType])> = tmp_samples.drain(..).map(|x| (x.0, x.2)).collect();

    println!("Samples stats:");
    println!("Total number of unique samples: {}", samples.len());
    let metrics_count: u64 = stats_map.iter().map(|x| x.0 as u64).sum();
    let average_source_length: u64 = {
        let sum: u64 = stats_map.iter().map(|x| x.1 as u64).sum();
        sum / (stats_map.len() as u64)
    };
    let max_source_length: u64 = stats_map.iter().map(|x| x.1 as u64).max().unwrap();
    let min_source_length: u64 = stats_map.iter().map(|x| x.1 as u64).min().unwrap();
    println!("Number of sources: {}, Number of metrics: {}", stats_map.len(), metrics_count);
    println!("Max source length: {}", max_source_length);
    println!("Average source length: {}", average_source_length);
    println!("Min source length: {}", min_source_length);
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

