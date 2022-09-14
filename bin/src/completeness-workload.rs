#[allow(dead_code)]
mod bytes_server;
#[allow(dead_code)]
mod completeness;
#[allow(dead_code)]
mod elastic;
mod kafka_utils;
#[allow(dead_code)]
mod prep_data;
#[allow(dead_code)]
mod snapshotter;
mod utils;

use crate::completeness::{kafka::init_kafka, mach::init_mach, Workload, COUNTERS};

use crate::completeness::mach::{MACH, MACH_WRITER};
use clap::*;
use lazy_static::lazy_static;
use mach::id::{SeriesId, SeriesRef};
use mach::sample::SampleType;
use std::fs::File;
use std::io::*;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use kafka_utils::KafkaTopicOptions;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

lazy_static! {
    static ref ARGS: Args = Args::parse();
    static ref BASE_DATA: HashMap<SeriesId, Vec<(u64, Vec<SampleType>)>> = {
        let mut file = File::open(&ARGS.file_path).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        let data: HashMap<SeriesId, Vec<(u64, Vec<SampleType>)>> = bincode::deserialize(&bytes).unwrap();
        println!("Read data for {} sources", data.len());
        data
        //prep_data::load_samples(ARGS.file_path.as_str())
    };
    static ref SAMPLES: Vec<(SeriesId, &'static [SampleType])> = {
        let keys: Vec<SeriesId> = BASE_DATA.keys().copied().collect();
        println!("Expanding data based on source_count = {}", ARGS.source_count);
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        let mut tmp_samples = Vec::new();
        let mut stats_map: Vec<(bool, usize)> = Vec::new();
        for id in 0..ARGS.source_count {
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
    };
    static ref SERIES_IDS: Vec<SeriesId> = {
        let mut set = HashSet::new();
        for sample in SAMPLES.iter() {
            set.insert(sample.0);
        }
        let ids: Vec<SeriesId> = set.drain().collect();
        println!("Number of IDs {}", ids.len());
        println!("Sample of IDs: {:?}", &ids[..10]);
        ids
    };
    static ref MACH_SAMPLES: Vec<(SeriesRef, &'static [SampleType])> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writer = MACH_WRITER.clone(); // ensure WRITER is initialized (prevent deadlock)
        let mut mach_guard = mach.lock().unwrap();
        let mut writer_guard = writer.lock().unwrap();
        let samples = SAMPLES.as_slice();
        prep_data::mach_register_samples(samples, &mut *mach_guard, &mut *writer_guard)

    };
    static ref DECOMPRESS_BUFFER: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(vec![0u8; 1_000_000]));
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = String::from("kafka"))]
    tsdb: String,

    #[clap(short, long, default_value_t = 4)]
    kafka_writers: usize,

    #[clap(short, long, default_value_t = 1)]
    writer_queue_len: usize,

    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka_bootstraps: String,

    #[clap(short, long, default_value_t = 3)]
    kafka_partitions: i32,

    #[clap(short, long, default_value_t = 3)]
    kafka_replicas: i32,

    #[clap(short, long, default_value_t = String::from("kafka-completeness-bench"))]
    kafka_topic: String,

    #[clap(short, long, default_value_t = 1_000_000)]
    batch_bytes: usize,

    #[clap(short, long, default_value_t = 1000)]
    source_count: u64,

    #[clap(short, long, default_value_t = String::from("/home/fsolleza/data/intel-telemetry/processed-data.bin"))]
    //#[clap(short, long, default_value_t = String::from("/home/sli/data/train-ticket-data"))]
    file_path: String,

    #[clap(short, long, default_value_t = 5.0)]
    counter_interval_seconds: f64,
}

fn main() {
    COUNTERS.init_watcher(Duration::from_secs_f64(ARGS.counter_interval_seconds));
    let workloads = &[
        Workload::new(500_000., Duration::from_secs(60), ARGS.batch_bytes),
        Workload::new(2_000_000., Duration::from_secs(60), ARGS.batch_bytes),
        //Workload::new(500_000., Duration::from_secs(60), ARGS.batch_size),
        //Workload::new(2_000_000., Duration::from_secs(60), ARGS.batch_size),
        //Workload::new(500_000., Duration::from_secs(60), ARGS.batch_size),
    ];
    match ARGS.tsdb.as_str() {
        "kafka-es" => {
            let samples = SAMPLES.as_slice();
            // Note: this is the producer part of the ES completeness workload.
            // The subsequent part of this pipeline consumes data from Kafka and
            // writes to ES (see kafka-es-connector).
            let kafka_es = init_kafka(
                ARGS.kafka_bootstraps.as_str(),
                ARGS.kafka_topic.as_str(),
                ARGS.kafka_writers,
                ARGS.writer_queue_len,
                KafkaTopicOptions {
                    num_replications: ARGS.kafka_replicas,
                    num_partitions: ARGS.kafka_partitions,
                },
            );
            COUNTERS.start_watcher();
            for workload in workloads {
                workload.run_with_writer_batching(&kafka_es, samples);
            }
            kafka_es.done();
        }
        "kafka" => {
            let samples = SAMPLES.as_slice();
            let _ = SERIES_IDS.len();
            let kafka = init_kafka(
                ARGS.kafka_bootstraps.as_str(),
                ARGS.kafka_topic.as_str(),
                ARGS.kafka_writers,
                ARGS.writer_queue_len,
                KafkaTopicOptions {
                    num_replications: ARGS.kafka_replicas,
                    num_partitions: ARGS.kafka_partitions,
                },
            );
            COUNTERS.init_kafka_consumer(ARGS.kafka_bootstraps.as_str(), ARGS.kafka_topic.as_str());
            COUNTERS.start_watcher();
            for workload in workloads {
                workload.run_with_writer_batching(&kafka, samples);
            }
            kafka.done();
        }
        "mach" => {
            let samples = MACH_SAMPLES.as_slice();
            let _ = SERIES_IDS.len();
            let mach = init_mach();
            COUNTERS.init_mach_querier(SERIES_IDS[0]);
            COUNTERS.start_watcher();
            snapshotter::initialize_snapshot_server(&mut *MACH.lock().unwrap());
            for workload in workloads {
                workload.run_with_writer_batching(&mach, samples);
            }
            mach.done();
        }
        _ => panic!(),
    }
}
