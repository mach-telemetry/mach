#[allow(dead_code)]
mod bytes_server;
mod completeness;
mod kafka_utils;
#[allow(dead_code)]
mod prep_data;
#[allow(dead_code)]
mod snapshotter;
mod utils;

use crate::completeness::{
    kafka::init_kafka, kafka_es::init_kafka_es, mach::init_mach, Workload, COUNTERS,
};

use crate::completeness::mach::{MACH, MACH_WRITER};
use clap::*;
use lazy_static::lazy_static;
use mach::id::SeriesId;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

lazy_static! {
    static ref ARGS: Args = Args::parse();
    static ref SAMPLES: Vec<prep_data::Sample> = prep_data::load_samples(ARGS.file_path.as_str());
    static ref SERIES_IDS: Vec<SeriesId> = {
        let mut set = HashSet::new();
        for sample in SAMPLES.iter() {
            set.insert(sample.0);
        }
        let ids = set.drain().collect();
        println!("IDs {:?}", ids);
        ids
    };
    static ref MACH_SAMPLES: Vec<prep_data::RegisteredSample> = {
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

    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka_bootstraps: String,

    #[clap(short, long, default_value_t = 3)]
    kafka_partitions: usize,

    #[clap(short, long, default_value_t = 3)]
    kafka_replicas: usize,

    #[clap(short, long, default_value_t = String::from("kafka-completeness-bench"))]
    kafka_topic: String,

    #[clap(short, long, default_value_t = 100000)]
    batch_size: u32,

    #[clap(short, long, default_value_t = String::from("/home/sli/data/train-ticket-data"))]
    file_path: String,

    #[clap(short, long, default_value_t = 5.0)]
    counter_interval_seconds: f64,
}

fn main() {
    COUNTERS.init_watcher(Duration::from_secs_f64(ARGS.counter_interval_seconds));
    let workloads = &[
        Workload::new(500_000., Duration::from_secs(30), ARGS.batch_size),
        Workload::new(2_000_000., Duration::from_secs(70), ARGS.batch_size),
        // Workload::new(500_000., Duration::from_secs(120), ARGS.batch_size),
        // Workload::new(3_000_000., Duration::from_secs(60), ARGS.batch_size),
        // Workload::new(500_000., Duration::from_secs(120), ARGS.batch_size),
    ];
    match ARGS.tsdb.as_str() {
        "es" => {
            let samples = SAMPLES.as_slice();
            let kafka_es = init_kafka_es(
                ARGS.kafka_bootstraps.as_str(),
                ARGS.kafka_topic.as_str(),
                ARGS.kafka_writers,
            );
            for workload in workloads {
                workload.run(&kafka_es, samples);
            }
            kafka_es.done();
        }
        "kafka" => {
            let samples = SAMPLES.as_slice();
            let kafka = init_kafka(
                ARGS.kafka_bootstraps.as_str(),
                ARGS.kafka_topic.as_str(),
                ARGS.kafka_writers,
            );
            COUNTERS.init_kafka_consumer(ARGS.kafka_bootstraps.as_str(), ARGS.kafka_topic.as_str());
            COUNTERS.start_watcher();
            for workload in workloads {
                workload.run(&kafka, samples);
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
                workload.run(&mach, samples);
            }
            mach.done();
        }
        _ => panic!(),
    }
}
