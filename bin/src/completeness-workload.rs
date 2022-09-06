#[allow(dead_code)]
mod bytes_server;
mod completeness;
#[allow(dead_code)]
mod elastic;
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
use mach::id::{SeriesId, SeriesRef};
use mach::sample::SampleType;
use rand::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

lazy_static! {
    static ref ARGS: Args = Args::parse();
    static ref BASE_DATA: HashMap<SeriesId, Vec<(u64, Vec<SampleType>)>> = {
        prep_data::load_samples(ARGS.file_path.as_str())
    };
    static ref SAMPLES: Vec<(SeriesId, &'static [SampleType])> = {
        let keys: Vec<SeriesId> = BASE_DATA.keys().copied().collect();

        println!("Expanding data based on source_count = {}", ARGS.source_count);
        let mut rng = rand::thread_rng();
        let mut tmp_samples = Vec::new();
        for id in 0..ARGS.source_count {
            let s = BASE_DATA.get(keys.choose(&mut rng).unwrap()).unwrap();
            for item in s.iter() {
                tmp_samples.push((SeriesId(id), item.0, item.1.as_slice()));
            }
        }

        println!("Sorting by time");
        tmp_samples.sort_by(|a, b| a.1.cmp(&b.1)); // sort by timestamp

        println!("Setting up final samples");
        let samples: Vec<(SeriesId, &[SampleType])> = tmp_samples.drain(..).map(|x| (x.0, x.2)).collect();
        println!("Samples len: {}", samples.len());
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
    //static ref MACH_SAMPLES: Vec<prep_data::RegisteredSample> = {
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

    #[clap(short, long, default_value_t = 1000)]
    source_count: u64,

    #[clap(short, long, default_value_t = String::from("/home/sli/data/train-ticket-data"))]
    file_path: String,

    #[clap(short, long, default_value_t = 5.0)]
    counter_interval_seconds: f64,
}

fn main() {
    COUNTERS.init_watcher(Duration::from_secs_f64(ARGS.counter_interval_seconds));
    let workloads = &[
        Workload::new(500_000., Duration::from_secs(3 * 60 * 60), ARGS.batch_size),
        //Workload::new(500_000., Duration::from_secs(60), ARGS.batch_size),
        //Workload::new(2_000_000., Duration::from_secs(60), ARGS.batch_size),
        //Workload::new(500_000., Duration::from_secs(60), ARGS.batch_size),
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
            let _ = SERIES_IDS.len();
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
