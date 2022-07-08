#![feature(llvm_asm)]
#[allow(unused_imports)]
mod kafka_utils;
mod rdtsc;

use clap::Parser;
use crossbeam_channel::bounded;
use crossbeam_channel::unbounded;
use crossbeam_channel::Receiver;
use kafka_utils::Producer;
use lazy_static::lazy_static;
use lzzzz::{lz4, lz4_hc};
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::fs::File;
use std::io::prelude::*;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread::JoinHandle;
use std::time::Duration;

use mach::{
    compression::{CompressFn, Compression},
    id::{SeriesId, SeriesRef, WriterId},
    sample::SampleType,
    series::{FieldType, SeriesConfig},
    tsdb::Mach,
    utils::random_id,
    writer::{Writer, WriterConfig},
};

lazy_static! {
    static ref DATA: Vec<Sample> = {
        println!("Loading data");
        let data = load_data("/home/sli/data/train-ticket-data", 1);
        println!("Extracting samples");
        let samples = prepare_kafka_samples(data);
        println!("{} samples extracted", samples.len());
        samples
    };
}

type TimeStamp = u64;
type Sample = (SeriesId, TimeStamp, Vec<SampleType>);

fn repeat_data<T: Clone>(data: Vec<T>, repeat_factor: usize) -> Vec<T> {
    match repeat_factor {
        0 | 1 => data,
        n => {
            let mut ret = data.clone();
            for _ in 0..n - 1 {
                ret.extend_from_slice(data.as_slice());
            }
            ret
        }
    }
}

fn load_data(path: &str, repeat_factor: usize) -> Vec<otlp::OtlpData> {
    let mut data = Vec::new();
    File::open(path).unwrap().read_to_end(&mut data).unwrap();
    let data: Vec<otlp::OtlpData> = bincode::deserialize(data.as_slice()).unwrap();
    repeat_data(data, repeat_factor)
}

fn otlp_data_to_samples(data: Vec<otlp::OtlpData>) -> Vec<Sample> {
    let data: Vec<mach_otlp::OtlpData> = data
        .iter()
        .map(|x| {
            let mut x: mach_otlp::OtlpData = x.into();
            match &mut x {
                mach_otlp::OtlpData::Spans(x) => x.iter_mut().for_each(|x| x.set_source_id()),
                _ => unimplemented!(),
            }
            x
        })
        .collect();

    let mut samples = Vec::new();

    for entry in data {
        match entry {
            mach_otlp::OtlpData::Spans(spans) => {
                for span in spans {
                    span.get_samples(&mut samples);
                }
            }
            _ => unimplemented!(),
        }
    }

    samples
}

fn prepare_kafka_samples(data: Vec<otlp::OtlpData>) -> Vec<Sample> {
    otlp_data_to_samples(data)
}

#[derive(Debug, Copy, Clone)]
struct Workload {
    samples_per_sec: f64,
    duration_secs: Duration,
}

fn kafka_parallel_workload(workload: Workload) {
    const KAFKA_BOOTSTRAP: &str = "localhost:9093,localhost:9094,localhost:9095";
    let num_flushers = 4;
    let batch_sz: usize = 100_000;
    let topic = random_id();
    kafka_utils::make_topic(KAFKA_BOOTSTRAP, &topic);

    let barr = Arc::new(Barrier::new(num_flushers + 1));

    let mut samples_iter = 0..DATA.len();
    let (tx, rx) = bounded::<Vec<(SeriesId, TimeStamp, &'static [SampleType])>>(1);
    let flushers: Vec<JoinHandle<()>> = (0..num_flushers)
        .map(|i| {
            let receiver = rx.clone();
            let barr = barr.clone();
            let topic = topic.clone();
            let mut producer = kafka_utils::Producer::new(KAFKA_BOOTSTRAP);
            std::thread::spawn(move || {
                while let Ok(data) = receiver.recv() {
                    let bytes = bincode::serialize(&data).unwrap();
                    let mut compressed = Vec::new();
                    lz4::compress_to_vec(bytes.as_slice(), &mut compressed, lz4::ACC_LEVEL_DEFAULT)
                        .unwrap();
                    producer.send(topic.as_str(), 0, compressed.as_slice());
                }
                barr.wait();
            })
        })
        .collect();

    let batch_interval =
        Duration::from_secs_f64(1.0 / workload.samples_per_sec) * batch_sz.try_into().unwrap();
    println!("Batch interval: {:?}", batch_interval);
    let mut num_samples_pushed = 0;
    let mut num_samples_dropped = 0;

    let mut last_batch = std::time::Instant::now();
    let mut batch = Vec::with_capacity(batch_sz);
    let start = std::time::Instant::now();
    'outer: loop {
        for idx in 0..DATA.len() {
            batch.push((DATA[idx].0, DATA[idx].1, DATA[idx].2.as_slice()));
            if batch.len() == batch_sz {
                while last_batch.elapsed() < batch_interval {}
                match tx.try_send(batch) {
                    Ok(_) => num_samples_pushed += batch_sz,
                    Err(_) => num_samples_dropped += batch_sz,
                }
                batch = Vec::with_capacity(batch_sz);
                last_batch = std::time::Instant::now();
                if start.elapsed() > workload.duration_secs {
                    break 'outer;
                }
            }
        }
    }
    let produce_end = start.elapsed();
    drop(tx);
    barr.wait();
    let consume_end = start.elapsed();

    let jt = std::time::Instant::now();
    for flusher in flushers {
        flusher.join().unwrap();
    }
    let join_time = jt.elapsed();

    let pushed_samples_per_sec = num_samples_pushed as f64 / consume_end.as_secs_f64();
    let produced_samples_per_sec =
        (num_samples_pushed + num_samples_dropped) as f64 / produce_end.as_secs_f64();
    let completeness =
        num_samples_pushed as f64 / (num_samples_pushed + num_samples_dropped) as f64;

    println!(
        "Rate (per sec): {}, Elapsed time secs: {}, join time: {}, produced samples/sec: {}, pushed samples/sec: {}, produced samples: {}, num samples dropped: {}, completeness: {}", workload.samples_per_sec, produce_end.as_secs_f64(), join_time.as_secs_f64(), produced_samples_per_sec, pushed_samples_per_sec, num_samples_pushed + num_samples_dropped, num_samples_dropped, completeness
    );
}

fn main() {
    let workload = vec![
        Workload {
            samples_per_sec: 500_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 600_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 700_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 800_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 900_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 1_000_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 1_100_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 1_200_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 1_300_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 1_400_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 1_500_000.,
            duration_secs: Duration::from_secs(60),
        },
        Workload {
            samples_per_sec: 2_000_000.,
            duration_secs: Duration::from_secs(60),
        },
    ];
    //for w in workload {
    //    kafka_parallel_workload_vec(w);
    //}
    kafka_parallel_workload(workload[workload.len() - 1]);
}
