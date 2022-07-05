#![feature(llvm_asm)]
#[allow(unused_imports)]
mod kafka_utils;
mod rdtsc;

use clap::Parser;
use crossbeam_channel::bounded;
use crossbeam_channel::unbounded;
use crossbeam_channel::Receiver;
use kafka_utils::Producer;
use lzzzz::{lz4, lz4_hc};
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::fs::File;
use std::io::prelude::*;
use std::sync::atomic::Ordering::SeqCst;
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

macro_rules! ensure_dur_micros {
    ($min_dur_us: expr, $body: expr) => {
        let duration = $min_dur_us as f64 * *rdtsc::TSC_HZ_MICROS;

        let t_curr = rdtsc!();
        let dur_target = t_curr + duration as u64;
        $body;
        while rdtsc!() < dur_target {
            std::hint::spin_loop()
        }
    };
}

macro_rules! repeat_for_micros {
    ($min_dur_us: expr, $body: expr) => {
        let duration = $min_dur_us as f64 * *rdtsc::TSC_HZ_MICROS;

        let t_curr = rdtsc!();
        let dur_target = t_curr + duration as u64;
        while rdtsc!() < dur_target {
            $body;
        }
    };
}

macro_rules! time {
    ($body: expr) => {
        let start = std::time::Instant::now();
        $body;
        start.elapsed()
    };
}

macro_rules! second_to_micros {
    ($second: expr) => {
        $second * 1_000_000
    };
}

#[derive(Debug)]
struct Workload {
    samples_per_sec: u64,
    duration_secs: u64,
}

fn kafka_parallel_workload(samples: Receiver<Vec<Sample>>, workload_schedule: &[Workload]) {
    const MICROSECONDS_IN_SEC: u64 = 1_000_000;
    const KAFKA_BOOTSTRAP: &str = "localhost:9093,localhost:9094,localhost:9095";
    let num_flushers = 10;
    let batch_sz: usize = 8096;
    let topic = random_id();

    kafka_utils::make_topic(KAFKA_BOOTSTRAP, &topic);

    let mut samples_iter = samples.recv().unwrap().into_iter();
    for workload in workload_schedule {
        let (tx, rx) = bounded::<Vec<Sample>>(100);

        let flushers: Vec<JoinHandle<()>> = (0..num_flushers)
            .map(|_| {
                let receiver = rx.clone();
                let topic = topic.clone();
                let mut producer = kafka_utils::Producer::new(KAFKA_BOOTSTRAP);
                std::thread::spawn(move || {
                    while let Ok(data) = receiver.recv() {
                        let bytes = bincode::serialize(&data).unwrap();
                        let mut compressed = Vec::new();
                        lz4::compress_to_vec(
                            bytes.as_slice(),
                            &mut compressed,
                            lz4::ACC_LEVEL_DEFAULT,
                        )
                        .unwrap();
                        producer.send(topic.as_str(), 0, compressed.as_slice());
                    }
                })
            })
            .collect();

        let sample_min_dur_micros: u128 = (MICROSECONDS_IN_SEC / workload.samples_per_sec).into();
        let batch_min_dur_micros = sample_min_dur_micros * batch_sz as u128;

        let mut num_samples_pushed = 0;
        let mut num_samples_dropped = 0;

        let start = std::time::Instant::now();
        repeat_for_micros!(second_to_micros!(workload.duration_secs), {
            ensure_dur_micros!(batch_min_dur_micros, {
                let mut batch = Vec::new();
                while batch.len() < batch_sz {
                    match samples_iter.next() {
                        Some(sample) => batch.push(sample),
                        None => {
                            let next_samples =
                                samples.try_recv().expect("data producer failed to keep up");
                            samples_iter = next_samples.into_iter();
                        }
                    }
                }
                let num_samples = batch.len();
                if num_samples == 0 {
                    unreachable!();
                }
                match tx.try_send(batch) {
                    Ok(_) => num_samples_pushed += num_samples,
                    Err(e) => {
                        if e.is_full() {
                            num_samples_dropped += num_samples;
                        } else {
                            unreachable!()
                        }
                    }
                };
            });
        });
        drop(tx);
        for flusher in flushers {
            flusher.join().unwrap();
        }

        let elapsed = start.elapsed();

        let pushed_samples_per_sec = num_samples_pushed as f64 / elapsed.as_secs_f64();
        let produced_samples_per_sec =
            (num_samples_pushed + num_samples_dropped) as f64 / elapsed.as_secs_f64();
        println!(
            "workload: {:?}, produced samples/sec: {}, pushed samples/sec: {}, num samples dropped: {}",
            workload, produced_samples_per_sec, pushed_samples_per_sec, num_samples_dropped
        );
    }
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long)]
    file_path: String,
}

fn main() {
    let args = Args::parse();

    println!("Loading data");
    let data = load_data(args.file_path.as_str(), 1);
    println!("Extracting samples");
    let samples = prepare_kafka_samples(data);
    println!("{} samples extracted", samples.len());

    let (data_tx, data_rx) = bounded::<Vec<Sample>>(25);
    let _data_producer = std::thread::spawn(move || loop {
        let new_samples = repeat_data(samples.clone(), 4);
        data_tx.send(new_samples).unwrap();
    });

    let workload = vec![
        Workload {
            samples_per_sec: 10_000,
            duration_secs: 120,
        },
        Workload {
            samples_per_sec: 100_000,
            duration_secs: 120,
        },
        Workload {
            samples_per_sec: 200_000,
            duration_secs: 120,
        },
        Workload {
            samples_per_sec: 400_000,
            duration_secs: 120,
        },
        Workload {
            samples_per_sec: 800_000,
            duration_secs: 120,
        },
        Workload {
            samples_per_sec: 1_000_000,
            duration_secs: 120,
        },
        Workload {
            samples_per_sec: 1_200_000,
            duration_secs: 120,
        },
    ];

    kafka_parallel_workload(data_rx, &workload);
}
