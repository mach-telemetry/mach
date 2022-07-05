#![feature(llvm_asm)]
#[allow(unused_imports)]
mod kafka_utils;
mod rdtsc;

use clap::Parser;
use crossbeam_channel::bounded;
use crossbeam_channel::unbounded;
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

fn load_data(path: &str, repeat_factor: usize) -> Vec<otlp::OtlpData> {
    let mut data = Vec::new();
    File::open(path).unwrap().read_to_end(&mut data).unwrap();
    let data: Vec<otlp::OtlpData> = bincode::deserialize(data.as_slice()).unwrap();

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

macro_rules! second_to_micros {
    ($second: expr) => {
        $second * 1_000_000
    };
}

struct Workload {
    samples_per_sec: u64,
    duration_secs: u64,
}

fn workload_gen(samples: Vec<Sample>, workload_schedule: &[Workload]) {
    const MICROSECONDS_IN_SEC: u64 = 1_000_000;
    let (tx, rx) = unbounded::<Sample>();

    let mut num_samples = 0;
    let now = std::time::Instant::now();

    let mut samples_iter = samples.into_iter();
    for workload in workload_schedule {
        let sample_min_dur_us: u128 = (MICROSECONDS_IN_SEC / workload.samples_per_sec).into();

        repeat_for_micros!(second_to_micros!(workload.duration_secs), {
            match samples_iter.next() {
                Some(sample) => {
                    ensure_dur_micros!(sample_min_dur_us, {
                        tx.send(sample).unwrap();
                        num_samples += 1;
                    });
                }
                None => break,
            }
        });
    }

    let elapsed = now.elapsed();
    let samples_per_sec = num_samples as f64 / elapsed.as_secs_f64();
    println!(
        "Written Samples {}, Elapsed {:?}, Samples/sec {}",
        num_samples, elapsed, samples_per_sec,
    );
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
    println!("{} samples ready", samples.len());

    let workload = vec![Workload {
        samples_per_sec: 10000,
        duration_secs: 60,
    }];

    workload_gen(samples, &workload);
}
