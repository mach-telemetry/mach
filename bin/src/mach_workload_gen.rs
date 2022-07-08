#![feature(vec_into_raw_parts)]
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
use std::sync::atomic::AtomicUsize;
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

type TimeStamp = u64;
type Sample = (SeriesId, TimeStamp, Vec<SampleType>);
type RegisteredSample = (SeriesRef, TimeStamp, Vec<SampleType>);

lazy_static!{
    static ref DATA: Vec<Sample> = {
        println!("Loading data");
        let data = load_data("/home/sli/data/train-ticket-data", 1);
        println!("Extracting samples");
        otlp_data_to_samples(data)
    };

}


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

fn register_samples(
    samples: &[Sample],
    mach: &mut Mach,
    writer: &mut Writer,
) -> Vec<RegisteredSample> {
    let mut refmap: HashMap<SeriesId, SeriesRef> = HashMap::new();

    for (id, _, values) in samples.iter() {
        let id_ref = *refmap.entry(*id).or_insert_with(|| {
            let conf = get_series_config(*id, values.as_slice());
            let (wid, _) = mach.add_series(conf).unwrap();
            let id_ref = writer.get_reference(*id);
            id_ref
        });
    }

    let mut registered_samples = Vec::new();
    for (series_id, ts, values) in samples {
        let series_ref = *refmap.get(&series_id).unwrap();
        registered_samples.push((series_ref, *ts, values.clone()));
    }
    registered_samples
}

fn get_series_config(id: SeriesId, values: &[SampleType]) -> SeriesConfig {
    let mut types = Vec::new();
    let mut compression = Vec::new();
    values.iter().for_each(|v| {
        let (t, c) = match v {
            //SampleType::U32(_) => (FieldType::U32, CompressFn::IntBitpack),
            SampleType::U64(_) => (FieldType::U64, CompressFn::LZ4),
            SampleType::F64(_) => (FieldType::F64, CompressFn::Decimal(3)),
            SampleType::Bytes(_) => (FieldType::Bytes, CompressFn::BytesLZ4),
            //SampleType::BorrowedBytes(_) => (FieldType::Bytes, CompressFn::NOOP),
            _ => unimplemented!(),
        };
        types.push(t);
        compression.push(c);
    });
    let compression = Compression::from(compression);
    let nvars = types.len();
    let conf = SeriesConfig {
        id,
        types,
        compression,
        seg_count: 3,
        nvars,
    };
    conf
}

fn load_mach_samples(mach: &mut Mach, writer: &mut Writer) -> Vec<RegisteredSample> {
    let samples = DATA.as_slice();
    let registered_samples = register_samples(samples, mach, writer);
    registered_samples
}

#[derive(Debug, Copy, Clone)]
struct Workload {
    samples_per_sec: f64,
    duration_secs: Duration,
}

fn mach_workload_vec(workload: Workload) {
    let mut mach = Mach::new();
    let writer_config = WriterConfig {
        active_block_flush_sz: 1_000_000,
    };
    let batch_sz = 100_000;
    let mut writer = mach.add_writer(writer_config.clone()).unwrap();
    let data = load_mach_samples(&mut mach, &mut writer);

    let (data, cap): (&'static[RegisteredSample], usize) = unsafe {
        let (ptr, len, cap) = data.into_raw_parts();
        (std::slice::from_raw_parts(ptr, len), cap)
    };

    let (tx, rx) = bounded::<Vec<(SeriesRef, TimeStamp, &'static [SampleType])>>(1);

    ////let sample_min_dur_micros: u128 = (MICROSECONDS_IN_SEC / workload.samples_per_sec).into();
    ////let batch_min_dur_micros = sample_min_dur_micros * batch_sz as u128;
    let interval = Duration::from_secs_f64(1.0 / workload.samples_per_sec) * (batch_sz.try_into().unwrap());
    println!("Interval: {:?}", interval);
    let mut num_samples_pushed = 0;
    let mut num_samples_dropped = 0;

    let barr = Arc::new(Barrier::new(2));
    let barr2 = barr.clone();
    std::thread::spawn(move || {
        //let mut try_again = Vec::with_capacity(batch_sz/2);
        while let Ok(data) = rx.recv() {
            for item in data.iter() {
                'push_loop: loop {
                    if writer.push(item.0, item.1, item.2).is_ok() {
                        break 'push_loop;
                    }
                }
            }
        }
        barr2.wait();
    });

    let start = std::time::Instant::now();
    let mut last_batch = start;
    let mut timestamp = 0;
    let mut batch = Vec::with_capacity(batch_sz);
    'outer: loop {
        for idx in 0..data.len() {
            batch.push((data[idx].0, timestamp, data[idx].2.as_slice()));
            if batch.len() == batch_sz {
                while last_batch.elapsed() < interval {}
                match tx.try_send(batch) {
                    Ok(_) => num_samples_pushed += batch_sz,
                    Err(_) => num_samples_dropped += batch_sz,
                };
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


    //let jt = std::time::Instant::now();
    //for flusher in flushers {
    //    flusher.join().unwrap();
    //}
    //let join_time = jt.elapsed();

    let vec: Vec<RegisteredSample> = unsafe {
        let (ptr, len) = (data.as_ptr(), data.len());
        Vec::from_raw_parts(ptr as *mut (SeriesRef, u64, Vec<SampleType>), len, cap)
    };

    let pushed_samples_per_sec = num_samples_pushed as f64 / consume_end.as_secs_f64();
    let produced_samples_per_sec =
        (num_samples_pushed + num_samples_dropped) as f64 / produce_end.as_secs_f64();
    let completeness =
        num_samples_pushed as f64 / (num_samples_pushed + num_samples_dropped) as f64;

    println!(
        "Rate (per sec): {}, Elapsed time secs: {}, produced samples/sec: {}, pushed samples/sec: {}, produced samples: {}, num samples dropped: {}, completeness: {}", workload.samples_per_sec, produce_end.as_secs_f64(), produced_samples_per_sec, pushed_samples_per_sec, num_samples_pushed + num_samples_dropped, num_samples_dropped, completeness
    );
}

//#[derive(Parser, Debug, Clone)]
//struct Args {
//    #[clap(short, long)]
//    file_path: String,
//}

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
            duration_secs: Duration::from_secs(300),
        },
    ];
    //for w in workload {
    //    mach_workload_vec(w);
    //}
    mach_workload_vec(workload[workload.len()-1]);
}
