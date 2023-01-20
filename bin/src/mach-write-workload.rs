#![feature(thread_id_value)]
// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


mod json_telemetry;
//mod data_generator;

#[allow(dead_code)]
mod bytes_server;
#[allow(dead_code)]
mod constants;
#[allow(dead_code)]
mod snapshotter;
#[allow(dead_code)]
mod utils;

use constants::*;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use lazy_static::*;
use mach::{
    compression::{compression_scheme::CompressionScheme, Compression},
    field_type::FieldType,
    sample::SampleType,
    source::SourceConfig,
    source::SourceId,
    tsdb::Mach,
    //writer::WriterConfig,
    writer::SourceRef,
    //constants::*,
    writer::Writer,
    counters::{self, Counter},
    rdtsc::rdtsc,
};
use num::NumCast;
use std::mem;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc, Barrier, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};
use utils::RemoteNotifier;
use env_logger;
use std::os::unix::thread::JoinHandleExt;
use std::collections::{HashMap, BTreeMap};
use log::{debug, error, info};

static MACH_WRITER_THREAD_ID: AtomicUsize = AtomicUsize::new(0);
static WRITER_CYCLES: Counter = Counter::new();

lazy_static! {
    static ref MACH: Arc<Mutex<Mach>> = Arc::new(Mutex::new(Mach::new()));
    static ref MACH_WRITERS: Arc<Vec<Mutex<Writer>>> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let mut vec = Vec::new();

        for _ in 0..PARAMETERS.mach_writers {
            let guard = mach.lock().unwrap();
            let writer = guard.add_writer();
            vec.push(Mutex::new(writer));
        }
        Arc::new(vec)
    };

    static ref MACH_WRITER_SENDER: Vec<Sender<Batch>> = {
        debug!("Spwaning sender");
        (0..MACH_WRITERS.len())
            .map(|i| {
                let (tx, rx) = if PARAMETERS.unbounded_queue {
                    unbounded()
                } else {
                    bounded(100)
                };
                let handle = thread::spawn(move || {
                    mach_writer(rx, i);
                });
                MACH_WRITER_THREAD_ID.store(handle.thread().id().as_u64().get() as usize, SeqCst);
                tx
            })
        .collect()
    };

    static ref SAMPLES: Vec<DataSample> = {
        let _mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writers = MACH_WRITERS.clone(); // ensure WRITER is initialized (prevent deadlock)
        let samples = json_telemetry::DATA.as_slice();

        //let mut mach_guard = mach.lock().unwrap();
        //let mut writer_guard = writer.lock().unwrap();

        debug!("Registering sources to Mach");
        let mut refmap: HashMap<SourceId, SourceRef> = HashMap::new();

        let registered_samples: Vec<DataSample> = samples
            .iter()
            .map(|x| {
                let id = x.source;
                let values = x.values.as_slice();
                let size: f64 = {
                    let mut size: usize = values.iter().map(|x| x.size()).sum();
                    size += 16; // id and timestamp;
                    <f64 as NumCast>::from(size).unwrap()
                };
                //let (id, values, size) = (x.source, x.values, x.values.iter().map(|x| x.size()).sum::<());
                let writer_idx = id.0 as usize % writers.len();
                let id_ref = *refmap.entry(id).or_insert_with(|| {
                    let conf = get_series_config(id, values);

                    let mut writer_guard = writers[writer_idx].lock().unwrap();
                    writer_guard.add_source(conf)
                });
                DataSample {
                    series_ref: id_ref,
                    data: values,
                    size: size,
                    writer_idx,
                }
            })
            .collect();
        registered_samples
    };

}

fn get_series_config(id: SourceId, values: &[SampleType]) -> SourceConfig {
    let mut types = Vec::new();
    let mut compression = Vec::new();
    values.iter().for_each(|v| {
        let (t, c) = match v {
            //SampleType::U32(_) => (FieldType::U32, CompressFn::IntBitpack),
            SampleType::U64(_) => (FieldType::U64, CompressionScheme::lz4()),
            SampleType::F64(_) => (FieldType::F64, CompressionScheme::lz4()),
            SampleType::Bytes(_) => (FieldType::Bytes, CompressionScheme::lz4()),
            _ => unimplemented!(),
        };
        types.push(t);
        compression.push(c);
    });
    let compression = Compression::new(compression);
    SourceConfig {
        id,
        types,
        compression,
    }
}

#[derive(Copy, Clone)]
struct DataSample {
    series_ref: SourceRef,
    data: &'static [SampleType],
    size: f64,
    writer_idx: usize,
}

#[derive(Copy, Clone)]
struct Sample {
    series_ref: SourceRef,
    timestamp: u64,
    data: &'static [SampleType],
    size: f64,
}

type Batch = Vec<Sample>;

struct ClosedBatch {
    batch: Batch,
    //batch_bytes: f64,
}

struct Batcher {
    batch: Batch,
    batch_bytes: f64,
}

impl Batcher {
    fn new() -> Self {
        Batcher {
            batch: Vec::new(),
            batch_bytes: 0.,
        }
    }

    fn push(&mut self, sample: Sample) -> Option<ClosedBatch> {
        self.batch_bytes += sample.size;
        self.batch.push(sample);
        if self.batch.len() == PARAMETERS.writer_batches {
            let batch = mem::take(&mut self.batch);
            let _batch_bytes = self.batch_bytes;
            self.batch_bytes = 0.;
            Some(ClosedBatch { batch })
        } else {
            None
        }
    }
}

static TOTAL_TIME: AtomicUsize = AtomicUsize::new(0);
static TOTAL_SAMPLES: AtomicUsize = AtomicUsize::new(0);

#[allow(unused)]
struct Index {
    trees: HashMap<u64, Vec<u8>>,
    buf: Vec<u8>,
}

#[allow(unused)]
impl Index {
    fn new() -> Self {
        Index {
            trees: HashMap::new(),
            buf: vec![0u8; 4096],
        }
    }
    fn insert(&mut self, id: u64, ts: u64, data: &[SampleType]) {
        let start_time = rdtsc();
        let item = (id, ts, data);
        let mut v = self.trees.entry(id).or_insert(Vec::new());
        let start = v.len();
        v.extend_from_slice(&0usize.to_be_bytes());
        // Write each byte
        for item in data {
            match item {
                SampleType::F64(x) => {
                    v.push(1u8);
                    v.extend_from_slice(&x.to_be_bytes());
                },
                SampleType::Bytes(x) => {
                    v.push(5u8);
                    v.extend_from_slice(&x.len().to_be_bytes());
                    v.extend_from_slice(&x);
                }
                SampleType::I64(x) => {
                    v.push(2u8);
                    v.extend_from_slice(&x.to_be_bytes());
                },
                SampleType::U64(x) => {
                    v.push(3u8);
                    v.extend_from_slice(&x.to_be_bytes());
                },
                _ => panic!("Unhandled type {:?}", item),
            }
        }
        let new_len = v.len();
        v[start..start+8].copy_from_slice(&new_len.to_be_bytes());
        WRITER_CYCLES.increment(rdtsc() - start_time);
    }
}

fn mach_writer(batches: Receiver<Batch>, writer_idx: usize) {
    //debug!(">>>> Mach writer thread: {}", gettid::gettid());
    let mut writer = MACH_WRITERS[writer_idx].lock().unwrap();
    let mut len = 0usize;

    let mut trees = Index::new();
    loop {
        if let Ok(batch) = batches.try_recv() {
            let start = rdtsc();
            let batch_len = batch.len();
            let now = Instant::now();

            // Noop operation
            //for item in batch {
            //    len += item.data.len();
            //}

            // Write into Mach
            for item in batch {
                len += writer.push(item.series_ref, item.timestamp, item.data).is_ok() as usize;
            }

            // Write into an in-mem index
            //for item in batch {
            //    trees.insert(item.series_ref.0, item.timestamp, item.data.into());
            //    len += 1;
            //}

            //MACH_WRITER_CYCLES.increment(rdtsc() - start);

            let elapsed = now.elapsed();
            info!(
                "Write rate (samples/second): {:?}",
                <f64 as NumCast>::from(len).unwrap() / elapsed.as_secs_f64()
            );
            len = 0;
        }
    }
}

fn run_workload(workload: Workload, samples: &[DataSample]) {
    let mut batches: Vec<Batcher> = (0..MACH_WRITERS.len()).map(|_| Batcher::new()).collect();
    let mut data_idx = 0;

    // Tracking workload totals
    let mut workload_total_samples = 0.;
    let mut workload_total_size = 0.;

    // Every workload.mbps check if we need to wait. This field tracks current check size
    let mut check_start = Instant::now(); // check this field to see if the workload needs to wait
    let samples_per_second: f64 = <f64 as NumCast>::from(workload.samples_per_second).unwrap(); // check every mbps (e.g., check every second)
    let check_duration = Duration::from_secs(1); // check duration should line up with mbps (e.g., 1 second)

    COUNTERS.set_current_workload_rate(workload.samples_per_second as usize);

    // Execute workload
    //let mut writer = MACH_WRITERS[0].lock().unwrap();
    //let samples = SAMPLES.clone();

    let workload_start = Instant::now(); // used to verify the MBPs rate of the workload
    //thread::spawn(move || {
    //    count_cpu_process();
    //});
    'outer: loop {
        //{
        //    let now = Instant::now();
        //    for sample in samples.iter() {
        //        let timestamp = utils::timestamp_now_micros().try_into().unwrap();
        //        while writer.push(sample.series_ref,timestamp, sample.data).is_err() {}
        //        workload_total_samples += 1.;
        //    }
        //    println!("{:?}", <f64 as NumCast>::from(samples.len()).unwrap() / now.elapsed().as_secs_f64());
        //}
        //if workload_start.elapsed() > workload.duration {
        //    break 'outer;
        //}

        let src = &samples[data_idx];
        let sample_size: usize = src.size as usize;
        let writer_idx = src.writer_idx;

        let sample = Sample {
            series_ref: src.series_ref,
            data: src.data,
            timestamp: utils::timestamp_now_micros(),
            size: src.size,
        };

        if let Some(closed_batch) = batches[writer_idx].push(sample) {
            let batch_len = closed_batch.batch.len();
            //let batch_bytes = closed_batch.batch_bytes as usize;
            info!("Queue length: {}", MACH_WRITER_SENDER[writer_idx].len());
            COUNTERS.add_samples_generated(batch_len);
            match MACH_WRITER_SENDER[writer_idx].try_send(closed_batch.batch) {
                Ok(_) => {}
                Err(_) => {
                    COUNTERS.add_samples_dropped(batch_len);
                } // drop batch
            }
        }

        //increment data_idx to next one
        data_idx += 1;
        if data_idx == samples.len() {
            data_idx = 0;
        }

        // Increment counters
        workload_total_samples += 1.;
        workload_total_size += <f64 as NumCast>::from(sample_size).unwrap();

        // Checking to see if workload should wait. These checks amortize the expensize
        // operations to every second
        if workload_total_samples > 0. && workload_total_samples % samples_per_second == 0. {
            // If behind, wait until the check duration
            while check_start.elapsed() < check_duration {}
            check_start = Instant::now();

            // Break out of the workload if workload is done
            if workload_start.elapsed() > workload.duration {
                break 'outer;
            }
        }
    }
    let workload_duration = workload_start.elapsed();
    info!(
        "Expected rate: {} samples per second, Samples per second: {:.2}, Mbps: {:.2}",
        workload.samples_per_second,
        workload_total_samples / workload_duration.as_secs_f64(),
        workload_total_size / 1_000_000. / workload_duration.as_secs_f64(),
    );
    //println!("{} {}", TOTAL_SAMPLES.load(SeqCst), TOTAL_TIME.load(SeqCst));
    let sleep_time = 10;
    info!("Moving to next workload, sleeping for {}s", sleep_time);
    std::thread::sleep(Duration::from_secs(10));

    // Get cycles
    let writer_cycles = WRITER_CYCLES.load();
    let mach_writer_cycles = mach::counters::cpu_cycles();

    WRITER_CYCLES.reset();
    mach::counters::reset_cpu_cycles();

    //let total = mach_writer_cycles + data_flusher_cycles + meta_flusher_cycles;
    info!("index cycles: {}, mach cycles: {}", writer_cycles, mach_writer_cycles);
}

fn workload_runner(workloads: Vec<Workload>, data: Vec<DataSample>) {
    info!(">>>> Workload thread: {}", gettid::gettid());
    info!("Sleeping 10 secs");
    thread::sleep(Duration::from_secs(10));
    info!("Workloads: {:?}", workloads);
    for workload in workloads {
        run_workload(workload, &data);
    }
}

fn validate_parameters() {
    assert!(PARAMETERS.data_generator_count <= PARAMETERS.mach_writers);
}

////fn main() {
////    let samples = SAMPLES.clone();
////    assert_eq!(MACH_WRITERS.len(), 1);
////    let mut writer = MACH_WRITERS[0].lock().unwrap();
////    let mut ts = 0;
////    let now = Instant::now();
////    for sample in SAMPLES.iter() {
////        //let ts = utils::timestamp_now_micros().try_into().unwrap();
////        while writer.push(sample.series_ref, ts, sample.data).is_err() {}
////        ts += 1;
////    }
////    let elapsed = now.elapsed();
////    println!("{}", <f64 as NumCast>::from(SAMPLES.len()).unwrap() / elapsed.as_secs_f64());
////}

fn main() {
    info!("Main thread: {}", gettid::gettid());
    env_logger::Builder::from_default_env().target(env_logger::Target::Stdout).init();
    validate_parameters();

    let querier_addr = format!("{}:{}", PARAMETERS.querier_ip, PARAMETERS.querier_port);
    let query_start_notifier = RemoteNotifier::new(querier_addr);

    let stats_barrier = utils::stats_printer();
    let data_generator_count = PARAMETERS.data_generator_count;
    let _samples = SAMPLES.clone();

    // Prep data for each data generator
    let mut data: Vec<Vec<DataSample>> = (0..data_generator_count).map(|_| Vec::new()).collect();
    for sample in SAMPLES.iter() {
        let generator_idx = sample.writer_idx % data_generator_count as usize;
        data[generator_idx].push(*sample);
    }

    // Prep workloads for each data generator
    let mut workloads: Vec<Vec<Workload>> = (0..data_generator_count).map(|_| Vec::new()).collect();
    for workload in constants::WORKLOAD.iter() {
        let workload = workload.split_rate(data_generator_count);
        workload
            .into_iter()
            .zip(workloads.iter_mut())
            .for_each(|(w, v)| v.push(w));
    }

    let start_barrier = Arc::new(Barrier::new((data_generator_count + 1) as usize));
    let done_barrier = Arc::new(Barrier::new((data_generator_count + 1) as usize));
    {
        let _ = MACH_WRITER_SENDER.len();
    }

    for i in 0..data_generator_count as usize {
        let start_barrier = start_barrier.clone();
        let done_barrier = done_barrier.clone();
        let data = data[i].clone();
        let workloads = workloads[i].clone();
        std::thread::spawn(move || {
            start_barrier.wait();
            workload_runner(workloads, data);
            done_barrier.wait();
        });
        //println!("Workload thread: {}", handle.as_pthread_t());
    }

    {
        snapshotter::initialize_snapshot_server(&MACH.lock().unwrap());
    }

    info!("You've got 30 seconds!");
    //std::thread::sleep(Duration::from_secs(30));
    query_start_notifier.notify();
    info!("Beginning workload");
    start_barrier.wait();
    stats_barrier.wait();
    done_barrier.wait();
    std::thread::sleep(Duration::from_secs(20));
}
