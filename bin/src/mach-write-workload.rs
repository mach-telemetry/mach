mod data_generator;

#[allow(dead_code)]
mod constants;
#[allow(dead_code)]
mod utils;
#[allow(dead_code)]
mod snapshotter;
#[allow(dead_code)]
mod bytes_server;

use constants::*;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use lazy_static::*;
use mach::{
    compression::{CompressFn, Compression},
    id::{SeriesId, SeriesRef},
    sample::SampleType,
    series::{FieldType, SeriesConfig},
    tsdb::{self, Mach},
    writer::Writer,
    writer::WriterConfig,
    constants::*,
};
use num::NumCast;
use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Barrier, Mutex, atomic::{AtomicUsize, Ordering::SeqCst}};
use std::thread;
use std::time::{Duration, Instant};
use utils::RemoteNotifier;

lazy_static! {
    static ref MACH: Arc<Mutex<Mach>> = Arc::new(Mutex::new(Mach::new()));
    static ref MACH_WRITERS: Arc<Vec<Mutex<Writer>>> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let mut vec = Vec::new();

        for _ in 0..PARAMETERS.mach_writers {
            let writer_config = WriterConfig {
                active_block_flush_sz: PARAMETERS.mach_block_sz,
            };
            let mut guard = mach.lock().unwrap();
            let writer = guard.add_writer(writer_config).unwrap();
            vec.push(Mutex::new(writer));
        }
        Arc::new(vec)
    };

    static ref MACH_WRITER_SENDER: Vec<Sender<Batch>> = {
        (0..MACH_WRITERS.len())
            .map(|i| {
                let (tx, rx) = if PARAMETERS.unbounded_queue {
                    unbounded()
                } else {
                    bounded(100)
                };
                thread::spawn(move || {
                    mach_writer(rx, i);
                });
                tx
            })
        .collect()
    };

    static ref SAMPLES: Vec<DataSample> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writers = MACH_WRITERS.clone(); // ensure WRITER is initialized (prevent deadlock)
        let _hot_source = data_generator::HOT_SOURCES.as_slice();
        let samples = data_generator::SAMPLES.as_slice();

        let mut mach_guard = mach.lock().unwrap();
        //let mut writer_guard = writer.lock().unwrap();

        println!("Registering sources to Mach");
        let mut refmap: HashMap<SeriesId, SeriesRef> = HashMap::new();

        let registered_samples: Vec<DataSample> = samples
            .iter()
            .map(|x| {
                let (id, values, size) = x;
                let writer_idx = id.0 as usize % writers.len();
                let id_ref = *refmap.entry(*id).or_insert_with(|| {
                    let conf = get_series_config(*id, &*values);

                    let mut writer_guard = writers[writer_idx].lock().unwrap();
                    let writer_id = writer_guard.id();
                    let _ = mach_guard.add_series_to_writer(conf, writer_id).unwrap();
                    let id_ref = writer_guard.get_reference(*id);
                    id_ref
                });
                DataSample {
                    series_ref: id_ref,
                    data: *values,
                    size: *size,
                    writer_idx,
                }
            })
            .collect();
        registered_samples
    };

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
        seg_count: 1,
        nvars,
    };
    conf
}

#[derive(Copy, Clone)]
struct DataSample {
    series_ref: SeriesRef,
    data: &'static [SampleType],
    size: f64,
    writer_idx: usize,
}

#[derive(Copy, Clone)]
struct Sample {
    series_ref: SeriesRef,
    timestamp: u64,
    data: &'static [SampleType],
    size: f64,
}

type Batch = Vec<Sample>;

struct ClosedBatch {
    batch: Batch,
    batch_bytes: f64,
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

    fn push(
        &mut self,
        sample: Sample,
    ) -> Option<ClosedBatch> {
        self.batch_bytes += sample.size;
        self.batch.push(sample);
        if self.batch.len() == PARAMETERS.writer_batches {
            let batch = mem::replace(&mut self.batch, Vec::new());
            let batch_bytes = self.batch_bytes;
            self.batch_bytes = 0.;
            Some(ClosedBatch { batch, batch_bytes })
        } else {
            None
        }
    }
}

static TOTAL_TIME: AtomicUsize = AtomicUsize::new(0);
static TOTAL_SAMPLES: AtomicUsize = AtomicUsize::new(0);

fn mach_writer(batches: Receiver<Batch>, writer_idx: usize) {
    let mut writer = MACH_WRITERS[writer_idx].lock().unwrap();
    loop {
        if let Ok(batch) = batches.try_recv() {
            let batch_len = batch.len();
            let now = Instant::now();
            for item in batch {
                while writer.push(item.series_ref, item.timestamp, item.data).is_err() {}
            }
            let elapsed = now.elapsed();
            println!("Write rate (samples/second): {:?}", <f64 as NumCast>::from(batch_len).unwrap() / elapsed.as_secs_f64());
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
            timestamp: utils::timestamp_now_micros().try_into().unwrap(),
            size: src.size,
        };


        if let Some(closed_batch) = batches[writer_idx].push(sample) {
            let batch_len = closed_batch.batch.len();
            let batch_bytes = closed_batch.batch_bytes as usize;
            println!("Queue length: {}", MACH_WRITER_SENDER[writer_idx].len());
            COUNTERS.add_samples_generated(batch_len);
            match MACH_WRITER_SENDER[writer_idx].try_send(closed_batch.batch) {
                Ok(_) => { },
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
        //workload_total_size += <f64 as NumCast>::from(sample_size).unwrap();

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
    println!(
        "Expected rate: {} samples per second, Samples per second: {:.2}, Mbps: {:.2}",
        workload.samples_per_second,
        workload_total_samples / workload_duration.as_secs_f64(),
        workload_total_size / 1_000_000. / workload_duration.as_secs_f64(),
    );
    println!("{} {}", TOTAL_SAMPLES.load(SeqCst), TOTAL_TIME.load(SeqCst));
}

fn workload_runner(workloads: Vec<Workload>, data: Vec<DataSample>) {
    println!("Workloads: {:?}", workloads);
    for workload in workloads {
        run_workload(workload, &data);
    }
}

fn validate_parameters() {
    assert!(PARAMETERS.data_generator_count <= PARAMETERS.mach_writers);
}

//fn main() {
//    let samples = SAMPLES.clone();
//    assert_eq!(MACH_WRITERS.len(), 1);
//    let mut writer = MACH_WRITERS[0].lock().unwrap();
//    let mut ts = 0;
//    let now = Instant::now();
//    for sample in SAMPLES.iter() {
//        //let ts = utils::timestamp_now_micros().try_into().unwrap();
//        while writer.push(sample.series_ref, ts, sample.data).is_err() {}
//        ts += 1;
//    }
//    let elapsed = now.elapsed();
//    println!("{}", <f64 as NumCast>::from(SAMPLES.len()).unwrap() / elapsed.as_secs_f64());
//}

fn main() {
    validate_parameters();

    let querier_addr = format!("{}:{}", PARAMETERS.querier_ip, PARAMETERS.querier_port);
    let mut query_start_notifier = RemoteNotifier::new(querier_addr);

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
        workload.into_iter().zip(workloads.iter_mut()).for_each(|(w, v)| v.push(w));
    }

    let start_barrier = Arc::new(Barrier::new((data_generator_count + 1) as usize));
    let done_barrier = Arc::new(Barrier::new((data_generator_count + 1) as usize));

    for i in 0..data_generator_count as usize {
        let start_barrier = start_barrier.clone();
        let done_barrier = done_barrier.clone();
        let data = data[i].clone();
        let workloads = workloads[i].clone();
        std::thread::spawn( move || {
            start_barrier.wait();
            workload_runner(workloads, data);
            done_barrier.wait();
        });
    }

    {
        snapshotter::initialize_snapshot_server(&mut *MACH.lock().unwrap());
    }

    start_barrier.wait();
    query_start_notifier.notify();
    println!("Beginning workload");
    stats_barrier.wait();
    done_barrier.wait();
}
