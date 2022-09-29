mod data_generator;

#[allow(dead_code)]
mod constants;
#[allow(dead_code)]
mod utils;

use constants::*;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use lazy_static::*;
use mach::{
    compression::{CompressFn, Compression},
    id::{SeriesId, SeriesRef},
    sample::SampleType,
    series::{FieldType, SeriesConfig},
    tsdb::Mach,
    writer::Writer,
    writer::WriterConfig,
};
use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Mutex, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use num::NumCast;

lazy_static! {
    pub static ref MACH: Arc<Mutex<Mach>> = Arc::new(Mutex::new(Mach::new()));
    pub static ref MACH_WRITERS: Arc<Vec<Mutex<Writer>>> = {
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

    pub static ref SAMPLES: Vec<(SeriesRef, &'static [SampleType], f64, usize)> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writers = MACH_WRITERS.clone(); // ensure WRITER is initialized (prevent deadlock)
        let samples = data_generator::SAMPLES.as_slice();

        let mut mach_guard = mach.lock().unwrap();
        //let mut writer_guard = writer.lock().unwrap();

        println!("Registering sources to Mach");
        let mut refmap: HashMap<SeriesId, SeriesRef> = HashMap::new();

        let registered_samples: Vec<(SeriesRef, &'static [SampleType], f64, usize)> = samples
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
                (id_ref, *values, *size, writer_idx)
            })
            .collect();
        registered_samples
    };

    static ref STATS_BARRIER: Barrier = Barrier::new(2);
    //static ref WRITERS_BARRIER: Barrier = Barrier::new(PARAMETERS.mach_writers + 1);
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

//type Batch = Vec<(SeriesRef, u64, &'static [SampleType])>;
//
//fn mach_writer(barrier: Arc<Barrier>, receiver: Receiver<Batch>) {
//    let mut writer_guard = MACH_WRITER.lock().unwrap();
//    let writer = &mut *writer_guard;
//    while let Ok(batch) = receiver.recv() {
//        //let mut raw_sz = 0;
//        for item in batch.iter() {
//            'push_loop: loop {
//                if writer.push(item.0, item.1, item.2).is_ok() {
//                    //for x in item.2.iter() {
//                    //    raw_sz += x.size();
//                    //}
//                    break 'push_loop;
//                }
//            }
//        }
//    }
//    barrier.wait();
//}

//fn init_mach() -> (Arc<Barrier>, Sender<Batch>) {
//    let (tx, rx) = if constants::PARAMETERS.bounded_queue {
//        bounded(1)
//    } else {
//        unbounded()
//    };
//    let barrier = Arc::new(Barrier::new(2));
//
//    {
//        let barrier = barrier.clone();
//        let rx = rx.clone();
//        thread::spawn(move || {
//            mach_writer(barrier, rx);
//        });
//    }
//    (barrier, tx)
//}

type Batch = Vec<(SeriesRef, u64, &'static [SampleType], f64)>;

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
        r: SeriesRef,
        ts: u64,
        samples: &'static [SampleType],
        sample_size: f64,
    ) -> Option<ClosedBatch> {
        self.batch.push((r, ts, samples, sample_size));
        self.batch_bytes += sample_size;
        if self.batch.len() == PARAMETERS.writer_batches {
            let batch = mem::replace(&mut self.batch, Vec::new());
            let batch_bytes = self.batch_bytes;
            self.batch_bytes = 0.;
            Some(ClosedBatch {batch, batch_bytes})
        } else {
            None
        }
    }
}

fn mach_writer(batches: Receiver<Batch>, writer_idx: usize) {
    let mut writer = MACH_WRITERS[writer_idx].lock().unwrap();
    loop {
        if let Ok(batch) = batches.try_recv() {
            for item in batch {
                while writer.push(item.0, item.1, item.2).is_err() {}
            }
        }
    }
}

fn stats_printer() {
    STATS_BARRIER.wait();

    let interval = PARAMETERS.print_interval_seconds as usize;
    let len = interval * 2;

    let mut samples_generated = vec![0; len];
    let mut samples_dropped = vec![0; len];
    let mut bytes_generated = vec![0; len];
    let mut bytes_dropped = vec![0; len];

    let mut counter = 0;


    thread::sleep(Duration::from_secs(10));
    loop {
        thread::sleep(Duration::from_secs(1));

        let idx = counter % len;
        counter += 1;

        samples_generated[idx] = COUNTERS.samples_generated();
        samples_dropped[idx] = COUNTERS.samples_dropped();
        bytes_generated[idx] = COUNTERS.bytes_generated();
        bytes_dropped[idx] = COUNTERS.bytes_dropped();

        if counter % interval == 0 {

            let max_min_delta = |a: &[usize]| -> usize {
                let mut min = usize::MAX;
                let mut max = 0;
                for idx in 0..a.len() {
                    min = min.min(a[idx]);
                    max = max.max(a[idx]);
                }
                max - min
            };

            let percent = |num: usize, den: usize| -> f64 {
                let num: f64 = <f64 as NumCast>::from(num).unwrap();
                let den: f64 = <f64 as NumCast>::from(den).unwrap();
                num / den
            };

            let samples_generated_delta = max_min_delta(&samples_generated);
            let samples_dropped_delta = max_min_delta(&samples_dropped);
            let samples_completeness = 1. - percent(samples_dropped_delta, samples_generated_delta);

            let bytes_generated_delta = max_min_delta(&bytes_generated);
            let bytes_dropped_delta = max_min_delta(&bytes_dropped);
            let bytes_completeness = 1. - percent(bytes_dropped_delta, bytes_generated_delta);

            //let samples_completeness = samples_completeness.iter().sum::<f64>() / denom;
            //let bytes_completeness = bytes_completeness.iter().sum::<f64>() / denom;
            //let bytes_rate = bytes_rate.iter().sum::<f64>() / denom;
            print!("Sample completeness: {:.2}, ", samples_completeness);
            print!("Bytes completeness: {:.2}, ", bytes_completeness);
            println!("");
        }
    }
}

fn main() {
    thread::spawn(stats_printer);
    let samples = SAMPLES.clone();
    //let mut writer = MACH_WRITER.lock().unwrap();

    let mut data_idx = 0; // index into the SAMPLES vector
    let mut sample_size_acc = 0; // total raw size of samples generated
    let mut sample_count_acc = 0; // total count of samples generated

    let mach_writers = PARAMETERS.mach_writers;
    let mut batches: Vec<Batcher> = (0..mach_writers).map(|_| Batcher::new()).collect();
    let writers: Vec<Sender<Batch>> = (0..mach_writers)
        .map(|i| {
            let (tx, rx) = if PARAMETERS.unbounded_queue {
                unbounded()
            } else {
                bounded(1)
            };
            thread::spawn(move || {
                mach_writer(rx, i);
            });
            tx
        })
        .collect();

    STATS_BARRIER.wait();
    for workload in WORKLOAD.iter() {
        let workload_start = Instant::now(); // used to verify the MBPs rate of the workload

        // Tracking workload totals
        let mut workload_total_size = 0.;
        let mut workload_total_samples = 0.;

        // Every workload.mbps check if we need to wait. This field tracks current check size
        let mut check_start = Instant::now(); // check this field to see if the workload needs to wait
        let mut current_check_size = 0.; // check this field to see size since last check
        let mbps: f64 = workload.mbps.try_into().unwrap(); // check every mbps (e.g., check every second)
        let check_duration = Duration::from_secs(1); // check duration should line up with mbps (e.g., 1 second)

        // Execute workload
        'outer: loop {
            let id = samples[data_idx].0;
            let items = samples[data_idx].1;
            let sz = samples[data_idx].2;
            let sample_size: usize = sz as usize;
            let sample_size_mb = samples[data_idx].2 / 1_000_000.;
            let writer_idx = samples[data_idx].3;
            let timestamp: u64 = utils::timestamp_now_micros().try_into().unwrap();

            if let Some(closed_batch) = batches[writer_idx].push(id, timestamp, items, sz) {
                let batch_len = closed_batch.batch.len();
                let batch_bytes = closed_batch.batch_bytes as usize;
                //println!("queue len {}", writers[writer_idx].len());
                COUNTERS.add_samples_generated(batch_len);
                COUNTERS.add_bytes_generated(batch_bytes);
                match writers[writer_idx].try_send(closed_batch.batch) {
                    Ok(_) => {}
                    Err(_) => {
                        COUNTERS.add_samples_dropped(batch_len);
                        COUNTERS.add_bytes_dropped(batch_bytes);
                    } // drop batch
                }
            }

            // increment data_idx to next one
            data_idx += 1;
            if data_idx == samples.len() {
                data_idx = 0;
            }

            // Increment counters
            current_check_size += sample_size_mb;
            workload_total_size += sample_size_mb;
            workload_total_samples += 1.;
            // sample_size_acc += sample_size;
            // sample_count_acc += 1;

            // Checking to see if workload should wait. These checks amortize the expensize
            // operations to every second
            if current_check_size >= mbps {
                // Reset the check size to accumulate for next check
                current_check_size = 0.;

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
             "Expected rate: {} mbps, Actual rate: {:.2} mbps, Samples per second: {:.2}",
             workload.mbps,
             workload_total_size / workload_duration.as_secs_f64(),
             workload_total_samples / workload_duration.as_secs_f64(),
         );
    }

    //WRITERS_BARRIER.wait();
}
