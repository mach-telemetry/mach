mod constants;
mod data_generator;

#[allow(dead_code)]
mod utils;

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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

lazy_static! {
    pub static ref MACH: Arc<Mutex<Mach>> = Arc::new(Mutex::new(Mach::new()));
    pub static ref MACH_WRITER: Arc<Mutex<Writer>> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writer_config = WriterConfig {
            active_block_flush_sz: constants::PARAMETERS.mach_block_sz,
        };
        let mut guard = mach.lock().unwrap();
        Arc::new(Mutex::new(guard.add_writer(writer_config).unwrap()))
    };

    pub static ref SAMPLES: Vec<(SeriesRef, &'static [SampleType], f64)> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writer = MACH_WRITER.clone(); // ensure WRITER is initialized (prevent deadlock)
        let mut mach_guard = mach.lock().unwrap();
        let mut writer_guard = writer.lock().unwrap();
        let samples = data_generator::SAMPLES.as_slice();

        println!("Registering sources to Mach");
        let mut refmap: HashMap<SeriesId, SeriesRef> = HashMap::new();

        let registered_samples: Vec<(SeriesRef, &'static [SampleType], f64)> = samples
            .iter()
            .map(|x| {
                let (id, values, size) = x;
                let id_ref = *refmap.entry(*id).or_insert_with(|| {
                    let conf = get_series_config(*id, &*values);
                    let (_, _) = mach_guard.add_series(conf).unwrap();
                    let id_ref = writer_guard.get_reference(*id);
                    id_ref
                });
                (id_ref, *values, *size)
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

fn main() {
    let q = constants::PARAMETERS.bounded_queue;
    println!("BOUNDED_QUEUE: {}", q);
    let samples = SAMPLES.clone();
    let mut writer = MACH_WRITER.lock().unwrap();

    let mut offset = 0;

    for workload in constants::WORKLOAD.iter() {
        let duration = workload.duration.clone();
        let workload_start = Instant::now();
        let mut batch_start = Instant::now();
        let mut current_check_size = 0.;
        let mut workload_total_size = 0.;
        let mut workload_total_samples = 0.;
        let mbps: f64 = workload.mbps.try_into().unwrap();
        'outer: loop {
            let id = samples[offset].0;
            let items = samples[offset].1;
            let sample_size = samples[offset].2 / 1_000_000.;
            let timestamp: u64 = utils::timestamp_now_micros().try_into().unwrap();

            'push: loop {
                if writer.push(id, timestamp, items).is_ok() {
                    break 'push;
                }
            }

            current_check_size += sample_size;
            workload_total_size += sample_size;
            workload_total_samples += 1.;
            offset += 1;
            if offset == samples.len() {
                offset = 0;
            }
            if current_check_size >= mbps {
                println!("Current check size: {}", current_check_size);
                current_check_size = 0.;
                // calculate expected time
                while batch_start.elapsed() < Duration::from_secs(1) {}
                batch_start = Instant::now();
                if workload_start.elapsed() > duration {
                    break 'outer;
                }
            }
        }
        println!(
            "Expected rate: {} mbps, Actual rate: {} mbps, Sampling rate: {}",
            workload.mbps,
            workload_total_size / duration.as_secs_f64(),
            workload_total_samples / duration.as_secs_f64()
        );
    }
}
