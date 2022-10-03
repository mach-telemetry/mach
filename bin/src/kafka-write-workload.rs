mod data_generator;
mod kafka_utils;

#[allow(dead_code)]
mod batching;
#[allow(dead_code)]
mod constants;
#[allow(dead_code)]
mod utils;

use data_generator::SAMPLES;
use constants::*;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use lazy_static::*;
use mach::{id::SeriesId, sample::SampleType};
use num::NumCast;
use num_format::{Locale, ToFormattedString};
use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

lazy_static! {
    static ref STATS_BARRIER: Barrier = Barrier::new(2);
    static ref PARTITION_WRITERS: Vec<Sender<Box<[u8]>>> = {
        (0..PARAMETERS.kafka_partitions).map(|partition| {
            let (tx, rx): (Sender<Box<[u8]>>, Receiver<Box<[u8]>>) = bounded(1);
            std::thread::spawn(move || partition_writer(partition, rx));
            tx
        }).collect()
    };

    static ref BATCHER_WRITERS: Vec<Sender<(i32, Batch)>> = {
        (0..PARAMETERS.kafka_writers).map(|writer| {
            let (tx, rx) = bounded(1);
            std::thread::spawn(move || kafka_batcher(rx));
            tx
        }).collect()
    };

    //static ref WORKLOAD_RUNNERS: Vec<Sender<Workload>> = {
    //}
}

struct ClosedBatch {
    batch: Batch,
    batch_size: usize,
}

type Batch = Vec<(SeriesId, u64, &'static [SampleType], usize)>;

fn partition_writer(partition: i32, rx: Receiver<Box<[u8]>>) {
    let mut producer = kafka_utils::Producer::new(PARAMETERS.kafka_bootstraps.as_str());
    while let Ok(bytes) = rx.recv() {
        producer.send(PARAMETERS.kafka_topic.as_str(), partition, &bytes);
    }
}

fn kafka_batcher(receiver: Receiver<(i32, Batch)>) {
    let mut batchers: Vec<batching::WriteBatch> = (0..PARAMETERS.kafka_partitions).map(|_| batching::WriteBatch::new(PARAMETERS.kafka_batch_bytes)).collect();
    while let Ok((partition, batch)) = receiver.recv() {
        let partition = partition as usize;
        let batcher = &mut batchers[partition];
        for item in batch {
            if batcher.insert(*item.0, item.1, item.2).is_err() {
                let old_batch = mem::replace(batcher, batching::WriteBatch::new(PARAMETERS.kafka_batch_bytes));
                let bytes = old_batch.close();
                PARTITION_WRITERS[partition].send(bytes).unwrap();
            }
        }
    }
}

struct Batcher {
    batch: Batch,
    batch_size: usize,
}

impl Batcher {
    fn new() -> Self {
        Batcher {
            batch: Vec::new(),
            batch_size: 0,
        }
    }

    fn push(
        &mut self,
        r: SeriesId,
        ts: u64,
        samples: &'static [SampleType],
        size: usize,
    ) -> Option<ClosedBatch> {
        self.batch.push((r, ts, samples, size));
        self.batch_size += size;
        if self.batch.len() == PARAMETERS.writer_batches {
            let batch = mem::replace(&mut self.batch, Vec::new());
            let batch_size = self.batch_size;
            self.batch_size = 0;
            Some(ClosedBatch { batch, batch_size })
        } else {
            None
        }
    }
}


fn run_workload(workload: Workload, samples: &[(SeriesId, &'static [SampleType], f64)]) {
    let mut batches: Vec<Batcher> = (0..PARAMETERS.kafka_partitions).map(|_| Batcher::new()).collect();
    let mut data_idx = 0;
    let duration = workload.duration.clone();
    let workload_start = Instant::now();
    let mut batch_start = Instant::now();
    let mut workload_total_mb = 0.;
    let mut workload_total_samples = 0;

    'outer: loop {
        let id = samples[data_idx].0;
        let partition_id = id.0 as usize % PARAMETERS.kafka_partitions as usize;
        let items = samples[data_idx].1;
        let sample_size = samples[data_idx].2 as usize;
        let sample_size_mb = samples[data_idx].2 / 1_000_000.;
        let timestamp: u64 = utils::timestamp_now_micros().try_into().unwrap();
        let batch = &mut batches[partition_id];

        if let Some(closed_batch) = batch.push(id, timestamp, items, sample_size) {
            let writer_id = partition_id % PARAMETERS.kafka_writers as usize;
            let batch_size = closed_batch.batch_size;
            let batch_count = closed_batch.batch.len();
            COUNTERS.add_samples_generated(batch_count);
            COUNTERS.add_bytes_generated(batch_size);
            match BATCHER_WRITERS[writer_id].try_send((partition_id as i32, closed_batch.batch)) {
                Ok(_) => {}
                Err(_) => {
                    COUNTERS.add_samples_dropped(batch_count);
                    COUNTERS.add_bytes_dropped(batch_size);
                } // drop batch
            }
        }
        workload_total_mb += sample_size_mb;
        workload_total_samples += 1;

        data_idx += 1;
        if data_idx == samples.len() {
            data_idx = 0;
        }
        if workload_total_samples > 0 && workload_total_samples % workload.samples_per_second == 0 {
            while batch_start.elapsed() < Duration::from_secs(1) {}
            batch_start = Instant::now();
            if workload_start.elapsed() > duration {
                break 'outer;
            }
        }
    }

    let expected_rate = workload.samples_per_second;
    let actual_rate = workload_total_samples as f64 / workload_start.elapsed().as_secs_f64();
    println!("Workload expected rate: {}, Actual rate: {}", expected_rate, actual_rate);
}

fn workload_runner(workloads: Vec<Workload>, data: Vec<(SeriesId, &'static[SampleType], f64)>) {
    println!("Workloads: {:?}", workloads);
    for workload in workloads {
        run_workload(workload, data.as_slice());
    }
}

fn main() {

    assert!(PARAMETERS.data_generator_count <= (PARAMETERS.kafka_writers as u64));

    init_kafka();
    std::thread::spawn(move || stats_printer());
    let _samples = SAMPLES.clone();

    let data_generator_count = PARAMETERS.data_generator_count;

    // Prep data for data generator
    let mut data: Vec<Vec<(SeriesId, &'static [SampleType], f64)>> = (0..data_generator_count).map(|_| Vec::new()).collect();
    for sample in SAMPLES.iter() {
        let generator = *sample.0 % PARAMETERS.kafka_partitions as u64 % data_generator_count;
        data[generator as usize].push(*sample)
    }
    //for generator in 0..data_generator_count {
    //    let generator_data: Vec<(SeriesId, &'static [SampleType], f64)> = 
    //        SAMPLES.iter().filter(|x| *x.0 % data_generator_count == 0).copied().collect();
    //    data.push(generator_data);
    //}

    // Prep Workloads
    let mut workloads: Vec<Vec<Workload>> = (0..data_generator_count).map(|_| Vec::new()).collect();
    for workload in constants::WORKLOAD.iter() {
        let mut samples_per_second = workload.samples_per_second / data_generator_count;
        let mut excess = workload.samples_per_second % data_generator_count;
        for generator in 0..data_generator_count as usize {
            let mut w = Workload {
                samples_per_second,
                duration: workload.duration,
            };
            if generator == data_generator_count as usize - 1 {
                w.samples_per_second += excess;
            }
            workloads[generator].push(w);
        }
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

    start_barrier.wait();
    STATS_BARRIER.wait();
    done_barrier.wait();
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

            print!("Sample completeness: {:.2}, ", samples_completeness);
            print!("Bytes completeness: {:.2}, ", bytes_completeness);
            println!("");
        }
    }
}


        //if let Some(closed_batch) = batches[partition_id].push(id, timestamp, items, sample_size) {
        //    let writer_id = partition_id % n_writers;
        //    let batch_size = closed_batch.batch_size;
        //    let batch_count = closed_batch.batch.len();
        //    COUNTERS.add_samples_generated(batch_count);
        //    COUNTERS.add_bytes_generated(batch_size);
        //    match writers[writer_id].try_send((partition_id as i32, closed_batch.batch)) {
        //        Ok(_) => {}
        //        Err(_) => {
        //            COUNTERS.add_samples_dropped(batch_count);
        //            COUNTERS.add_bytes_dropped(batch_size);
        //        } // drop batch
        //    }
        //}

        //workload_total_mb += sample_size_mb;
        //workload_total_samples += 1;

        //data_idx += 1;
        //if data_idx == samples.len() {
        //    data_idx = 0;
        //}
        //if workload_total_samples > 0 && workload_total_samples % workload.samples_per_second == 0 {
        //    while batch_start.elapsed() < Duration::from_secs(1) {}
        //    batch_start = Instant::now();
        //    if workload_start.elapsed() > duration {
        //        break 'outer;
        //    }
        //}


//    let n_partitions = PARAMETERS.kafka_partitions as usize;
//    let n_writers = writers.len();
//
//    let duration = workload.duration.clone();
//    let workload_start = Instant::now();
//    let mut batch_start = Instant::now();
//    let mut workload_total_mb = 0.;
//    let mut workload_total_samples = 0;
//
//    'outer: loop {
//        let id = samples[data_idx].0;
//        let partition_id = id.0 as usize % n_partitions;
//        let items = samples[data_idx].1;
//        let sample_size = samples[data_idx].2 as usize;
//        let sample_size_mb = samples[data_idx].2 / 1_000_000.;
//        let timestamp: u64 = utils::timestamp_now_micros().try_into().unwrap();
//
//        if let Some(closed_batch) = batches[partition_id].push(id, timestamp, items, sample_size) {
//            let writer_id = partition_id % n_writers;
//            let batch_size = closed_batch.batch_size;
//            let batch_count = closed_batch.batch.len();
//            COUNTERS.add_samples_generated(batch_count);
//            COUNTERS.add_bytes_generated(batch_size);
//            match writers[writer_id].try_send((partition_id as i32, closed_batch.batch)) {
//                Ok(_) => {}
//                Err(_) => {
//                    COUNTERS.add_samples_dropped(batch_count);
//                    COUNTERS.add_bytes_dropped(batch_size);
//                } // drop batch
//            }
//        }
//
//        workload_total_mb += sample_size_mb;
//        workload_total_samples += 1;
//
//        data_idx += 1;
//        if data_idx == samples.len() {
//            data_idx = 0;
//        }
//        if workload_total_samples > 0 && workload_total_samples % workload.samples_per_second == 0 {
//            while batch_start.elapsed() < Duration::from_secs(1) {}
//            batch_start = Instant::now();
//            if workload_start.elapsed() > duration {
//                break 'outer;
//            }
//        }
//    }
//
//    WorkloadRunResult {
//        duration,
//        total_samples: workload_total_samples,
//        total_mb: workload_total_mb,
//    }
//}




//impl PartitionWriter {
//    fn new(partition: PartitionId) -> Self {
//        PartitionWriter {
//            sender: tx,
//            id: partition,
//            batching: batching::WriteBatch::new(PARAMETERS.kafka_batch_bytes),
//        }
//    }
//
//    fn write(&mut self, id: SeriesId, timestamp: u64, value: &[SampleType]) {
//        if self.batching.insert(*id, timestamp, value).is_err() {
//            let old_batch = mem::replace(&mut self.batching, batching::WriteBatch::new(PARAMETERS.kafka_batch_bytes));
//            let bytes = old_batch.close();
//            self.sender.send(bytes).unwrap();
//        }
//    }
//}
//
//struct KafkaBatcherEntry {
//    partition: PartitionId,
//    batch: Batch,
//}
//
//struct KafkaBatcher {
//    sender: Sender<KafkaBatcherEntry>,
//    //map: HashMap<PartitionId, PartitionWriter>,
//    batcher: Vec<Batcher>,
//}
//
//impl KafkaBatcher {
//    fn new(partitions: Vec<PartitionId>) -> Self {
//
//        let (tx, rx) = utils::parameterized_queue::<KafkaBatcherEntry>();
//        std::thread::spawn(move || {
//            let mut partition_writers = HashMap::new();
//            for id in partitions {
//                partition_writers.insert(id, PartitionWriter::new(id));
//            }
//
//            while let Ok(item) = rx.recv() {
//                let KafkaBatcherEntry {
//                    partition,
//                    batch,
//                } = item;
//                let partition_writer = partition_writers.get_mut(&partition).unwrap();
//                for item in batch {
//                    partition_writer.write(item.0, item.1, item.2);
//                }
//            }
//        });
//
//        Self {
//            sender: tx
//            batcher: Batcher::new(),
//        }
//    }
//
//    fn write(&mut self, r: SeriesId, ts: u64, samples: &'static [SampleType], size: usize) {
//        if let Some(closed_batch) = self.batcher.push(r, ts, samples, size) {
//        }
//    }
//}
//
//fn main() {
//}
//
//
fn init_kafka() {
    let kafka_topic_options = kafka_utils::KafkaTopicOptions {
        num_partitions: PARAMETERS.kafka_partitions,
        num_replicas: PARAMETERS.kafka_replicas,
    };
    kafka_utils::make_topic(
        PARAMETERS.kafka_bootstraps.as_str(),
        PARAMETERS.kafka_topic.as_str(),
        kafka_topic_options,
    );
}
//
//
//struct DataGeneratorSample {
//    id: SeriesId,
//    values: &'static [SampleType],
//    size: f64,
//    writer_idx: usize,
//}
//
//struct DataGenerator {
//    samples: Vec<DataGeneratorSample>,
//    writers: Vec<Sender<(i32, Batch)>>,
//    workloads: Vec<Workload>,
//}
//
//impl DataGenerator {
//    fn run(&self) {
//        for w in self.workloads {
//        }
//    }
//}
//
//
//
//struct WorkloadRunResult {
//    total_mb: f64,
//    total_samples: usize,
//    duration: Duration,
//}
//
//fn produce_samples(workload: &Workload, writers: &Vec<Sender<(i32, Batch)>>) -> WorkloadRunResult {
//    let samples = data_generator::SAMPLES.clone();
//    let n_partitions = PARAMETERS.kafka_partitions as usize;
//    let n_writers = writers.len();
//    let mut batches: Vec<Batcher> = (0..n_partitions).map(|_| Batcher::new()).collect();
//
//    let mut data_idx = 0;
//
//    let duration = workload.duration.clone();
//    let workload_start = Instant::now();
//    let mut batch_start = Instant::now();
//    let mut workload_total_mb = 0.;
//    let mut workload_total_samples = 0;
//
//    'outer: loop {
//        let id = samples[data_idx].0;
//        let partition_id = id.0 as usize % n_partitions;
//        let items = samples[data_idx].1;
//        let sample_size = samples[data_idx].2 as usize;
//        let sample_size_mb = samples[data_idx].2 / 1_000_000.;
//        let timestamp: u64 = utils::timestamp_now_micros().try_into().unwrap();
//
//        if let Some(closed_batch) = batches[partition_id].push(id, timestamp, items, sample_size) {
//            let writer_id = partition_id % n_writers;
//            let batch_size = closed_batch.batch_size;
//            let batch_count = closed_batch.batch.len();
//            COUNTERS.add_samples_generated(batch_count);
//            COUNTERS.add_bytes_generated(batch_size);
//            match writers[writer_id].try_send((partition_id as i32, closed_batch.batch)) {
//                Ok(_) => {}
//                Err(_) => {
//                    COUNTERS.add_samples_dropped(batch_count);
//                    COUNTERS.add_bytes_dropped(batch_size);
//                } // drop batch
//            }
//        }
//
//        workload_total_mb += sample_size_mb;
//        workload_total_samples += 1;
//
//        data_idx += 1;
//        if data_idx == samples.len() {
//            data_idx = 0;
//        }
//        if workload_total_samples > 0 && workload_total_samples % workload.samples_per_second == 0 {
//            while batch_start.elapsed() < Duration::from_secs(1) {}
//            batch_start = Instant::now();
//            if workload_start.elapsed() > duration {
//                break 'outer;
//            }
//        }
//    }
//
//    WorkloadRunResult {
//        duration,
//        total_samples: workload_total_samples,
//        total_mb: workload_total_mb,
//    }
//}
//
//#[allow(dead_code)]
//fn find_max_production_rate() {
//    let max_production_rate = || {
//        let (tx, rx) = bounded(1);
//        thread::spawn(move || while let Ok(_) = rx.recv() {});
//        let writers = vec![tx];
//
//        let mut target_samples_per_sec = 50_000_000;
//
//        loop {
//            println!("Trying {} samples/sec", target_samples_per_sec);
//
//            let w = Workload::new(target_samples_per_sec, Duration::from_secs(60));
//            let r = produce_samples(&w, &writers);
//            let actual_samples_per_sec = r.total_samples as f64 / r.duration.as_secs_f64();
//
//            if actual_samples_per_sec < target_samples_per_sec as f64 * 0.8 {
//                return actual_samples_per_sec as usize;
//            } else {
//                target_samples_per_sec *= 2;
//            }
//        }
//    };
//
//    println!("Probing max single-thread production rate");
//    let max_rate = max_production_rate();
//    println!(
//        "Single-thread max samples/sec: {}",
//        max_rate.to_formatted_string(&Locale::en)
//    );
//}

//fn main() {
//    thread::spawn(stats_printer);
//    init_kafka();
//
//    let n_writers = PARAMETERS.kafka_writers as usize;
//    let unbounded_queue = PARAMETERS.unbounded_queue;
//
//    println!("KAFKA WRITERS: {}", n_writers);
//
//    let writers: Vec<Sender<(i32, Batch)>> = (0..n_writers)
//        .map(|_| {
//            let (tx, rx) = {
//                if unbounded_queue {
//                    unbounded()
//                } else {
//                    bounded(1)
//                }
//            };
//            thread::spawn(move || {
//                kafka_writer(rx);
//            });
//            tx
//        })
//        .collect();
//
//    STATS_BARRIER.wait();
//
//    for workload in WORKLOAD.iter() {
//        let r = produce_samples(workload, &writers);
//
//        println!(
//            "Expected rate: {} samples per second, Actual rate: {:.2} mbps, Samples/sec: {:.2}",
//            workload.samples_per_second,
//            r.total_mb / r.duration.as_secs_f64(),
//            r.total_samples as f64 / r.duration.as_secs_f64()
//        );
//    }
//}
