mod data_generator;
mod kafka_utils;

#[allow(dead_code)]
mod batching;
#[allow(dead_code)]
mod constants;
#[allow(dead_code)]
mod utils;

use constants::*;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use lazy_static::*;
use mach::{id::SeriesId, sample::SampleType};
use std::mem;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashMap;

lazy_static! {
    static ref STATS_BARRIER: Barrier = Barrier::new(2);
}

fn kafka_writer(receiver: Receiver<(i32, Batch)>) {
    let mut partition_batch_map = HashMap::new();

    let mut producer = kafka_utils::Producer::new(PARAMETERS.kafka_bootstraps.as_str());
    let kafka_batch_size = PARAMETERS.kafka_batch_bytes;

    //let mut batcher = batching::WriteBatch::new(kafka_batch_size);

    while let Ok((partition, batch)) = receiver.recv() {
        let mut batcher = partition_batch_map.entry(partition).or_insert_with(|| {
            batching::WriteBatch::new(kafka_batch_size)
        });
        let batch_count = batch.len();
        let mut batch_size = 0;
        for item in batch {
            batch_size += item.3;
            if batcher.insert(*item.0, item.1, item.2).is_err() {
                // replace batcher in the partition batch map, close the old batch
                let bytes = partition_batch_map
                    .insert(partition, batching::WriteBatch::new(kafka_batch_size))
                    .unwrap()
                    .close();
                batcher = partition_batch_map.get_mut(&partition).unwrap();
                producer.send(PARAMETERS.kafka_topic.as_str(), partition, &bytes);
            }
        }
        COUNTERS.add_samples_written(batch_count);
        COUNTERS.add_bytes_written(batch_size);
    }
}

type Batch = Vec<(SeriesId, u64, &'static [SampleType], usize)>;
struct Batcher {
    batch: Batch,
    batch_size: usize,
}

impl Batcher {
    fn new(batch_size: usize) -> Self {
        Batcher {
            batch: Vec::new(),
            batch_size,
        }
    }

    fn push(
        &mut self,
        r: SeriesId,
        ts: u64,
        samples: &'static [SampleType],
        size: usize,
    ) -> Option<Batch> {
        self.batch.push((r, ts, samples, size));
        if self.batch.len() == self.batch_size {
            let batch = mem::replace(&mut self.batch, Vec::new());
            Some(batch)
        } else {
            None
        }
    }
}

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

fn stats_printer() {
    STATS_BARRIER.wait();
    println!("Samples generated, Samples written, Bytes generated, Bytes written");
    loop {
        let samples_generated = COUNTERS.samples_generated();
        let samples_written = COUNTERS.samples_written();
        let samples_completeness = {
            let a: i32 = samples_written.try_into().unwrap();
            let a: f64 = a.try_into().unwrap();
            let b: i32 = samples_generated.try_into().unwrap();
            let b: f64 = b.try_into().unwrap();
            a / b
        };
        let bytes_generated = COUNTERS.bytes_generated();
        let bytes_written = COUNTERS.bytes_written();
        //let mb_generated = COUNTERS.bytes_generated() / 1_000_000;
        //let mb_written = COUNTERS.bytes_written() / 1_000_000;
        //let mb_completeness = {
        //    let a: i32 = mb_written.try_into().unwrap();
        //    let a: f64 = a.try_into().unwrap();
        //    let b: i32 = mb_generated.try_into().unwrap();
        //    let b: f64 = b.try_into().unwrap();
        //    a / b
        //};
        print!("Samples generated: {}, ", samples_generated);
        print!("Samples written: {}, ", samples_written);
        print!("Sample completeness: {}, ", samples_completeness);
        print!("Bytes generated: {}, ", bytes_generated);
        print!("Bytes written: {}, ", bytes_written);
        println!("");
        thread::sleep(Duration::from_secs(PARAMETERS.print_interval_seconds));
    }
}

fn main() {
    thread::spawn(stats_printer);
    init_kafka();

    let n_writers = PARAMETERS.kafka_writers as usize;
    let n_partitions = PARAMETERS.kafka_partitions as usize;
    let batch_size = PARAMETERS.writer_batches;
    let unbounded_queue = PARAMETERS.unbounded_queue;
    let samples = data_generator::SAMPLES.clone();

    println!("KAFKA WRITERS: {}", n_writers);
    let mut batches: Vec<Batcher> = (0..n_partitions).map(|_| Batcher::new(batch_size)).collect();

    let writers: Vec<Sender<(i32, Batch)>> = (0..n_writers)
        .map(|_| {
            let (tx, rx) = {
                if unbounded_queue {
                    unbounded()
                } else {
                    bounded(1)
                }
            };
            thread::spawn(move || {
                kafka_writer(rx);
            });
            tx
        })
        .collect();

    let mut data_idx = 0;
    let mut sample_size_acc = 0;
    let mut sample_count_acc = 0;

    STATS_BARRIER.wait();

    for workload in WORKLOAD.iter() {
        let duration = workload.duration.clone();
        let workload_start = Instant::now();
        let mut batch_start = Instant::now();
        let mut current_check_size = 0.;
        let mut workload_total_size = 0.;
        let mut workload_total_samples = 0.;
        let mbps: f64 = workload.mbps.try_into().unwrap();
        'outer: loop {
            let id = samples[data_idx].0;
            let partition_id = id.0 as usize % n_partitions;
            let items = samples[data_idx].1;
            let sample_size = samples[data_idx].2 as usize;
            let sample_size_mb = samples[data_idx].2 / 1_000_000.;
            let timestamp: u64 = utils::timestamp_now_micros().try_into().unwrap();

            if let Some(batch) = batches[partition_id].push(id, timestamp, items, sample_size) {
                let writer_id = partition_id % n_writers;
                match writers[writer_id].try_send((partition_id as i32, batch)) {
                    Ok(_) => {}
                    Err(_) => {} // drop batch
                }
            }

            current_check_size += sample_size_mb;
            workload_total_size += sample_size_mb;
            workload_total_samples += 1.;

            sample_size_acc += sample_size;
            sample_count_acc += 1;

            data_idx += 1;
            if data_idx == samples.len() {
                data_idx = 0;
            }
            if current_check_size >= mbps {
                current_check_size = 0.;

                // Store samples generated and total size since last check. Reset these counters
                COUNTERS.add_samples_generated(sample_count_acc);
                COUNTERS.add_bytes_generated(sample_size_acc);
                sample_count_acc = 0;
                sample_size_acc = 0;

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
        //println!("Samples produced: {}, Samples dropped: {}", total_samples, samples_dropped);
    }
}
