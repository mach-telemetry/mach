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
use std::mem;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

lazy_static! {
    static ref PARTITION_WRITERS: Vec<Sender<(Box<[u8]>, u64)>> = {
        (0..PARAMETERS.kafka_partitions).map(|partition| {
            let (tx, rx) = bounded(1);
            thread::spawn(move || partition_writer(partition, rx));
            tx
        }).collect()
    };

    static ref BATCHER_WRITERS: Vec<Sender<(i32, Batch, u64)>> = {
        (0..PARAMETERS.kafka_writers).map(|writer| {
            let (tx, rx) = if PARAMETERS.unbounded_queue {
                unbounded()
            } else {
                bounded(1)
            };
            thread::spawn(move || kafka_batcher(writer, rx));
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

fn partition_writer(partition: i32, rx: Receiver<(Box<[u8]>, u64)>) {
    let mut producer = kafka_utils::Producer::new(PARAMETERS.kafka_bootstraps.as_str());
    let mut last_batch_writer = u64::MAX;
    while let Ok((bytes, batch_writer)) = rx.recv() {
        if last_batch_writer == u64::MAX {
            last_batch_writer = batch_writer;
        } else {
            assert_eq!(last_batch_writer, batch_writer);
        }
        producer.send(PARAMETERS.kafka_topic.as_str(), partition, &bytes);
        COUNTERS.add_bytes_written_to_kafka(bytes.len());
        COUNTERS.add_messages_written_to_kafka(1);
    }
}

fn kafka_batcher(i: u64, receiver: Receiver<(i32, Batch, u64)>) {
    let mut batchers: Vec<batching::WriteBatch> = (0..PARAMETERS.kafka_partitions).map(|_| batching::WriteBatch::new(PARAMETERS.kafka_batch_bytes)).collect();
    let mut last_data_generator = u64::MAX;
    while let Ok((partition, batch, data_generator)) = receiver.recv() {
        if last_data_generator == u64::MAX {
            last_data_generator = data_generator;
        } else {
            assert_eq!(last_data_generator, data_generator);
        }
        let partition = partition as usize;
        let batcher = &mut batchers[partition];
        for item in batch {
            if batcher.insert(*item.0, item.1, item.2).is_err() {
                let old_batch = mem::replace(batcher, batching::WriteBatch::new(PARAMETERS.kafka_batch_bytes));
                let bytes = old_batch.close();
                PARTITION_WRITERS[partition].send((bytes, i)).unwrap();
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


fn run_workload(workload: Workload, samples: &[(SeriesId, &'static [SampleType], f64)], data_generator: u64) {
    let mut batches: Vec<Batcher> = (0..PARAMETERS.kafka_partitions).map(|_| Batcher::new()).collect();
    let mut data_idx = 0;
    let duration = workload.duration.clone();
    let workload_start = Instant::now();
    let mut batch_start = Instant::now();
    //let mut workload_total_mb = 0.;
    let mut workload_total_samples = 0;

    'outer: loop {
        let id = samples[data_idx].0;
        let partition_id = id.0 as usize % PARAMETERS.kafka_partitions as usize;
        let items = samples[data_idx].1;
        let sample_size = samples[data_idx].2 as usize;
        //let sample_size_mb = samples[data_idx].2 / 1_000_000.;
        let timestamp: u64 = utils::timestamp_now_micros().try_into().unwrap();
        let batch = &mut batches[partition_id];

        if let Some(closed_batch) = batch.push(id, timestamp, items, sample_size) {
            let writer_id = partition_id % PARAMETERS.kafka_writers as usize;
            let batch_size = closed_batch.batch_size;
            let batch_count = closed_batch.batch.len();
            COUNTERS.add_samples_generated(batch_count);
            COUNTERS.add_bytes_generated(batch_size);
            match BATCHER_WRITERS[writer_id].try_send((partition_id as i32, closed_batch.batch, data_generator)) {
                Ok(_) => {}
                Err(_) => {
                    COUNTERS.add_samples_dropped(batch_count);
                    COUNTERS.add_bytes_dropped(batch_size);
                } // drop batch
            }
        }
        //workload_total_mb += sample_size_mb;
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

fn workload_runner(workloads: Vec<Workload>, data: Vec<(SeriesId, &'static[SampleType], f64)>, data_generator: u64) {
    println!("Workloads: {:?}", workloads);
    for workload in workloads {
        run_workload(workload, data.as_slice(), data_generator);
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

fn validate_parameters() {
    assert!(PARAMETERS.data_generator_count <= PARAMETERS.kafka_writers as u64);
}

fn main() {

    validate_parameters();
    let stats_barrier = utils::stats_printer();

    init_kafka();
    let _samples = SAMPLES.clone();

    let data_generator_count = PARAMETERS.data_generator_count;

    // Prep data for data generator
    let mut data: Vec<Vec<(SeriesId, &'static [SampleType], f64)>> = (0..data_generator_count).map(|_| Vec::new()).collect();
    for sample in SAMPLES.iter() {
        let generator = *sample.0 % PARAMETERS.kafka_partitions as u64 % data_generator_count;
        data[generator as usize].push(*sample)
    }

    // Prep Workloads
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
        thread::spawn( move || {
            start_barrier.wait();
            workload_runner(workloads, data, i as u64);
            done_barrier.wait();
        });
    }

    start_barrier.wait();
    stats_barrier.wait();
    done_barrier.wait();
}

#[allow(dead_code)]
fn find_max_production_rate() {
    let base_rate = 500_000;

    for i in 1.. {
        let rate = base_rate * i;

        println!("Trying {} samples/sec", rate);

        let w = Workload::new(rate, Duration::from_secs(60));
        run_workload(w, &SAMPLES[..], 0u64);
    }
}

