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
use data_generator::SAMPLES;
use lazy_static::*;
use mach::{id::SeriesId, sample::SampleType};
use num::NumCast;
use std::mem;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use utils::RemoteNotifier;

lazy_static! {
    static ref PARTITION_WRITERS: Vec<Sender<(Box<[u8]>, u64)>> = {
        (0..PARAMETERS.kafka_partitions).map(|partition| {
            let (tx, rx) = bounded(1);
            thread::spawn(move || partition_writer(partition, rx));
            tx
        }).collect()
    };

    static ref BATCHER_WRITERS: Vec<Sender<(Batch, u64)>> = {
        (0..PARAMETERS.kafka_writers).map(|writer| {
            let (tx, rx) = if PARAMETERS.unbounded_queue {
                unbounded()
            } else {
                bounded(100)
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

fn kafka_batcher(i: u64, receiver: Receiver<(Batch, u64)>) {
    let mut batchers: Vec<batching::WriteBatch> = (0..PARAMETERS.kafka_partitions)
        .map(|_| batching::WriteBatch::new(PARAMETERS.kafka_batch_bytes))
        .collect();
    loop {
        if let Ok((batch, data_generator)) = receiver.try_recv() {
            let now = Instant::now();
            //let partition = partition as usize;
            //let batcher = &mut batchers[partition];
            let batch_len = batch.len();
            for item in batch {
                let partition = (*item.0) as usize % PARAMETERS.kafka_partitions as usize;
                let batcher = &mut batchers[partition];
                if batcher.insert(*item.0, item.1, item.2).is_err() {
                    let old_batch = mem::replace(
                        batcher,
                        batching::WriteBatch::new(PARAMETERS.kafka_batch_bytes),
                    );
                    let bytes = old_batch.close();
                    PARTITION_WRITERS[partition].send((bytes, i)).unwrap();
                }
            }
            //COUNTERS.add_samples_written(batch_len);
            println!(
                "Write rate (samples/second): {:?}",
                <f64 as NumCast>::from(batch_len).unwrap() / now.elapsed().as_secs_f64()
            );
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

fn run_workload(
    workload: Workload,
    samples: &[(SeriesId, &'static [SampleType], f64)],
    data_generator: u64,
) {
    let mut batches: Vec<Batcher> = (0..PARAMETERS.kafka_writers)
        .map(|_| Batcher::new())
        .collect();
    let mut data_idx = 0;
    let duration = workload.duration.clone();
    let workload_start = Instant::now();
    let mut batch_start = Instant::now();
    let mut workload_total_mb = 0.;
    let mut workload_total_samples = 0;

    COUNTERS.set_current_workload_rate(workload.samples_per_second as usize);

    'outer: loop {
        let id = samples[data_idx].0;
        let partition_id = id.0 as usize % PARAMETERS.kafka_partitions as usize;
        let writer_id = partition_id % PARAMETERS.kafka_writers as usize;
        let items = samples[data_idx].1;
        let sample_size = samples[data_idx].2 as usize;
        let sample_size_mb = samples[data_idx].2 / 1_000_000.;
        let timestamp: u64 = utils::timestamp_now_micros().try_into().unwrap();
        let batch = &mut batches[writer_id];

        if let Some(closed_batch) = batch.push(id, timestamp, items, sample_size) {
            let writer_id = partition_id % PARAMETERS.kafka_writers as usize;
            let batch_count = closed_batch.batch.len();
            println!("Queue length: {}", BATCHER_WRITERS[writer_id].len());
            COUNTERS.add_samples_generated(batch_count);
            match BATCHER_WRITERS[writer_id].try_send((
                //partition_id as i32,
                closed_batch.batch,
                data_generator,
            )) {
                Ok(_) => {}
                Err(_) => {
                    // drop batch
                    COUNTERS.add_samples_dropped(batch_count);
                }
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
    let actual_mbps = workload_total_mb as f64 / workload_start.elapsed().as_secs_f64();
    thread::sleep(Duration::from_secs(2));
    println!(
        "Workload expected rate: {}, Actual rate: {}, Mbps: {}",
        expected_rate, actual_rate, actual_mbps
    );
}

fn workload_runner(
    workloads: Vec<Workload>,
    data: Vec<(SeriesId, &'static [SampleType], f64)>,
    data_generator: u64,
) {
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
    let querier_addr = format!("{}:{}", PARAMETERS.querier_ip, PARAMETERS.querier_port);
    let mut query_start_notifier = RemoteNotifier::new(querier_addr);

    validate_parameters();
    let stats_barrier = utils::stats_printer();

    init_kafka();
    let _samples = SAMPLES.clone();

    let data_generator_count = PARAMETERS.data_generator_count;

    // Prep data for data generator
    let mut data: Vec<Vec<(SeriesId, &'static [SampleType], f64)>> =
        (0..data_generator_count).map(|_| Vec::new()).collect();
    for sample in SAMPLES.iter() {
        let generator = *sample.0 % PARAMETERS.kafka_partitions as u64 % data_generator_count;
        data[generator as usize].push(*sample)
    }

    // Prep Workloads
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

    for i in 0..data_generator_count as usize {
        let start_barrier = start_barrier.clone();
        let done_barrier = done_barrier.clone();
        let data = data[i].clone();
        let workloads = workloads[i].clone();
        thread::spawn(move || {
            start_barrier.wait();
            workload_runner(workloads, data, i as u64);
            done_barrier.wait();
        });
    }

    start_barrier.wait();
    query_start_notifier.notify();
    stats_barrier.wait();
    done_barrier.wait();
}
