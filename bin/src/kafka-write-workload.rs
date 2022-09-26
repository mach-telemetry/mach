mod batching;
mod data_generator;
mod constants;
mod utils;
mod kafka_utils;

use std::time::{Instant, Duration};
use std::mem;
use std::thread;
use crossbeam::channel::{Sender, Receiver, bounded, unbounded};

fn kafka_writer(
    partition: i32,
    receiver: Receiver<batching::WriteBatch>,
) {
    let mut producer = kafka_utils::Producer::new(constants::PARAMETERS.kafka_bootstraps.as_str());
    while let Ok(batch) = receiver.recv() {
        let bytes = batch.close();
        producer.send(
                constants::PARAMETERS.kafka_topic.as_str(),
            partition,
            &bytes,
        );
    }
}

fn init_kafka() {
    let kafka_topic_options = kafka_utils::KafkaTopicOptions {
        num_partitions: constants::PARAMETERS.kafka_partitions,
        num_replicas: constants::PARAMETERS.kafka_replicas,
    };
    kafka_utils::make_topic(
        constants::PARAMETERS.kafka_bootstraps.as_str(),
        constants::PARAMETERS.kafka_topic.as_str(),
        kafka_topic_options,
    );
}

fn main() {

    init_kafka();

    let n_writers = constants::PARAMETERS.kafka_partitions as usize;
    let batch_size = constants::PARAMETERS.kafka_batch_bytes;
    let bounded_queue = constants::PARAMETERS.bounded_queue;
    let samples = data_generator::SAMPLES.clone();

    println!("KAFKA WRITERS: {}", n_writers);
    let mut batches: Vec<batching::WriteBatch> = (0..n_writers).map(|_| batching::WriteBatch::new(batch_size)).collect();
    let mut writers: Vec<Sender<batching::WriteBatch>> = (0..n_writers).map(|i| {
        let (tx, rx) = {
            if bounded_queue {
                bounded(1)
            } else {
                unbounded()
            }
        };
        thread::spawn(move || {
            kafka_writer(i as i32, rx);
        });
        tx
    }).collect();

    let mut offset = 0;
    let mut total_samples = 0;
    let mut samples_dropped = 0;

    for workload in constants::WORKLOAD.iter() {
        let duration = workload.duration.clone();
        let workload_start = Instant::now();
        let mut batch_start = Instant::now();
        let mut current_check_size = 0.;
        let mut workload_total_size = 0.;
        let mut workload_total_samples = 0.;
        let mbps: f64 = workload.mbps.try_into().unwrap();
        'outer: loop {
            let id = samples[offset].0.0;
            let batch_id = id as usize % n_writers;
            let items = samples[offset].1;
            let sample_size = samples[offset].2 / 1_000_000.;
            let timestamp: u64 = utils::timestamp_now_micros().try_into().unwrap();

            match batches[batch_id].insert(id, timestamp, items) {
                Ok(_) => {},
                Err(_) => {
                    let old_batch = mem::replace(&mut batches[batch_id], batching::WriteBatch::new(batch_size));
                    let count = old_batch.count();
                    match writers[batch_id].try_send(old_batch) {
                        Ok(_) => {},
                        Err(_) => {
                            samples_dropped += count
                        }
                    };
                    // TODO do something with old batch
                }
            }

            current_check_size += sample_size;
            workload_total_size += sample_size;
            workload_total_samples += 1.;

            total_samples += 1;

            offset += 1;
            if offset == samples.len() {
                offset = 0;
            }
            if current_check_size >= mbps {
                println!("Current check size: {}", current_check_size);
                current_check_size = 0.;
                // calculate expected time
                while batch_start.elapsed() < Duration::from_secs(1) { }
                batch_start = Instant::now();
                if workload_start.elapsed() > duration {
                    break 'outer;
                }
            }
        }
        println!("Expected rate: {} mbps, Actual rate: {} mbps, Sampling rate: {}", workload.mbps, workload_total_size / duration.as_secs_f64(), workload_total_samples / duration.as_secs_f64());
        println!("Samples produced: {}, Samples dropped: {}", total_samples, samples_dropped);
    }
}

