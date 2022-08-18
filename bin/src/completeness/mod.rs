pub mod kafka;
pub mod kafka_es;
pub mod mach;

mod mach_extern {
    pub use mach::*;
}

use crate::completeness::{kafka::init_kafka_consumer, mach::init_mach_querier};
use crate::utils::timestamp_now_micros;
use crossbeam_channel::Sender;
use lazy_static::lazy_static;
use mach_extern::{
    id::SeriesId,
    mem_list::{FLUSHER_QUEUE_LEN, TOTAL_BYTES_FLUSHED, UNFLUSHED_COUNT},
    sample::SampleType,
    writer::QUEUE_LEN,
};
use std::sync::{atomic::AtomicUsize, atomic::Ordering::SeqCst, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

lazy_static! {
    pub static ref COUNTERS: Counters = Counters::new();
}

pub type Sample<'s, I> = (I, u64, &'s [SampleType]);
pub type SampleOwned<I> = (I, u64, Vec<SampleType>);

pub struct Counters {
    samples_enqueued: Arc<AtomicUsize>,
    samples_dropped: Arc<AtomicUsize>,
    samples_written: Arc<AtomicUsize>,
    unflushed_count: Arc<AtomicUsize>,
    bytes_flushed: Arc<AtomicUsize>,
    raw_data_size: Arc<AtomicUsize>,
    data_age: Arc<AtomicUsize>,
    start_gate: Arc<Barrier>,
    buffer_queue: Arc<AtomicUsize>,
    flusher_queue: Arc<AtomicUsize>,
}

impl Counters {
    pub fn new() -> Self {
        let r = Self {
            samples_enqueued: Arc::new(AtomicUsize::new(0)),
            samples_dropped: Arc::new(AtomicUsize::new(0)),
            samples_written: Arc::new(AtomicUsize::new(0)),
            raw_data_size: Arc::new(AtomicUsize::new(0)),
            data_age: Arc::new(AtomicUsize::new(0)),
            unflushed_count: UNFLUSHED_COUNT.clone(),
            bytes_flushed: TOTAL_BYTES_FLUSHED.clone(),
            start_gate: Arc::new(Barrier::new(2)),
            buffer_queue: QUEUE_LEN.clone(),
            flusher_queue: FLUSHER_QUEUE_LEN.clone(),
        };
        r
    }

    pub fn start_watcher(&self) {
        self.start_gate.wait();
    }

    pub fn init_watcher(&self, interval: Duration) {
        let b = self.start_gate.clone();
        thread::spawn(move || {
            watcher(b, interval);
        });
    }

    pub fn init_kafka_consumer(&self, kafka_bootstraps: &'static str, kafka_topic: &'static str) {
        thread::spawn(move || {
            init_kafka_consumer(kafka_bootstraps, kafka_topic);
        });
    }

    pub fn init_mach_querier(&self, series_id: SeriesId) {
        thread::spawn(move || {
            init_mach_querier(series_id);
        });
    }
}

fn watcher(start_gate: Arc<Barrier>, interval: Duration) {
    start_gate.wait();
    let mut last_pushed = 0;
    let mut last_dropped = 0;
    loop {
        let pushed = COUNTERS.samples_enqueued.load(SeqCst);
        let dropped = COUNTERS.samples_dropped.load(SeqCst);
        let _written = COUNTERS.samples_written.load(SeqCst);
        let raw_data_size = COUNTERS.raw_data_size.load(SeqCst);

        let pushed_since = pushed - last_pushed;
        let dropped_since = dropped - last_dropped;
        let total_since = pushed_since + dropped_since;
        let completeness: f64 = {
            let pushed: f64 = (pushed_since as u32).try_into().unwrap();
            let total: f64 = (total_since as u32).try_into().unwrap();
            pushed / total
        };
        let rate: f64 = {
            let pushed: f64 = (pushed_since as u32).try_into().unwrap();
            let secs = interval.as_secs_f64();
            pushed / secs
        };

        let data_age = COUNTERS.data_age.load(SeqCst) as u64;
        let delay = Duration::from_micros(data_age);

        last_pushed = pushed;
        last_dropped = dropped;

        let bytes_flushed = COUNTERS.bytes_flushed.load(SeqCst);
        let _unflushed_count = COUNTERS.unflushed_count.load(SeqCst);
        let buffer_queue = COUNTERS.buffer_queue.load(SeqCst);
        let flusher_queue = COUNTERS.flusher_queue.load(SeqCst);

        //println!("Completeness: {}, Buffer Queue: {}, Unflushed: {}, Throughput: {}, Data age (seconds): {:?}, Raw data size: {} bytes, Data flushed: {} bytes", completeness, buffer_queue, unflushed_count, rate, delay.as_secs_f64(), raw_data_size, bytes_flushed);
        println!("Completeness: {}, Buffer Queue: {}, Flusher Queue: {}, Throughput: {}, Raw data size: {}, Data flushed: {}, Data age: {:?}", completeness, buffer_queue, flusher_queue, rate, raw_data_size, bytes_flushed, delay);
        thread::sleep(interval);
    }
}

pub struct Writer<I> {
    sender: Sender<Vec<Sample<'static, I>>>,
    barrier: Arc<Barrier>,
}

impl<I> Writer<I> {
    pub fn done(self) {
        drop(self.sender);
        self.barrier.wait();
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Workload {
    samples_per_second: f64,
    duration: Duration,
    //sample_interval: Duration,
    batch_interval: Duration,
    batch_size: usize,
}

impl Workload {
    pub fn new(samples_per_second: f64, duration: Duration, batch_size: u32) -> Self {
        let sample_interval = Duration::from_secs_f64(1.0 / samples_per_second);
        let batch_interval = sample_interval * batch_size;
        Self {
            samples_per_second,
            duration,
            batch_interval,
            batch_size: batch_size as usize,
        }
    }

    pub fn run<I: Copy>(&self, writer: &Writer<I>, samples: &'static [(I, u64, Vec<SampleType>)]) {
        println!("Running rate: {}", self.samples_per_second);
        let start = Instant::now();
        let mut last_batch = start;
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut produced_samples = 0u32;
        'outer: loop {
            for item in samples {
                let timestamp: u64 = timestamp_now_micros();
                batch.push((item.0, timestamp, item.2.as_slice()));
                if batch.len() == self.batch_size {
                    while last_batch.elapsed() < self.batch_interval {}
                    match writer.sender.try_send(batch) {
                        Ok(_) => {
                            COUNTERS.samples_enqueued.fetch_add(self.batch_size, SeqCst);
                        }
                        Err(_) => {
                            COUNTERS.samples_dropped.fetch_add(self.batch_size, SeqCst);
                        }
                    };
                    produced_samples += self.batch_size as u32;
                    batch = Vec::with_capacity(self.batch_size);
                    last_batch = std::time::Instant::now();
                    if start.elapsed() > self.duration {
                        break 'outer;
                    }
                }
            }
        }
        let produce_duration = start.elapsed().as_secs_f64();
        let produced_samples: f64 = produced_samples.try_into().unwrap();
        println!(
            "Expected rate: {}, measured rate: {}",
            self.samples_per_second,
            produced_samples / produce_duration
        );
    }
}
