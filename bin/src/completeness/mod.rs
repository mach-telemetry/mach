pub mod kafka;
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
    mem_list::{PENDING_UNFLUSHED_BLOCKLISTS, PENDING_UNFLUSHED_BLOCKS, TOTAL_BYTES_FLUSHED},
    sample::SampleType,
    writer::WRITER_FLUSH_QUEUE,
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
    unflushed_blocklists: Arc<AtomicUsize>,
    bytes_flushed: Arc<AtomicUsize>,
    raw_data_size: Arc<AtomicUsize>,
    data_age: Arc<AtomicUsize>,
    start_gate: Arc<Barrier>,
    writer_flush_queue: Arc<AtomicUsize>,
    unflushed_blocks: Arc<AtomicUsize>,
}

impl Counters {
    pub fn new() -> Self {
        let r = Self {
            samples_enqueued: Arc::new(AtomicUsize::new(0)),
            samples_dropped: Arc::new(AtomicUsize::new(0)),
            samples_written: Arc::new(AtomicUsize::new(0)),
            raw_data_size: Arc::new(AtomicUsize::new(0)),
            data_age: Arc::new(AtomicUsize::new(0)),
            unflushed_blocklists: PENDING_UNFLUSHED_BLOCKLISTS.clone(),
            bytes_flushed: TOTAL_BYTES_FLUSHED.clone(),
            start_gate: Arc::new(Barrier::new(2)),
            writer_flush_queue: WRITER_FLUSH_QUEUE.clone(),
            unflushed_blocks: PENDING_UNFLUSHED_BLOCKS.clone(),
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
        let _unflushed_blocklists = COUNTERS.unflushed_blocklists.load(SeqCst);
        let writer_flush_queue = COUNTERS.writer_flush_queue.load(SeqCst);
        let unflushed_blocks = COUNTERS.unflushed_blocks.load(SeqCst);

        //println!("Completeness: {}, Buffer Queue: {}, Unflushed: {}, Throughput: {}, Data age (seconds): {:?}, Raw data size: {} bytes, Data flushed: {} bytes", completeness, buffer_queue, unflushed_count, rate, delay.as_secs_f64(), raw_data_size, bytes_flushed);
        println!("Completeness: {}, Writer Flush Queue: {}, Unflushed Blocks: {}, Throughput: {}, Raw data size: {}, Data flushed: {}, Data age: {:?}", completeness, writer_flush_queue, unflushed_blocks, rate, raw_data_size, bytes_flushed, delay);
        thread::sleep(interval);
    }
}

type SampleBatch<I> = (u64, u64, Vec<Sample<'static, I>>);

pub struct WriterGroup<I> {
    senders: Vec<Sender<SampleBatch<I>>>,
    barrier: Arc<Barrier>,
}

impl<I> WriterGroup<I> {
    pub fn done(self) {
        drop(self.senders);
        self.barrier.wait();
    }

    pub fn num_writers(&self) -> usize {
        self.senders.len()
    }
}

pub enum BatchStrategy {
    BatchByWriter,
    BatchBySourceId(u64),
}

struct RunWorkloadResult {
    duration: Duration,
    num_samples_produced: u32,
}

impl RunWorkloadResult {
    fn to_samples_per_sec(&self) -> f64 {
        f64::from(self.num_samples_produced) / self.duration.as_secs_f64()
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Workload {
    samples_per_second: f64,
    duration: Duration,
    batch_interval: Duration,
    sample_interval: Duration,
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
            sample_interval,
            batch_size: batch_size as usize,
        }
    }

    pub fn run<I: Copy + Into<usize>>(
        &self,
        writer: &WriterGroup<I>,
        samples: &'static [(I, &[SampleType])],
        batch_strategy: BatchStrategy,
    ) {
        println!("Running rate: {}", self.samples_per_second);

        let result = self._run(writer, samples, batch_strategy);

        println!(
            "Expected rate: {}, measured rate: {}",
            self.samples_per_second,
            result.to_samples_per_sec()
        );
    }

    fn _run<I: Copy + Into<usize>>(
        &self,
        writer: &WriterGroup<I>,
        samples: &'static [(I, &[SampleType])],
        batch_strategy: BatchStrategy,
    ) -> RunWorkloadResult {
        let num_batches = match batch_strategy {
            BatchStrategy::BatchByWriter => writer.num_writers(),
            BatchStrategy::BatchBySourceId(num_sources) => num_sources as usize,
        };

        let mut batches: Vec<Vec<(I, u64, &[SampleType])>> = (0..num_batches)
            .map(|_| Vec::with_capacity(self.batch_size))
            .collect();

        let mut produced_samples = 0u32;
        let mut batched_samples = 0;

        let start = Instant::now();
        let mut batch_start = start;

        'outer: loop {
            for item in samples {
                let batch_idx: usize = item.0.into() % batches.len();
                let batch = &mut batches[batch_idx];
                let timestamp: u64 = timestamp_now_micros();

                batch.push((item.0, timestamp, item.1));
                batched_samples += 1;

                if batched_samples == self.batch_size {
                    while batch_start.elapsed() < self.batch_interval {}
                    batch_start = std::time::Instant::now();
                    batched_samples = 0;
                }

                if batch.len() == self.batch_size {
                    let batch = std::mem::replace(
                        &mut batches[batch_idx],
                        Vec::with_capacity(self.batch_size),
                    );

                    let to_send = (batch[0].1, batch.last().unwrap().1, batch);

                    let selected_writer = &writer.senders[item.0.into() % writer.num_writers()];
                    match selected_writer.try_send(to_send) {
                        Ok(_) => {
                            COUNTERS.samples_enqueued.fetch_add(self.batch_size, SeqCst);
                        }
                        Err(_) => {
                            COUNTERS.samples_dropped.fetch_add(self.batch_size, SeqCst);
                        }
                    };

                    produced_samples += self.batch_size as u32;
                    if start.elapsed() > self.duration {
                        break 'outer;
                    }
                }
            }
        }

        RunWorkloadResult {
            duration: start.elapsed(),
            num_samples_produced: produced_samples,
        }
    }
}
