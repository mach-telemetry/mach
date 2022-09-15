pub mod kafka;
pub mod mach;

mod mach_extern {
    pub use mach::*;
}

use crate::completeness::{kafka::init_kafka_consumer, mach::init_mach_querier};
use crate::utils::timestamp_now_micros;
use crossbeam_channel::Sender;
use lazy_static::lazy_static;
use lzzzz::lz4;
use mach_extern::{
    id::SeriesId,
    mem_list::{PENDING_UNFLUSHED_BLOCKLISTS, PENDING_UNFLUSHED_BLOCKS, TOTAL_BYTES_FLUSHED},
    sample::SampleType,
    writer::WRITER_FLUSH_QUEUE,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::mem::{size_of, size_of_val};
use std::sync::atomic::AtomicU64;
use std::sync::{atomic::AtomicUsize, atomic::Ordering::SeqCst, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

lazy_static! {
    pub static ref COUNTERS: Counters = Counters::new();
}

pub type SampleRef<I> = (I, u64, &'static [SampleType]);
pub type SampleOwned<I> = (I, u64, Vec<SampleType>);

pub struct Counters {
    samples_enqueued: Arc<AtomicUsize>,
    samples_dropped: Arc<AtomicUsize>,
    samples_written: Arc<AtomicUsize>,
    unflushed_blocklists: Arc<AtomicUsize>,
    bytes_flushed: Arc<AtomicUsize>,
    raw_data_size: Arc<AtomicUsize>,
    data_age: Arc<AtomicU64>,
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
            data_age: Arc::new(AtomicU64::new(0)),
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

        let data_age = COUNTERS.data_age.load(SeqCst);
        let delay = Duration::from_micros(data_age);

        last_pushed = pushed;
        last_dropped = dropped;

        let bytes_flushed = COUNTERS.bytes_flushed.load(SeqCst);
        let _unflushed_blocklists = COUNTERS.unflushed_blocklists.load(SeqCst);
        let writer_flush_queue = COUNTERS.writer_flush_queue.load(SeqCst);
        let unflushed_blocks = COUNTERS.unflushed_blocks.load(SeqCst);

        println!("Completeness: {}, Writer Flush Queue: {}, Unflushed Blocks: {}, Throughput: {}, Raw data size: {}, Bytes flushed: {}, Data age: {:?}", completeness, writer_flush_queue, unflushed_blocks, rate, raw_data_size, bytes_flushed, delay);
        thread::sleep(interval);
    }
}

pub struct Writer<B: Batch> {
    sender: Sender<B>,
    barrier: Arc<Barrier>,
}

impl<B: Batch> Writer<B> {
    fn done(self) {
        drop(self.sender);
        self.barrier.wait();
    }
}

pub struct WriterGroup<B: Batch> {
    senders: Vec<Sender<B>>,
    barrier: Arc<Barrier>,
}

impl<B: Batch> WriterGroup<B> {
    fn done(self) {
        drop(self.senders);
        self.barrier.wait();
    }

    pub fn num_writers(&self) -> usize {
        self.senders.len()
    }
}

pub trait Compress {
    fn compress(&self) -> Vec<u8>;
}

pub trait Decompress {
    type DecompressedType;

    fn decompress(compressed: &[u8], decompression_buffer: &mut [u8]) -> Self::DecompressedType;
}

pub trait Batch: Compress {
    fn len(&self) -> usize;
}

pub struct SingleSourceBatch<I: Copy + Serialize + DeserializeOwned + Into<usize>> {
    pub start: u64,
    pub end: u64,
    pub source_id: I,
    pub data: Vec<(u64, &'static [SampleType])>,
}

pub struct SingleSourceOwnedBatch {
    pub start: u64,
    pub end: u64,
    pub source_id: usize,
    pub data: Vec<(u64, Vec<SampleType>)>,
}

impl<I: Copy + Serialize + DeserializeOwned + Into<usize>> SingleSourceBatch<I> {
    fn new(start: u64, end: u64, source_id: I, data: Vec<(u64, &'static [SampleType])>) -> Self {
        Self {
            start,
            end,
            source_id,
            data,
        }
    }

    pub fn peek_source_id(msg: &[u8]) -> usize {
        let source_id = usize::from_be_bytes(msg[16..24].try_into().unwrap());
        source_id
    }
}

impl<I: Copy + Serialize + DeserializeOwned + Into<usize>> Batch for SingleSourceBatch<I> {
    fn len(&self) -> usize {
        self.data.len()
    }
}

impl<I: Copy + Serialize + DeserializeOwned + Into<usize>> Compress for SingleSourceBatch<I> {
    fn compress(&self) -> Vec<u8> {
        let mut compressed: Vec<u8> = Vec::new();
        let source_id: usize = self.source_id.into();

        compressed.extend_from_slice(&self.start.to_be_bytes()[..]);
        compressed.extend_from_slice(&self.end.to_be_bytes()[..]);
        compressed.extend_from_slice(&source_id.to_be_bytes()[..]);

        let bytes = bincode::serialize(&self.data).unwrap();
        lz4::compress_to_vec(bytes.as_slice(), &mut compressed, lz4::ACC_LEVEL_DEFAULT).unwrap();
        compressed.extend_from_slice(&bytes.len().to_be_bytes()[..]); // size of uncompressed bytes

        compressed
    }
}

impl<I: Copy + Serialize + DeserializeOwned + Into<usize>> Decompress for SingleSourceBatch<I> {
    type DecompressedType = SingleSourceOwnedBatch;

    fn decompress(msg: &[u8], buffer: &mut [u8]) -> Self::DecompressedType {
        let start = u64::from_be_bytes(msg[0..8].try_into().unwrap());
        let end = u64::from_be_bytes(msg[8..16].try_into().unwrap());
        let source_id = Self::peek_source_id(msg);

        let original_sz = usize::from_be_bytes(msg[msg.len() - 8..msg.len()].try_into().unwrap());
        let sz = lz4::decompress(&msg[24..msg.len() - 8], &mut buffer[..original_sz]).unwrap();
        let data: Vec<(u64, Vec<SampleType>)> = bincode::deserialize(&buffer[..sz]).unwrap();

        SingleSourceOwnedBatch {
            start,
            end,
            source_id,
            data,
        }
    }
}

pub struct MultiSourceBatch<I: Serialize + DeserializeOwned> {
    pub start: u64,
    pub end: u64,
    pub data: Vec<SampleRef<I>>,
}

pub struct MultiSourceOwnedBatch<I: Serialize + DeserializeOwned> {
    pub start: u64,
    pub end: u64,
    pub data: Vec<SampleOwned<I>>,
}

impl<I: Serialize + DeserializeOwned> MultiSourceBatch<I> {
    fn new(start: u64, end: u64, data: Vec<(I, u64, &'static [SampleType])>) -> Self {
        Self { start, end, data }
    }
}

impl<I: Serialize + DeserializeOwned> Batch for MultiSourceBatch<I> {
    fn len(&self) -> usize {
        self.data.len()
    }
}

impl<I: Serialize + DeserializeOwned> Compress for MultiSourceBatch<I> {
    fn compress(&self) -> Vec<u8> {
        let mut compressed: Vec<u8> = Vec::new();

        compressed.extend_from_slice(&self.start.to_be_bytes()[..]);
        compressed.extend_from_slice(&self.end.to_be_bytes()[..]);

        let bytes = bincode::serialize(&self.data).unwrap();
        lz4::compress_to_vec(bytes.as_slice(), &mut compressed, lz4::ACC_LEVEL_DEFAULT).unwrap();
        compressed.extend_from_slice(&bytes.len().to_be_bytes()[..]); // size of uncompressed bytes

        compressed
    }
}

impl<I: Serialize + DeserializeOwned> Decompress for MultiSourceBatch<I> {
    type DecompressedType = MultiSourceOwnedBatch<I>;

    fn decompress(msg: &[u8], buffer: &mut [u8]) -> Self::DecompressedType {
        let start = u64::from_be_bytes(msg[0..8].try_into().unwrap());
        let end = u64::from_be_bytes(msg[8..16].try_into().unwrap());

        let original_sz = usize::from_be_bytes(msg[msg.len() - 8..msg.len()].try_into().unwrap());
        let sz = lz4::decompress(&msg[16..msg.len() - 8], &mut buffer[..original_sz]).unwrap();
        let data: Vec<(I, u64, Vec<SampleType>)> = bincode::deserialize(&buffer[..sz]).unwrap();

        MultiSourceOwnedBatch { start, end, data }
    }
}

pub enum BatchThreshold {
    SampleCount(usize),
    Bytes(usize),
}

pub struct RunWorkloadOutput {
    duration: Duration,
    num_samples_sent: u32,
}

impl RunWorkloadOutput {
    fn samples_per_sec(&self) -> f64 {
        f64::from(self.num_samples_sent) / self.duration.as_secs_f64()
    }
}

pub trait RunWorkload<S> {
    fn run(&self, workload: &Workload, samples: &'static [S]) -> RunWorkloadOutput;
    fn done(self);
}

pub struct MultiWriterBatchProducer<B: Batch> {
    writers: WriterGroup<B>,
    batch_size: BatchThreshold,
}

impl<B: Batch> MultiWriterBatchProducer<B> {
    pub fn new(writers: WriterGroup<B>, batch_size: BatchThreshold) -> Self {
        Self {
            writers,
            batch_size,
        }
    }
}

impl<I> RunWorkload<(I, &'static [SampleType])> for MultiWriterBatchProducer<MultiSourceBatch<I>>
where
    I: Copy + Into<usize> + Serialize + DeserializeOwned,
{
    fn run(
        &self,
        workload: &Workload,
        samples: &'static [(I, &[SampleType])],
    ) -> RunWorkloadOutput {
        let batch_bytes = match self.batch_size {
            BatchThreshold::SampleCount(_) => unimplemented!("not supported"),
            BatchThreshold::Bytes(batch_bytes) => batch_bytes,
        };

        let mut batches: Vec<Vec<(I, u64, &[SampleType])>> = (0..self.writers.num_writers())
            .map(|_| Vec::new())
            .collect();
        let mut batch_size_bytes: Vec<usize> = batches.iter().map(|_| 0).collect();

        let production_interval = Duration::from_millis(50);
        let production_sz = (workload.samples_per_second / 20.0) as u64;

        let mut produced_samples = 0u32;
        let mut batched_samples = 0;

        let start = Instant::now();
        let mut batch_start = start;

        'outer: loop {
            for item in samples {
                let batch_idx: usize = item.0.into() % batches.len();
                let batch = &mut batches[batch_idx];
                let timestamp: u64 = timestamp_now_micros();

                let sample_size = size_of::<u64>() + size_of::<I>() + size_of_val(item.1);
                batch_size_bytes[batch_idx] += sample_size;
                batch.push((item.0, timestamp, item.1));
                batched_samples += 1;

                if batched_samples == production_sz {
                    while batch_start.elapsed() < production_interval {}
                    batch_start = std::time::Instant::now();
                    batched_samples = 0;
                }

                if batch_size_bytes[batch_idx] >= batch_bytes {
                    let batch_size = batches[batch_idx].len();

                    batch_size_bytes[batch_idx] = 0;
                    let batch = std::mem::replace(
                        &mut batches[batch_idx],
                        Vec::with_capacity(batch_size * 2),
                    );

                    let (start_ts, end_ts) = (batch[0].1, batch.last().unwrap().1);
                    let batch = MultiSourceBatch::new(start_ts, end_ts, batch);

                    match self.writers.senders[batch_idx].try_send(batch) {
                        Ok(_) => {
                            COUNTERS.samples_enqueued.fetch_add(batch_size, SeqCst);
                        }
                        Err(_) => {
                            COUNTERS.samples_dropped.fetch_add(batch_size, SeqCst);
                        }
                    };

                    produced_samples += batch_size as u32;
                    if start.elapsed() > workload.duration {
                        break 'outer;
                    }
                }
            }
        }

        RunWorkloadOutput {
            duration: start.elapsed(),
            num_samples_sent: produced_samples,
        }
    }

    fn done(self) {
        self.writers.done();
    }
}

pub struct BatchProducer<B: Batch> {
    writer: Writer<B>,
    batch_size: BatchThreshold,
}

impl<B: Batch> BatchProducer<B> {
    pub fn new(writer: Writer<B>, batch_size: BatchThreshold) -> Self {
        Self { writer, batch_size }
    }
}

impl<I> RunWorkload<(I, &'static [SampleType])> for BatchProducer<MultiSourceBatch<I>>
where
    I: Copy + Serialize + DeserializeOwned,
{
    fn run(
        &self,
        workload: &Workload,
        samples: &'static [(I, &[SampleType])],
    ) -> RunWorkloadOutput {
        let batch_size = match self.batch_size {
            BatchThreshold::SampleCount(num_samples) => num_samples,
            BatchThreshold::Bytes(_) => unimplemented!("not supported"),
        };

        let batch_interval = workload.sample_interval * batch_size.try_into().unwrap();

        let start = Instant::now();
        let mut last_batch = start;
        let mut batch = Vec::with_capacity(batch_size);
        let mut produced_samples = 0u32;
        'outer: loop {
            for item in samples {
                let timestamp: u64 = timestamp_now_micros();
                batch.push((item.0, timestamp, item.1));
                if batch.len() == batch_size {
                    while last_batch.elapsed() < batch_interval {}

                    let (start_ts, end_ts) = (batch[0].1, batch.last().unwrap().1);
                    let batch_to_send = MultiSourceBatch::new(start_ts, end_ts, batch);

                    match self.writer.sender.try_send(batch_to_send) {
                        Ok(_) => {
                            COUNTERS.samples_enqueued.fetch_add(batch_size, SeqCst);
                        }
                        Err(_) => {
                            COUNTERS.samples_dropped.fetch_add(batch_size, SeqCst);
                        }
                    };
                    produced_samples += batch_size as u32;
                    batch = Vec::with_capacity(batch_size);
                    last_batch = std::time::Instant::now();
                    if start.elapsed() > workload.duration {
                        break 'outer;
                    }
                }
            }
        }

        RunWorkloadOutput {
            duration: start.elapsed(),
            num_samples_sent: produced_samples,
        }
    }

    fn done(self) {
        self.writer.done();
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Workload {
    samples_per_second: f64,
    duration: Duration,
    sample_interval: Duration,
}

impl Workload {
    pub fn new(samples_per_second: f64, duration: Duration) -> Self {
        let sample_interval = Duration::from_secs_f64(1.0 / samples_per_second);
        Self {
            samples_per_second,
            sample_interval,
            duration,
        }
    }

    pub fn run<I, R: RunWorkload<(I, &'static [SampleType])>>(
        &self,
        runner: &R,
        samples: &'static [(I, &[SampleType])],
    ) {
        println!("Running rate: {}", self.samples_per_second);

        let result = runner.run(self, samples);

        println!(
            "Expected rate: {}, measured rate: {}",
            self.samples_per_second,
            result.samples_per_sec(),
        );
    }
}
