mod prep_data;

use clap::*;
use lazy_static::lazy_static;
use mach::{
    id::{SeriesId, SeriesRef},
    sample::SampleType,
    utils::kafka,
    tsdb::Mach,
    writer::{Writer as MachWriter, WriterConfig},
};
use std::{
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    sync::{Arc, Barrier, Mutex, atomic::{AtomicUsize, Ordering::SeqCst}},
    thread,
};
use lzzzz::lz4;

use crossbeam::channel::{bounded, Sender, Receiver};

lazy_static! {
    static ref ARGS: Args = Args::parse();
    static ref SAMPLES: Vec<prep_data::Sample> = prep_data::load_samples(ARGS.file_path.as_str());
    static ref MACH_SAMPLES: Vec<prep_data::RegisteredSample> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writer = MACH_WRITER.clone(); // ensure WRITER is initialized (prevent deadlock)
        let mut mach_guard = mach.lock().unwrap();
        let mut writer_guard = writer.lock().unwrap();
        let samples = SAMPLES.as_slice();
        prep_data::mach_register_samples(samples, &mut *mach_guard, &mut *writer_guard)
    };
    static ref MACH: Arc<Mutex<Mach>> = Arc::new(Mutex::new(Mach::new()));
    static ref MACH_WRITER: Arc<Mutex<MachWriter>> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writer_config = WriterConfig {
            active_block_flush_sz: 1_000_000,
        };
        let mut guard = mach.lock().unwrap();
        Arc::new(Mutex::new(guard.add_writer(writer_config).unwrap()))
    };
}

#[derive(Parser, Debug, Clone)]
struct Args {

    #[clap(short, long, default_value_t = String::from("kafka"))]
    tsdb: String,

    #[clap(short, long, default_value_t = 4)]
    kafka_writers: usize,

    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka_bootstraps: String,

    #[clap(short, long, default_value_t = 3)]
    kafka_partitions: usize,

    #[clap(short, long, default_value_t = 3)]
    kafka_replicas: usize,

    #[clap(short, long, default_value_t = String::from("kafka-completeness-bench"))]
    kafka_topic: String,

    #[clap(short, long, default_value_t = 100000)]
    batch_size: u32,

    #[clap(short, long, default_value_t = String::from("/home/sli/data/train-ticket-data"))]
    file_path: String,

    #[clap(short, long, default_value_t = 5.0)]
    counter_interval_seconds: f64
}


struct Counters {
    samples_pushed: Arc<AtomicUsize>,
    samples_dropped: Arc<AtomicUsize>,
    last_timestamp: u64,
    start_gate: Arc<Barrier>,
}

impl Counters {
    fn new() -> Self {
        let r = Self {
            samples_pushed: Arc::new(AtomicUsize::new(0)),
            samples_dropped: Arc::new(AtomicUsize::new(0)),
            last_timestamp: micros_from_epoch().try_into().unwrap(),
            start_gate: Arc::new(Barrier::new(2)),
        };

        let p = r.samples_pushed.clone();
        let d = r.samples_dropped.clone();
        let b = r.start_gate.clone();
        thread::spawn(move || {
            completeness_watcher(p, d, b);
        });

        r
    }

    fn init(&self) {
        self.start_gate.wait();
    }
}

fn completeness_watcher(pushed: Arc<AtomicUsize>, dropped: Arc<AtomicUsize>, start_gate: Arc<Barrier>) {
    const look_back: usize = 5;
    let interval = Duration::from_secs_f64(ARGS.counter_interval_seconds);
    let look_back_f64: f64 = (look_back as u32).try_into().unwrap();
    start_gate.wait();
    //let mut look_back_array: [f64; look_back] = [0.; look_back];
    let mut last_pushed = 0;
    let mut last_dropped = 0;
    let mut counter = 0;
    loop {
        let pushed = pushed.load(SeqCst);
        let dropped = dropped.load(SeqCst);

        let pushed_since = pushed - last_pushed;
        let dropped_since = dropped - last_dropped;
        let total_since = pushed_since + dropped_since;
        let completeness: f64 = {
            let pushed: f64 = (pushed_since as u32).try_into().unwrap();
            let total: f64 = (total_since as u32).try_into().unwrap();
            pushed / total
        };
        //look_back_array[counter % look_back] = completeness;
        //let sum = look_back_array.iter().sum::<f64>();
        //let mean_completeness: f64 = sum / look_back_f64;

        last_pushed = pushed;
        last_dropped = dropped;
        //counter += 1;

        println!("Completeness: {}", completeness);
        thread::sleep(interval);
    }
}

type Sample<I> = (I, u64, &'static [SampleType]);
//type MachSample = (SeriesRef, u64, &'static [SampleType]);

struct Writer<I> {
    sender: Sender<Vec<Sample<I>>>,
    barrier: Arc<Barrier>,
}

impl<I> Writer<I> {
    fn done(self) {
        drop(self.sender);
        self.barrier.wait();
    }
}

fn kafka_writer(barrier: Arc<Barrier>, receiver: Receiver<Vec<Sample<SeriesId>>>) {
    let topic = ARGS.kafka_topic.clone();
    let mut producer = kafka::Producer::new(&ARGS.kafka_bootstraps);
    while let Ok(data) = receiver.recv() {
        let bytes = bincode::serialize(&data).unwrap();
        let mut compressed = Vec::new();
        lz4::compress_to_vec(
            bytes.as_slice(),
            &mut compressed,
            lz4::ACC_LEVEL_DEFAULT,
        ).unwrap();
        producer.send(topic.as_str(), 0, compressed.as_slice());
    }
    barrier.wait();
}

fn init_kafka() -> Writer<SeriesId> {
    kafka::make_topic(&ARGS.kafka_bootstraps, &ARGS.kafka_topic);
    let (tx, rx) = bounded(1);
    let barrier = Arc::new(Barrier::new(ARGS.kafka_writers + 1));

    for _ in 0..ARGS.kafka_writers {
        let barrier = barrier.clone();
        let rx = rx.clone();
        thread::spawn(move || {
            kafka_writer(barrier, rx);
        });
    }

    Writer {
        sender: tx,
        barrier,
    }
}

fn mach_writer(barrier: Arc<Barrier>, receiver: Receiver<Vec<Sample<SeriesRef>>>) {
    let mut writer_guard = MACH_WRITER.lock().unwrap();
    let writer = &mut *writer_guard;
    while let Ok(data) = receiver.recv() {
        for item in data.iter() {
            'push_loop: loop {
                if writer.push(item.0, item.1, item.2).is_ok() {
                    break 'push_loop;
                }
            }
        }
    }
    barrier.wait();
}

fn init_mach() -> Writer<SeriesRef> {
    let (tx, rx) = bounded(1);
    let barrier = Arc::new(Barrier::new(2));

    {
        let barrier = barrier.clone();
        let rx = rx.clone();
        thread::spawn(move || {
            mach_writer(barrier, rx);
        });
    }

    Writer {
        sender: tx,
        barrier,
    }
}


fn micros_from_epoch() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros()
}

#[derive(Debug, Copy, Clone)]
struct Workload {
    samples_per_second: f64,
    duration: Duration,
    sample_interval: Duration,
    batch_interval: Duration,
    batch_size: usize
}

impl Workload {
    fn new(samples_per_second: f64, duration: Duration) -> Self {
        let sample_interval = Duration::from_secs_f64(1.0 / samples_per_second);
        let batch_interval = sample_interval * ARGS.batch_size;
        Self {
            samples_per_second,
            duration,
            sample_interval,
            batch_interval,
            batch_size: ARGS.batch_size as usize,
        }
    }

    fn run<I: Copy>(&self, writer: &Writer<I>, counters: &mut Counters, samples: &'static[(I, u64, Vec<SampleType>)]) {
        println!("Running rate: {}", self.samples_per_second);
        let start = Instant::now();
        let mut last_batch = start;
        let mut batch = Vec::with_capacity(self.batch_size);
        let interval_increment: u64 = self.sample_interval.as_micros().try_into().unwrap();
        let mut timestamp = micros_from_epoch().try_into().unwrap();
        let mut produced_samples = 0u32;
        'outer: loop {
            for item in samples {
                timestamp += interval_increment;
                batch.push((item.0, timestamp, item.2.as_slice()));
                if batch.len() == self.batch_size {
                    while last_batch.elapsed() < self.batch_interval {}
                    match writer.sender.try_send(batch) {
                        Ok(_) => counters.samples_pushed.fetch_add(self.batch_size, SeqCst),
                        Err(_) => counters.samples_dropped.fetch_add(self.batch_size, SeqCst),
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
        println!("Expected rate: {}, measured rate: {}", self.samples_per_second, produced_samples / produce_duration);
        counters.last_timestamp = timestamp;
    }
}

fn main() {
    let mut counters = Counters::new();
    let workloads = &[
        Workload::new(500_000., Duration::from_secs(60)),
        Workload::new(600_000., Duration::from_secs(60)),
        Workload::new(700_000., Duration::from_secs(60)),
        Workload::new(800_000., Duration::from_secs(60)),
        Workload::new(900_000., Duration::from_secs(60)),
        Workload::new(1_000_000., Duration::from_secs(60)),
        Workload::new(1_100_000., Duration::from_secs(60)),
        Workload::new(1_200_000., Duration::from_secs(60)),
        Workload::new(1_300_000., Duration::from_secs(60)),
        Workload::new(1_400_000., Duration::from_secs(60)),
        Workload::new(1_500_000., Duration::from_secs(60)),
        //Workload::new(1_600_000., Duration::from_secs(60)),
        //Workload::new(1_700_000., Duration::from_secs(60)),
        //Workload::new(1_800_000., Duration::from_secs(60)),
        //Workload::new(1_900_000., Duration::from_secs(60)),
        //Workload::new(2_000_000., Duration::from_secs(60)),
    ];
    match ARGS.tsdb.as_str() {
        "kafka" => {
            let samples = SAMPLES.as_slice();
            let kafka = init_kafka();
            counters.init();
            for workload in workloads {
                workload.run(&kafka, &mut counters, samples);
            }
            kafka.done();
        },
        "mach" => {
            let samples = MACH_SAMPLES.as_slice();
            let mach = init_mach();
            counters.init();
            for workload in workloads {
                workload.run(&mach, &mut counters, samples);
            }
            mach.done();
        },
        _ => panic!(),
    }
}
