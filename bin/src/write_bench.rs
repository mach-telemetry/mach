mod kafka_utils;
mod prep_data;
mod utils;

use clap::Parser;
use crossbeam_channel::bounded;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use lzzzz::lz4;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread::JoinHandle;

use mach::{
    compression::{CompressFn, Compression},
    id::{SeriesId, SeriesRef},
    sample::SampleType,
    series::{FieldType, SeriesConfig},
    tsdb::Mach,
    utils::random_id,
    writer::{Writer, WriterConfig},
};

use crate::kafka_utils::{make_topic, KafkaTopicOptions};
use crate::prep_data::load_samples;
use crate::utils::timestamp_now_micros;

const GB_IN_BYTES: u64 = 1_073_741_824;

type TimeStamp = u64;
type Sample = (SeriesId, TimeStamp, Vec<SampleType>);
type RegisteredSample = (SeriesRef, TimeStamp, Vec<SampleType>);

fn register_samples(
    samples: Vec<Sample>,
    mach: &mut Mach,
    writer: &mut Writer,
) -> Vec<RegisteredSample> {
    let mut refmap: HashMap<SeriesId, SeriesRef> = HashMap::new();

    for (id, _, values) in samples.iter() {
        refmap.entry(*id).or_insert_with(|| {
            let conf = get_series_config(*id, values.as_slice());
            mach.add_series(conf).unwrap();
            let id_ref = writer.get_reference(*id);
            id_ref
        });
    }

    let mut registered_samples = Vec::new();
    for (series_id, ts, values) in samples {
        let series_ref = *refmap.get(&series_id).unwrap();
        registered_samples.push((series_ref, ts, values));
    }

    registered_samples
}

fn new_writer(mach: &mut Mach, kafka_bootstraps: String) -> Writer {
    let writer_config = WriterConfig {
        active_block_flush_sz: 1_000_000,
    };

    mach.add_writer(writer_config.clone()).unwrap()
}

fn kafka_writer(recv: Receiver<Vec<Sample>>, mut producer: kafka_utils::Producer, topic: String) {
    while let Ok(sample_batch) = recv.recv() {
        let serialized_samples = bincode::serialize(&sample_batch).unwrap();

        let mut compressed_samples = Vec::new();
        lz4::compress_to_vec(
            serialized_samples.as_slice(),
            &mut compressed_samples,
            lz4::ACC_LEVEL_DEFAULT,
        )
        .unwrap();

        producer.send(topic.as_str(), 0, compressed_samples.as_slice());
    }
}

struct BatchedDataProducer<'a> {
    num_samples_sent: usize,
    num_batches_sent: usize,
    bytes_sent: u64,
    data: &'a Vec<Sample>,
}

impl<'a> BatchedDataProducer<'a> {
    pub fn new(data: &'a Vec<Sample>) -> Self {
        Self {
            num_samples_sent: 0,
            num_batches_sent: 0,
            bytes_sent: 0,
            data,
        }
    }

    pub fn run(
        &mut self,
        total_bytes_target: u64,
        batch_size_target: u64,
        sender: Sender<Vec<Sample>>,
    ) {
        self.reset_metrics();

        let mut batch_size_bytes = 0;
        let mut batch = Vec::new();
        'outer: loop {
            for s in self.data {
                let mut sample = s.clone();
                sample.1 = timestamp_now_micros();

                let sample_size = bincode::serialized_size(&sample).unwrap();
                batch_size_bytes += sample_size;
                self.bytes_sent += sample_size;

                batch.push(sample);

                if batch_size_bytes >= batch_size_target {
                    batch_size_bytes = 0;
                    self.num_samples_sent += batch.len();
                    self.num_batches_sent += 1;
                    sender.send(batch).unwrap();
                    if self.bytes_sent >= total_bytes_target {
                        break 'outer;
                    }
                    batch = Vec::new();
                }
            }
        }

        drop(sender);
    }

    fn reset_metrics(&mut self) {
        self.num_samples_sent = 0;
        self.num_batches_sent = 0;
        self.bytes_sent = 0;
    }
}

#[inline(never)]
fn kafka_ingest(samples: Vec<Sample>, args: Args) {
    make_topic(
        &args.kafka_bootstraps,
        &args.kafka_topic,
        KafkaTopicOptions::default(),
    );
    let barr = Arc::new(Barrier::new(args.kafka_flushers + 1));

    let (flusher_tx, flusher_rx) = bounded::<Vec<Sample>>(50);
    let flushers: Vec<JoinHandle<()>> = (0..args.kafka_flushers)
        .map(|_| {
            let recv = flusher_rx.clone();
            let topic = args.kafka_topic.clone();
            let barr = barr.clone();
            let producer = kafka_utils::Producer::new(args.kafka_bootstraps.as_str());
            std::thread::spawn(move || {
                kafka_writer(recv, producer, topic);
                barr.wait();
            })
        })
        .collect();

    let mut data_producer = BatchedDataProducer::new(&samples);
    let total_bytes_target = args.kafka_bench_total_gb * GB_IN_BYTES;

    let now = std::time::Instant::now();
    data_producer.run(total_bytes_target, args.kafka_batch_bytes, flusher_tx);
    barr.wait();

    for flusher in flushers {
        flusher.join().unwrap();
    }

    let elapsed_sec = now.elapsed().as_secs_f64();
    let samples_per_sec = data_producer.num_samples_sent as f64 / elapsed_sec;
    let bytes_per_sec = data_producer.bytes_sent as f64 / elapsed_sec;
    let avg_batch_size =
        data_producer.num_samples_sent as f64 / data_producer.num_batches_sent as f64;
    println!(
        "Written Samples {}, Elapsed secs {:?}, samples/sec {}, bytes/sec {}, avg batch size: {}",
        data_producer.num_samples_sent, elapsed_sec, samples_per_sec, bytes_per_sec, avg_batch_size
    );
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

#[inline(never)]
fn mach_ingest(samples: Vec<RegisteredSample>, mut writer: Writer) {
    let now = std::time::Instant::now();
    let mut raw_byte_sz = 0;

    let mut ts_curr = 0;
    let mut num_samples = 0;

    loop {
        for (id_ref, _, values) in samples.iter() {
            let id_ref = *id_ref;
            let ts = ts_curr;
            ts_curr += 1;
            raw_byte_sz += match &values[0] {
                SampleType::Bytes(x) => x.len(),
                _ => unimplemented!(),
            };
            loop {
                if writer.push(id_ref, ts, values.as_slice()).is_ok() {
                    num_samples += 1;
                    break;
                }
            }
        }
        if now.elapsed().as_secs() >= 60 {
            break;
        }
    }

    let push_time = now.elapsed();
    let elapsed = push_time;
    let elapsed_sec = elapsed.as_secs_f64();
    println!(
        "Written Samples {}, Elapsed {:?}, Samples/sec {}",
        num_samples,
        elapsed,
        num_samples as f64 / elapsed_sec
    );
    // let total_sz_written = TOTAL_SZ.load(std::sync::atomic::Ordering::SeqCst);
    // println!("Total Size written: {}", total_sz_written);
    println!("Raw size written {}", raw_byte_sz);
}

fn validate_tsdb(s: &str) -> Result<BenchTarget, String> {
    Ok(match s {
        "mach" => BenchTarget::Mach,
        "kafka" => BenchTarget::Kafka,
        _ => {
            return Err(format!(
                "Invalid option {}, valid options are \"mach\", \"kafka\".",
                s
            ))
        }
    })
}

#[derive(Debug, Clone)]
enum BenchTarget {
    Mach,
    Kafka,
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long,  parse(try_from_str=validate_tsdb))]
    tsdb: BenchTarget,

    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka_bootstraps: String,

    #[clap(short, long, default_value_t = random_id())]
    kafka_topic: String,

    #[clap(short, long, default_value_t = 400_000_000)]
    kafka_batch_bytes: u64,

    #[clap(short, long, default_value_t = 512)]
    kafka_bench_total_gb: u64,

    #[clap(short, long, default_value_t = 4)]
    kafka_flushers: usize,

    #[clap(short, long)]
    file_path: String,
}

fn main() {
    let args = Args::parse();

    println!("Loading data...");
    let id_to_samples = load_samples(args.file_path.as_str());
    let samples: Vec<Sample> =
        id_to_samples
            .into_iter()
            .fold(Vec::new(), |mut samples, (id, mut new_samples)| {
                for (ts, sample_data) in new_samples.drain(..) {
                    samples.push((id, ts, sample_data));
                }
                samples
            });
    println!("{} samples ready", samples.len());

    match args.tsdb {
        BenchTarget::Mach => {
            let mut mach = Mach::new();
            let mut writer = new_writer(&mut mach, args.kafka_bootstraps.clone());

            let samples = register_samples(samples, &mut mach, &mut writer);
            std::thread::sleep(std::time::Duration::from_secs(10));
            println!("starting push");
            mach_ingest(samples, writer);
        }
        BenchTarget::Kafka => {
            kafka_ingest(samples, args);
        }
    }
}
