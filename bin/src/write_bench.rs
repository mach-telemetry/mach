#![allow(warnings)]

mod kafka_utils;
mod prep_data;
mod utils;

use clap::Parser;
use crossbeam_channel::bounded;
use crossbeam_channel::unbounded;
use crossbeam_channel::Receiver;
use lzzzz::{lz4, lz4_hc};
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::fs::File;
use std::io::prelude::*;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread::JoinHandle;
use std::time::Duration;
use zstd::zstd_safe::WriteBuf;

use mach::{
    compression::{CompressFn, Compression},
    id::{SeriesId, SeriesRef, WriterId},
    // kafka_utils::TOTAL_MB_WRITTEN,
    sample::SampleType,
    series::{FieldType, SeriesConfig},
    tsdb::Mach,
    utils::random_id,
    writer::{Writer, WriterConfig},
};

use crate::prep_data::load_samples;
use crate::utils::timestamp_now_micros;

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
        let id_ref = *refmap.entry(*id).or_insert_with(|| {
            let conf = get_series_config(*id, values.as_slice());
            let (wid, _) = mach.add_series(conf).unwrap();
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

#[inline(never)]
fn kafka_ingest_parallel(samples: Vec<Sample>, args: Args) {
    let topic = random_id();
    kafka_utils::make_topic(&args.kafka_bootstraps, &topic);
    let barr = Arc::new(Barrier::new(args.kafka_flushers + 1));

    let (flusher_tx, flusher_rx) = bounded::<Vec<Sample>>(args.kafka_flush_queue_len);
    let flushers: Vec<JoinHandle<()>> = (0..args.kafka_flushers)
        .map(|_| {
            let recv = flusher_rx.clone();
            let topic = topic.clone();
            let barr = barr.clone();
            let mut producer = kafka_utils::Producer::new(args.kafka_bootstraps.as_str());
            std::thread::spawn(move || {
                kafka_writer(recv, producer, topic);
                barr.wait();
            })
        })
        .collect();

    let now = std::time::Instant::now();
    let mut num_samples = 0;
    let mut data = Vec::new();
    let mut serialized_size = 0;
    for mut sample in samples {
        sample.1 = timestamp_now_micros();
        serialized_size += bincode::serialized_size(&sample).unwrap();
        data.push(sample);
        if serialized_size >= args.kafka_batch_bytes {
            serialized_size = 0;
            num_samples += data.len();
            flusher_tx.send(data);
            data = Vec::new();
        }
    }
    if !data.is_empty() {
        num_samples += data.len();
        flusher_tx.send(data);
    }
    drop(flusher_tx);
    barr.wait();

    let push_time = now.elapsed();
    let elapsed = push_time;
    let elapsed_sec = elapsed.as_secs_f64();
    println!(
        "Written Samples {}, Elapsed {:?}, Samples/sec {}",
        num_samples,
        elapsed,
        num_samples as f64 / elapsed_sec
    );

    for flusher in flushers {
        flusher.join().unwrap();
    }
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

    //#[clap(short, long, default_value_t = random_id())]
    //kafka_topic: String,

    //#[clap(short, long, default_value_t = 1)]
    //kafka_partitions: i32,

    //#[clap(short, long, default_value_t = 3)]
    //kafka_replication: i32,

    //#[clap(short, long, default_value_t = String::from("all"), parse(try_from_str=validate_ack))]
    //kafka_acks: String,
    #[clap(short, long, default_value_t = 400_000_000)]
    kafka_batch_bytes: u64,

    #[clap(short, long, default_value_t = 4)]
    kafka_flushers: usize,

    #[clap(short, long, default_value_t = 100)]
    kafka_flush_queue_len: usize,

    #[clap(short, long)]
    file_path: String,
    //#[clap(short, long, default_value_t = 1000000)]
    //mach_active_block_sz: usize,
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
            kafka_ingest_parallel(samples, args);
        }
    }
}
