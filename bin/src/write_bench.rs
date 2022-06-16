#![allow(warnings)]

mod kafka_utils;

use clap::Parser;
use crossbeam_channel::bounded;
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::fs::File;
use std::io::prelude::*;
use std::sync::atomic::Ordering::SeqCst;
use std::thread::JoinHandle;
use std::time::Duration;
use zstd::zstd_safe::WriteBuf;

use mach::{
    compression::{CompressFn, Compression},
    id::{SeriesId, SeriesRef, WriterId},
    // kafka_utils::TOTAL_MB_WRITTEN,
    sample::Type,
    series::{SeriesConfig, Types},
    tsdb::Mach,
    utils::random_id,
    writer::{Writer, WriterConfig},
};

type TimeStamp = u64;
type Sample = (SeriesId, TimeStamp, Vec<Type>);
type RegisteredSample = (SeriesRef, TimeStamp, Vec<Type>);

fn load_data(path: &str, repeat_factor: usize) -> Vec<otlp::OtlpData> {
    let mut data = Vec::new();
    File::open(path).unwrap().read_to_end(&mut data).unwrap();
    let data: Vec<otlp::OtlpData> = bincode::deserialize(data.as_slice()).unwrap();

    match repeat_factor {
        0 | 1 => data,
        n => {
            let mut ret = data.clone();
            for _ in 0..n - 1 {
                ret.extend_from_slice(data.as_slice());
            }
            ret
        }
    }
}

fn rewrite_timestamps(data: &mut Vec<otlp::OtlpData>) {
    let mut now = std::time::SystemTime::now();
    for item in data.iter_mut() {
        let ts: u64 = (now.duration_since(std::time::UNIX_EPOCH))
            .unwrap()
            .as_nanos()
            .try_into()
            .unwrap();
        item.update_timestamp(ts);
        now += std::time::Duration::from_secs(1);
    }
}

fn otlp_data_to_samples(data: Vec<otlp::OtlpData>) -> Vec<Sample> {
    let mut data: Vec<mach_otlp::OtlpData> = data
        .iter()
        .map(|x| {
            let mut x: mach_otlp::OtlpData = x.into();
            match &mut x {
                mach_otlp::OtlpData::Spans(x) => x.iter_mut().for_each(|x| x.set_source_id()),
                _ => unimplemented!(),
            }
            x
        })
        .collect();

    let mut samples = Vec::new();

    for entry in data {
        match entry {
            mach_otlp::OtlpData::Spans(spans) => {
                for span in spans {
                    span.get_samples(&mut samples);
                }
            }
            _ => unimplemented!(),
        }
    }

    samples
}

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

fn prepare_kafka_samples(data: Vec<otlp::OtlpData>) -> Vec<Sample> {
    otlp_data_to_samples(data)
}

fn prepare_mach_samples(
    data: Vec<otlp::OtlpData>,
    mach: &mut Mach,
    writer: &mut Writer,
) -> Vec<RegisteredSample> {
    let samples = otlp_data_to_samples(data);
    let registered_samples = register_samples(samples, mach, writer);
    registered_samples
}

fn new_writer(mach: &mut Mach, kafka_bootstraps: String) -> Writer {
    let writer_config = WriterConfig {
        active_block_flush_sz: 1_000_000,
    };

    mach.add_writer(writer_config.clone()).unwrap()
}

#[inline(never)]
fn kafka_ingest(samples: Vec<Sample>, args: Args) {
    let topic = random_id();
    kafka_utils::make_topic(&args.kafka_bootstraps, &topic);

    let (tx, rx) = bounded::<Vec<Sample>>(args.kafka_flush_queue_len);
    let flushers: Vec<JoinHandle<()>> = (0..args.kafka_flushers)
        .map(|_| {
            let recv = rx.clone();
            let topic = topic.clone();
            let mut producer = kafka_utils::Producer::new(args.kafka_bootstraps.as_str());
            std::thread::spawn(move || {
                while let Ok(data) = recv.recv() {
                    let bytes = bincode::serialize(&data).unwrap();
                    let compressed = zstd::encode_all(bytes.as_slice(), 0).unwrap();
                    producer.send(topic.as_str(), 0, compressed.as_slice());
                }
            })
        })
        .collect();

    let num_samples = samples.len();

    let now = std::time::Instant::now();

    let mut data = Vec::with_capacity(args.kafka_batch);
    for sample in samples {
        data.push(sample);
        if data.len() == args.kafka_batch {
            tx.send(data);
            data = Vec::with_capacity(args.kafka_batch);
        }
    }
    if !data.is_empty() {
        tx.send(data);
    }

    drop(tx);

    for flusher in flushers {
        flusher.join().unwrap();
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
    // println!("Raw size written {}", raw_byte_sz);
}

fn get_series_config(id: SeriesId, values: &[Type]) -> SeriesConfig {
    let mut types = Vec::new();
    let mut compression = Vec::new();
    values.iter().for_each(|v| {
        let (t, c) = match v {
            Type::U32(_) => (Types::U32, CompressFn::IntBitpack),
            Type::U64(_) => (Types::U64, CompressFn::LZ4),
            Type::F64(_) => (Types::F64, CompressFn::Decimal(3)),
            Type::Bytes(_) => (Types::Bytes, CompressFn::BytesLZ4),
            Type::BorrowedBytes(_) => (Types::Bytes, CompressFn::NOOP),
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
    let mut last = now.clone();
    let mut raw_byte_sz = 0;

    for (id_ref, ts, values) in samples.iter() {
        let id_ref = *id_ref;
        let ts = *ts;
        raw_byte_sz += match &values[0] {
            Type::Bytes(x) => x.len(),
            _ => unimplemented!(),
        };
        loop {
            if writer.push(id_ref, ts, values.as_slice()).is_ok() {
                break;
            }
        }
    }

    let num_samples = samples.len();
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
    #[clap(short, long, default_value_t = 100_000)]
    kafka_batch: usize,

    #[clap(short, long, default_value_t = 4)]
    kafka_flushers: usize,

    #[clap(short, long, default_value_t = 20)]
    kafka_flush_queue_len: usize,

    #[clap(short, long)]
    file_path: String,
    //#[clap(short, long, default_value_t = 1000000)]
    //mach_active_block_sz: usize,
}

fn main() {
    let args = Args::parse();
    println!("Args: {:#?}", args);

    println!("Loading data");
    let mut data = load_data(args.file_path.as_str(), 4);
    println!("Rewriting timestamps");
    rewrite_timestamps(&mut data);

    match args.tsdb {
        BenchTarget::Mach => {
            let mut mach = Mach::new();
            let mut writer = new_writer(&mut mach, args.kafka_bootstraps.clone());

            println!("Extracting samples");
            let samples = prepare_mach_samples(data, &mut mach, &mut writer);
            println!("{} samples ready", samples.len());

            mach_ingest(samples, writer);
        }
        BenchTarget::Kafka => {
            println!("Extracting samples");
            let samples = prepare_kafka_samples(data);
            println!("{} samples ready", samples.len());

            kafka_ingest(samples, args);
        }
    }
}
