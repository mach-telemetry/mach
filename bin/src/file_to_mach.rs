mod sample;

use lazy_static::*;
use mach::{
    compression::{CompressFn, Compression},
    durable_queue::{FileConfig, KafkaConfig, QueueConfig},
    id::{SeriesId, WriterId},
    reader::{ReadResponse, ReadServer},
    sample::Type,
    series::{SeriesConfig, Types},
    tags::Tags,
    tsdb::Mach,
    utils::{bytes::Bytes, random_id},
    writer::{Writer, WriterConfig},
};
use serde::*;
use serde_json;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc, Barrier,
};
use std::thread;
use std::time::Duration;

lazy_static! {
    static ref FILE_CONF: QueueConfig = FileConfig {
        dir: "/home/ubuntu".into(),
        file: random_id(),
    }
    .config();
    static ref KAFKA_CONF: QueueConfig = KafkaConfig {
        bootstrap: String::from("b-1.demo-cluster.5nabvl.c25.kafka.us-east-1.amazonaws.com:9092,b-2.demo-cluster.5nabvl.c25.kafka.us-east-1.amazonaws.com:9092,b-3.demo-cluster.5nabvl.c25.kafka.us-east-1.amazonaws.com:9092"),
        topic: random_id(),
    }
    .config();
    static ref DATA: String = String::from("/home/ubuntu/demo_data");
}

pub fn main() {
    let counter = Arc::new(AtomicUsize::new(0));
    let cc = counter.clone();
    let barrier = Arc::new(Barrier::new(2));
    let cb = barrier.clone();
    thread::spawn(move || {
        let mut last = cc.load(SeqCst);
        cb.wait();
        loop {
            thread::sleep(Duration::from_secs(1));
            let cur = cc.load(SeqCst);
            println!("Rate {} / sec", cur - last);
            last = cur;
        }
    });

    // Setup mach and writer
    let mut mach = Mach::new();
    let _queue_config = FILE_CONF.clone();
    //let _queue_config = KAFKA_CONF.clone();

    let writer_config = WriterConfig {
        queue_config: _queue_config,
        active_block_flush_sz: 1_000_000,
    };
    let mut writer = mach.add_writer(writer_config).unwrap();

    // Load data into memory
    println!("Loading data");
    let reader = BufReader::new(fs::File::open(&*DATA).unwrap());
    let mut data: Vec<sample::Sample> = reader
        .lines()
        .map(|x| serde_json::from_str(&x.unwrap()).unwrap())
        .collect();

    // Register series
    println!("Register series");
    //let mut map = HashMap::new();
    let mut samples = Vec::new();
    let mut set = HashMap::new();
    for sample in data.iter() {
        let tags = Tags::from(sample.tags.clone());
        let series_id = tags.id();
        let series_ref = *set.entry(series_id).or_insert_with(|| {
            let _ = mach.add_series(detect_config(&tags, &sample)).unwrap();
            let series_ref = writer.get_reference(series_id);
            series_ref
        });
        //series_refs.push(*series_ref);
        let mut values = Vec::new();
        for v in &sample.values {
            let item = match v {
                sample::Type::F64(x) => Type::F64(*x),
                sample::Type::Str(x) => Type::Bytes(Bytes::from_slice(x.as_bytes())),
                _ => panic!("Unhandled value type in sample"),
            };
            values.push(item);
        }
        samples.push((series_ref, sample.timestamp, values))
    }
    println!("Done registeringer");
    barrier.wait();

    // Write data to single writer
    println!("Writing data");
    let mut values = Vec::new();
    for (series_ref, timestamp, mut items) in samples.drain(..) {
        //let tags = Tags::from(sample.tags);
        //let series_id = tags.id();
        //let series_ref = *map.get(&series_id).unwrap();
        // Push the sample
        values.clear();
        values.append(&mut items);
        //for item in items {
        //    values.push(item);
        //}
        loop {
            if writer
                .push(series_ref, timestamp, values.as_slice())
                .is_ok()
            {
                counter.fetch_add(1, SeqCst);
                break;
            }
        }
    }
}

fn detect_config(tags: &Tags, sample: &sample::Sample) -> SeriesConfig {
    let mut types = Vec::new();
    let mut compression = Vec::new();
    for v in sample.values.iter() {
        match v {
            sample::Type::F64(_) => {
                types.push(Types::F64);
                compression.push(CompressFn::Decimal(3));
            }
            sample::Type::Str(_) => {
                types.push(Types::Bytes);
                compression.push(CompressFn::BytesLZ4);
            }
            _ => panic!("Unhandled value type in sample"),
        }
    }
    let compression = Compression::from(compression);
    let seg_count = 3;
    let nvars = types.len();

    SeriesConfig {
        tags: tags.clone(),
        types,
        compression,
        seg_count,
        nvars,
    }
}
