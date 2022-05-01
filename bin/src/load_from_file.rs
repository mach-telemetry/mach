mod sample;

use std::fs;
use std::io::*;
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering::SeqCst}};
use std::time::Duration;
use std::thread;
use serde::*;
use serde_json;
use mach::{
    compression::{CompressFn, Compression},
    durable_queue::{QueueConfig, KafkaConfig},
    id::{SeriesId, WriterId},
    reader::{ReadResponse, ReadServer},
    sample::Type,
    series::{SeriesConfig, Types},
    tags::Tags,
    tsdb::Mach,
    utils::{bytes::Bytes, random_id},
    writer::{Writer, WriterConfig},
};

pub fn main() {

    let counter = Arc::new(AtomicUsize::new(0));
    let cc = counter.clone();
    thread::spawn(move || {
        let mut last = cc.load(SeqCst);
        loop {
            thread::sleep(Duration::from_secs(1));
            let cur = cc.load(SeqCst);
            println!("Rate {} / sec", cur - last);
            last = cur;
        }
    });

    // Setup mach and writer
    let mut mach = Mach::new();
    let queue_config = KafkaConfig {
        bootstrap: String::from("localhost:9093,localhost:9094,localhost:9095"),
        topic: random_id(),
    }
    .config();

    let writer_config = WriterConfig {
        queue_config,
        active_block_flush_sz: 1_000_000,
    };
    let mut writer = mach.add_writer(writer_config).unwrap();


    // Load data into memory
    println!("Loading data");
    let reader = BufReader::new(fs::File::open("/home/fsolleza/data/mach/demo_data").unwrap());
    let mut data: Vec<sample::Sample> = reader.lines().map(|x| serde_json::from_str(&x.unwrap()).unwrap()).collect();

    // Write data to single writer
    println!("Writing data");
    let mut values = Vec::new();

    let mut map = HashMap::new();
    for mut sample in data.drain(..) {
        let tags = Tags::from(sample.tags.clone());
        let series_id = tags.id();
        let series_ref = *map.entry(series_id).or_insert_with(|| {
            let (w, s) = mach.add_series(detect_config(&tags, &sample)).unwrap();
            //assert_eq!(w, WriterId(0));
            let r = writer.get_reference(series_id);
            r
        });
        values.clear();
        for v in sample.values.drain(..) {
            let item = match v {
                sample::Type::F64(x) => Type::F64(x),
                sample::Type::Str(x) => {
                    Type::Bytes(Bytes::from_slice(x.as_bytes()))
                }
                _ => panic!("Unhandled value type in sample"),
            };
        }

        // Push the sample
        loop {
            if writer
            .push(series_ref, sample.timestamp, values.as_slice())
            .is_ok() {
                counter.fetch_add(1, SeqCst);
                break
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
    let seg_count = 1;
    let nvars = types.len();

    SeriesConfig {
        tags: tags.clone(),
        types,
        compression,
        seg_count,
        nvars,
    }
}

