#![deny(unused_must_use)]
#![feature(get_mut_unchecked)]
#![feature(is_sorted)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
#![feature(cell_update)]
#![feature(box_syntax)]
#![feature(thread_id_value)]
#![allow(clippy::new_without_default)]
#![allow(clippy::len_without_is_empty)]
#![allow(warnings)]

mod backend;
mod chunk;
mod compression;
mod flush_buffer;
mod segment;
mod series_metadata;
mod tags;
mod test_utils;
mod utils;
mod writer;

use crate::{
    backend::{fs, kafka},
    chunk::*,
    compression::{Compression, DecompressBuffer},
    series_metadata::SeriesMetadata,
    tags::Tags,
    test_utils::*,
    writer::{file::FileWriter, kafka::KafkaWriter},
};
use dashmap::DashMap;
use std::{
    sync::Arc,
    time::{Instant, Duration},
};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    config::ClientConfig,
};

fn main() {
    let len = &SYNTHETIC_DATA.len();
    run_file();
    run_kafka();
    //async_std::task::block_on(run_kafka_naive());
    //async_std::task::block_on(run_kafka_batched(100));
    async_std::task::block_on(run_kafka_batched(1000));
}

fn file_wait_loop(ref_id: u64, writer: &mut FileWriter, ts: u64, values: &[[u8; 8]]) {
    loop {
        if writer.push(ref_id, ts, values).is_ok() {
            break;
        }
    }
}

fn gen_data() -> &'static [Sample] {
    let data = &MULTIVARIATE_DATA[1].1;
    //let data = &SYNTHETIC_DATA;
    data.as_slice()
}

fn run_file() {
    std::fs::remove_dir_all(&*backend::fs::DATADIR).unwrap();
    std::fs::create_dir_all(&*backend::fs::DATADIR).unwrap();
    // Setup data
    let mut tags = Tags::new();
    tags.insert(("A".to_string(), "B".to_string()));
    tags.insert(("C".to_string(), "D".to_string()));
    let data = gen_data();
    let nvars = data[0].values.len();

    let meta = Arc::new(SeriesMetadata::with_file_backend(
            5,
            nvars,
            3,
            &tags,
            Compression::LZ4(1),
    ));

    // Setup writer
    let reference = Arc::new(DashMap::new());
    reference.insert(tags.clone(), meta.clone());
    let mut writer = FileWriter::new(5, reference.clone(), SHARED_FILE_ID.clone());
    let ref_id = writer.init_series(&tags).unwrap();

    let mut exp_ts = Vec::new();
    let mut exp_values = Vec::new();
    for _ in 0..nvars {
        exp_values.push(Vec::new());
    }

    // Push data into the writer
    let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
        let mut values = vec![[0u8; 8]; nvars];
        for (i, v) in items.iter().enumerate() {
            values[i] = v.to_be_bytes();
        }
        values
    };

    let samples = data
        .iter()
        .map(|item| (item.ts, to_values(&*item.values)))
        .collect::<Vec<(u64, Vec<[u8; 8]>)>>();

    // Gather the data
    for item in samples.iter() {
        exp_ts.push(item.0);
        for i in 0..nvars {
            exp_values[i].push(item.1[i]);
        }
    }

    // Write the data
    let start = Instant::now();
    for item in samples.iter() {
        if writer.push(ref_id, item.0, &item.1[..]).is_err() {
            file_wait_loop(ref_id, &mut writer, item.0, &item.1[..]);
        }
    }
    writer.close();
    let dur = start.elapsed();
    println!("file dur: {:?}", dur);

    let mut file_list_iterator = meta.backend.file_backend().list.reader().unwrap();
    let mut count = 0;
    let rev_exp_ts = exp_ts.iter().rev().copied().collect::<Vec<u64>>();
    let mut timestamps = Vec::new();
    while let Some(byte_entry) = file_list_iterator.next_item().unwrap() {
        count += 1;
        let chunk = SerializedChunk::new(byte_entry.bytes).unwrap();
        let counter = chunk.n_segments();
        for i in 0..counter {
            let mut decompressed = DecompressBuffer::new();
            let bytes = chunk.get_segment_bytes(i);
            let bytes_read = Compression::decompress(bytes, &mut decompressed).unwrap();
            for i in 0..decompressed.len() {
                timestamps.push(decompressed.timestamp_at(i));
            }
        }
    }
    assert_eq!(timestamps, rev_exp_ts);
}

fn kafka_wait_loop(ref_id: u64, writer: &mut KafkaWriter, ts: u64, values: &[[u8; 8]]) {
    loop {
        if writer.push(ref_id, ts, values).is_ok() {
            break;
        }
    }
}

async fn run_kafka_batched(sz: usize) {

    let data = gen_data();
    let sample_to_bytes = |ts: u64, val: &[f64]| -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&ts.to_be_bytes()[..]);
        for item in val.iter() {
            v.extend_from_slice(&item.to_be_bytes()[..]);
        }
        v
    };

    let mut batches = Vec::new();
    for chunk in data.chunks(sz) {
        let mut v = Vec::new();
        for item in chunk {
            v.extend_from_slice(&item.ts.to_be_bytes()[..]);
            for item in item.values.iter() {
                v.extend_from_slice(&item.to_be_bytes()[..]);
            }
        }
        batches.push(v);
    }

    // connect to kafka
    let bootstrap = "localhost:29092";
    let topic = "RawWrites";
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("Producer create error");
    let dur = Duration::from_secs(0);

    let mut len = 0;
    let start = Instant::now();
    for batch in batches.iter() {
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(&*topic)
            .payload(&batch[..]);
        len += batch.len();
        match producer.send(to_send, dur).await {
            Ok(_) => {},
            Err((e, _)) => {
                println!("Error: {:?}", e);
            }
        }
    }
    let elapsed = start.elapsed();
    println!("kafka batch size {} dur: {:?}", sz, elapsed);
    println!("kafka batch written: {}", len);
}


async fn run_kafka_naive() {

    let data = gen_data();
    let sample_to_bytes = |ts: u64, val: &[f64]| -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&ts.to_be_bytes()[..]);
        for item in val.iter() {
            v.extend_from_slice(&item.to_be_bytes()[..]);
        }
        v
    };

    let samples = data
        .iter()
        .map(|item| sample_to_bytes(item.ts, &*item.values))
        .collect::<Vec<Vec<u8>>>();

    // connect to kafka
    let bootstrap = "localhost:29092";
    let topic = "RawWrites";
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("Producer create error");
    let dur = Duration::from_secs(0);

    let start = Instant::now();
    for sample in samples.iter() {
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(&*topic)
            .payload(&sample[..]);
        match producer.send(to_send, dur).await {
            Ok(_) => {},
            Err((e, _)) => {
                println!("Error: {:?}", e);
            }
        }
    }
    println!("kafka naive dur: {:?}", start.elapsed());
}

fn run_kafka() {
    // Setup data
    let mut tags = Tags::new();
    tags.insert(("A".to_string(), "B".to_string()));
    tags.insert(("C".to_string(), "D".to_string()));
    let data = gen_data();
    let nvars = data[0].values.len();

    let meta = Arc::new(SeriesMetadata::with_kafka_backend(
        5,
        nvars,
        3,
        &tags,
        Compression::LZ4(1),
    ));

    // Setup writer
    let reference = Arc::new(DashMap::new());
    reference.insert(tags.clone(), meta.clone());
    let mut writer = KafkaWriter::new(5, reference.clone());
    let ref_id = writer.init_series(&tags).unwrap();

    // Push data into the writer
    let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
        let mut values = vec![[0u8; 8]; nvars];
        for (i, v) in items.iter().enumerate() {
            values[i] = v.to_be_bytes();
        }
        values
    };

    let mut exp_ts = Vec::new();
    let mut exp_values = Vec::new();
    for _ in 0..nvars {
        exp_values.push(Vec::new());
    }

    let samples = data
        .iter()
        .map(|item| (item.ts, to_values(&*item.values)))
        .collect::<Vec<(u64, Vec<[u8; 8]>)>>();

    // Gather the data
    for item in samples.iter() {
        exp_ts.push(item.0);
        for i in 0..nvars {
            exp_values[i].push(item.1[i]);
        }
    }

    // Write the data
    let start = Instant::now();
    for item in samples.iter() {
        if writer.push(ref_id, item.0, &item.1[..]).is_err() {
            kafka_wait_loop(ref_id, &mut writer, item.0, &item.1[..]);
        }
    }
    writer.close();
    let dur = start.elapsed();
    println!("kafka dur: {:?}", dur);

    let consumer = kafka::default_consumer().unwrap();
    let timeout = kafka::Timeout::After(Duration::from_secs(1));
    let mut file_list_iterator = meta
        .backend
        .kafka_backend()
        .list
        .reader(consumer, timeout)
        .unwrap();
    let mut count = 0;
    let rev_exp_ts = exp_ts.iter().rev().copied().collect::<Vec<u64>>();
    let mut timestamps = Vec::new();
    while let Some(byte_entry) = file_list_iterator.next_item().unwrap() {
        count += 1;
        let chunk = SerializedChunk::new(byte_entry.bytes).unwrap();
        let counter = chunk.n_segments();
        for i in 0..counter {
            let mut decompressed = DecompressBuffer::new();
            let bytes = chunk.get_segment_bytes(i);
            let bytes_read = Compression::decompress(bytes, &mut decompressed).unwrap();
            for i in 0..decompressed.len() {
                timestamps.push(decompressed.timestamp_at(i));
            }
        }
    }
    assert_eq!(timestamps, rev_exp_ts);
}
