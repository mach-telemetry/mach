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

fn main() {
    run_file();
    run_kafka();
}

fn file_wait_loop(ref_id: u64, writer: &mut FileWriter, ts: u64, values: &[[u8; 8]]) {
    loop {
        if writer.push(ref_id, ts, values).is_ok() {
            break;
        }
    }
}


fn run_file() {
    // Setup data
    let mut tags = Tags::new();
    tags.insert(("A".to_string(), "B".to_string()));
    tags.insert(("C".to_string(), "D".to_string()));
    let data = &MULTIVARIATE_DATA[0].1;
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
    //println!("ORIGIN ORDER");
    //println!("{:?}", exp_ts);
    //println!("REVERSE ORDER");
    //println!("{:?}", rev_exp_ts);
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

fn run_kafka() {
    // Setup data
    let mut tags = Tags::new();
    tags.insert(("A".to_string(), "B".to_string()));
    tags.insert(("C".to_string(), "D".to_string()));
    let data = &MULTIVARIATE_DATA[0].1;
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
