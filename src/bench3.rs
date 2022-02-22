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
#![allow(unused)]
#![allow(private_in_public)]
#![feature(llvm_asm)]
#![feature(proc_macro_hygiene)]
#![feature(trait_alias)]

pub mod tsdb;
pub mod compression;
pub mod constants;
pub mod id;
pub mod persistent_list;
pub mod sample;
pub mod segment;
pub mod series;
pub mod tags;
pub mod utils;
pub mod writer;
pub mod runtime;
mod zipf;



#[macro_use]
mod rdtsc;

use rand::Rng;
use serde::*;
use std::{
    collections::HashMap,
    convert::TryInto,
    fs::OpenOptions,
    io::prelude::*,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Barrier, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use compression::*;
use constants::*;
use dashmap::DashMap;
use id::SeriesId;
use lazy_static::lazy_static;
use persistent_list::*;
use sample::*;
use seq_macro::seq;
use tags::*;
use writer::*;
use zipf::*;

const BLOCKING_RETRY: bool = false;
const ZIPF: f64 = 0.99;
const NSERIES: usize = 10_000;
const NTHREADS: usize = 1;
const BUFSZ: usize = 1_000_000;
const NSEGMENTS: usize = 3;
const UNIVARIATE: bool = false;
//const COMPRESSION: Compression = Compression::XOR;
//const COMPRESSION: Compression = Compression::Decimal(3);
const PARTITIONS: usize = 10;

lazy_static! {
    static ref DATAPATH: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
    static ref LOGS: PathBuf = DATAPATH.join("github");
    static ref OUTDIR: PathBuf = DATAPATH.join("out");
    static ref DATA: Vec<Sample<1>> = read_data();
}

#[derive(Serialize, Deserialize)]
struct Item {
    timestamp: u64,
    value: String,
}

fn read_data() -> Vec<Sample<1>> {
    println!("LOADING DATA");
    let mut file = OpenOptions::new().read(true).open(&*LOGS).unwrap();
    let mut json = String::new();
    file.read_to_string(&mut json).unwrap();
    let mut data: Vec<Item> = serde_json::from_str(json.as_str()).unwrap();
    data.drain(0..)
        .map(|x| {
            let sample: Sample<1> = Sample {
                timestamp: x.timestamp,
                values: [(Bytes::from_slice(x.value.as_bytes()).into_raw() as u64).to_be_bytes()],
            };
            sample
        })
        .collect()
}

fn main() {
    println!("DATA LEN {}", DATA.len());
    let mut mach = tsdb::Mach::<VectorBackend>::new();

    let buffer = ListBuffer::new(BUFSZ);
    let tags = {
        let mut map = HashMap::new();
        map.insert(String::from("foo"), String::from("bar"));
        Tags::from(map)
    };
    let series_id = tags.id();
    let series_config = series::SeriesConfig {
        tags: tags.clone(),
        compression: Compression::from(vec![CompressFn::BytesLZ4]),
        seg_count: NSEGMENTS,
        nvars: 1,
    };

    let mut writer = mach.new_writer().unwrap();
    let _writer_id = mach.add_series(series_config).unwrap();
    let ref_id = writer.get_reference(series_id);

    let mut samples = 0;
    let mut retries = 0;
    let now = Instant::now();
    println!("INSERTING DATA");
    for (idx, sample) in DATA.iter().enumerate() {
        if idx % 1_000_000 == 0 {
            let elapsed = now.elapsed().as_secs_f64();
            println!(
                "INSERTED {} @ RATE: {}",
                samples,
                (samples as f64 / elapsed) / 1_000_000.
            );
        }
        'inner: loop {
            let res = writer.push_sample(ref_id, *sample);
            match res {
                Ok(_) => {
                    samples += 1;
                    break 'inner;
                }
                Err(_) => {
                    retries += 1;
                }
            }
        }
    }
    let elapsed = now.elapsed().as_secs_f64();
    println!("INSERTING SAMPLES: {}", samples);
    println!("ELAPSED: {}", elapsed);
    println!("RATE: {}", (samples as f64 / elapsed) / 1_000_000.);
}
