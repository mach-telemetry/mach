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

mod compression2;
mod constants;
mod id;
mod persistent_list;
mod sample;
mod segment;
mod tags;
mod test_utils;
mod tsdb;
mod utils;
mod writer;
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
use tsdb::{Mach, SeriesConfig};

use compression2::*;
use constants::*;
use dashmap::DashMap;
use id::*;
use lazy_static::lazy_static;
use persistent_list::*;
use sample::*;
use seq_macro::seq;
use tags::*;
use writer::*;
use zipf::*;

const UNIVARIATE: bool = false;
const NTHREADS: usize = 4;
const NVARS: usize = 8;

lazy_static! {
    static ref DATAPATH: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
    static ref BENCHPATH: PathBuf = DATAPATH.join("bench_data");
    static ref METRICSPATH: PathBuf = BENCHPATH.join("metrics");
    static ref LOGSPATH: PathBuf = BENCHPATH.join("logs");
    static ref OUTDIR: PathBuf = DATAPATH.join("out");
    static ref DATA: Vec<Vec<RawSample>> = read_data();
    static ref TOTAL_RATE: Arc<Mutex<f64>> = Arc::new(Mutex::new(0.0f64));
    static ref BARRIERS: Arc<Barrier> = Arc::new(Barrier::new(NTHREADS));
}

struct RawSample(
    u64,                // timestamp
    Box<[[u8; NVARS]]>, // data
);

#[derive(Serialize, Deserialize)]
struct DataEntry {
    timestamps: Vec<u64>,
    values: HashMap<String, Vec<f64>>,
}

impl DataEntry {
    fn to_series(self) -> Vec<RawSample> {
        let mut res = Vec::new();
        for i in 0..self.timestamps.len() {
            let ts = self.timestamps[i];
            let mut values: Vec<[u8; NVARS]> = Vec::new();
            for (_, var) in self.values.iter() {
                values.push(var[i].to_be_bytes());
            }
            res.push(RawSample(ts, values.into_boxed_slice()));
        }
        res
    }
}

fn load_data() -> HashMap<String, DataEntry> {
    println!("LOADING DATA");
    let file_path = if UNIVARIATE {
        METRICSPATH.join("bench1_univariate_small.json")
    } else {
        METRICSPATH.join("bench1_multivariate_small.json")
    };
    let mut file = OpenOptions::new().read(true).open(file_path).unwrap();
    let mut json = String::new();
    file.read_to_string(&mut json).unwrap();
    let dict: HashMap<String, DataEntry> = serde_json::from_str(json.as_str()).unwrap();
    dict
}

fn read_data() -> Vec<Vec<RawSample>> {
    let mut dict = load_data();
    println!("MAKING DATA");
    dict.drain()
        .filter(|(_, d)| d.timestamps.len() >= 30_000)
        .map(|(_, d)| d.to_series())
        .collect()
}

fn make_writers<B: PersistentListBackend>(
    writer_count: usize,
    mach: &mut Mach<B>,
) -> HashMap<WriterId, Writer> {
    (0..writer_count)
        .map(|_| mach.add_writer())
        .map(|r| r.expect("should be able to instantiate writer"))
        .fold(HashMap::new(), |mut map, w| {
            map.insert(w.id(), w);
            map
        })
}

fn main() {
    let outdir = &*OUTDIR;
    std::fs::remove_dir_all(outdir);
    std::fs::create_dir_all(outdir).unwrap();

    let backend = VectorBackend::new();
    let mut mach = Mach::new(backend).expect("should be able to instantiate Mach");

    let mut writers_map = make_writers(NTHREADS, &mut mach);
}
