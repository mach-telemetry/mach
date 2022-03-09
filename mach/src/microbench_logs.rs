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
#![feature(proc_macro_hygiene)]
#![feature(trait_alias)]

mod compression;
mod constants;
mod durability;
mod id;
mod persistent_list;
mod reader;
mod runtime;
mod sample;
mod segment;
mod series;
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
use std::fs;
use std::marker::PhantomData;
use std::{
    collections::HashMap,
    convert::TryInto,
    fs::OpenOptions,
    io::prelude::*,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Barrier, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use tsdb::Mach;

use compression::*;
use constants::*;
use dashmap::DashMap;
use id::*;
use lazy_static::lazy_static;
use persistent_list::*;
use sample::*;
use seq_macro::seq;
use series::{SeriesConfig, Types};
use tags::*;
use writer::*;
use zipf::*;

const ZIPF: f64 = 0.99;
const UNIVARIATE: bool = true;
const NTHREADS: usize = 1;
const NSERIES: usize = 10_000;
const NSEGMENTS: usize = 1;
const NUM_INGESTS_PER_THR: usize = 100_000_000;

lazy_static! {
    static ref DATAPATH: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
    static ref BENCHPATH: PathBuf = DATAPATH.join("bench_data");
    static ref METRICSPATH: PathBuf = BENCHPATH.join("metrics");
    static ref LOGSPATH: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("data_logs");
    static ref OUTDIR: PathBuf = DATAPATH.join("out");
    static ref TOTAL_RATE: Arc<Mutex<f64>> = Arc::new(Mutex::new(0.0f64));
    static ref BARRIERS: Arc<Barrier> = Arc::new(Barrier::new(NTHREADS));
}

struct DataSrc {
    src_names: Vec<String>,
}

impl DataSrc {
    fn new(data_path: &Path) -> Self {
        let src_names = fs::read_dir(data_path)
            .expect("Could not read from data path")
            .flat_map(|res| {
                res.map(|e| {
                    e.file_name()
                        .into_string()
                        .expect("datasrc file name not converable into string")
                })
            })
            .collect();

        Self { src_names }
    }

    fn size(&self) -> usize {
        self.src_names.len()
    }
}

fn main() {
    let datasrc = DataSrc::new(LOGSPATH.as_path());
}
