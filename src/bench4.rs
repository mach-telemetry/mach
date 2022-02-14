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
use std::marker::PhantomData;
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
const NSERIES: usize = 10_000;
const NVARS: usize = 8;
const NSEGMENTS: usize = 1;

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

fn make_uniform_compression(scheme: CompressFn, nvars: usize) -> Compression {
    Compression::from((0..nvars).map(|_| scheme).collect::<Vec<CompressFn>>())
}

struct IngestionWorker {
    writer: Writer,
    refids: Vec<usize>,
    series: Vec<&'static [RawSample]>,
}

impl IngestionWorker {
    fn new(writer: Writer) -> Self {
        IngestionWorker {
            writer,
            refids: Vec::new(),
            series: Vec::new(),
        }
    }

    fn register_series(&mut self, series_id: SeriesId, series_data: &'static Vec<RawSample>) {
        let refid = self.writer.register(series_id);

        self.refids.push(refid);
        self.series.push(series_data);
    }
}

struct IngestionMetadata<'a, B: PersistentListBackend + 'static> {
    writer_data_map: HashMap<WriterId, IngestionWorker>,
    data_src: &'static Vec<Vec<RawSample>>,
    mach: &'a mut Mach<B>,
}

impl<'a, B: PersistentListBackend> IngestionMetadata<'a, B> {
    fn new(
        mach: &'a mut Mach<B>,
        n_writers: usize,
        data_src: &'static Vec<Vec<RawSample>>,
    ) -> Self {
        let mut writer_data_map = HashMap::new();

        for _ in 0..n_writers {
            let writer = mach.add_writer().expect("should be able to add new writer");
            writer_data_map.insert(writer.id(), IngestionWorker::new(writer));
        }

        IngestionMetadata {
            writer_data_map,
            data_src,
            mach,
        }
    }

    fn add_series(&mut self) {
        let idx: usize = rand::thread_rng().gen_range(0..DATA.len());
        let series_data = &DATA[idx];
        let nvars = series_data[0].1.len();
        let compression = make_uniform_compression(CompressFn::Decimal(3), nvars);

        let series_config = SeriesConfig {
            compression,
            seg_count: NSEGMENTS,
            nvars,
        };

        let (series_id, writer_id) = self
            .mach
            .register(series_config)
            .expect("add series should succeed");

        self.writer_data_map
            .get_mut(&writer_id)
            .expect("found writer id for nonexistent writer")
            .register_series(series_id, series_data);
    }
}

fn main() {
    let outdir = &*OUTDIR;
    std::fs::remove_dir_all(outdir);
    std::fs::create_dir_all(outdir).unwrap();

    let backend = VectorBackend::new();
    let mut mach = Mach::new(backend).expect("should be able to instantiate Mach");

    let mut ingestion_meta = IngestionMetadata::new(&mut mach, NTHREADS, &DATA);

    for _ in 0..NSERIES * NTHREADS {
        ingestion_meta.add_series();
    }
}
