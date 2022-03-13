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

use crossbeam::channel::{bounded, Receiver, Sender};
use rand::Rng;
use serde::*;
use std::fs;
use std::marker::PhantomData;
use std::{
    collections::HashMap,
    convert::TryInto,
    fs::File,
    fs::OpenOptions,
    io::prelude::*,
    io::BufReader,
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

fn clean_outdir() {
    let outdir = &*OUTDIR;
    std::fs::remove_dir_all(outdir);
    std::fs::create_dir_all(outdir).unwrap();
}

struct DataSrcName(String);

struct DataSrc {
    src_names: Vec<DataSrcName>,
}

impl DataSrc {
    fn new(data_path: &Path) -> Self {
        let src_names = fs::read_dir(data_path)
            .expect("Could not read from data path")
            .flat_map(|res| {
                res.map(|e| {
                    DataSrcName(
                        e.file_name()
                            .into_string()
                            .expect("datasrc file name not converable into string"),
                    )
                })
            })
            .collect();

        Self { src_names }
    }

    fn pick_from_series_id(&self, series_id: SeriesId) -> DataSrcName {
        self.src_names[*series_id as usize % self.src_names.len()]
    }

    fn len(&self) -> usize {
        self.src_names.len()
    }
}

struct ZipfianPicker {
    selection_pool: Vec<usize>,
    counter: usize,
}

impl ZipfianPicker {
    fn new(size: u64) -> Self {
        let mut z = Zipfian::new(size, ZIPF);

        ZipfianPicker {
            selection_pool: (0..1000).map(|_| z.next_item() as usize).collect(),
            counter: 0,
        }
    }

    fn next(&mut self) -> usize {
        let selected = self.selection_pool[self.counter];
        self.counter = match self.counter {
            _ if self.counter == self.selection_pool.len() - 1 => 0,
            _ => self.counter + 1,
        };
        selected
    }
}
#[derive(Serialize, Deserialize)]
struct Item {
    timestamp: u64,
    value: String,
}

#[derive(Serialize, Deserialize)]
struct IngestionSample {
    timestamp: u64,
    value: String,
    serid: SeriesId,
    refid: SeriesRef,
}

struct BenchWriterMeta {
    writer: Writer,
    series_refs: Vec<SeriesRef>,
    series_ids: Vec<SeriesId>,
    datasrc_names: Vec<DataSrcName>,
}

impl BenchWriterMeta {
    fn new(mut writer: Writer) -> Self {
        Self {
            writer,
            series_refs: Vec::new(),
            series_ids: Vec::new(),
            datasrc_names: Vec::new(),
        }
    }

    fn register_series(&mut self, series_id: SeriesId, datasrc_name: DataSrcName) {
        let series_ref = self.writer.get_reference(series_id);
        self.series_refs.push(series_ref);
        self.series_ids.push(series_id);
        self.datasrc_names.push(datasrc_name);
    }

    fn run(&mut self) {
        let (s, r) = bounded::<IngestionSample>(1);

        let writer = thread::spawn(|| {
            while let Ok(sample) = r.recv() {
                self.writer.push_type(
                    sample.refid,
                    sample.serid,
                    sample.timestamp,
                    &vec![Type::Bytes(Bytes::from_slice(sample.value.as_bytes()))],
                );
            }
        });

        let loader = thread::spawn(|| {
            let zipf = ZipfianPicker::new(self.series_refs.len() as u64);

            let readers = Vec::new();
            let line_itr = Vec::new();

            for name in self.datasrc_names {
                let file = File::open(name.0).expect("open datasrc file failed");
                let reader = BufReader::new(file);
                line_itr.push(Some(reader.lines()));
                readers.push(reader);
            }

            let finished = 0;

            loop {
                let picked = zipf.next();
                match line_itr[picked] {
                    None => continue,
                    Some(itr) => match itr.next() {
                        None => {
                            line_itr[picked] = None;
                            finished += 1;
                            if finished == self.series_refs.len() {
                                break;
                            }
                        }
                        Some(line) => {
                            let line = line.expect("cannot read line");
                            let item: Item = serde_json::from_str(&line)
                                .expect("unrecognized data item in file");

                            s.send(IngestionSample {
                                timestamp: item.timestamp,
                                value: item.value,
                                serid: self.series_ids[picked],
                                refid: self.series_refs[picked],
                            })
                            .expect("failed to send sample to writer");
                        }
                    },
                }
            }

            drop(s);
        });

        writer.join();
        loader.join();
    }
}

struct Microbench<B: PersistentListBackend> {
    mach: Mach<B>,
    data_src: DataSrc,
    writer_map: HashMap<WriterId, BenchWriterMeta>,
}

impl<B: PersistentListBackend> Microbench<B> {
    fn new(mach: Mach<B>, data_src: DataSrc, nthreads: usize) -> Self {
        let writer_map = HashMap::new();
        for _ in 0..nthreads {
            let writer = mach.new_writer().expect("writer creation failed");
            writer_map.insert(writer.id(), BenchWriterMeta::new(writer));
        }

        Self {
            mach,
            data_src,
            writer_map,
        }
    }

    fn with_n_series(&self, n_series: usize) {
        for _ in 0..n_series {
            let config = SeriesConfig {
                tags: Tags::from(HashMap::new()),
                compression: Compression::from(vec![CompressFn::BytesLZ4]),
                seg_count: NSEGMENTS,
                nvars: 1,
                types: vec![Types::Bytes],
            };

            let (writer_id, series_id) = self
                .mach
                .add_series(config)
                .expect("add new series failed unexpectedly");

            let bench_writer = self
                .writer_map
                .get_mut(&writer_id)
                .expect("writer_map is incomplete");

            bench_writer.register_series(series_id, self.data_src.pick_from_series_id(series_id));
        }
    }

    fn run(&self) {
        let handles = Vec::new();

        for (id, mut writer) in self.writer_map {
            handles.push(thread::spawn(|| writer.run()));
        }

        println!("Waiting for ingestion to finish");
        handles.drain(..).for_each(|h| h.join().unwrap());
        println!("Finished!");
    }
}

fn main() {
    clean_outdir();

    let mut mach = Mach::<FileBackend>::new();
    let datasrc = DataSrc::new(LOGSPATH.as_path());

    let bench = Microbench::new(mach, datasrc, NTHREADS);
    bench.with_n_series(NTHREADS * NSERIES);

    bench.run();
}
