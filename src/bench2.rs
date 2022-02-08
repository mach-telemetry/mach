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

mod compression;
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
const NSEGMENTS: usize = 1;
const UNIVARIATE: bool = true;
//const COMPRESSION: Compression = Compression::XOR;
const COMPRESSION: Compression = Compression::Fixed(10);
//const COMPRESSION: Compression = Compression::Decimal(3);
const PARTITIONS: usize = 10;

lazy_static! {
    static ref DATAPATH: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
    static ref BENCHPATH: PathBuf = DATAPATH.join("bench_data");
    static ref METRICSPATH: PathBuf = BENCHPATH.join("metrics");
    static ref LOGSPATH: PathBuf = BENCHPATH.join("logs");
    static ref OUTDIR: PathBuf = DATAPATH.join("out");
    static ref DATA: Vec<Vec<(u64, Box<[[u8; 8]]>)>> = read_data();
    static ref TOTAL_RATE: Arc<Mutex<f64>> = Arc::new(Mutex::new(0.0f64));
    static ref BARRIERS: Arc<Barrier> = Arc::new(Barrier::new(NTHREADS));
}

#[derive(Serialize, Deserialize)]
struct DataEntry {
    timestamps: Vec<u64>,
    values: HashMap<String, Vec<f64>>,
}

impl DataEntry {
    fn to_series(self) -> Vec<(u64, Box<[[u8; 8]]>)> {
        let mut res = Vec::new();
        for i in 0..self.timestamps.len() {
            let ts = self.timestamps[i];
            let mut values: Vec<[u8; 8]> = Vec::new();
            for (_, var) in self.values.iter() {
                values.push(var[i].to_be_bytes());
            }
            res.push((ts, values.into_boxed_slice()));
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

fn read_data() -> Vec<Vec<(u64, Box<[[u8; 8]]>)>> {
    let mut dict = load_data();
    println!("MAKING DATA");
    dict.drain()
        .filter(|(_, d)| d.timestamps.len() >= 30_000)
        .map(|(_, d)| d.to_series())
        .collect()
}

fn consume(mut writer: Writer, mut data: Vec<&[(u64, Box<[[u8; 8]]>)]>, mut refs: Vec<usize>) {
    let mut z1000 = Zipfian::new(1000, ZIPF);
    let mut z100 = Zipfian::new(100, ZIPF);
    let mut z10 = Zipfian::new(10, ZIPF);
    let mut selection1000: Vec<usize> = (0..1000).map(|_| z1000.next_item() as usize).collect();
    let mut selection100: Vec<usize> = (0..1000).map(|_| z100.next_item() as usize).collect();
    let mut selection10: Vec<usize> = (0..1000).map(|_| z10.next_item() as usize).collect();
    let mut selection1: Vec<usize> = (0..1000).map(|_| 0).collect();

    let mut selection = &selection1000;
    let mut loop_counter = 0;
    let mut floats = 0;
    let mut retries = 0;

    BARRIERS.wait();

    let mut cycles: u64 = 0;
    let now = Instant::now();
    let mut remove = Duration::from_secs(0);

    'outer: loop {
        selection = match data.len() {
            1..=9 => &selection1,
            10..=99 => &selection10,
            100..=999 => &selection100,
            _ => &selection1000,
        };
        let idx = selection[loop_counter % selection.len()];
        let sample = &data[idx][0];
        let ref_id = refs[idx];
        'inner: loop {
            seq!(N in 1..10 {
                let (start, res) = match sample.1.len() {
                    #(
                    N => {
                        let sample: Sample<N> = Sample {
                            timestamp: sample.0,
                            values: (*sample.1).try_into().unwrap()
                        };
                        let start = rdtsc!();
                        let res = writer.push_sample(ref_id, sample);
                        (start, res)
                    },
                    )*
                    _ => unimplemented!()
                };
            });

            match res {
                Ok(_) => {
                    let end = rdtsc!();
                    cycles += end - start;
                    floats += sample.1.len();
                    data[idx] = &data[idx][1..];
                    if data[idx].len() == 0 {
                        refs.remove(idx);
                        data.remove(idx);
                    }
                    if data.len() == 0 {
                        break 'outer;
                    }
                    break 'inner;
                }
                Err(_) => {
                    retries += 1;
                    let end = rdtsc!();
                    cycles += end - start;
                    if !BLOCKING_RETRY {
                        // If error, we'll try again next time
                        break 'inner;
                    }
                }
            }
        }

        loop_counter += 1;
    }
    println!("Gross duration (including other crap) {:?}", now.elapsed());
    let dur = rdtsc::cycles_to_seconds(cycles);
    let rate = (floats as f64 / dur) / 1_000_000.;
    //println!("Rate mfps: {}", rate);
    println!("Retries: {}", retries);
    *TOTAL_RATE.lock().unwrap() += rate;
}

fn file_writer(id: usize) -> FileWriter {
    let p = &*OUTDIR.join(format!("file_{}", id));
    FileWriter::new(p).unwrap()
}

fn main() {
    let outdir = &*OUTDIR;
    std::fs::remove_dir_all(outdir);

    std::fs::create_dir_all(outdir).unwrap();

    let mut handles = Vec::new();

    let backend = {
        #[cfg(feature = "file-backend")]
        let backend = FileBackend::new(outdir.into(), i.try_into().unwrap());

        #[cfg(feature = "kafka-backend")]
        let backend = KafkaBackend::new(KAFKA_BOOTSTRAP);

        #[cfg(feature = "redis-backend")]
        let backend = RedisBackend::new(REDIS_ADDR);

        #[cfg(feature = "vector-backend")]
        let backend = VectorBackend::new();

        backend
    };
    let mut mach = Mach::new(backend).expect("should be able to instantiate Mach");

    let mut writers_map = (0..NTHREADS)
        .map(|_| mach.add_writer())
        .map(|r| r.expect("should be able to instantiate writer"))
        .fold(HashMap::new(), |mut map, w| {
            map.insert(w.id(), w);
            map
        });

    let mut refmap: HashMap<WriterId, Vec<usize>> =
        writers_map.keys().fold(HashMap::new(), |mut map, wid| {
            map.insert(*wid, Vec::new());
            map
        });
    let mut datamap: HashMap<WriterId, Vec<&[(u64, Box<[[u8; 8]]>)]>> =
        writers_map.keys().fold(HashMap::new(), |mut map, wid| {
            map.insert(*wid, Vec::new());
            map
        });

    for _ in 0..NSERIES * NTHREADS {
        let idx: usize = rand::thread_rng().gen_range(0..DATA.len());
        let d = &DATA[idx];
        let nvars = d[0].1.len();

        // TODO: tags do not contain series id yet
        let tags = Tags::from(HashMap::new());

        let series_config = SeriesConfig {
            compression: COMPRESSION,
            seg_count: NSEGMENTS,
            nvars,
            tags,
        };

        let (series_id, writerid) = mach
            .register(series_config)
            .expect("add series should succeed");

        let writer = writers_map
            .get_mut(&writerid)
            .expect("writer should've been created");

        let refid = writer.register(series_id);
        refmap.get_mut(&writerid).unwrap().push(refid);
        datamap.get_mut(&writerid).unwrap().push(d.as_slice());
    }

    for (writer_id, writer) in writers_map {
        let mut data = datamap.remove(&writer_id).unwrap();
        let mut refs = refmap.remove(&writer_id).unwrap();
        handles.push(thread::spawn(move || consume(writer, data, refs)));
    }

    println!("Waiting for ingestion to finish");
    handles.drain(..).for_each(|h| h.join().unwrap());
    println!("TOTAL RATE: {}", TOTAL_RATE.lock().unwrap());
}
