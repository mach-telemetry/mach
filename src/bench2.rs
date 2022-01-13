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

mod compression;
mod constants;
mod persistent_list;
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
    time::{Instant, Duration},
};

use compression::*;
use dashmap::DashMap;
use lazy_static::lazy_static;
use persistent_list::*;
use seq_macro::seq;
use tags::*;
use tsdb::SeriesId;
use writer::*;
use zipf::*;

const BLOCKING_RETRY: bool = false;
const ZIPF: f64 = 0.99;
const NSERIES: usize = 10_000;
const NTHREADS: usize = 1;
const BUFSZ: usize = 1_000_000;
const NSEGMENTS: usize = 1;
const UNIVARIATE: bool = true;
const KAFKA_TOPIC: &str = "MACHSTORAGE";
const KAFKA_BOOTSTRAP: &str = "localhost:29092";
const COMPRESSION: Compression = Compression::Fixed(10);
const PARTITIONS: usize = 10;

lazy_static! {
    static ref DATAPATH: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data").join("data_json");
    static ref OUTDIR: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data").join("out");
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
        (&*DATAPATH).join("bench1_univariate.json")
    } else {
        (&*DATAPATH).join("bench1_multivariate.json")
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

fn consume<W: ChunkWriter + 'static>(
    id_counter: Arc<AtomicUsize>,
    global: Arc<DashMap<SeriesId, SeriesMetadata>>,
    persistent_writer: W,
) {
    // Setup write thread
    let mut write_thread = Writer::new(global.clone(), persistent_writer);

    // The buffer used by all time series in this writer
    let buffer = Buffer::new(BUFSZ);

    // Load data
    let mut base_data = &DATA;

    // Series will use fixed compression with precision of 10 bits
    let compression = COMPRESSION;

    // Vectors to hold series-specific information
    let mut data: Vec<&[(u64, Box<[[u8; 8]]>)]> = Vec::new();
    //let mut meta: Vec<SeriesMetadata> = Vec::new();
    let mut refs: Vec<usize> = Vec::new();

    // Generate series specific information, register, then collect the information into the vecs
    // each series uses 3 active segments
    // println!("TOTAL BASE_DATA {}", base_data.len());
    for _ in 0..NSERIES {
        let i = SeriesId(id_counter.fetch_add(1, SeqCst));
        let idx = rand::thread_rng().gen_range(0..base_data.len());
        let d = &base_data[idx];
        let nvars = d[0].1.len();
        let mut tags = Tags::new();
        tags.insert((String::from("id"), format!("{}", i.inner())));
        let series_meta = SeriesMetadata::new(tags, NSEGMENTS, nvars, compression, buffer.clone());
        global.insert(i, series_meta.clone());
        refs.push(write_thread.register(i));
        data.push(d.as_slice());
        //meta.push(series_meta);
    }

    // Change zipfian when avaiable data become less than the zipfian possible values
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
        if data.len() < 10 {
            selection = &selection1;
        } else if data.len() < 100 {
            selection = &selection10;
        } else if data.len() < 1000 {
            selection = &selection100;
        }
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
                        let res = write_thread.push_sample(ref_id, sample);
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
                    //let s = Instant::now();
                    floats += sample.1.len();
                    data[idx] = &data[idx][1..];
                    if data[idx].len() == 0 {
                        refs.remove(idx);
                        data.remove(idx);
                    }
                    //let e = s.elapsed();
                    //remove = remove + e;
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
    //let dur = (now.elapsed() - remove).as_secs_f64();
    let dur = rdtsc::cycles_to_seconds(cycles);
    //println!("floats: {}", floats);
    //println!("dur: {:?} seconds", dur);
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
    match std::fs::remove_dir_all(outdir) {
        _ => {}
    };
    std::fs::create_dir_all(outdir).unwrap();
    let _data = DATA.len();
    let mut handles = Vec::new();
    let mut backend = FileBackend::new(outdir.into());
    //let mut backend = KafkaBackend::new()
    //    .bootstrap_servers(KAFKA_BOOTSTRAP)
    //    .topic(KAFKA_TOPIC)
    //    .partitions(PARTITIONS);
    //let mut backend = VectorBackend::new();
    let global = Arc::new(DashMap::new());
    let id_counter = Arc::new(AtomicUsize::new(0));
    for i in 0..NTHREADS {
        let (mut persistent_writer, _) = backend.make_backend().unwrap();
        let id_counter = id_counter.clone();
        let global = global.clone();
        handles.push(thread::spawn(move || {
            consume(id_counter, global, persistent_writer);
        }));
    }
    println!("Waiting for ingestion to finish");
    handles.drain(..).for_each(|h| h.join().unwrap());
    println!("TOTAL RATE: {}", TOTAL_RATE.lock().unwrap());
}
