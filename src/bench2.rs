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

mod compression;
mod constants;
mod persistent_list;
mod segment;
mod tags;
mod test_utils;
mod utils;
mod write_thread;
mod zipf;

use serde::*;
use std::{
    cmp::Reverse,
    collections::HashMap,
    fs::OpenOptions,
    io::prelude::*,
    time::{Duration, Instant},
    sync::{Arc, Mutex, Barrier},
    thread,
    path::{Path, PathBuf},
};
use rand::Rng;

use compression::*;
use persistent_list::*;
use tags::*;
use write_thread::*;
use zipf::*;
use lazy_static::lazy_static;

//const DATAPATH: &str = "/Users/fsolleza/Downloads/data_json/bench1_multivariate.json";
//const OUTPATH: &str = "/Users/fsolleza/Downloads/temp_data";

const DATAPATH: &str = "/home/fsolleza/Projects/mach-bench-private/rust/mach/data/data_json/bench1_multivariate.json";
const OUTDIR: &str = "/home/fsolleza/Projects/mach-bench-private/rust/mach/data/out/";

//const DATAPATH: &str = "/data/data_json/bench1_multivariate.json";
//const OUTDIR: &str = "/data/out/";

const BLOCKING_RETRY: bool = false;
const ZIPF: f64 = 0.5;
const NSERIES: usize = 10_000;
const NTHREADS: usize = 2;
const BUFSZ: usize = 1_000_000;

lazy_static! {
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

fn read_data() -> Vec<Vec<(u64, Box<[[u8; 8]]>)>> {
    println!("LOADING DATA");
    let mut file = OpenOptions::new().read(true).open(DATAPATH).unwrap();
    let mut json = String::new();
    file.read_to_string(&mut json).unwrap();
    let mut dict: HashMap<String, DataEntry> = serde_json::from_str(json.as_str()).unwrap();
    dict.drain()
        .filter(|(_, d)| d.timestamps.len() >= 30_000)
        .map(|(_, d)| d.to_series())
        .collect()
}

fn consume<W: ChunkWriter + 'static>(persistent_writer: W) {
    // Setup write thread
    let mut write_thread = WriteThread::new(persistent_writer);

    // The buffer used by all time series in this writer
    let buffer = Buffer::new(BUFSZ);

    // Load data
    let mut base_data = &DATA;


    // Series will use fixed compression with precision of 10 bits
    let compression = Compression::Fixed(10);
    //let compression = Compression::LZ4(1);

    // Vectors to hold series-specific information
    let mut data: Vec<&[(u64, Box<[[u8; 8]]>)]> = Vec::new();
    let mut meta: Vec<SeriesMetadata> = Vec::new();
    let mut refs: Vec<usize> = Vec::new();

    // Generate series specific information, register, then collect the information into the vecs
    // each series uses 3 active segments
    // println!("TOTAL BASE_DATA {}", base_data.len());
    for i in 0..NSERIES {
        let idx = rand::thread_rng().gen_range(0..base_data.len());
        let d = &base_data[idx];
        let nvars = d[0].1.len();
        let mut tags = Tags::new();
        tags.insert((String::from("id"), format!("{}", i)));
        let series_meta = SeriesMetadata::new(tags, 1, nvars, compression, buffer.clone());
        refs.push(write_thread.register(i as u64, series_meta.clone()));
        data.push(d.as_slice());
        meta.push(series_meta);
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
    println!("RUNNING");
    let now = Instant::now();
    'outer: loop {
        let idx = selection[loop_counter % selection.len()];
        let sample = &data[idx][0];
        let ref_id = refs[idx];
        'inner: loop {
            match write_thread.push(ref_id, sample.0, &sample.1[..]) {
                Ok(_) => {
                    floats += sample.1.len();
                    data[idx] = &data[idx][1..];
                    if data[idx].len() == 0 {
                        refs.remove(idx);
                        data.remove(idx);
                    }
                    if data.len() < 10 {
                        selection = &selection1;
                    } else if data.len() < 100 {
                        selection = &selection10;
                    } else if data.len() < 1000 {
                        selection = &selection100;
                    }
                    if data.len() == 0 {
                        break 'outer;
                    }
                    break 'inner;
                }
                Err(_) => {
                    retries += 1;
                    if !BLOCKING_RETRY {
                        // If error, we'll try again next time 
                        break 'inner;
                    }
                }
            }
        }
        loop_counter += 1;
    }
    let dur = now.elapsed();
    println!("floats: {}", floats);
    println!("dur: {:?}", dur);
    let mut secs = dur.as_secs_f64();
    let rate = (floats as f64 / secs)/1_000_000.;
    println!("Rate mfps: {}", rate);
    println!("Retries: {}", retries);
    *TOTAL_RATE.lock().unwrap() += rate;
}

fn main() {
    match std::fs::remove_dir_all(OUTDIR) { _ => {} };
    std::fs::create_dir_all(OUTDIR).unwrap();
    let mut handles = Vec::new();
    for i in 0..NTHREADS {
        //let p = PathBuf::from(OUTDIR).join(format!("file_{}", i));
        //let mut persistent_writer = FileWriter::new(p).unwrap();
        let mut persistent_writer = KafkaWriter::new().unwrap();
        handles.push(thread::spawn(move || {
            consume(persistent_writer);
        }));
    }
    handles.drain(..).for_each(|h| h.join().unwrap());
    println!("TOTAL RATE: {}", TOTAL_RATE.lock().unwrap());
}
