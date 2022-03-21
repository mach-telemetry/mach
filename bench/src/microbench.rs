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

mod zipf;

use dashmap::DashMap;
use lazy_static::lazy_static;
use rand::Rng;
use seq_macro::seq;
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

use zipf::*;

use mach::{
    compression::*,
    constants::*,
    id::*,
    persistent_list::*,
    sample::*,
    series::{SeriesConfig, Types},
    tags::*,
    tsdb::Mach,
    writer::*,
};

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
    static ref LOGSPATH: PathBuf = BENCHPATH.join("logs");
    static ref OUTDIR: PathBuf = DATAPATH.join("out");
    static ref DATA: Vec<Vec<RawSample>> = read_data();
    static ref TOTAL_RATE: Arc<Mutex<f64>> = Arc::new(Mutex::new(0.0f64));
    static ref BARRIERS: Arc<Barrier> = Arc::new(Barrier::new(NTHREADS));
}

struct RawSample(
    u64,         // timestamp
    Box<[Type]>, // data
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
            let mut values: Vec<Type> = Vec::new();
            for (_, var) in self.values.iter() {
                values.push(Type::F64(var[i]));
            }
            res.push(RawSample(ts, values.into_boxed_slice()));
        }
        res
    }
}

fn load_data() -> HashMap<String, DataEntry> {
    println!("LOADING DATA");
    let file_path = if UNIVARIATE {
        METRICSPATH.join("univariate.json")
    } else {
        METRICSPATH.join("multivariate.json")
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

struct IngestionWorker {
    writer: Writer,
    num_pushed: usize,

    // Vectors below are index-aligned; items at the same index correspond to
    // the same time series.
    //
    /// SeriesIds for each time series
    seriesids: Vec<u64>,
    /// RefIDs for each time series
    refids: Vec<usize>,
    /// Ingestion source data. Samples are reused during ingestion.
    series: Vec<&'static [RawSample]>,
    /// Offset pointing to current ingestion progress for each time series.
    next: Vec<usize>,
    /// Number of times each series's circular buffer has been wrapped around.
    wraparounds: Vec<usize>,
}

impl IngestionWorker {
    fn new(writer: Writer) -> Self {
        IngestionWorker {
            writer,
            seriesids: Vec::new(),
            refids: Vec::new(),
            series: Vec::new(),
            next: Vec::new(),
            wraparounds: Vec::new(),
            num_pushed: 0,
        }
    }

    fn register_series(&mut self, series_id: SeriesId, series_data: &'static Vec<RawSample>) {
        let refid = self.writer.get_reference(series_id);

        self.seriesids.push(*series_id);
        self.refids.push(*refid);
        self.series.push(series_data);
        self.next.push(0);
        self.wraparounds.push(0);
    }

    fn ingest(&mut self) {
        if self.did_ingest() {
            panic!("ingest() cannot be invoked multiple times on an ingestion worker.")
        }

        let mut zipf_picker = ZipfianPicker::new(self.series.len() as u64);

        let now = Instant::now();

        let mut interval = Instant::now();
        let mut last_num_pushed = 0;
        while self.num_pushed < NUM_INGESTS_PER_THR {
            if self.num_pushed > last_num_pushed && self.num_pushed % 1_000_000 == 0 {
                last_num_pushed = self.num_pushed;
                let elapsed = interval.elapsed();
                println!("Rate: {}", 1_000_000. / elapsed.as_secs_f64());
                interval = Instant::now();
            }
            match self._ingest_sample(&mut zipf_picker) {
                Ok(..) => self.num_pushed += 1,
                Err(..) => continue,
            }
        }

        let elapsed = now.elapsed().as_secs_f64();

        println!("Elapsed seconds: {}", elapsed);
        println!(
            "Number of samples per second: {}",
            NUM_INGESTS_PER_THR as f64 / elapsed
        );
    }

    fn _ingest_sample(
        &mut self,
        mut zipf_picker: &mut ZipfianPicker,
    ) -> Result<(), mach::writer::Error> {
        let victim = zipf_picker.next();
        let series = self.series[victim];
        let seriesid = self.seriesids[victim];
        let refid = self.refids[victim];
        let raw_sample = &series[self.next[victim]];

        // shift each sample's timestamp forward in time to avoid duplicated timestamps.
        let ts_offset = series.last().unwrap().0 - series[0].0 + series[1].0 - series[0].0;
        let timestamp = raw_sample.0 + ts_offset * self.wraparounds[victim] as u64;

        self.writer
            .push_type(SeriesRef(refid), timestamp, &raw_sample.1[..])?;

        match self.next[victim] {
            _ if self.next[victim] == series.len() - 1 => {
                self.next[victim] = 0;
                self.wraparounds[victim] += 1;
            }
            _ => self.next[victim] += 1,
        }
        Ok(())
    }

    fn did_ingest(&self) -> bool {
        self.num_pushed != 0
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
            let writer = mach.new_writer().expect("should be able to add new writer");
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
            tags: Tags::from(HashMap::new()),
            compression,
            seg_count: NSEGMENTS,
            nvars,
            types: vec![Types::F64; nvars],
        };

        let (writer_id, series_id) = self
            .mach
            .add_series(series_config)
            .expect("should add new series without error.");

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

    //let backend = FileBackend::new();
    let mut mach = Mach::<FileBackend>::new();

    let mut ingestion_meta = IngestionMetadata::new(&mut mach, NTHREADS, &DATA);

    for _ in 0..NSERIES * NTHREADS {
        ingestion_meta.add_series();
    }

    let mut handles = Vec::new();
    for (writer_id, mut ingestion_worker) in ingestion_meta.writer_data_map {
        handles.push(thread::spawn(move || ingestion_worker.ingest()));
    }

    println!("Waiting for ingestion to finish");
    handles.drain(..).for_each(|h| h.join().unwrap());
    println!("Finished!");
}
