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
use rtrb::{Consumer, Producer, RingBuffer};
use serde::*;
use std::fs;
use std::io;
use std::marker::PhantomData;
use std::{
    collections::HashMap,
    convert::TryInto,
    fs::File,
    fs::OpenOptions,
    io::prelude::*,
    io::BufReader,
    io::SeekFrom,
    iter,
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
const NSERIES: usize = 100_000;
const NSEGMENTS: usize = 1;
const N_SAMPLES_PER_THR: usize = 10_000_000;

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

type DataSrcName = String;

struct DataSrc {
    src_names: Vec<DataSrcName>,
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

    fn pick_from_series_id(&self, series_id: SeriesId) -> &DataSrcName {
        &self.src_names[*series_id as usize % self.src_names.len()]
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

/// BufReader with seek_set capability.
struct SeekableBufReader<R> {
    reader: BufReader<R>,
    /// Seek location.
    offset: i64,
}

impl<R: Read + Seek> SeekableBufReader<R> {
    fn new(inner: R) -> Self {
        Self {
            reader: BufReader::new(inner),
            offset: 0,
        }
    }

    fn read_line(&mut self, buf: &mut String) -> io::Result<usize> {
        let nread = self.reader.read_line(buf)?;
        self.offset += nread as i64;
        Ok(nread)
    }

    fn seek_relative(&mut self, offset: i64) -> io::Result<()> {
        match self.reader.seek_relative(offset) {
            Ok(_) => {
                self.offset += offset;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn seek_set(&mut self, offset: i64) -> io::Result<()> {
        let offset_relative = offset - self.offset;
        self.seek_relative(offset_relative)
    }
}

/// Manages one file reader per file for a list of files, with potentially
/// duplicated file entries.
struct ReaderSet {
    readers: Vec<SeekableBufReader<File>>,
    refs: Vec<usize>,
}

impl ReaderSet {
    fn new(filenames: &Vec<DataSrcName>) -> Self {
        let mut readers = Vec::new();
        let mut refs = Vec::<usize>::new();
        let mut file_reader_map = HashMap::new();

        for name in filenames {
            match file_reader_map.get(name) {
                Some(idx) => refs.push(*idx),
                None => {
                    let file = OpenOptions::new()
                        .read(true)
                        .open(&*LOGSPATH.join(name))
                        .expect("could not open data file");
                    let mut reader = SeekableBufReader::new(file);
                    readers.push(reader);
                    refs.push(readers.len() - 1);
                    file_reader_map.insert(name, readers.len() - 1);
                }
            }
        }

        Self { readers, refs }
    }

    /// Gets a reader for a file, using the file's index in the `filenames`
    /// vector provided during this struct's construction.
    fn get(&mut self, refid: usize) -> &mut SeekableBufReader<File> {
        &mut self.readers[self.refs[refid]]
    }
}

struct IngestionWorker {
    writer: Writer,
    r: Consumer<IngestionSample>,
    c: Producer<()>,
}

impl IngestionWorker {
    fn new(writer: Writer, recv: Consumer<IngestionSample>, completed: Producer<()>) -> Self {
        IngestionWorker {
            writer,
            r: recv,
            c: completed,
        }
    }

    fn ingest(&mut self) {
        let mut c = 0;
        let mut i: u64 = 0;

        while c < N_SAMPLES_PER_THR {
            i += 1;
            if i % 2_000_000_000 == 0 {
                println!("progress: {}/{}", c, N_SAMPLES_PER_THR);
            }

            match self.r.pop() {
                Ok(sample) => {
                    let r = self.writer.push_type(
                        sample.refid,
                        sample.serid,
                        sample.timestamp,
                        &vec![Type::Bytes(Bytes::from_slice(sample.value.as_bytes()))],
                    );
                    if r.is_ok() {
                        c += 1;
                    }
                }
                Err(..) => (),
            }
        }
        self.notify_completed()
    }

    fn notify_completed(&mut self) {
        self.c.push(()).unwrap();
    }
}

struct DataLoader {
    s: Producer<IngestionSample>,
    terminated: Consumer<()>,
    series_refs: Vec<SeriesRef>,
    series_ids: Vec<SeriesId>,
    datasrc_names: Vec<DataSrcName>,
}

impl DataLoader {
    fn new(
        s: Producer<IngestionSample>,
        terminated: Consumer<()>,
        series_refs: Vec<SeriesRef>,
        series_ids: Vec<SeriesId>,
        datasrc_names: Vec<DataSrcName>,
    ) -> Self {
        DataLoader {
            s,
            terminated,
            series_refs,
            series_ids,
            datasrc_names,
        }
    }

    fn num_series(&self) -> usize {
        self.series_refs.len()
    }

    fn load(&mut self) {
        let mut zipf = ZipfianPicker::new(self.num_series() as u64);

        let mut readers = ReaderSet::new(&self.datasrc_names);

        let mut ts_min = Vec::new();
        let mut ts_max = Vec::new();
        let mut nwraps: Vec<u64> = iter::repeat(0).take(self.num_series()).collect();
        let mut offsets: Vec<i64> = iter::repeat(0).take(self.num_series()).collect();
        let mut line_bufs: Vec<String> = iter::repeat_with(String::new)
            .take(self.num_series())
            .collect();

        // get min timestamp in all files (assume data items are sorted chronologically)
        for i in 0..self.num_series() {
            let reader = readers.get(i);
            reader.seek_set(0);
            let nread = reader
                .read_line(&mut line_bufs[i])
                .expect("empty data file");
            offsets[i] += nread as i64;

            let item: Item = serde_json::from_str(&line_bufs[i]).expect("cannot parse data item");
            ts_min.push(item.timestamp);
            ts_max.push(item.timestamp);
        }

        let mut c = 0;
        loop {
            let picked = zipf.next();

            let mut reader = &mut readers.get(picked);
            let offset = offsets[picked];
            reader.seek_set(offset).expect("data file seek failed");

            line_bufs[picked].clear();
            let nread = reader
                .read_line(&mut line_bufs[picked])
                .expect("read line failed") as i64;
            if nread == 0 {
                nwraps[picked] += 1;
                reader.seek_set(0).unwrap();
                offsets[picked] = reader
                    .read_line(&mut line_bufs[picked])
                    .expect("read line failed unexpectedly")
                    as i64;
                if offsets[picked] == 0 {
                    panic!("unexpected empty file")
                }
            } else {
                offsets[picked] += nread;
            }

            let item: Item =
                serde_json::from_str(&line_bufs[picked]).expect("unrecognized data item in file");

            if nwraps[picked] == 0 {
                ts_max[picked] = item.timestamp;
            }

            // amount of time to shift a timestamp during wraparounds, to avoid
            // duplicated timestamps.
            let ts_shift = 100;
            let timestamp = match nwraps[picked] {
                0 => item.timestamp,
                _ => item.timestamp + (ts_max[picked] - ts_min[picked] + ts_shift) * nwraps[picked],
            };

            match self.s.push(IngestionSample {
                timestamp,
                value: item.value,
                serid: self.series_ids[picked],
                refid: self.series_refs[picked],
            }) {
                Ok(_) => c += 1,
                Err(_) => {
                    if !self.terminated.is_empty() {
                        break;
                    }
                }
            }
        }
    }
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

    fn spawn(self) -> (IngestionWorker, DataLoader) {
        let (s, r) = RingBuffer::<IngestionSample>::new(1);
        let (completed, terminated) = RingBuffer::<()>::new(1);

        (
            IngestionWorker::new(self.writer, r, completed),
            DataLoader::new(
                s,
                terminated,
                self.series_refs,
                self.series_ids,
                self.datasrc_names,
            ),
        )
    }
}

struct Microbench<B: PersistentListBackend> {
    mach: Mach<B>,
    data_src: DataSrc,
    writer_map: HashMap<WriterId, BenchWriterMeta>,
}

impl<B: PersistentListBackend> Microbench<B> {
    fn new(mut mach: Mach<B>, data_src: DataSrc, nthreads: usize) -> Self {
        let mut writer_map = HashMap::new();
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

    fn with_n_series(&mut self, n_series: usize) {
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

            bench_writer.register_series(
                series_id,
                self.data_src.pick_from_series_id(series_id).clone(),
            );
        }
    }

    fn run(&mut self) {
        let mut handles = Vec::new();

        for (id, writer_meta) in self.writer_map.drain() {
            let (mut writer, mut loader) = writer_meta.spawn();
            handles.push(thread::spawn(move || writer.ingest()));
            handles.push(thread::spawn(move || loader.load()));
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

    let mut bench = Microbench::new(mach, datasrc, NTHREADS);
    bench.with_n_series(NTHREADS * NSERIES);

    let now = Instant::now();
    bench.run();
    let dur = now.elapsed();

    println!(
        "Elapsed: {:.2?}, {:.2} samples / sec",
        dur,
        (N_SAMPLES_PER_THR * NTHREADS) as f32 / dur.as_secs_f32()
    );
}
