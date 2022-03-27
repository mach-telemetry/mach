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

mod config;
#[macro_use]
mod rdtsc;
mod zipf;

use config::*;
use lazy_static::lazy_static;
use mach::{
    compression::{CompressFn, Compression},
    id::{SeriesId, SeriesRef, WriterId},
    persistent_list::{FileBackend, PersistentListBackend},
    sample::Type,
    series::{SeriesConfig, Types},
    tags::Tags,
    tsdb::{self, Mach},
    utils::bytes::Bytes,
    writer::Writer,
};
use num_format::{Locale, ToFormattedString};
use rand::Rng;
use rtrb::{Consumer, Producer, RingBuffer};
use serde::*;
use serde_json::*;
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
use zipf::*;
include!(concat!(env!("OUT_DIR"), "/type_defs.rs"));

lazy_static! {
    //static ref TOTAL_RATE: Arc<Mutex<f64>> = Arc::new(Mutex::new(0.0f64));
    static ref SAMPLE_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static ref BARRIERS: Arc<Barrier> = Arc::new(Barrier::new(CONF.threads));
    static ref CONF: Config = load_conf();
}

fn clean_outdir() {
    std::fs::remove_dir_all(&CONF.out_path);
    std::fs::create_dir_all(&CONF.out_path).unwrap();
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
                        .expect("datasrc file name not a valid string")
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
        let mut z = Zipfian::new(size, CONF.zipf);

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

//#[derive(Serialize, Deserialize)]
//struct Item {
//    timestamp: u64,
//    value: String,
//}
//
//impl Item {
//    fn from_str(s: &String) -> Item {
//        let item: Item = serde_json::from_str(s).expect("cannot parse data item");
//        item
//    }
//}

struct IngestionSample {
    timestamp: u64,
    value: Vec<Type>,
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
                        .open(CONF.data_path.join(name))
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
        //let mut idle_cycles = 0;
        //let mut idle_start = None;

        let start = rdtsc!();

        let mut counter: usize = 0;
        let mut miss: usize = 0;
        let start = rdtsc!();
        let mut busy: u64 = 0;
        let mut pop_busy: u64 = 0;
        let mut idle: u64 = 0;
        while c < CONF.samples_per_thread {
            let now = rdtsc!();
            let r = self.r.pop();
            pop_busy += rdtsc!() - now;

            match r {
            //match self.r.pop() {
                Ok(sample) => {
                    counter += 1;
                    let now = rdtsc!();
                    //if idle_start.is_some() {
                    //    let idle_end = rdtsc!();
                    //    idle_cycles += idle_end - idle_start.unwrap();
                    //    idle_start = None;
                    //}

                    let r = self
                        .writer
                        .push_type(sample.refid, sample.timestamp, &sample.value);
                    if r.is_ok() {
                        c += 1;
                        SAMPLE_COUNTER.fetch_add(1, SeqCst);
                    }
                    busy += rdtsc!() - now;
                    //busy = busy + now.elapsed();
                }
                Err(..) => {
                    counter += 1;
                    miss += 1;
                    //if idle_start.is_none() {
                    //    idle_start = Some(rdtsc!());
                    //}
                }
            }
        }
        let dur = rdtsc::cycles_to_seconds(rdtsc!() - start);
        let busy = rdtsc::cycles_to_seconds(busy);
        let pop_busy = rdtsc::cycles_to_seconds(pop_busy);


        //let end = rdtsc!();
        //let dur = end - start;
        println!("overall loops: {}", counter.to_formatted_string(&Locale::en));
        println!("loops that missed: {}", miss.to_formatted_string(&Locale::en));
        println!("overall time: {:.2}s", dur);
        println!("time doing push: {:.2}s", busy);
        println!("time doing pop: {:.2}s", pop_busy);

        self.notify_completed()
    }

    fn notify_completed(&mut self) {
        self.c.push(()).expect("cannot notify complete twice");
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

            let item = Item::from_str(&line_bufs[i]);
            ts_min.push(item.timestamp);
            ts_max.push(item.timestamp);
        }

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

            let item = Item::from_str(&line_bufs[picked]);

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

            let value = item.value_types();
            match self.s.push(IngestionSample {
                timestamp,
                value,
                serid: self.series_ids[picked],
                refid: self.series_refs[picked],
            }) {
                Ok(_) => (),
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
            let (types, compression) = schema();
            let config = SeriesConfig {
                tags: Tags::from(HashMap::new()),
                compression: Compression::from(compression),
                seg_count: CONF.segments,
                nvars: types.len(),
                types,
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
    //&*CONF;

    std::thread::spawn(|| {
        let mut current_count = 0;
        let dur = std::time::Duration::from_secs(1);
        let total = CONF.samples_per_thread * CONF.threads;
        loop {
            std::thread::sleep(dur);
            let count = SAMPLE_COUNTER.swap(0, SeqCst);
            current_count += count;
            let completion = current_count as f64 / total as f64;
            //let m = (count - last_count) as f64 / 1_000_000.0;
            println!(
                "{} samples per second, completed {:.2}%",
                count.to_formatted_string(&Locale::en),
                completion * 100.
            );
        }
    });

    let conf = tsdb::Config::default().with_directory(CONF.out_path.clone());
    let mut mach = Mach::<FileBackend>::new(conf);
    let datasrc = DataSrc::new(CONF.data_path.as_path());

    let mut bench = Microbench::new(mach, datasrc, CONF.threads);
    bench.with_n_series(CONF.threads * CONF.series_per_thread);

    let now = Instant::now();
    bench.run();
    let dur = now.elapsed();

    println!(
        "Elapsed: {:.2?}, {:.2} samples/sec for 1 thread",
        dur,
        CONF.samples_per_thread as f32 / dur.as_secs_f32()
    );
}
