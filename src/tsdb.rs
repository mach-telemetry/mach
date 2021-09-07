use crate::{
    active_segment::{ActiveSegment, ActiveSegmentWriter},
    active_block::{ActiveBlock, ActiveBlockWriter},
    block::{
        BlockStore,
        BlockWriter,
        BlockReader,
        BlockKey,
        file::{
            FileStore,
            ThreadFileWriter,
            FileBlockLoader,
        },
    },
};
use std::{
    path::Path,
    marker::PhantomData,
    collections::HashMap,
    sync::{Arc, Mutex},
};
use dashmap::DashMap;

pub type Dt = u64;
pub type Fl = f64;

#[derive(Hash, Copy, Clone, PartialEq, Eq)]
pub struct SeriesId(u64);

#[derive(Copy, Clone)]
pub struct SeriesOptions {
    nvars: usize
}

pub struct Sample {
    pub ts: Dt,
    pub values: Box<[Fl]>,
}

#[derive(Copy, Clone)]
struct SeriesMetadata {
    varopts: SeriesOptions,
    thread_id: usize,
}

pub struct Db<B, W, R>
where
    B: BlockStore<W, R>,
    W: BlockWriter,
    R: BlockReader,
{

    // Global metadata information
    map: Arc<DashMap<SeriesId, SeriesMetadata>>,
    threads: Vec<WriterMetadata<B, W, R>>,

    // File storage information
    file_store: B,
    _w: PhantomData<W>,
    _r: PhantomData<R>,
}

impl Db<FileStore, ThreadFileWriter, FileBlockLoader> {
    pub fn new<P: AsRef<Path>>(dir: P, threads: usize) -> Self {
        Self {
            map: DashMap::new(),
            threads: Vec::new(),
            file_store: FileStore::new(dir),
            _w: PhantomData,
            _r: PhantomData,
        }
    }

    pub fn add_series(&mut self, id: SeriesId, varopts: SeriesOptions) -> usize {
        let thread_id = id.0 as usize % self.threads.len(); // this is probably where we'll have to futz with autoscaling
        let meta = SeriesMetadata {
            varopts,
            thread_id
        };
        self.threads[thread_id].add_series(id, meta);
        thread_id
    }
}

struct MemSeries {
    active_segment: ActiveSegment,
    active_block: ActiveBlock,
    snapshot_lock: Arc<Mutex<()>>,
}

pub struct WriterMetadata<B, W, R>
where
    B: BlockStore<W, R>,
    W: BlockWriter,
    R: BlockReader,
{
    map: Arc<DashMap<SeriesId, MemSeries>>,

    // File storage information
    file_store: B,
    _w: PhantomData<W>,
    _r: PhantomData<R>,
}

impl WriterMetadata<FileStore, ThreadFileWriter, FileBlockLoader> {
    fn new(file_store: FileStore) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            file_store,
            _w: PhantomData,
            _r: PhantomData,
        }
    }

    fn add_series(&self, id: SeriesId, metadata: SeriesMetadata) {
        let ser = MemSeries {
            active_segment: ActiveSegment::new(metadata.varopts.nvars),
            active_block: ActiveBlock::new(),
            snapshot_lock: Arc::new(Mutex::new(())),
        };
        self.map.insert(id, ser);
    }

    pub fn writer(&self) -> Writer<ThreadFileWriter> {
        Writer::new(self.map.clone(), self.file_store.writer())
    }
}

pub struct Writer<W: BlockWriter> {
    series_map: Arc<DashMap<SeriesId, MemSeries>>,
    ref_map: HashMap<SeriesId, usize>,
    active_segments: Vec<ActiveSegmentWriter>,
    active_blocks: Vec<(Arc<Mutex<()>>, ActiveBlockWriter)>,
    block_writer: W,
}

impl<W: BlockWriter> Writer<W> {
    fn new(series_map: Arc<DashMap<SeriesId, MemSeries>>, block_writer: W) -> Self {
        Self {
            series_map,
            ref_map: HashMap::new(),
            active_segments: Vec::new(),
            active_blocks: Vec::new(),
            block_writer,
        }
    }

    pub fn lookup(&mut self, id: SeriesId) -> Option<usize> {
        if let Some(idx) = self.ref_map.get(&id) {
            Some(*idx)
        } else if let Some(ser) = self.series_map.get_mut(&id) {
            let result = self.active_segments.len();
            self.active_segments.push(ser.active_segment.writer());
            let active_block_writer = ser.active_block.writer();
            let snapshot_lock = ser.snapshot_lock.clone();
            self.active_blocks.push((snapshot_lock, active_block_writer));
            Some(result)
        } else {
            None
        }
    }

    pub fn push(&mut self, id: usize, sample: Sample) -> Result<(), &'static str> {
        let full = self.active_segments[id].push(sample);
        Ok(())
    }
}
