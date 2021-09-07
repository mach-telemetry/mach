use crate::{
    active_block::{ActiveBlock, ActiveBlockWriter},
    active_segment::{ActiveSegment, ActiveSegmentWriter},
    block::{
        file::{FileBlockLoader, FileStore, ThreadFileWriter},
        BlockReader, BlockStore, BlockWriter,
    },
    read_set::SeriesReadSet,
};
use dashmap::DashMap;
use std::{
    collections::HashMap,
    marker::PhantomData,
    path::Path,
    sync::{Arc, RwLock},
};

pub type Dt = u64;
pub type Fl = f64;

#[derive(Hash, Copy, Clone, PartialEq, Eq)]
pub struct SeriesId(u64);

#[derive(Copy, Clone)]
pub struct SeriesOptions {
    nvars: usize,
}

pub struct Sample {
    pub ts: Dt,
    pub values: Box<[Fl]>,
}

#[derive(Clone)]
struct MemSeries {
    active_segment: ActiveSegment,
    active_block: ActiveBlock,
    snapshot_lock: Arc<RwLock<()>>,
}

impl MemSeries {
    fn new(nvars: usize) -> Self {
        Self {
            active_segment: ActiveSegment::new(nvars),
            active_block: ActiveBlock::new(),
            snapshot_lock: Arc::new(RwLock::new(())),
        }
    }
}

struct SeriesMetadata {
    options: SeriesOptions,
    mem_series: MemSeries,
    _thread_id: usize,
}

pub struct Db<B, W, R>
where
    B: BlockStore<W, R>,
    W: BlockWriter,
    R: BlockReader,
{
    // Global metadata information
    map: DashMap<SeriesId, SeriesMetadata>,
    threads: Vec<WriterMetadata<B, W, R>>,

    // File storage information
    file_store: B,
    _w: PhantomData<W>,
    _r: PhantomData<R>,
}

impl Db<FileStore, ThreadFileWriter, FileBlockLoader> {
    pub fn new<P: AsRef<Path>>(dir: P, threads: usize) -> Self {
        let file_store = FileStore::new(dir);
        Self {
            map: DashMap::new(),
            threads: (0..threads).map(|_| WriterMetadata::new(file_store.clone())).collect(),
            file_store,
            _w: PhantomData,
            _r: PhantomData,
        }
    }

    pub fn add_series(&self, id: SeriesId, options: SeriesOptions) -> usize {
        let thread_id = id.0 as usize % self.threads.len(); // this is probably where we'll have to futz with autoscaling
        let mem_series = MemSeries::new(options.nvars);
        self.threads[thread_id].add_series(id, mem_series.clone());
        let meta = SeriesMetadata { options, mem_series, _thread_id: thread_id };
        self.map.insert(id, meta);
        thread_id
    }

    pub fn snapshot(&self, id: SeriesId) -> Result<SeriesReadSet<FileBlockLoader>, &'static str> {
        let metadata = self.map.get(&id).ok_or("id not found")?;
        let segment = metadata.mem_series.active_segment.snapshot();
        let block = metadata.mem_series.active_block.snapshot();
        let blocks = self.file_store.reader(id).ok_or("ID not found")?;
        Ok(SeriesReadSet::new(metadata.options, segment, block, blocks))
    }
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

    fn add_series(&self, id: SeriesId, ser: MemSeries) {
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
    active_blocks: Vec<(Arc<RwLock<()>>, ActiveBlockWriter)>,
    _block_writer: W,
}

impl<W: BlockWriter> Writer<W> {
    fn new(series_map: Arc<DashMap<SeriesId, MemSeries>>, block_writer: W) -> Self {
        Self {
            series_map,
            ref_map: HashMap::new(),
            active_segments: Vec::new(),
            active_blocks: Vec::new(),
            _block_writer: block_writer,
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
            self.active_blocks
                .push((snapshot_lock, active_block_writer));
            Some(result)
        } else {
            None
        }
    }

    pub fn push(&mut self, id: usize, sample: Sample) -> Result<(), &'static str> {
        let active_segment = &mut self.active_segments[id];
        let full = active_segment.push(sample);
        if full {
            let x = self.active_blocks[id].0.write();
            let _ = active_segment.yield_replace();
            drop(x)
        }
        Ok(())
    }
}
