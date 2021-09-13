use crate::{
    active_block::{ActiveBlock, ActiveBlockWriter},
    active_segment::{ActiveSegment, ActiveSegmentWriter, SEGSZ},
    block::{
        file::{FileBlockLoader, FileStore, ThreadFileWriter},
        BlockReader, BlockStore, BlockWriter, BLOCKSZ,
    },
    compression::Compression,
    read_set::SeriesReadSet,
    segment::SegmentLike,
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
pub struct SeriesId(pub u64);

#[derive(Clone)]
pub struct SeriesOptions {
    pub nvars: usize,
    pub compressor: Compression,
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

#[derive(Clone)]
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
            threads: (0..threads)
                .map(|_| WriterMetadata::new(file_store.clone()))
                .collect(),
            file_store,
            _w: PhantomData,
            _r: PhantomData,
        }
    }

    pub fn add_series(&self, id: SeriesId, options: SeriesOptions) -> usize {
        // TODO: Optimizations
        // 1. Autoscaling can be performed somewhere by moving writers around the writer metadata
        let thread_id = id.0 as usize % self.threads.len();
        let mem_series = MemSeries::new(options.nvars);
        let meta = SeriesMetadata {
            options,
            mem_series,
            _thread_id: thread_id,
        };
        self.threads[thread_id].add_series(id, meta.clone());
        self.map.insert(id, meta);
        thread_id
    }

    pub fn snapshot(&self, id: SeriesId) -> Result<SeriesReadSet<FileBlockLoader>, &'static str> {
        let metadata = self.map.get(&id).ok_or("id not found")?;
        let segment = metadata.mem_series.active_segment.snapshot();
        let block = metadata.mem_series.active_block.snapshot();
        let blocks = self.file_store.reader(id).ok_or("ID not found")?;
        Ok(SeriesReadSet::new(
            metadata.options.clone(),
            segment,
            block,
            blocks,
        ))
    }
}

pub struct WriterMetadata<B, W, R>
where
    B: BlockStore<W, R>,
    W: BlockWriter,
    R: BlockReader,
{
    map: Arc<DashMap<SeriesId, SeriesMetadata>>,

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

    fn add_series(&self, id: SeriesId, ser: SeriesMetadata) {
        self.map.insert(id, ser);
    }

    pub fn writer(&self) -> Writer<ThreadFileWriter> {
        Writer::new(self.map.clone(), self.file_store.writer())
    }
}

pub struct Writer<W: BlockWriter> {
    series_map: Arc<DashMap<SeriesId, SeriesMetadata>>,
    ref_map: HashMap<SeriesId, usize>,

    // Reference these items in the push method
    active_segments: Vec<ActiveSegmentWriter>,
    snapshot_locks: Vec<Arc<RwLock<()>>>,
    series_options: Vec<SeriesOptions>,
    active_blocks: Vec<ActiveBlockWriter>,

    // Shared buffer for the compressor and the block writer
    buf: [u8; BLOCKSZ],
    block_writer: W,
}

impl<W: BlockWriter> Writer<W> {
    fn new(series_map: Arc<DashMap<SeriesId, SeriesMetadata>>, block_writer: W) -> Self {
        Self {
            series_map,
            ref_map: HashMap::new(),
            active_segments: Vec::new(),
            snapshot_locks: Vec::new(),
            series_options: Vec::new(),
            active_blocks: Vec::new(),
            buf: [0u8; BLOCKSZ],
            block_writer,
        }
    }

    pub fn lookup(&mut self, id: SeriesId) -> Option<usize> {
        if let Some(idx) = self.ref_map.get(&id) {
            Some(*idx)
        } else if let Some(ser) = self.series_map.get_mut(&id) {
            let result = self.active_segments.len();
            self.active_segments
                .push(ser.mem_series.active_segment.writer());
            self.snapshot_locks
                .push(ser.mem_series.snapshot_lock.clone());
            self.active_blocks
                .push(ser.mem_series.active_block.writer());
            self.series_options.push(ser.options.clone());
            Some(result)
        } else {
            None
        }
    }

    // TODO: Optimization + API
    // 1. Return ThreadID? Last serialized timestamp?
    pub fn push(
        &mut self,
        series_id: SeriesId,
        id: usize,
        sample: Sample,
    ) -> Result<(), &'static str> {
        let active_segment = &mut self.active_segments[id];
        let segment_len = active_segment.push(sample);
        if segment_len == SEGSZ {
            // TODO: Optimizations:
            // 1. minimize mtx_guard by not yielding and replacing until after compression and
            //    flushing
            // 2. concurrent compress + flush

            // Get block information and take the snapshot lock
            let mtx_guard = self.snapshot_locks[id].write();

            // Take segment
            let segment = active_segment.yield_replace();
            let (mint, maxt) = {
                let timestamps = segment.timestamps();
                (timestamps[0], *timestamps.last().unwrap())
            };

            // And then comrpess it
            let compressor = &self.series_options[id].compressor;
            let bytes = compressor.compress(&segment, &mut self.buf[..]);

            // Write the compressed segment into the active block
            let active_block_writer = &mut self.active_blocks[id];
            if bytes > active_block_writer.remaining() {
                let block = active_block_writer.yield_replace();
                self.block_writer.write_block(
                    series_id,
                    block.mint(),
                    block.maxt(),
                    block.slice(),
                )?;
            }
            active_block_writer.push_segment(mint, maxt, &self.buf[..bytes]);
            drop(mtx_guard)
        }
        Ok(())
    }
}
