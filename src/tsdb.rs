use crate::{
    active_block::{ActiveBlock, ActiveBlockWriter},
    active_segment::{ActiveSegment, ActiveSegmentWriter},
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
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    fs::{self, OpenOptions},
    io::{Write},
};
use serde::*;

pub type Dt = u64;
pub type Fl = f64;

#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesId(pub u64);

#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq)]
pub struct RefId(pub u64);

#[derive(Clone, Serialize, Deserialize)]
pub struct SeriesOptions {
    pub nvars: usize,
    pub default_compressor: Compression,
    pub fallback_compressor: Compression,
    pub fall_back_sz: usize,

    fall_back: bool,
    max_compressed_sz: usize,
    block_bytes: usize,
    block_bytes_remaining: usize,
}

impl Default for SeriesOptions {
    fn default() -> Self {
        Self {
            nvars: 1,
            default_compressor: Compression::Simple { precision: vec![3] },
            fallback_compressor: Compression::Rows { precision: vec![3] },
            fall_back_sz: 50,
            max_compressed_sz: 0,
            block_bytes: BLOCKSZ,
            block_bytes_remaining: BLOCKSZ,
            fall_back: false,
        }
    }
}

#[derive(Clone)]
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

struct GlobalMetadata {
    map: DashMap<SeriesId, SeriesMetadata>,
    path: PathBuf,
}

impl std::ops::Deref for GlobalMetadata {
    type Target = DashMap<SeriesId, SeriesMetadata>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl std::ops::DerefMut for GlobalMetadata {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl GlobalMetadata {
    fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            map: DashMap::new(),
            path: PathBuf::from(path.as_ref()),
        }
    }

    fn flush_series_options<P: AsRef<Path>>(&self, path: P) -> Result<(), &'static str> {
        let mut data = HashMap::new();
        for item in self.map.iter() {
            let k = item.key();
            let v = item.value();
            data.insert(*k, v.options.clone());
        }
        let json = serde_json::to_string(&data).map_err(|_| "Can't parse to json")?;
        let mut f = OpenOptions::new().write(true).create(true).truncate(true).open(&path).map_err(|_| "Cant open path")?;
        f.write_all(json.as_bytes()).map_err(|_| "Cant write json")?;
        Ok(())
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
    map: GlobalMetadata,
    threads: Vec<WriterMetadata<B, W, R>>,
    dir: PathBuf,

    // File storage information
    file_store: B,
    _w: PhantomData<W>,
    _r: PhantomData<R>,
}

impl Db<FileStore, ThreadFileWriter, FileBlockLoader> {
    pub fn new<P: AsRef<Path>>(dir: P, threads: usize) -> Result<Self, &'static str> {
        let data_path = dir.as_ref().join("data");
        fs::create_dir_all(&data_path).map_err(|_| "can't create dir")?;
        let meta_path = dir.as_ref().join("meta");
        let file_store = FileStore::new(data_path);
        Ok(Self {
            map: GlobalMetadata::new(meta_path),
            threads: (0..threads)
                .map(|_| WriterMetadata::new(file_store.clone()))
                .collect(),
            file_store,
            dir: PathBuf::from(dir.as_ref()),
            _w: PhantomData,
            _r: PhantomData,
        })
    }

    pub fn load<P: AsRef<Path>>(dir: P, threads: usize) -> Result<Self, &'static str> {
        let data_path = dir.as_ref().join("data");
        fs::create_dir_all(&data_path).map_err(|_| "can't create dir")?;
        let meta_path = dir.as_ref().join("meta");

        // Load filestore information
        let file_store = FileStore::load(data_path)?;
        let threads = (0..threads).map(|_| WriterMetadata::new(file_store.clone())).collect();

        // Load meta path
        let data = fs::read_to_string(&meta_path).map_err(|_| "Can't read global metadata file")?;
        let map: HashMap<SeriesId, SeriesOptions> = serde_json::from_str(&data).map_err(|_| "Can't parse metadata file")?;

        let db = Db {
            map: GlobalMetadata::new(meta_path),
            threads,
            file_store,
            dir: PathBuf::from(dir.as_ref()),
            _w: PhantomData,
            _r: PhantomData,
        };

        // Load metadata file
        for (k, v) in map {
            db.add_series(k, v);
        }

        Ok(db)
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

    pub fn flush_series_options(&self) -> Result<(), &'static str> {
        self.map.flush_series_options(self.dir.join("meta"))
    }

    pub fn snapshot(&self, id: SeriesId) -> Result<SeriesReadSet<FileBlockLoader>, &'static str> {
        let metadata = self.map.get(&id).ok_or("id not found")?;
        let segment = metadata.mem_series.active_segment.snapshot();
        let block = metadata.mem_series.active_block.snapshot();
        let blocks = self.file_store.reader(id).ok_or("ID not found")?;
        Ok(SeriesReadSet::new(segment, block, blocks))
    }

    pub fn writer(&self, thread_id: usize) -> Writer<ThreadFileWriter> {
        self.threads[thread_id].writer()
    }

    fn _set_thread_count(&mut self, thread_count: usize) {
        let old_thread_count = self.threads.len();
        let fstore = self.file_store.clone();

        if thread_count == old_thread_count {
            return;
        }
        let series_migration_map = self._compute_series_migration_map(thread_count);

        if thread_count > old_thread_count {
            // data series migration will only be safe after new threads are allocated
            self.threads
                .resize_with(thread_count, || WriterMetadata::new(fstore.clone()));
        }

        // migrate data series
        for entry in series_migration_map.iter() {
            let serid = entry.key();
            let (old_thread_id, new_thread_id) = entry.value();
            self._move_series(*serid, *old_thread_id, *new_thread_id)
                .unwrap_or(()); // unable to move series; TODO: consider how to handle exception here
        }

        if thread_count < old_thread_count {
            // can safely delete threads b/c all data series have been migrated
            self.threads.truncate(thread_count);
        }
    }

    fn _move_series(
        &mut self,
        series_id: SeriesId,
        from_thread: usize,
        to_thread: usize,
    ) -> Result<(), String> {
        let old_thread = self.threads.get(from_thread).ok_or(format!(
            "Cannot move series '{}' in thread '{}': thread does not exist.",
            series_id.0 as usize, from_thread,
        ))?;
        let new_thread = self.threads.get(to_thread).ok_or(format!(
            "Cannot move series '{}' to thread '{}': thread does not exist.",
            series_id.0 as usize, to_thread
        ))?;

        let (_, series_meta) = old_thread.map.remove(&series_id).ok_or(format!(
            "Series '{}' not found on thread '{}'",
            series_id.0 as usize, from_thread
        ))?;

        new_thread.add_series(series_id, series_meta.clone());

        Ok(())
    }

    /// Returns a mapping that describes how data series will be migrated
    /// with the new thread count.
    ///
    /// The mapping's keys are IDs of data series that need to be moved.
    /// The mapping's values are 2-element tuples: the first value is the
    /// series' current thread ID, and the second is the thread ID to migrate to.
    ///
    /// Data series that do not need to be moved are not contained in the
    /// returned mapping.
    fn _compute_series_migration_map(
        &self,
        new_thread_count: usize,
    ) -> DashMap<SeriesId, (usize, usize)> {
        let migration_map: DashMap<SeriesId, (usize, usize)> = DashMap::new();

        for (curr_thread_id, thread) in self.threads.iter().enumerate() {
            for item in thread.map.iter() {
                let serid = item.key();
                let new_thread_id = serid.0 as usize % new_thread_count;
                if new_thread_id != curr_thread_id {
                    migration_map.insert(*serid, (curr_thread_id, new_thread_id));
                }
            }
        }

        migration_map
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
    ref_map: HashMap<SeriesId, RefId>,

    // Reference these items in the push method
    active_segments: Vec<ActiveSegmentWriter>,
    snapshot_locks: Vec<Arc<RwLock<()>>>,
    series_options: Vec<SeriesOptions>,
    active_blocks: Vec<ActiveBlockWriter>,

    // Shared buffer for the compressor and the block writer
    buf: [u8; BLOCKSZ],
    block_writer: W,
}

impl<W: BlockWriter> Drop for Writer<W> {
    fn drop(&mut self) {
        self.flush().expect("Can't flush files");
    }
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

    pub fn lookup(&mut self, id: SeriesId) -> Option<RefId> {
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
            let refid = RefId(result as u64);
            self.ref_map.insert(id, refid);
            Some(refid)
        } else {
            None
        }
    }

    pub fn flush(&mut self) -> Result<(), &'static str> {
        for (sid, rid) in self.ref_map.iter() {
            let id = rid.0 as usize;
            let series_id = *sid;
            let mtx_guard = self.snapshot_locks[id].write();

            let segment = self.active_segments[id].yield_replace();
            let (mint, maxt) = {
                let timestamps = segment.timestamps();
                (timestamps[0], *timestamps.last().unwrap())
            };

            let opts = &mut self.series_options[id];
            let active_block_writer = &mut self.active_blocks[id];

            // And then compress it
            let bytes = if opts.fall_back {
                opts.fallback_compressor
                    .compress(&segment, &mut self.buf[..])
            } else {
                opts.default_compressor
                    .compress(&segment, &mut self.buf[..])
            };

            // If there is no room in the current active block, flush the active block
            let remaining = active_block_writer.remaining();
            if bytes > remaining {
                let block = active_block_writer.yield_replace();
                self.block_writer.write_block(
                    series_id,
                    block.mint(),
                    block.maxt(),
                    block.slice(),
                )?;
                opts.max_compressed_sz = 0;
                opts.block_bytes_remaining = opts.block_bytes;
                opts.fall_back = false;
            }

            // Write compressed data into active segment, then flush
            active_block_writer.push_segment(mint, maxt, &self.buf[..bytes]);
            let block = active_block_writer.yield_replace();
            self.block_writer.write_block(
                series_id,
                block.mint(),
                block.maxt(),
                block.slice(),
            )?;
            opts.max_compressed_sz = 0;
            opts.block_bytes_remaining = opts.block_bytes;
            opts.fall_back = false;

            // Update metadata for the next push:
            // 1. Largest compressed size
            // 2. Should we use fall back compression in this next segment
            // TODO: determine the block packing heuristic. For now, if bytes remaining is smaller
            // than the maximum recorded compressed size, fall back
            opts.max_compressed_sz = opts.max_compressed_sz.max(bytes);
            opts.block_bytes_remaining -= bytes;
            if opts.block_bytes_remaining < opts.max_compressed_sz {
                //println!("SETTING FALLBACK FOR NEXT BLOCK");
                opts.fall_back = true;
                self.active_segments[id].set_capacity(opts.fall_back_sz);
            }
            drop(mtx_guard)
        }
        Ok(())
    }

    // TODO: Optimization + API
    // 1. Return ThreadID? Last serialized timestamp?
    pub fn push(
        &mut self,
        reference_id: RefId,
        series_id: SeriesId,
        sample: Sample,
    ) -> Result<(), &'static str> {
        let id = reference_id.0 as usize;
        let active_segment = &mut self.active_segments[id];
        let segment_len = active_segment.push(sample);
        if segment_len == active_segment.capacity() {

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

            let opts = &mut self.series_options[id];
            let active_block_writer = &mut self.active_blocks[id];

            // And then compress it
            // TODO: Here, we could probably use something smarter to determine how to pack blocks
            let bytes = if opts.fall_back {
                //println!("USING FALLBACK");
                opts.fallback_compressor
                    .compress(&segment, &mut self.buf[..])
            } else {
                //println!("USING DEFAULT");
                opts.default_compressor
                    .compress(&segment, &mut self.buf[..])
            };

            // If there is no room in the current active block, flush the active block
            let remaining = active_block_writer.remaining();
            //println!("BLOCK IS {}% FULL", (1.0 - (remaining as f64 / BLOCKSZ as f64)) * 100.);
            if bytes > remaining {
                //println!("FLUSHING BLOCK");
                let block = active_block_writer.yield_replace();
                self.block_writer.write_block(
                    series_id,
                    block.mint(),
                    block.maxt(),
                    block.slice(),
                )?;
                opts.max_compressed_sz = 0;
                opts.block_bytes_remaining = opts.block_bytes;
                opts.fall_back = false;
            }

            // Write compressed data into active segment
            active_block_writer.push_segment(mint, maxt, &self.buf[..bytes]);

            // Update metadata for the next push:
            // 1. Largest compressed size
            // 2. Should we use fall back compression in this next segment
            // TODO: determine the block packing heuristic. For now, if bytes remaining is smaller
            // than the maximum recorded compressed size, fall back
            opts.max_compressed_sz = opts.max_compressed_sz.max(bytes);
            opts.block_bytes_remaining -= bytes;
            if opts.block_bytes_remaining < opts.max_compressed_sz {
                //println!("SETTING FALLBACK FOR NEXT BLOCK");
                opts.fall_back = true;
                active_segment.set_capacity(opts.fall_back_sz);
            }
            drop(mtx_guard)
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils;
    use tempdir::TempDir;

    #[test]
    fn test_set_thread_count() {
        let get_db_series_count = |db: &Db<FileStore, ThreadFileWriter, FileBlockLoader>| -> usize {
            let series_count: usize = db.threads.iter().map(|t| t.map.iter().count()).sum();
            series_count
        };

        let dir = TempDir::new("tsdb0").unwrap();
        let mut db = Db::new(dir.path(), 3).unwrap();

        assert_eq!(db.threads.len(), 3);

        db.add_series(SeriesId(0), SeriesOptions::default());
        db.add_series(SeriesId(1), SeriesOptions::default());
        db.add_series(SeriesId(2), SeriesOptions::default());

        assert_eq!(get_db_series_count(&db), 3);

        // can scale up while keeping all data series
        db._set_thread_count(10);
        assert_eq!(db.threads.len(), 10);
        assert_eq!(get_db_series_count(&db), 3);

        // can scale down without deleting data series
        db._set_thread_count(1);
        assert_eq!(db.threads.len(), 1);
        assert_eq!(get_db_series_count(&db), 3);
    }

    #[test]
    fn test_univariate() {
        let data = test_utils::UNIVARIATE_DATA[0].1.clone();
        let dir = TempDir::new("tsdb1").unwrap();
        let db = Db::new(dir.path(), 1).unwrap();

        let opts = SeriesOptions::default();
        let serid = SeriesId(0);

        db.add_series(serid, opts);

        let mut writer = db.writer(0);
        let refid = writer.lookup(serid).unwrap();

        let mut expected_timestamps = Vec::new();
        let mut expected_values = Vec::new();
        for item in data.iter() {
            expected_timestamps.push(item.ts);
            expected_values.extend_from_slice(&item.values[..]);
            writer.push(refid, serid, (*item).clone()).unwrap();
        }

        let mut snapshot = db.snapshot(serid).unwrap();
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        while let Some(segment) = snapshot.next_segment() {
            timestamps.extend_from_slice(segment.timestamps());
            values.extend_from_slice(segment.variable(0));
        }

        assert_eq!(timestamps, expected_timestamps);
        for (x, y) in values.iter().zip(expected_values.iter()) {
            assert!((x - y).abs() < 0.01);
        }
    }

    #[test]
    fn test_multivariate() {
        let data = test_utils::MULTIVARIATE_DATA[0].1.clone();
        let dir = TempDir::new("tsdb2").unwrap();
        let db = Db::new(dir.path(), 1).unwrap();

        let nvars = data[0].values.len();
        let mut opts = SeriesOptions::default();
        opts.nvars = nvars;
        opts.default_compressor = Compression::Simple {
            precision: vec![3, 3],
        };
        opts.fallback_compressor = Compression::Simple {
            precision: vec![3, 3],
        };
        let serid = SeriesId(0);

        db.add_series(serid, opts);

        let mut writer = db.writer(0);
        let refid = writer.lookup(serid).unwrap();

        let mut expected_timestamps = Vec::new();
        let mut expected_values = Vec::new();
        for item in data.iter() {
            expected_timestamps.push(item.ts);
            expected_values.push(item.values[1]);
            writer.push(refid, serid, (*item).clone()).unwrap();
        }

        let mut snapshot = db.snapshot(serid).unwrap();
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        while let Some(segment) = snapshot.next_segment() {
            timestamps.extend_from_slice(segment.timestamps());
            values.extend_from_slice(segment.variable(1));
        }

        assert_eq!(timestamps, expected_timestamps);
        for (x, y) in values.iter().zip(expected_values.iter()) {
            assert!((x - y).abs() < 0.01);
        }
    }

    #[test]
    fn test_reload() {
        let dir = TempDir::new("tsdb3").unwrap();
        let db = Db::new(dir.path(), 1).unwrap();

        for i in 0..3 {
            let data = test_utils::MULTIVARIATE_DATA[i].1.clone();
            let nvars = data[0].values.len();
            let mut opts = SeriesOptions::default();
            opts.nvars = nvars;
            opts.default_compressor = Compression::Simple {
                precision: vec![3, 3],
            };
            opts.fallback_compressor = Compression::Simple {
                precision: vec![3, 3],
            };
            let serid = SeriesId(i as u64);

            db.add_series(serid, opts);

            let mut writer = db.writer(0);
            let refid = writer.lookup(serid).unwrap();

            for item in data[..20_000].iter() {
                writer.push(refid, serid, (*item).clone()).unwrap();
            }
        }
        db.flush_series_options().unwrap();
        drop(db);

        let mut expected_timestamps = Vec::new();
        let mut expected_values = Vec::new();
        for item in test_utils::MULTIVARIATE_DATA[0].1.clone().iter() {
            expected_timestamps.push(item.ts);
            expected_values.push(item.values[1]);
        }

        let serid = SeriesId(0);
        let data = test_utils::MULTIVARIATE_DATA[0].1.clone();
        let db = Db::load(dir.path(), 1).unwrap();

        let mut writer = db.writer(0);
        let refid = writer.lookup(serid).unwrap();
        for item in data[20_000..].iter() {
            writer.push(refid, serid, (*item).clone()).unwrap();
        }

        let mut snapshot = db.snapshot(serid).unwrap();
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        while let Some(segment) = snapshot.next_segment() {
            timestamps.extend_from_slice(segment.timestamps());
            values.extend_from_slice(segment.variable(1));
        }

        assert_eq!(timestamps.len(), expected_timestamps.len());
        assert_eq!(timestamps, expected_timestamps);
        for (x, y) in values.iter().zip(expected_values.iter()) {
            assert!((x - y).abs() < 0.01);
        }
    }

    //#[test]
    //fn test_multivariate_concurrent_read() {
    //    let data = test_utils::MULTIVARIATE_DATA[0].1.clone();
    //    let dir = TempDir::new("tsdb2").unwrap();
    //    let db = Db::new(dir.path(), 1);

    //    let nvars = data[0].values.len();
    //    let mut opts = SeriesOptions::default();
    //    opts.nvars = nvars;
    //    opts.default_compressor = Compression::Simple { precision: vec![3, 3] };
    //    opts.fallback_compressor = Compression::Simple { precision: vec![3, 3] };
    //    let serid = SeriesId(0);

    //    db.add_series(serid, opts);

    //    let mut writer = db.writer(0);
    //    let refid = writer.lookup(serid).unwrap();

    //    let mut expected_timestamps = Vec::new();
    //    let mut expected_values = Vec::new();
    //    for item in data.iter() {
    //        expected_timestamps.push(item.ts);
    //        expected_values.push(item.values[1]);
    //        writer.push(refid, serid, (*item).clone()).unwrap();
    //    }

    //    let mut snapshot = db.snapshot(serid).unwrap();
    //    let mut timestamps = Vec::new();
    //    let mut values = Vec::new();
    //    while let Some(segment) = snapshot.next_segment() {
    //        println!("HERE");
    //        timestamps.extend_from_slice(segment.timestamps());
    //        values.extend_from_slice(segment.variable(1));
    //    }

    //    assert_eq!(timestamps, expected_timestamps);
    //    for (x, y) in values.iter().zip(expected_values.iter()) {
    //        assert!((x - y).abs() < 0.01);
    //    }
    //}
}
