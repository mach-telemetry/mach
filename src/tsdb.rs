use crate::{
    active_block::{ActiveBlock, ActiveBlockWriter},
    active_segment::{ActiveSegment, ActiveSegmentWriter, SEGSZ},
    block::{
        file::{FileBlockLoader, FileStore, ThreadFileWriter},
        BlockReader, BlockStore, BlockWriter, BLOCKSZ,
    },
    types::{Sample, Type},
    //compression::Compression,
    //read_set::SeriesReadSet,
    //segment::SegmentLike,
};
use core::cmp::{Ordering, Reverse};
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering::SeqCst;
use dashmap::DashMap;
use serde::*;
use std::collections::BinaryHeap;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesId(pub u64);

#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq)]
pub struct RefId(pub u64);

#[derive(Clone, Serialize, Deserialize)]
pub struct SeriesOptions {
    schema: Vec<Type>,
}

impl SeriesOptions {
    fn set_schema(mut self, schema: Vec<Type>) -> Self {
        self.schema = schema;
        self
    }
}

impl Default for SeriesOptions {
    fn default() -> Self {
        Self {
            schema: Vec::new()
        }
    }
}

#[derive(Clone)]
struct MemSeries {
    active_segment: ActiveSegment,
    active_block: ActiveBlock,
}

impl MemSeries {
    fn new() -> Self {
        Self {
            active_segment: ActiveSegment::new(),
            active_block: ActiveBlock::new(),
        }
    }
}

struct GlobalMetadata {
    map: DashMap<SeriesId, SeriesMetadata>,
    _path: PathBuf,
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
            _path: PathBuf::from(path.as_ref()),
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
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|_| "Cant open path")?;
        f.write_all(json.as_bytes())
            .map_err(|_| "Cant write json")?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct SeriesMetadata {
    pub options: SeriesOptions,
    mem_series: MemSeries,
    thread_id: Arc<AtomicUsize>,
}

pub struct WriterMetadata {
    map: Arc<DashMap<SeriesId, SeriesMetadata>>,

    // File storage information
    file_store: FileStore,
    thread_id: usize,
}

impl WriterMetadata {
    fn new(file_store: FileStore, thread_id: usize) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            file_store,
            thread_id,
        }
    }

    fn add_series(&self, id: SeriesId, ser: SeriesMetadata) {
        self.map.insert(id, ser);
    }

    pub fn writer(&self) -> Writer {
        Writer::new(self.map.clone(), self.file_store.writer(), self.thread_id)
    }
}

pub struct Db {
    // Global metadata information
    map: GlobalMetadata,
    threads: Vec<WriterMetadata>,
    dir: PathBuf,

    // File storage information
    file_store: FileStore,
}

impl Db {
    pub fn new<P: AsRef<Path>>(dir: P, threads: usize) -> Result<Self, &'static str> {
        let data_path = dir.as_ref().join("data");
        fs::create_dir_all(&data_path).map_err(|_| "can't create dir")?;
        let meta_path = dir.as_ref().join("meta");
        let file_store = FileStore::new(data_path);
        Ok(Self {
            map: GlobalMetadata::new(meta_path),
            threads: (0..threads)
                .map(|thread_id| WriterMetadata::new(file_store.clone(), thread_id))
                .collect(),
            file_store,
            dir: PathBuf::from(dir.as_ref()),
        })
    }

    pub fn load<P: AsRef<Path>>(dir: P, threads: usize) -> Result<Self, &'static str> {
        let data_path = dir.as_ref().join("data");
        fs::create_dir_all(&data_path).map_err(|_| "can't create dir")?;
        let meta_path = dir.as_ref().join("meta");

        // Load filestore information
        let file_store = FileStore::load(data_path)?;
        let threads = (0..threads)
            .map(|thread_id| WriterMetadata::new(file_store.clone(), thread_id))
            .collect();

        // Load meta path
        let data = fs::read_to_string(&meta_path).map_err(|_| "Can't read global metadata file")?;
        let map: HashMap<SeriesId, SeriesOptions> =
            serde_json::from_str(&data).map_err(|_| "Can't parse metadata file")?;

        let db = Db {
            map: GlobalMetadata::new(meta_path),
            threads,
            file_store,
            dir: PathBuf::from(dir.as_ref()),
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
        let mem_series = MemSeries::new();
        let meta = SeriesMetadata {
            options,
            mem_series,
            thread_id: Arc::new(AtomicUsize::new(thread_id)),
        };
        self.threads[thread_id].add_series(id, meta.clone());
        self.map.insert(id, meta);
        thread_id
    }

    pub fn flush_series_options(&self) -> Result<(), &'static str> {
        self.map.flush_series_options(self.dir.join("meta"))
    }

    //pub fn snapshot(&self, id: SeriesId) -> Result<SeriesReadSet<FileBlockLoader>, &'static str> {
    //    let metadata = self.map.get(&id).ok_or("id not found")?;
    //    let segment = metadata.mem_series.active_segment.snapshot();
    //    let block = metadata.mem_series.active_block.snapshot();
    //    let blocks = self.file_store.reader(id).ok_or("ID not found")?;
    //    Ok(SeriesReadSet::new(segment, block, blocks))
    //}

    pub fn writer(&self, thread_id: usize) -> Writer {
        self.threads[thread_id].writer()
    }

    //fn _set_thread_count(&mut self, thread_count: usize) {
    //    match thread_count.cmp(&self.threads.len()) {
    //        Ordering::Equal => {}
    //        Ordering::Less => self._scale_down_threads(thread_count),
    //        Ordering::Greater => self._scale_up_threads(thread_count),
    //    }
    //}

    //fn _scale_down_threads(&mut self, target_thread_count: usize) {
    //    assert!(target_thread_count < self.threads.len());

    //    self._optimize_series_thread_assignment(target_thread_count);

    //    // can safely delete threads b/c all data series have been migrated
    //    self.threads.truncate(target_thread_count);
    //}

    //fn _scale_up_threads(&mut self, target_thread_count: usize) {
    //    assert!(target_thread_count > self.threads.len());

    //    let fstore = self.file_store.clone();

    //    // data series migration will only be safe after new threads are allocated
    //    let new_threads = (self.threads.len()..target_thread_count)
    //        .map(|thread_id| WriterMetadata::new(fstore.clone(), thread_id));
    //    self.threads.extend(new_threads);

    //    self._optimize_series_thread_assignment(target_thread_count);
    //}

    //fn _optimize_series_thread_assignment(&mut self, target_thread_count: usize) {
    //    let migration_map = self._compute_series_migration_map(target_thread_count);

    //    for entry in migration_map.iter() {
    //        let serid = entry.key();
    //        let (old_thread_id, new_thread_id) = entry.value();
    //        self._move_series(*serid, *old_thread_id, *new_thread_id)
    //            .unwrap_or(()); // unable to move series; TODO: consider how to handle exception here
    //    }
    //}

    //fn _move_series(
    //    &mut self,
    //    series_id: SeriesId,
    //    from_thread: usize,
    //    to_thread: usize,
    //) -> Result<(), String> {
    //    let old_thread = self.threads.get(from_thread).ok_or(format!(
    //        "Cannot move series '{}' in thread '{}': thread does not exist.",
    //        series_id.0 as usize, from_thread,
    //    ))?;
    //    let new_thread = self.threads.get(to_thread).ok_or(format!(
    //        "Cannot move series '{}' to thread '{}': thread does not exist.",
    //        series_id.0 as usize, to_thread
    //    ))?;

    //    let (_, series_meta) = old_thread.map.remove(&series_id).ok_or(format!(
    //        "Series '{}' not found on thread '{}'",
    //        series_id.0 as usize, from_thread
    //    ))?;

    //    series_meta.thread_id.store(to_thread, SeqCst);
    //    new_thread.add_series(series_id, series_meta);

    //    Ok(())
    //}

    ///// Returns a mapping that describes how data series will be migrated
    ///// with the new thread count.
    /////
    ///// The mapping's keys are IDs of data series that need to be moved.
    ///// The mapping's values are 2-element tuples: the first value is the
    ///// series' current thread ID, and the second is the thread ID to migrate to.
    /////
    ///// Data series that do not need to be moved are not contained in the
    ///// returned mapping.
    //fn _compute_series_migration_map(
    //    &self,
    //    new_thread_count: usize,
    //) -> DashMap<SeriesId, (usize, usize)> {
    //    let migration_map: DashMap<SeriesId, (usize, usize)> = DashMap::new();

    //    for (curr_thread_id, thread) in self.threads.iter().enumerate() {
    //        for item in thread.map.iter() {
    //            let serid = item.key();
    //            let new_thread_id = serid.0 as usize % new_thread_count;
    //            if new_thread_id != curr_thread_id {
    //                migration_map.insert(*serid, (curr_thread_id, new_thread_id));
    //            }
    //        }
    //    }
    //    migration_map
    //}

    //pub fn init_writer(&self, thread_id: usize) -> Writer<ThreadFileWriter> {
    //    self.threads[thread_id].writer()
    //}
}

pub struct Writer {
    pub series_map: Arc<DashMap<SeriesId, SeriesMetadata>>,
    ref_map: HashMap<SeriesId, RefId>,

    // Reference these items in the push method
    // slots may be empty
    mem_series: Vec<Option<WriterMemSeries>>,
    active_segment: Vec<Option<ActiveSegmentWriter>>, // for cache friendlyness, set this aside

    // Free mem series vec indices to write to, in increasing order.
    free_ref_ids: BinaryHeap<Reverse<usize>>,

    // Shared buffer for the compressor and the block writer
    buf: [u8; BLOCKSZ],
    block_writer: ThreadFileWriter,
    thread_id: usize,
}

impl Drop for Writer {
    fn drop(&mut self) {
        //self.flush().expect("Can't flush files");
    }
}

struct WriterMemSeries {
    active_block: ActiveBlockWriter,
    series_option: SeriesOptions,
    thread_id: Arc<AtomicUsize>,
}

impl WriterMemSeries {
    fn new(mem_series: MemSeries, options: SeriesOptions, thread_id: Arc<AtomicUsize>) -> Self {
        Self {
            active_block: mem_series.active_block.writer(),
            series_option: options,
            thread_id,
        }
    }
}

pub struct PushResult {
    pub thread_id: usize,
}

impl Writer {
    fn new(
        series_map: Arc<DashMap<SeriesId, SeriesMetadata>>,
        block_writer: ThreadFileWriter,
        thread_id: usize,
    ) -> Self {
        Self {
            series_map,
            ref_map: HashMap::new(),
            mem_series: Vec::new(),
            active_segment: Vec::new(),
            free_ref_ids: BinaryHeap::new(),
            buf: [0u8; BLOCKSZ],
            block_writer,
            thread_id,
        }
    }

    pub fn series(&self) -> &DashMap<SeriesId, SeriesMetadata> {
        &self.series_map
    }

    pub fn lookup(&mut self, id: SeriesId) -> Option<RefId> {
        if let Some(idx) = self.ref_map.get(&id) {
            Some(*idx)
        } else if let Some(ser) = self.series_map.get_mut(&id) {
            let active_segment = ser.mem_series.active_segment.writer();
            let mem_series = WriterMemSeries::new(
                ser.mem_series.clone(),
                ser.options.clone(),
                ser.thread_id.clone(),
            );

            let ref_id = match self.free_ref_ids.pop() {
                Some(free_ref_id) => free_ref_id.0,
                None => self.mem_series.len(),
            };

            if ref_id >= self.mem_series.len() {
                self.mem_series.push(Some(mem_series));
                self.active_segment.push(Some(active_segment));
            } else {
                self.mem_series[ref_id] = Some(mem_series);
                self.active_segment[ref_id] = Some(active_segment);
            }

            let result = RefId(ref_id as u64);
            self.ref_map.insert(id, result);

            Some(result)
        } else {
            None
        }
    }

    // TODO: Optimization + API
    // 1. Return ThreadID? Last serialized timestamp?
    pub fn push(
        &mut self,
        reference_id: RefId,
        series_id: SeriesId,
        ts: u64,
        sample: Sample,
    ) -> Result<PushResult, &'static str> {
        let id = reference_id.0 as usize;

        let active_segment = match self.active_segment[id].as_mut() {
            Some(x) => x,
            None => return Err("Refid invalid"),
        };

        let segment_len = unsafe { active_segment.push(ts, &sample.data)};

        Ok(PushResult {
            thread_id: self.thread_id,
        })
    }

    //pub fn flush(&mut self) -> Result<(), &'static str> {
    //    for (sid, rid) in self.ref_map.iter() {
    //        let id = rid.0 as usize;
    //        let series_id = *sid;
    //        let mem_series = self.mem_series[id].as_mut().unwrap();

    //        let mtx_guard = mem_series.snapshot_lock.write();

    //        let segment = self.active_segment[id].as_mut().unwrap().yield_replace();
    //        let opts = &mut mem_series.series_option;
    //        let active_block_writer = &mut mem_series.active_block;

    //        let bytes = match segment.len() {
    //            0 => 0, // There's nothing in the segment so we do nothing
    //            _ => {
    //                // There's something in the segment so we flush compress and flush
    //                let (mint, maxt) = {
    //                    let timestamps = segment.timestamps();
    //                    (timestamps[0], *timestamps.last().unwrap())
    //                };

    //                // And then compress it
    //                let bytes = if opts.fall_back {
    //                    opts.fallback_compressor
    //                        .compress(&segment, &mut self.buf[..])
    //                } else {
    //                    opts.default_compressor
    //                        .compress(&segment, &mut self.buf[..])
    //                };

    //                // If there is no room in the current active block, flush the active block
    //                let remaining = active_block_writer.remaining();
    //                if bytes > remaining {
    //                    let block = active_block_writer.yield_replace();
    //                    self.block_writer.write_block(
    //                        series_id,
    //                        block.mint(),
    //                        block.maxt(),
    //                        block.slice(),
    //                    )?;
    //                    opts.max_compressed_sz = 0;
    //                    opts.block_bytes_remaining = opts.block_bytes;
    //                    opts.fall_back = false;
    //                    self.active_segment[id]
    //                        .as_mut()
    //                        .unwrap()
    //                        .set_capacity(SEGSZ);
    //                    //mem_series.active_segment.set_capacity(SEGSZ);
    //                }

    //                // Write compressed data into active segment, then flush
    //                active_block_writer.push_segment(mint, maxt, &self.buf[..bytes]);
    //                bytes
    //            }
    //        };

    //        // Flush the last block
    //        let block = active_block_writer.yield_replace();
    //        self.block_writer
    //            .write_block(series_id, block.mint(), block.maxt(), block.slice())?;
    //        opts.max_compressed_sz = 0;
    //        opts.block_bytes_remaining = opts.block_bytes;
    //        opts.fall_back = false;
    //        opts.max_compressed_sz = opts.max_compressed_sz.max(bytes);

    //        drop(mtx_guard)
    //    }
    //    Ok(())
    //}

    //// TODO: Optimization + API
    //// 1. Return ThreadID? Last serialized timestamp?
    //pub fn push(
    //    &mut self,
    //    reference_id: RefId,
    //    series_id: SeriesId,
    //    sample: Sample,
    //) -> Result<PushResult, &'static str> {
    //    let id = reference_id.0 as usize;

    //    let active_segment = match self.active_segment[id].as_mut() {
    //        Some(x) => x,
    //        None => return Err("Refid invalid"),
    //    };

    //    //let active_segment = self.active_segment[id].as_mut().unwrap();
    //    let segment_len = unsafe { active_segment.ptr.as_mut().unwrap().push(sample) };
    //    if segment_len == active_segment.capacity() {
    //        // TODO: Optimizations:
    //        // 1. minimize mtx_guard by not yielding and replacing until after compression and
    //        //    flushing
    //        // 2. concurrent compress + flush

    //        // Get block information and take the snapshot lock
    //        let mem_series = self.mem_series[id].as_mut().unwrap();
    //        let mtx_guard = mem_series.snapshot_lock.write();

    //        // Take segment
    //        let segment = active_segment.yield_replace();
    //        let (mint, maxt) = {
    //            let timestamps = segment.timestamps();
    //            (timestamps[0], *timestamps.last().unwrap())
    //        };

    //        let opts = &mut mem_series.series_option;
    //        let active_block_writer = &mut mem_series.active_block;

    //        // And then compress it
    //        // TODO: Here, we could probably use something smarter to determine how to pack blocks
    //        let bytes = if opts.fall_back {
    //            //println!("USING FALLBACK");
    //            opts.fallback_compressor
    //                .compress(&segment, &mut self.buf[..])
    //        } else {
    //            //println!("USING DEFAULT");
    //            opts.default_compressor
    //                .compress(&segment, &mut self.buf[..])
    //        };

    //        // If there is no room in the current active block, flush the active block
    //        let remaining = active_block_writer.remaining();
    //        //println!("{:?} BLOCK IS {}% FULL", series_id, (1.0 - (remaining as f64 / BLOCKSZ as f64)) * 100.);
    //        if bytes > remaining {
    //            //println!("{:?} FLUSHING BLOCK", series_id);
    //            let block = active_block_writer.yield_replace();
    //            self.block_writer.write_block(
    //                series_id,
    //                block.mint(),
    //                block.maxt(),
    //                block.slice(),
    //            )?;
    //            opts.max_compressed_sz = 0;
    //            opts.block_bytes_remaining = opts.block_bytes;
    //            opts.fall_back = false;
    //            active_segment.set_capacity(SEGSZ);
    //        }

    //        // Write compressed data into active segment
    //        active_block_writer.push_segment(mint, maxt, &self.buf[..bytes]);

    //        // Update metadata for the next push:
    //        // 1. Largest compressed size
    //        // 2. Should we use fall back compression in this next segment
    //        // TODO: determine the block packing heuristic. For now, if bytes remaining is smaller
    //        // than the maximum recorded compressed size, fall back
    //        opts.max_compressed_sz = opts.max_compressed_sz.max(bytes);
    //        opts.block_bytes_remaining -= bytes;
    //        if opts.block_bytes_remaining < opts.max_compressed_sz {
    //            //println!("SETTING FALLBACK FOR NEXT BLOCK");
    //            opts.fall_back = true;
    //            active_segment.set_capacity(opts.fall_back_sz);
    //        }
    //        drop(mtx_guard);
    //        let series_thread_id = mem_series.thread_id.load(SeqCst);
    //        if series_thread_id != self.thread_id {
    //            // perform cleanup because series has been reassigned to another thread
    //            self.mem_series[id] = None;
    //            self.ref_map.remove(&series_id);
    //            self.free_ref_ids.push(Reverse(id));
    //        }
    //        return Ok(PushResult {
    //            thread_id: series_thread_id,
    //        });
    //    }

    //    Ok(PushResult {
    //        thread_id: self.thread_id,
    //    })
    //}
}

/*
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

        let assert_series_only_in_thread =
            |db: &Db<FileStore, ThreadFileWriter, FileBlockLoader>,
             series_id,
             expected_thread: usize| {
                // assert series is in the expected thread
                let series = &db.threads[expected_thread].map.get(&series_id).unwrap();
                let tid = &series.value().thread_id;
                assert_eq!(tid.load(SeqCst), expected_thread);

                // assert series is not in any other thread
                for thread in db.threads.iter() {
                    if thread.thread_id == expected_thread {
                        continue;
                    }
                    assert!(thread.map.get(&series_id).is_none());
                }
            };

        let dir = TempDir::new("tsdb0").unwrap();
        let mut db = Db::new(dir.path(), 3).unwrap();

        assert_eq!(db.threads.len(), 3);

        db.add_series(SeriesId(0), SeriesOptions::default());
        db.add_series(SeriesId(5), SeriesOptions::default());
        db.add_series(SeriesId(8), SeriesOptions::default());

        assert_eq!(get_db_series_count(&db), 3);
        assert_series_only_in_thread(&db, SeriesId(0), 0);
        assert_series_only_in_thread(&db, SeriesId(5), 2);
        assert_series_only_in_thread(&db, SeriesId(8), 2);

        // can scale up while keeping all data series
        db._set_thread_count(10);
        assert_eq!(db.threads.len(), 10);
        assert_eq!(get_db_series_count(&db), 3);
        assert_series_only_in_thread(&db, SeriesId(0), 0);
        assert_series_only_in_thread(&db, SeriesId(5), 5);
        assert_series_only_in_thread(&db, SeriesId(8), 8);

        // can scale down without deleting data series
        db._set_thread_count(1);
        assert_eq!(db.threads.len(), 1);
        assert_eq!(get_db_series_count(&db), 3);
        assert_series_only_in_thread(&db, SeriesId(0), 0);
        assert_series_only_in_thread(&db, SeriesId(5), 0);
        assert_series_only_in_thread(&db, SeriesId(8), 0);
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
*/
