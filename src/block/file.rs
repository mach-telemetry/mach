use crate::{
    block::{BlockReader, BlockStore, BlockWriter, BLOCKSZ},
    tsdb::{Dt, SeriesId},
    utils::{
        list::{List, ListReader, ListWriter},
        overlaps,
    },
};
use dashmap::DashMap;
use std::{
    collections::{hash_map::Entry, HashMap},
    convert::AsRef,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

// File Size of 1GB
#[cfg(not(test))]
const FILESZ: usize = 1_000_000_000;

#[cfg(test)]
const FILESZ: usize = 1_000_000;

#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct BlockId {
    file_id: usize,
    block_id: usize,
    mint: u64,
    maxt: u64,
}

#[derive(Clone)]
pub struct FileStore {
    index: Arc<DashMap<SeriesId, List<BlockId>>>,
    file_allocator: Arc<AtomicUsize>,
    dir_path: PathBuf,
}

impl FileStore {
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        Self {
            index: Arc::new(DashMap::new()),
            file_allocator: Arc::new(AtomicUsize::new(0)),
            dir_path: PathBuf::from(dir.as_ref()),
        }
    }

    fn thread_writer(&self) -> ThreadFileWriter {
        ThreadFileWriter {
            index: self.index.clone(),
            index_writers: HashMap::new(),
            file_allocator: self.file_allocator.clone(),
            file: None,
            file_id: 0,
            block_count: 0,
            max_blocks: FILESZ / BLOCKSZ,
            dir_path: self.dir_path.clone(),
        }
    }

    fn block_reader(&self, id: SeriesId) -> Option<FileBlockLoader> {
        let snapshot = self.index.get(&id)?.snapshot();
        Some(FileBlockLoader::new(&self.dir_path, snapshot))
    }
}

impl BlockStore<ThreadFileWriter, FileBlockLoader> for FileStore {
    fn writer(&self) -> ThreadFileWriter {
        self.thread_writer()
    }

    fn reader(&self, id: SeriesId) -> Option<FileBlockLoader> {
        self.block_reader(id)
    }
}

pub struct ThreadFileWriter {
    index: Arc<DashMap<SeriesId, List<BlockId>>>,
    index_writers: HashMap<SeriesId, ListWriter<BlockId>>,
    file_allocator: Arc<AtomicUsize>,
    file: Option<File>,
    file_id: usize,
    block_count: usize,
    max_blocks: usize,
    dir_path: PathBuf,
}

impl ThreadFileWriter {
    fn open_file(&mut self) -> Result<(), &'static str> {
        self.file_id = self.file_allocator.fetch_add(1, SeqCst);
        let df = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.dir_path.join(self.file_id.to_string()))
            .map_err(|_| "Can't open file")?;
        self.file = Some(df);
        self.block_count = 0;
        Ok(())
    }
}

impl BlockWriter for ThreadFileWriter {
    fn write_block(
        &mut self,
        series_id: SeriesId,
        mint: Dt,
        maxt: Dt,
        d: &[u8],
    ) -> Result<(), &'static str> {
        match &self.file {
            None => self.open_file()?,
            Some(_) => {
                if self.block_count == self.max_blocks {
                    self.open_file()?
                }
            }
        }
        let block_id = BlockId {
            mint: mint,
            maxt: maxt,
            file_id: self.file_id,
            block_id: self.block_count,
        };

        let entry = self.index_writers.entry(series_id);

        // This entry shenegans needed because borrowing doesnt play nicely in entry.or_insert
        // unless I'm doing something wrong
        match entry {
            Entry::Occupied(mut e) => e.get_mut().push(block_id),
            Entry::Vacant(e) => {
                let w = self.index.entry(series_id).or_insert(List::new()).writer();
                e.insert(w).push(block_id);
            }
        }

        self.file
            .as_mut()
            .unwrap()
            .write_all(d)
            .map_err(|_| "Can't flush file")?;
        self.block_count += 1;
        Ok(())
    }
}

pub struct FileBlockLoader {
    blocks: ListReader<BlockId>,
    items: Vec<BlockId>,
    next_idx: usize,
    current_file_id: usize,
    file: Option<File>,
    dir: PathBuf,
    //buf: [u8; BLOCKSZ],
}

impl FileBlockLoader {
    fn new<P: AsRef<Path>>(dir: P, blocks: ListReader<BlockId>) -> Self {
        let dir = PathBuf::from(dir.as_ref());
        Self {
            blocks,
            items: Vec::new(),
            next_idx: 0,
            current_file_id: 0,
            file: None,
            dir,
            //buf: [0u8; BLOCKSZ],
        }
    }

    fn open_file(&mut self, file_id: usize) {
        self.file = Some(
            File::open(self.dir.join(file_id.to_string()))
                .expect("Can't find a file that's supposed to exist"),
        );
        self.current_file_id = file_id;
    }

    fn read_block(&mut self, block_id: usize, buf: &mut [u8]) {
        let offset = block_id * BLOCKSZ;
        let f = self.file.as_mut().unwrap();
        f.seek(SeekFrom::Start(offset as u64)).unwrap();
        f.read_exact(buf).unwrap();
    }

    fn reset(&mut self) {
        self.items.clear();
        self.next_idx = 0;
        self.current_file_id = 0;
        self.file = None;
    }
}

impl BlockReader for FileBlockLoader {
    fn set_range(&mut self, mint: Dt, maxt: Dt) {
        self.reset();
        for item in self.blocks.iter() {
            if overlaps(item.mint, item.maxt, mint, maxt) {
                self.items.push(*item);
            } else if item.mint > maxt {
                break;
            }
        }
        assert!(self.items.is_sorted());
    }

    fn next_block(&mut self, buf: &mut [u8]) -> Option<usize> {
        if self.next_idx >= self.items.len() {
            None
        } else {
            let current_idx = self.next_idx;
            self.next_idx += 1;
            let BlockId {
                file_id,
                block_id,
                mint: _,
                maxt: _,
            } = self.items[current_idx];

            if self.file.is_none() || self.current_file_id != file_id {
                self.open_file(file_id);
            }

            self.read_block(block_id, buf);

            Some(BLOCKSZ)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::prelude::*;
    use tempdir::TempDir;

    //fn make_blocks() -> HashMap<SeriesId, Vec<(BlockKey, Vec<u8>)>> {
    //    let mut rng = thread_rng();

    //    let mut map = HashMap::new();
    //    for i in 0..3 {
    //        let mut v = Vec::new();
    //        for dt in (0..7).step_by(3) {
    //            let block = BlockKey {
    //                id: SeriesId(i),
    //                mint: dt,
    //                maxt: dt + 1,
    //            };
    //            let mut data = vec![0u8; 8192];
    //            rng.try_fill(&mut data[..]).unwrap();
    //            v.push((block, data));
    //        }
    //        map.insert(SeriesId(i), v);
    //    }
    //    map
    //}

    #[test]
    fn test_write_read() {
        let dir = TempDir::new("test").unwrap();
        let file_store = FileStore::new(dir.path());
        let mut rng = thread_rng();
        let mut thread_writer = file_store.thread_writer();
        let id = SeriesId(0);
        let blocks = (0..5)
            .map(|_| {
                let mut v = vec![0u8; BLOCKSZ];
                rng.try_fill(&mut v[..]).unwrap();
                v
            })
            .collect::<Vec<Vec<u8>>>();

        let mut currt = 0;
        for block in blocks.iter() {
            let maxt = currt + 3;
            thread_writer
                .write_block(id, currt, maxt, &block[..])
                .unwrap();
            currt = maxt;
        }

        let mut reader = file_store.block_reader(SeriesId(0)).unwrap();
        reader.set_range(0, 10);

        //let blocks = data.get(&SeriesId(0)).unwrap();
        let mut counter = 0;
        let mut buf = [0u8; BLOCKSZ];
        while let Some(sz) = reader.next_block(&mut buf[..]) {
            assert_eq!(sz, BLOCKSZ);
            assert_eq!(buf, &blocks[counter][..]);
            counter += 1;
        }
    }

    #[test]
    fn test_write_read_fill_file() {
        let dir = TempDir::new("test2").unwrap();
        let file_store = FileStore::new(dir.path());
        let mut thread_writer = file_store.thread_writer();
        let mut rng = thread_rng();

        let data = (0..FILESZ / BLOCKSZ + 100)
            .map(|_| {
                let mut v = vec![0u8; BLOCKSZ];
                rng.try_fill(&mut v[..]).unwrap();
                v
            })
            .collect::<Vec<Vec<u8>>>();

        let id = SeriesId(0);
        let mut currt = 0;
        for block in data.iter() {
            let maxt = currt + 3;
            thread_writer
                .write_block(id, currt, maxt, &block[..])
                .unwrap();
            currt = maxt;
        }

        let mut reader = file_store.block_reader(SeriesId(0)).unwrap();
        reader.set_range(0, currt);

        let mut counter = 0;
        let mut buf = [0u8; BLOCKSZ];
        while let Some(sz) = reader.next_block(&mut buf[..]) {
            assert_eq!(sz, BLOCKSZ);
            assert_eq!(buf, data[counter].as_slice());
            counter += 1;
        }
    }

    #[test]
    fn test_write_some_read() {
        let dir = TempDir::new("test3").unwrap();
        let file_store = FileStore::new(dir.path());
        let mut thread_writer = file_store.thread_writer();
        let mut rng = thread_rng();

        let data = (0..FILESZ / BLOCKSZ + 100)
            .map(|_| {
                let mut v = vec![0u8; BLOCKSZ];
                rng.try_fill(&mut v[..]).unwrap();
                v
            })
            .collect::<Vec<Vec<u8>>>();

        let id = SeriesId(0);
        let mut currt = 0;
        for block in data.iter() {
            let maxt = currt + 3;
            thread_writer
                .write_block(id, currt, maxt, &block[..])
                .unwrap();
            currt = maxt;
        }

        let mut reader = file_store.block_reader(SeriesId(0)).unwrap();
        reader.set_range(5, 11);

        let mut counter = 1;
        let mut buf = [0u8; BLOCKSZ];
        while let Some(sz) = reader.next_block(&mut buf) {
            assert_eq!(sz, BLOCKSZ);
            assert_eq!(buf, data[counter].as_slice());
            counter += 1;
        }
    }
}
