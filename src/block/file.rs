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
    mem::size_of,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

// File Size of 1GB
#[cfg(not(test))]
const BLOCKS_IN_FILE: usize = 121_000; // File size ~1GB

#[cfg(test)]
const BLOCKS_IN_FILE: usize = 121; // File size ~1MB

const BLOCK_ENTRY_SZ: usize = size_of::<BlockId>();

#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct BlockEntry {
    series_id: u64,
    offset: u64,
    mint: u64,
    maxt: u64,
}

impl BlockEntry {
    fn to_be_bytes(&self) -> [u8; BLOCK_ENTRY_SZ] {
        let mut buf = [0u8; BLOCK_ENTRY_SZ];
        let mut offt = 0;

        buf[offt..offt + size_of::<u64>()].copy_from_slice(&self.series_id.to_be_bytes()[..]);
        offt += size_of::<u64>();

        buf[offt..offt + size_of::<u64>()].copy_from_slice(&self.offset.to_be_bytes()[..]);
        offt += size_of::<u64>();

        buf[offt..offt + size_of::<u64>()].copy_from_slice(&self.mint.to_be_bytes()[..]);
        offt += size_of::<u64>();

        buf[offt..offt + size_of::<u64>()].copy_from_slice(&self.maxt.to_be_bytes()[..]);

        buf
    }
}

#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct BlockId {
    file_id: usize,
    offset: u64,
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
            max_blocks: BLOCKS_IN_FILE,
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

struct FileItem {
    file_id: usize,
    file: File,
    head_offset: u64,
    blocks_offset: u64,
    flush_counter: usize,
    flush_channel: async_std::channel::Sender<()>,
}

impl FileItem {
    fn new<P: AsRef<Path>>(dir: P, fid: usize) -> Result<Self, &'static str> {
        let df = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(dir.as_ref().join(fid.to_string()))
            .map_err(|_| "Can't open file")?;
        let (tx, rx) = async_std::channel::bounded(1);
        let flush_channel = tx;
        let head_offset = 0;
        let blocks_offset = (BLOCKS_IN_FILE * BLOCK_ENTRY_SZ) as u64;
        let flush_counter = 0;
        async_std::task::spawn(flush_worker(PathBuf::from(dir.as_ref()), fid, rx));
        Ok(Self {
            file_id: fid,
            file: df,
            flush_channel,
            head_offset,
            blocks_offset,
            flush_counter,
        })
    }

    fn write(
        &mut self,
        series_id: SeriesId,
        mint: Dt,
        maxt: Dt,
        data: &[u8],
    ) -> Result<BlockId, &'static str> {
        assert_eq!(data.len(), BLOCKSZ);

        // Write all data
        let offset = self.blocks_offset;
        self.blocks_offset += BLOCKSZ as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|_| "Can't seek file")?;
        self.file.write_all(data).map_err(|_| "Can't write data")?;

        // Write the block entry
        let block_entry = BlockEntry {
            series_id: series_id.0,
            mint,
            maxt,
            offset: self.blocks_offset,
        };

        self.file
            .seek(SeekFrom::Start(self.head_offset))
            .map_err(|_| "Can't seek file")?;
        self.file
            .write_all(&block_entry.to_be_bytes()[..])
            .map_err(|_| "Cant write block id")?;
        self.head_offset += BLOCK_ENTRY_SZ as u64;

        self.flush_counter += 1;
        if self.flush_counter % 5 == 0
            && self.flush_counter > 0
            && self.flush_channel.try_send(()).is_ok()
        {
            {}
        }

        Ok(BlockId {
            mint,
            maxt,
            offset,
            file_id: self.file_id,
        })
    }
}

impl Drop for FileItem {
    fn drop(&mut self) {
        self.flush_channel.close();
    }
}

pub struct ThreadFileWriter {
    index: Arc<DashMap<SeriesId, List<BlockId>>>,
    index_writers: HashMap<SeriesId, ListWriter<BlockId>>,
    file_allocator: Arc<AtomicUsize>,
    file: Option<FileItem>,
    file_id: usize,
    block_count: usize,
    max_blocks: usize,
    dir_path: PathBuf,
}

impl ThreadFileWriter {
    fn open_file(&mut self) -> Result<(), &'static str> {
        self.file_id = self.file_allocator.fetch_add(1, SeqCst);
        self.file = Some(FileItem::new(&self.dir_path, self.file_id)?);
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

        // Write the item into the file
        let block_id = self
            .file
            .as_mut()
            .unwrap()
            .write(series_id, mint, maxt, d)?;

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

        self.block_count += 1;
        Ok(())
    }
}

async fn flush_worker(dir: PathBuf, fid: usize, rx: async_std::channel::Receiver<()>) {
    use std::os::unix::io::AsRawFd;

    //let mut path = async_std::path::PathBuf::from(&*KEYDIR);
    //path.push(fid.to_string());
    //let kf = async_std::fs::OpenOptions::new()
    //    .write(true)
    //    .open(path)
    //    .await
    //    .unwrap();

    let mut path = async_std::path::PathBuf::from(dir);
    path.push(fid.to_string());
    let df = async_std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .unwrap();

    while let Ok(()) = rx.recv().await {
        df.sync_all().await.unwrap();
    }

    //let dur = Duration::from_secs(1);
    //while !done.load(SeqCst) {
    //    df.sync_all().await.unwrap();
    //    //kf.sync_all().await.unwrap();
    //    task::sleep(dur).await;
    //}
    df.sync_all().await.unwrap();
    //kf.sync_all().await.unwrap();

    #[cfg(target_os = "linux")]
    unsafe {
        let df_fd = df.as_raw_fd();
        //let kf_fd = kf.as_raw_fd();
        libc::posix_fadvise64(df_fd, 0, 0, libc::POSIX_FADV_DONTNEED);
        //libc::posix_fadvise64(kf_fd, 0, 0, libc::POSIX_FADV_DONTNEED);
    };
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

    fn read_block(&mut self, offset: u64, buf: &mut [u8]) {
        let f = self.file.as_mut().unwrap();
        f.seek(SeekFrom::Start(offset)).unwrap();
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
                offset,
                mint: _,
                maxt: _,
            } = self.items[current_idx];

            if self.file.is_none() || self.current_file_id != file_id {
                self.open_file(file_id);
            }

            self.read_block(offset, buf);

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

        let data = (0..BLOCKS_IN_FILE + 100)
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

        let data = (0..BLOCKS_IN_FILE + 100)
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
