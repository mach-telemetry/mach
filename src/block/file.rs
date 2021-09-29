use crate::{
    block::{BlockReader, BlockStore, BlockWriter, BLOCKSZ},
    tsdb::{Dt, SeriesId},
    utils::{
        list::{List, ListReader, ListWriter},
        overlaps,
    },
};
use dashmap::{self, DashMap};
use std::{
    collections::{hash_map::Entry, HashMap},
    convert::{AsRef, TryInto},
    fs::{read_dir, File, OpenOptions},
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
const MAGIC: [u8; 24] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3,
];

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct BlockEntry {
    series_id: u64,
    offset: u64,
    mint: u64,
    maxt: u64,
}

impl BlockEntry {
    fn as_be_bytes(&self) -> [u8; BLOCK_ENTRY_SZ] {
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

    fn from_be_bytes(bytes: &[u8; BLOCK_ENTRY_SZ]) -> Self {
        let sz = size_of::<u64>();
        let mut offt = 0;

        let series_id = u64::from_be_bytes(bytes[offt..offt + sz].try_into().unwrap());
        offt += sz;

        let offset = u64::from_be_bytes(bytes[offt..offt + sz].try_into().unwrap());
        offt += sz;

        let mint = u64::from_be_bytes(bytes[offt..offt + sz].try_into().unwrap());
        offt += sz;

        let maxt = u64::from_be_bytes(bytes[offt..offt + sz].try_into().unwrap());

        Self {
            series_id,
            offset,
            mint,
            maxt,
        }
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
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

    pub fn load<P: AsRef<Path>>(dir: P) -> Result<Self, &'static str> {
        let err = "Can't load from directory";
        let filestore = Self::new(&dir);

        // Get all files in the directory, sort them because read_dir doesn't list files in
        // lexicographic order
        let read_dir = read_dir(&dir).map_err(|_| err)?;
        let mut paths: Vec<PathBuf> = Vec::new();
        for p in read_dir {
            paths.push(p.map_err(|_| err)?.path());
        }
        paths.sort();

        // For each file, get entries in those files
        let mut max_file_id = 0;
        for p in paths {
            let fid = file_id_from_path(&p)?;
            max_file_id = max_file_id.max(fid);
            let mut entries = FileItem::entries(p)?;
            for (id, blocks) in entries.drain() {
                let mut writer = filestore.index.entry(id).or_insert(List::new()).writer();
                for block in blocks {
                    writer.push(block);
                }
            }
        }

        // Update the next file
        filestore.file_allocator.store(max_file_id + 1, SeqCst);
        Ok(filestore)
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

    fn ids(&self) -> Vec<SeriesId> {
        self.index.iter().map(|x| *x.key()).collect()
    }
}

struct FileItem {
    file_id: usize,
    file: File,
    head_offset: u64,
    block_count: usize,
    blocks_offset: u64,
    flush_channel: async_std::channel::Sender<()>,
}

impl FileItem {
    fn entries<P: AsRef<Path>>(path: P) -> Result<HashMap<SeriesId, Vec<BlockId>>, &'static str> {
        // Get the FileID, open the file
        let file_id = file_id_from_path(&path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|_| "Can't open path")?;

        // Read the magic header and confirm
        let mut magic = [0u8; 24];
        file.read_exact(&mut magic[..])
            .map_err(|_| "Can't read magic")?;
        if magic != MAGIC {
            return Err("Magic does not match");
        }

        // Get the number of blocks in this file
        let mut block_count_bytes = [0u8; size_of::<usize>()];
        file.read_exact(&mut block_count_bytes[..])
            .map_err(|_| "Can't block count")?;
        let block_count = usize::from_be_bytes(block_count_bytes);

        // Read all block entries
        let mut map = HashMap::new();
        let mut block_entry_buf = [0u8; size_of::<BlockEntry>()];
        for _ in 0..block_count {
            file.read_exact(&mut block_entry_buf[..])
                .map_err(|_| "Can't read block entry")?;

            // Reading the block entry, then converting that to a block ID
            let BlockEntry {
                series_id,
                offset,
                mint,
                maxt,
            } = BlockEntry::from_be_bytes(&block_entry_buf);
            let block_id = BlockId {
                file_id,
                offset,
                mint,
                maxt,
            };
            map.entry(SeriesId(series_id))
                .or_insert_with(Vec::new)
                .push(block_id);
        }
        Ok(map)
    }

    fn new<P: AsRef<Path>>(dir: P, fid: usize) -> Result<Self, &'static str> {
        let mut df = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(dir.as_ref().join(fid.to_string()))
            .map_err(|_| "Can't open file")?;
        df.write_all(&MAGIC[..]).map_err(|_| "Can't write magic")?;
        df.write_all(&0usize.to_be_bytes()[..])
            .map_err(|_| "Can't write bytes")?;

        let (tx, rx) = async_std::channel::bounded(1);
        let flush_channel = tx;
        let head_offset = 32; // 24 (magic) + 8 (count)
        let block_count = 0;
        let blocks_offset = (BLOCKS_IN_FILE * (BLOCK_ENTRY_SZ + 1)) as u64; // Because of header
        async_std::task::spawn(flush_worker(PathBuf::from(dir.as_ref()), fid, rx));
        Ok(Self {
            file_id: fid,
            file: df,
            flush_channel,
            head_offset,
            blocks_offset,
            block_count,
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
            offset,
        };

        self.file
            .seek(SeekFrom::Start(self.head_offset))
            .map_err(|_| "Can't seek file")?;
        self.file
            .write_all(&block_entry.as_be_bytes()[..])
            .map_err(|_| "Cant write block id")?;
        self.head_offset += BLOCK_ENTRY_SZ as u64;

        // Write nblocks
        self.block_count += 1;
        self.file
            .seek(SeekFrom::Start(24))
            .map_err(|_| "Can't seek file")?;
        self.file
            .write_all(&self.block_count.to_be_bytes()[..])
            .map_err(|_| "Can't write n blocks")?;

        // Send flush signal to flush worker
        if self.block_count % 5 == 0
            && self.block_count > 0
            && self.flush_channel.try_send(()).is_ok()
        {
            {}
        }

        let block_id = BlockId {
            mint,
            maxt,
            offset,
            file_id: self.file_id,
        };
        Ok(block_id)
    }
}

impl Drop for FileItem {
    fn drop(&mut self) {
        self.file.sync_all().unwrap();
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

    df.sync_all().await.unwrap();

    #[cfg(target_os = "linux")]
    unsafe {
        let df_fd = df.as_raw_fd();
        libc::posix_fadvise64(df_fd, 0, 0, libc::POSIX_FADV_DONTNEED);
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
            //println!("LOADING BLOCK AT OFFSET: {}", offset);

            if self.file.is_none() || self.current_file_id != file_id {
                self.open_file(file_id);
            }

            self.read_block(offset, buf);

            Some(BLOCKSZ)
        }
    }
}

fn file_id_from_path<P: AsRef<Path>>(path: P) -> Result<usize, &'static str> {
    path.as_ref()
        .file_name()
        .ok_or("Can't get file name")?
        .to_str()
        .ok_or("Can't parse to str")?
        .parse::<usize>()
        .map_err(|_| "Cant parse string to file id")
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::prelude::*;
    use tempdir::TempDir;

    #[test]
    fn test_write_read() {
        let dir = TempDir::new("filetest").unwrap();
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
        let dir = TempDir::new("filetest2").unwrap();
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
            assert_eq!(&buf[..50], &data[counter].as_slice()[..50]);
            counter += 1;
        }
    }

    #[test]
    fn test_write_some_read() {
        let dir = TempDir::new("filetest3").unwrap();
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

    #[test]
    fn test_write_restart() {
        let dir = TempDir::new("filetest4").unwrap();
        let file_store = FileStore::new(dir.path());
        let mut rng = thread_rng();
        let mut thread_writer = file_store.thread_writer();
        let id = SeriesId(0);
        let blocks = (0..1000)
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

        drop(thread_writer);

        let file_store2 = FileStore::load(dir.path()).unwrap();

        for item in file_store.index.iter() {
            let k = item.key();
            let blocks = item.value();
            let blocks2 = file_store2.index.get(k).unwrap();
            let snapshot = blocks.snapshot();
            let snapshot2 = blocks2.snapshot();
            for (a, b) in snapshot.iter().zip(snapshot2.iter()) {
                assert_eq!(a, b);
            }
        }
        assert_eq!(file_store2.file_allocator.load(SeqCst), 9);
    }
}
