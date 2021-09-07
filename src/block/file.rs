use crate::{
    block::{BlockError, BlockKey, BlockReader, BlockStore, BlockWriter, BLOCKSZ},
    tsdb::{Dt, SeriesId},
    utils::list::{List, ListReader, ListWriter},
};
use std::{
    collections::HashMap,
    convert::AsRef,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};
//use std::{
//};
use dashmap::DashMap;

//const BLOCKSZ: usize = 8192;

#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct BlockId {
    file_id: usize,
    block_id: usize,
    mint: u64,
    maxt: u64,
}

#[derive(Clone)]
pub struct FileStore {
    index: DashMap<SeriesId, List<BlockId>>,
    file_allocator: Arc<AtomicUsize>,
    dir_path: PathBuf,
}

impl FileStore {
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        Self {
            index: DashMap::new(),
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
            max_blocks: 8092,
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
    index: DashMap<SeriesId, List<BlockId>>,
    index_writers: HashMap<SeriesId, ListWriter<BlockId>>,
    file_allocator: Arc<AtomicUsize>,
    file: Option<File>,
    file_id: usize,
    block_count: usize,
    max_blocks: usize,
    dir_path: PathBuf,
}

impl ThreadFileWriter {
    fn open_file(&mut self) -> Result<(), BlockError> {
        self.file_id = self.file_allocator.fetch_add(1, SeqCst);
        let df = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.dir_path.join(self.file_id.to_string()))
            .map_err(|_| BlockError("Can't open file"))?;
        self.file = Some(df);
        self.block_count = 0;
        Ok(())
    }
}

impl BlockWriter for ThreadFileWriter {
    fn write_block(&mut self, key: BlockKey, block: &[u8]) -> Result<(), BlockError> {
        match &self.file {
            None => self.open_file()?,
            Some(_) => {
                if self.block_count == self.max_blocks {
                    self.open_file()?
                }
            }
        }
        let block_id = BlockId {
            mint: key.mint,
            maxt: key.maxt,
            file_id: self.file_id,
            block_id: self.block_count,
        };

        #[allow(clippy::or_fun_call)]
        self.index_writers
            .entry(key.id)
            .or_insert(self.index.entry(key.id).or_insert(List::new()).writer())
            .push(block_id);

        self.file
            .as_mut()
            .unwrap()
            .write_all(block)
            .map_err(|_| BlockError("Can't flush file"))?;
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
    buf: [u8; BLOCKSZ],
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
            buf: [0u8; BLOCKSZ],
        }
    }

    fn open_file(&mut self, file_id: usize) {
        self.file = Some(
            File::open(self.dir.join(file_id.to_string()))
                .expect("Can't find a file that's supposed to exist"),
        );
        self.current_file_id = file_id;
    }

    fn read_block(&mut self, block_id: usize) {
        let offset = block_id * BLOCKSZ;
        let f = self.file.as_mut().unwrap();
        f.seek(SeekFrom::Start(offset as u64)).unwrap();
        f.read_exact(&mut self.buf).unwrap();
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
            if item.mint <= maxt && mint <= item.maxt {
                self.items.push(*item);
            } else if item.mint > maxt {
                break;
            }
        }
        assert!(self.items.is_sorted());
    }

    fn next_block(&mut self) -> Option<&[u8]> {
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

            self.read_block(block_id);

            Some(&self.buf)
        }
    }
}
