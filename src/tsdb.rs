use crate::block::{
    BlockStore,
    BlockWriter,
    BlockReader,
    BlockKey,
    file::{
        FileStore,
        ThreadFileWriter,
        FileBlockLoader,
    },
};
use std::{
    path::Path,
    marker::PhantomData,
};

#[derive(Clone)]
pub struct Db<B, W, R>
where
    B: BlockStore<W, R>,
    W: BlockWriter,
    R: BlockReader,
{
    file_store: B,
    _w: PhantomData<W>,
    _r: PhantomData<R>,
}

impl Db<FileStore, ThreadFileWriter, FileBlockLoader> {
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        Self {
            file_store: FileStore::new(dir),
            _w: PhantomData,
            _r: PhantomData,
        }
    }
}

impl<B, W, R> Db<B, W, R>
where
    B: BlockStore<W, R>,
    W: BlockWriter,
    R: BlockReader,
{
    pub fn writer(&self) -> DbWriter<W> {
        DbWriter {
            _block_writer: self.file_store.writer()
        }
    }

    pub fn get_range(&self, id: u64, mint: u64, maxt: u64) -> DbReader<R> {
        let block_key = BlockKey { id, mint, maxt };
        DbReader {
            _block_reader: self.file_store.reader(block_key)
        }
    }
}

pub struct DbWriter<W>
where
    W: BlockWriter
{
    _block_writer: W
}

pub struct DbReader<R>
where
    R: BlockReader
{
    _block_reader: R,
}

