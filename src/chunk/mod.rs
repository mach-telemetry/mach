mod inner;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, AtomicBool, Ordering::SeqCst}
    },
    mem::{self, MaybeUninit},
    ops::{Deref, DerefMut},
};
use crate::compression::Compression;
use crate::utils::{QueueAllocator, Qrc};
use inner::{InnerChunk, ChunkEntry};
//use crate::compression::byte_vec::Compression;

const CHUNK_THRESHOLD_SIZE: usize = 8192;
const CHUNK_THRESHOLD_COUNT: usize = 16;

pub enum Error {
    PushIntoFull,
    InconsistentChunkGeneration,
    ChunkEntryLoad,
}

#[derive(Clone)]
pub struct Chunk {
    inner: Arc<InnerChunk>,
    has_writer: Arc<AtomicBool>,
}

impl Chunk {
    pub fn new(tsid: u64, compression: Compression) -> Self {
        let inner = Arc::new(InnerChunk::new(tsid, compression));
        let has_writer = Arc::new(AtomicBool::new(false));

        Chunk {
            inner,
            has_writer
        }
    }

    pub fn writer(&self) -> WriteChunk {
        if self.has_writer.swap(true, SeqCst) {
            panic!("multiple writers")
        } else {
            WriteChunk {
                inner: self.inner.clone(),
                has_writer: self.has_writer.clone(),
            }
        }
    }

    pub fn read(&self) -> Result<Vec<ChunkEntry>, Error> {
        self.inner.read()
    }
}

pub struct WriteChunk {
    inner: Arc<InnerChunk>,
    has_writer: Arc<AtomicBool>,
}

impl WriteChunk {
    pub fn push(&mut self, ts: &[u64], values: &[&[[u8; 8]]]) -> Result<(), Error> {
        // Safety: There's only one writer. Concurrent readers are coordinated with writers based
        // on the Entry and InnerChunk structs. Entry coordinates by versions, and InnerChunk
        // coordinates by atomic counter
        unsafe { Arc::get_mut_unchecked(&mut self.inner).push(ts, values) }
    }

    pub fn flush_chunk(&self) -> FlushChunk {
        FlushChunk {
            inner: self.inner.clone()
        }
    }
}

impl Drop for WriteChunk {
    fn drop(&mut self) {
        self.has_writer.swap(false, SeqCst);
    }
}

pub struct FlushChunk {
    inner: Arc<InnerChunk>
}

//impl FlushChunk {
//    pub fn generate_chunk(&self) -> 
//}

