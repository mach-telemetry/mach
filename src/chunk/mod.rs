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
    has_flusher: Arc<AtomicBool>,
}

impl Chunk {
    pub fn new(tsid: u64, compression: Compression) -> Self {
        let inner = Arc::new(InnerChunk::new(tsid, compression));
        let has_writer = Arc::new(AtomicBool::new(false));
        let has_flusher = Arc::new(AtomicBool::new(false));

        Chunk {
            inner,
            has_writer,
            has_flusher,
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

    pub fn flusher(&self) -> FlushChunk {
        if self.has_flusher.swap(true, SeqCst) {
            panic!("multiple writers")
        } else {
            FlushChunk {
                inner: self.inner.clone(),
                has_flusher: self.has_flusher.clone(),
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
        // Safety: There's only one writer and at most one flusher.
        //
        // Concurrent readers are coordinated with writers based on the Entry and InnerChunk
        // structs. Entry coordinates by versions, and InnerChunk coordinates by atomic counter.
        //
        // Concurrent flusher do not overrun with writer. See generate_chunk method for why.
        unsafe { Arc::get_mut_unchecked(&mut self.inner).push(ts, values) }
    }
}

impl Drop for WriteChunk {
    fn drop(&mut self) {
        self.has_writer.swap(false, SeqCst);
    }
}

pub struct FlushChunk {
    inner: Arc<InnerChunk>,
    has_flusher: Arc<AtomicBool>,
}

impl Drop for FlushChunk {
    fn drop(&mut self) {
        self.has_flusher.swap(false, SeqCst);
    }
}

//impl FlushChunk {
//    pub fn generate_chunk(&self) -> 
//}

