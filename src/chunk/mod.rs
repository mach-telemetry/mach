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
use crate::segment::FullSegment;
use inner::{InnerChunk, ChunkEntry};
//use crate::compression::byte_vec::Compression;

const CHUNK_THRESHOLD_SIZE: usize = 8192;
const CHUNK_THRESHOLD_COUNT: usize = 16;

#[derive(Eq, PartialEq, Debug)]
pub enum Error {
    PushIntoFull,
    InconsistentChunkGeneration,
    ChunkEntryLoad,
    MultipleWriters,
    MultipleFlushers,
}

#[derive(Eq, PartialEq, Debug)]
pub enum PushStatus {
    Done,
    Flush,
}

pub struct SerializedChunk {
    pub bytes: usize,
    pub tsid: u64,
    pub mint: u64,
    pub maxt: u64,
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

    pub fn writer(&self) -> Result<WriteChunk, Error> {
        if self.has_writer.swap(true, SeqCst) {
            Err(Error::MultipleWriters)
        } else {
            Ok(WriteChunk {
                inner: self.inner.clone(),
                has_writer: self.has_writer.clone(),
            })
        }
    }

    pub fn flusher(&self) -> Result<FlushChunk, Error> {
        if self.has_flusher.swap(true, SeqCst) {
            Err(Error::MultipleFlushers)
        } else {
            Ok(FlushChunk {
                inner: self.inner.clone(),
                has_flusher: self.has_flusher.clone(),
            })
        }
    }

    pub fn read(&self) -> Result<ReadChunk, Error> {
        Ok(ReadChunk {
            inner: self.inner.read()?
        })
    }
}

pub struct WriteChunk {
    inner: Arc<InnerChunk>,
    has_writer: Arc<AtomicBool>,
}

impl WriteChunk {
    pub fn push(&mut self, segment: &FullSegment) -> Result<PushStatus, Error> {
        // Safety: There's only one writer and at most one flusher.
        //
        // Concurrent readers are coordinated with writers based on the Entry and InnerChunk
        // structs. Entry coordinates by versions, and InnerChunk coordinates by atomic counter.
        //
        // Concurrent flusher do not overrun with writer. See generate_chunk method for why.
        unsafe { Arc::get_mut_unchecked(&mut self.inner).push(segment) }
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

impl FlushChunk {

    /// Serialize the chunk into bytes
    pub fn serialize(&self, v: &mut Vec<u8>) -> Result<SerializedChunk, Error> {
        self.inner.serialize(v)
    }

    /// This clears the counter and size of the chunk. Only do this if they data have been
    /// serialized!
    pub fn clear(&self) {
        self.inner.clear()
    }
}

pub struct ReadChunk {
    inner: Vec<ChunkEntry>,
}

impl Deref for ReadChunk {
    type Target = [ChunkEntry];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;
    use crate::segment::{self, Segment};
    use crate::compression::DecompressBuffer;

    #[test]
    fn test_push_behavior() {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let segment = Segment::new(3, nvars);
        let mut writer = segment.writer().unwrap();
        let mut flusher = segment.flusher().unwrap();

        let mut chunk = Chunk::new(0, Compression::LZ4(1));
        let mut chunk_writer = chunk.writer().unwrap();

        let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
            let mut values = vec![[0u8; 8]; nvars];
            for (i, v) in items.iter().enumerate() {
                values[i] = v.to_be_bytes();
            }
            values
        };

        for item in &data[..256 * CHUNK_THRESHOLD_COUNT - 1] {
            let v = to_values(&item.values[..]);
            match writer.push(item.ts, &v[..]) {
                Ok(segment::PushStatus::Done) => {},
                Ok(segment::PushStatus::Flush) => {
                    let seg = flusher.to_flush().unwrap();
                    assert_eq!(chunk_writer.push(&seg), Ok(PushStatus::Done));
                    flusher.flushed();
                },
                Err(_) => unimplemented!(),
            }
        }

        {
            let item = &data[256 * CHUNK_THRESHOLD_COUNT];
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(segment::PushStatus::Flush));
            let seg = flusher.to_flush().unwrap();
            assert_eq!(chunk_writer.push(&seg), Ok(PushStatus::Flush));
            flusher.flushed();
        }

        for item in &data[256 * CHUNK_THRESHOLD_COUNT..256 * (CHUNK_THRESHOLD_COUNT + 1)] {
            let v = to_values(&item.values[..]);
            match writer.push(item.ts, &v[..]) {
                Ok(segment::PushStatus::Done) => {},
                Ok(segment::PushStatus::Flush) => { break; },
                Err(_) => unimplemented!(),
            }
        }

        {
            let seg = flusher.to_flush().unwrap();
            assert_eq!(chunk_writer.push(&seg), Err(Error::PushIntoFull));
        }
    }

    #[test]
    fn test_check_data() {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let segment = Segment::new(3, nvars);
        let mut writer = segment.writer().unwrap();
        let mut flusher = segment.flusher().unwrap();

        let mut chunk = Chunk::new(0, Compression::LZ4(1));
        let mut chunk_writer = chunk.writer().unwrap();

        let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
            let mut values = vec![[0u8; 8]; nvars];
            for (i, v) in items.iter().enumerate() {
                values[i] = v.to_be_bytes();
            }
            values
        };

        let mut exp_ts = Vec::new();
        let mut exp_values = Vec::new();
        for _ in 0..nvars {
            exp_values.push(Vec::new());
        }

        for item in &data[..256 * CHUNK_THRESHOLD_COUNT] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            for i in 0..nvars {
                exp_values[i].push(v[i]);
            }
            match writer.push(item.ts, &v[..]) {
                Ok(segment::PushStatus::Done) => {},
                Ok(segment::PushStatus::Flush) => {
                    let seg = flusher.to_flush().unwrap();
                    assert!(chunk_writer.push(&seg).is_ok());
                    flusher.flushed();
                },
                Err(_) => unimplemented!(),
            }
        }

        let chunk_entries = chunk.read().unwrap();
        let mut buf = DecompressBuffer::new();
        for entry in chunk_entries.iter() {
            let decompressed = Compression::decompress(entry.bytes(), &mut buf).unwrap();
        }

        assert_eq!(buf.timestamps().len(), 256 * CHUNK_THRESHOLD_COUNT);
        assert_eq!(buf.timestamps(), exp_ts.as_slice());
        for i in 0..nvars {
            assert_eq!(buf.variable(i).len(), buf.timestamps().len());
        }
    }
}
