mod buffer;
mod segment;
mod wrapper;
mod full_segment;

use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    Arc,
};

pub use full_segment::FullSegment;

//pub use wrapper::Segment;

#[derive(Eq, PartialEq, Debug)]
pub enum Error {
    PushIntoFull,
    InconsistentCopy,
    MultipleWriters,
    UnsupportedVariables,
    UnsupportedSegments,
    FlushFailed,
    FlushingHead,
    MultipleFlushers,
}

#[derive(Eq, PartialEq, Debug)]
pub enum PushStatus {
    Done,
    Flush
}

#[derive(Clone)]
pub struct Segment {
    has_writer: Arc<AtomicBool>,
    has_flusher: Arc<AtomicBool>,
    inner: wrapper::Segment,
}

pub struct WriteSegment {
    inner: wrapper::Segment,
    has_writer: Arc<AtomicBool>,
}

pub struct FlushSegment {
    inner: wrapper::Segment,
    has_flusher: Arc<AtomicBool>,
}

pub struct ReadSegment {
    inner: Vec<buffer::ReadBuffer>,
}

/// Safety for send and sync: there can only be one writer and the writes and concurrent reads are
/// protected (no races) within buffer
unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}

impl Segment {
    pub fn new(b: usize, v: usize) -> Self {
        Self {
            has_writer: Arc::new(AtomicBool::new(false)),
            has_flusher: Arc::new(AtomicBool::new(false)),
            inner: wrapper::Segment::new(b, v),
        }
    }

    pub fn writer(&self) -> Result<WriteSegment, Error> {
        if self.has_writer.swap(true, SeqCst) {
            Err(Error::MultipleWriters)
        } else {
            Ok(WriteSegment {
                inner: self.inner.clone(),
                has_writer: self.has_writer.clone(),
            })
        }
    }

    pub fn flusher(&self) -> Result<FlushSegment, Error> {
        if self.has_flusher.swap(true, SeqCst) {
            Err(Error::MultipleFlushers)
        } else {
            Ok(FlushSegment {
                inner: self.inner.clone(),
                has_flusher: self.has_flusher.clone(),
            })
        }
    }

    pub fn snapshot(&self) -> Result<ReadSegment, Error> {
        // Safety: Safe because a reader and a flusher do not race (see to_flush), and a reader and
        // writer can race but the reader checks the version number before returning
        unsafe {
            Ok(ReadSegment {
                inner: self.inner.read()?
            })
        }
    }
}

impl WriteSegment {
    pub fn push(&mut self, ts: u64, val: &[[u8; 8]]) -> Result<PushStatus, Error> {
        // Safety: Safe because there is only one writer, one flusher, and many concurrent readers.
        // Readers don't race with the writer because of the atomic counter. Writer and flusher do
        // not race because the writer is bounded by the flush_counter which can only be
        // incremented by the flusher
        unsafe {
            self.inner.push(ts, val)
        }
    }
}

impl FlushSegment {
    pub fn to_flush(&self) -> Option<FullSegment> {
        // Safety: Safe because there is only one flusher, one writer, and many concurrent readers.
        // Readers don't race with the flusher because the flusher does not modify the segments.
        // Writer and flusher do not race because the writer is bounded by the flush_counter,
        // incremented by this struct using the flushed method
        unsafe {
            self.inner.to_flush()
        }
    }

    pub fn flushed(&self) {
        self.inner.flushed()
    }
}
