mod buffer;
mod segment;
mod wrapper;

use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    Arc,
};
pub use buffer::Flushable;

//pub use wrapper::Segment;

#[derive(Debug)]
pub enum Error {
    PushIntoFull,
    InconsistentCopy,
    MultipleWriters,
    UnsupportedVariables,
    UnsupportedSegments,
    FlushFailed,
    FlushingHead,
}

pub enum PushStatus {
    Done,
    Flush
}

#[derive(Clone)]
pub struct Segment {
    has_writer: Arc<AtomicBool>,
    inner: wrapper::Segment,
}

pub struct WriteSegment {
    inner: wrapper::Segment,
    has_writer: Arc<AtomicBool>,
}

pub struct FlushSegment {
    inner: wrapper::Segment,
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

    pub fn snapshot(&self) -> Result<ReadSegment, Error> {
        Ok(ReadSegment {
            inner: self.inner.read()?
        })
    }
}

impl WriteSegment {
    pub fn push(&mut self, ts: u64, val: &[[u8; 8]]) -> Result<PushStatus, Error> {
        self.inner.push(ts, val)
    }

    pub fn flush_segment(&self) -> FlushSegment {
        FlushSegment {
            inner: self.inner.clone()
        }
    }

    pub fn close(self) {
        self.has_writer.swap(false, SeqCst);
    }
}

impl FlushSegment {
    pub fn to_flush(&self) -> Option<Flushable> {
        self.inner.to_flush()
    }
}
