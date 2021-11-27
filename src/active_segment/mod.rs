mod buffer;
mod segment;
mod wrapper;

use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    Arc,
};

//pub use wrapper::Segment;

pub enum Error {
    PushIntoFull,
    InconsistentCopy,
    MultipleWriters,
    UnsupportedVariables,
    UnsupportedSegments,
    Flushing,
}

#[derive(Clone)]
pub struct Segment {
    has_writer: Arc<AtomicBool>,
    inner: wrapper::Segment,
}

pub struct WriteSegment {
    inner: wrapper::Segment,
    has_writer: Arc<AtomicBool>,
    flusher: fn(FlushSegment),
}

pub struct FlushSegment {
    inner: wrapper::Segment,
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

    pub fn writer(&self, flusher: fn(FlushSegment)) -> Result<WriteSegment, Error> {
        if self.has_writer.swap(true, SeqCst) {
            Err(Error::MultipleWriters)
        } else {
            Ok(WriteSegment {
                inner: self.inner,
                has_writer: self.has_writer.clone(),
                flusher,
            })
        }
    }
}

impl WriteSegment {
    pub fn push(&mut self, ts: u64, val: &[[u8; 8]]) -> Result<(), Error> {
        match self.inner.push(ts, val) {
            Ok(()) => Ok(()),
            Err(Error::PushIntoFull) => {
                (self.flusher)(FlushSegment {
                    inner: self.inner.clone(),
                });
                Err(Error::PushIntoFull)
            }
            Err(x) => Err(x),
        }
    }

    pub fn close(self) {
        self.has_writer.swap(false, SeqCst);
    }
}
