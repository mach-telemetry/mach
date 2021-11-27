mod buffer;
mod segment;
mod wrapper;

use std::{
    sync::atomic::{AtomicBool, Ordering::SeqCst},
};

pub enum Error {
    PushIntoFull,
    InconsistentCopy,
    MultipleWriters,
    UnsupportedVariables,
    UnsupportedSegments,
    Flushing,
}

pub struct ActiveSegment {
    has_writer: AtomicBool,
    inner: wrapper::Segment,
}

pub struct Writer<'active> {
    inner: wrapper::Segment,
    lifetime: &'active ActiveSegment,
    flusher: fn(wrapper::Segment),
}

/// Safety for send and sync: there can only be one writer and the writes and concurrent reads are
/// protected (no races) within buffer
unsafe impl Send for ActiveSegment {}
unsafe impl Sync for ActiveSegment {}

impl ActiveSegment {
    pub fn new(b: usize, v: usize) -> Self {
        Self {
            has_writer: AtomicBool::new(false),
            inner: wrapper::Segment::new(b, v),
        }
    }

    pub fn writer(&self, flusher: fn(wrapper::Segment)) -> Result<Writer, Error> {
        if self.has_writer.swap(true, SeqCst) {
            Err(Error::MultipleWriters)
        } else {
            Ok(Writer {
                inner: self.inner,
                lifetime: self,
                flusher,
            })
        }
    }
}

impl<'active> Writer<'active> {
    pub fn push(&mut self, ts: u64, val: &[[u8; 8]]) -> Result<(), Error> {
        match self.inner.push(ts, val) {
            Ok(()) => Ok(()),
            Err(Error::PushIntoFull) => {
                (self.flusher)(self.inner.clone());
                Err(Error::PushIntoFull)
            },
            Err(x) => Err(x),
        }
    }
}
