mod buffer;
mod segment;
mod wrapper;
mod full_segment;

use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    Arc,
};
use std::ops::Deref;

pub use full_segment::FullSegment;
pub use buffer::ReadBuffer;

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

impl Deref for ReadSegment {
    type Target = [buffer::ReadBuffer];
    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
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
                inner: self.inner.read()?,
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;

    #[test]
    fn test_push_flush_behavior() {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let segment = Segment::new(3, nvars);
        let mut writer = segment.writer().unwrap();
        let mut flusher = segment.flusher().unwrap();

        let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
            let mut values = vec![[0u8; 8]; nvars];
            for (i, v) in items.iter().enumerate() {
                values[i] = v.to_be_bytes();
            }
            values
        };

        for item in &data[..255] {
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(PushStatus::Done));
        }

        {
            let item = &data[255];
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(PushStatus::Flush));
        }

        for item in &data[256..512-1] {
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(PushStatus::Done));
        }

        {
            let item = &data[511];
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(PushStatus::Flush));
        }

        for item in &data[512..767] {
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(PushStatus::Done));
        }

        {
            let item = &data[767];
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(PushStatus::Flush));
        }

        {
            let item = &data[768];
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Err(Error::PushIntoFull));
        }

        flusher.flushed();

        for item in &data[768..1023] {
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(PushStatus::Done));
        }

        {
            let item = &data[1023];
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(PushStatus::Flush));
        }

        {
            let item = &data[1024];
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Err(Error::PushIntoFull));
        }

        flusher.flushed();

        {
            let item = &data[1024];
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(PushStatus::Done));
        }
    }

    #[test]
    fn test_push_flush_data() {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let segment = Segment::new(3, nvars);
        let mut writer = segment.writer().unwrap();
        let mut flusher = segment.flusher().unwrap();

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

        for item in &data[..767] {
            let v = to_values(&item.values[..]);
            assert!(writer.push(item.ts, &v[..]).is_ok());
            exp_ts.push(item.ts);
            for (e, i) in exp_values.iter_mut().zip(v.iter()) {
                e.push(*i)
            }
        }

        let seg = flusher.to_flush().unwrap();
        assert_eq!(seg.len, 256);
        assert_eq!(seg.nvars, nvars);
        assert_eq!(seg.timestamps(), &exp_ts[..256]);
        for i in 0..nvars {
            assert_eq!(seg.values(i), &exp_values[i][..256]);
        }
        flusher.flushed();

        let seg = flusher.to_flush().unwrap();
        assert_eq!(seg.len, 256);
        assert_eq!(seg.timestamps(), &exp_ts[256..512]);
        for i in 0..nvars {
            assert_eq!(seg.values(i), &exp_values[i][256..512]);
        }
        flusher.flushed();

        assert!(flusher.to_flush().is_none()) // the current buffer is not flushable yet
    }

}
