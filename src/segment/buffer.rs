use crate::segment::{full_segment::FullSegment, Error};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

pub const SEGSZ: usize = 256;

pub type Column = [[u8; 8]; 256];
pub type ColumnSet = [Column];

#[repr(C)]
pub struct Buffer<const V: usize> {
    pub id: AtomicUsize,
    pub atomic_len: AtomicUsize,
    pub len: usize,
    pub ts: [u64; SEGSZ],
    pub data: [Column; V],
}

impl<const V: usize> Buffer<V> {
    pub fn new() -> Self {
        Buffer {
            id: AtomicUsize::new(0),
            ts: [0u64; SEGSZ],
            len: 0,
            atomic_len: AtomicUsize::new(0),
            data: [[[0u8; 8]; 256]; V],
        }
    }

    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
        if self.len == SEGSZ {
            Err(Error::PushIntoFull)
        } else {
            let nvars = self.data.len();

            self.ts[self.len] = ts;
            for (i, b) in (&item[..nvars]).iter().enumerate() {
                self.data[i][self.len] = *b;
            }
            self.len += 1;
            self.atomic_len.store(self.len, SeqCst);
            Ok(())
        }
    }

    pub fn to_flush(&self) -> FullSegment {
        FullSegment {
            len: self.len,
            nvars: V,
            ts: &self.ts,
            data: &self.data[..],
        }
    }

    fn len(&self) -> usize {
        self.atomic_len.load(SeqCst)
    }

    pub fn reuse(&mut self, id: usize) {
        self.id.store(id, SeqCst);
        self.atomic_len.store(0, SeqCst);
        self.len = 0;
    }

    pub fn read(&self) -> Result<ReadBuffer, Error> {
        let id = self.id.load(SeqCst);
        let len = self.atomic_len.load(SeqCst);
        let mut data = Vec::new();

        // Note: In case this buffer is being recycled, this would race. That's fine because we can
        // just treat the data as junk and return an error
        for v in self.data.iter() {
            data.extend_from_slice(&v[..len]);
        }
        if self.id.load(SeqCst) == id {
            Ok(ReadBuffer { len, data })
        } else {
            Err(Error::InconsistentCopy)
        }
    }
}

pub struct ReadBuffer {
    len: usize,
    data: Vec<[u8; 8]>,
}
