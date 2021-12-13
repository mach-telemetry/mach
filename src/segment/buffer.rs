use crate::segment::{full_segment::FullSegment, Error, InnerPushStatus};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

pub const SEGSZ: usize = 256;

pub type Column = [[u8; 8]; SEGSZ];
pub type ColumnSet = [Column];

#[repr(C)]
pub struct Buffer<const V: usize> {
    pub id: AtomicUsize,
    pub atomic_len: AtomicUsize,
    pub len: usize,
    pub ts: [u64; SEGSZ],
    pub data: [Column; V],
    pub reuse_flag: AtomicBool,
}

impl<const V: usize> Buffer<V> {
    pub fn new() -> Self {
        Buffer {
            id: AtomicUsize::new(0),
            ts: [0u64; SEGSZ],
            len: 0,
            atomic_len: AtomicUsize::new(0),
            reuse_flag: AtomicBool::new(false),
            data: [[[0u8; 8]; 256]; V],
        }
    }

    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
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
            if self.len < SEGSZ {
                Ok(InnerPushStatus::Done)
            } else {
                Ok(InnerPushStatus::Flush)
            }
        }
    }

    pub fn to_flush(&self) -> Option<FullSegment> {
        let len = self.atomic_len.load(SeqCst);
        if self.len > 0 {
            Some( FullSegment {
                len: self.atomic_len.load(SeqCst),
                nvars: V,
                ts: &self.ts,
                data: &self.data[..],
            })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.atomic_len.load(SeqCst)
    }

    pub fn reuse(&mut self, id: usize) {
        self.reuse_flag.store(true, SeqCst);
        self.id.store(id, SeqCst);
        self.atomic_len.store(0, SeqCst);
        self.len = 0;
        self.reuse_flag.store(false, SeqCst);
    }

    pub fn read(&self) -> Result<ReadBuffer, Error> {
        let id = self.id.load(SeqCst);

        while self.reuse_flag.load(SeqCst) {}

        let len = self.atomic_len.load(SeqCst);
        let mut data = Vec::new();
        let mut ts = [0u64; 256];

        // Note: In case this buffer is being recycled, this would race. That's fine because we can
        // just treat the data as junk and return an error
        ts[..len].copy_from_slice(&self.ts[..len]);
        for v in self.data.iter() {
            data.extend_from_slice(&v[..len]);
        }

        while self.reuse_flag.load(SeqCst) {}
        if self.id.load(SeqCst) == id {
            Ok(ReadBuffer { len, id, ts, data })
        } else {
            Err(Error::InconsistentCopy)
        }
    }
}

pub struct ReadBuffer {
    pub id: usize,
    pub len: usize,
    pub ts: [u64; 256],
    pub data: Vec<[u8; 8]>,
}

impl ReadBuffer {
    pub fn get_timestamp_at(&self, i: usize) -> u64 {
        let i = self.len - i - 1;
        self.ts[i]
    }

    pub fn get_value_at(&self, var: usize, i: usize) -> [u8; 8] {
        let i = self.len - i - 1;
        self.variable(var)[i]
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn timestamps(&self) -> &[u64] {
        &self.ts[..self.len]
    }

    pub fn variable(&self, id: usize) -> &[[u8; 8]] {
        let start = self.len * id;
        &self.data[start..start + self.len]
    }
}
