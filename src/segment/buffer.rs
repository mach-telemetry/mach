use crate::segment::{full_segment::FullSegment, Error, InnerPushStatus};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::cell::UnsafeCell;
use crate::utils::wp_lock::*;

pub const SEGSZ: usize = 256;

pub type Column = [[u8; 8]; SEGSZ];
pub type ColumnSet = [Column];

struct Inner<const V: usize> {
    len: usize,
    ts: [u64; SEGSZ],
    data: [Column; V],
}

struct InnerBuffer<const V: usize> {
    atomic_len: AtomicUsize,
    inner: UnsafeCell<Inner<V>>,
}

impl<const V: usize> InnerBuffer<V> {
    fn new() -> Self {
        InnerBuffer {
            inner: UnsafeCell::new(Inner {
                len: 0,
                ts: [0u64; SEGSZ],
                data: [[[0u8; 8]; 256]; V],
            }),
            atomic_len: AtomicUsize::new(0),
        }
    }

    fn push(&self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
        let inner: &mut Inner<V> = unsafe { self.inner.get().as_mut().unwrap() };
        if inner.len == SEGSZ {
            Err(Error::PushIntoFull)
        } else {
            inner.ts[inner.len] = ts;
            for (var, col) in inner.data.iter_mut().enumerate() {
                col[inner.len] = item[var];
            }
            inner.len += 1;
            self.atomic_len.store(inner.len, SeqCst);
            if inner.len < SEGSZ {
                Ok(InnerPushStatus::Done)
            } else {
                Ok(InnerPushStatus::Flush)
            }
        }
    }

    fn reset(&mut self) {
        let inner: &mut Inner<V> = unsafe { self.inner.get().as_mut().unwrap() };
        inner.len = 0;
        self.atomic_len.store(0, SeqCst);
    }

    fn read(&self) -> ReadBuffer {
        let len = self.atomic_len.load(SeqCst);
        let mut data = Vec::new();
        let mut ts = [0u64; 256];
        let inner: &Inner<V> = unsafe { self.inner.get().as_ref().unwrap() };
        ts[..len].copy_from_slice(&inner.ts[..len]);
        for v in inner.data.iter() {
            data.extend_from_slice(&v[..len]);
        }
        ReadBuffer { len, ts, data }
    }

    fn to_flush(&self) -> Option<FullSegment> {
        let len = self.atomic_len.load(SeqCst);
        let inner: &Inner<V> = unsafe { self.inner.get().as_ref().unwrap() };
        if inner.len > 0 {
            Some(FullSegment {
                len: self.atomic_len.load(SeqCst),
                nvars: V,
                ts: &inner.ts,
                data: &inner.data[..],
            })
        } else {
            None
        }
    }
}

impl<const V: usize> ReadCopy for InnerBuffer<V> {
    type Copied = ReadBuffer;
    fn read_copy(&self) -> Self::Copied {
        self.read()
    }
}

#[repr(C)]
pub struct Buffer<const V: usize> {
    inner: WpLock<InnerBuffer<V>>
}

impl<const V: usize> Buffer<V> {
    pub fn new() -> Self {
        Self {
            inner: WpLock::new(InnerBuffer::new())
        }
    }

    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
        // Safe because the push method does not race with another method in buffer
        unsafe { self.inner.as_ref().push(ts, item) }
    }

    pub fn reset(&mut self) {
        self.inner.write().reset()
    }

    pub fn read(&self) -> Option<ReadBuffer> {
        self.inner.read()
    }

    pub fn to_flush(&self) -> Option<FullSegment> {
        // Safe because the to_flush method does not race with another method requiring mutable
        // access. Uses ref because we can't use the wp lock guard as the lifetime
        unsafe { self.inner.as_ref().to_flush() }
    }
}

pub struct ReadBuffer {
    len: usize,
    ts: [u64; 256],
    data: Vec<[u8; 8]>,
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
