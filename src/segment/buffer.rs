use crate::constants::*;
use crate::segment::{full_segment::FullSegment, Error};
use crate::utils::wp_lock::*;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

pub type Column = [[u8; 8]; SEGSZ];
//pub type ColumnSet = [Column];

#[derive(Eq, PartialEq, Debug)]
pub enum InnerPushStatus {
    Done,
    Flush,
}

struct Inner<const V: usize> {
    ts: [u64; SEGSZ],
    data: [Column; V],
}

struct InnerBuffer<const V: usize> {
    atomic_len: AtomicUsize,
    len: usize,
    inner: Inner<V>,
}

impl<const V: usize> InnerBuffer<V> {
    fn new() -> Self {
        InnerBuffer {
            len: 0,
            inner: Inner {
                ts: [0u64; SEGSZ],
                data: [[[0u8; 8]; 256]; V],
            },
            atomic_len: AtomicUsize::new(0),
        }
    }

    fn push_item(
        &mut self,
        ts: u64,
        item: [[u8; 8]; V],
    ) -> Result<InnerPushStatus, Error> {
        let len = self.len;
        if len < SEGSZ - 1 {
            self.inner.ts[len] = ts;
            for i in 0..V {
                self.inner.data[i][len] = item[i];
            }
            //self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            self.len += 1;
            Ok(InnerPushStatus::Done)
        } else if len == SEGSZ - 1 {
            self.inner.ts[len] = ts;
            for i in 0..V {
                self.inner.data[i][len] = item[i];
            }
            //self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            self.len += 1;
            Ok(InnerPushStatus::Flush)
        } else {
            Err(Error::PushIntoFull)
        }
    }

    fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
        let len = self.len;
        if len < SEGSZ - 1 {
            self.inner.ts[len] = ts;
            for i in 0..V {
                self.inner.data[i][len] = item[i];
            }
            //self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            self.len += 1;
            Ok(InnerPushStatus::Done)
        } else if len == SEGSZ - 1 {
            self.inner.ts[len] = ts;
            for i in 0..V {
                self.inner.data[i][len] = item[i];
            }
            //self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            self.len += 1;
            Ok(InnerPushStatus::Flush)
        } else {
            Err(Error::PushIntoFull)
        }
    }

    fn push_univariate(&mut self, ts: u64, item: [u8; 8]) -> Result<InnerPushStatus, Error> {
        let len = self.len;
        if len < SEGSZ - 1 {
            self.inner.ts[len] = ts;
            self.inner.data[0][len] = item;
            //self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            self.len += 1;
            Ok(InnerPushStatus::Done)
        } else if len == SEGSZ - 1 {
            self.inner.ts[len] = ts;
            self.inner.data[0][len] = item;
            //self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            self.len += 1;
            Ok(InnerPushStatus::Flush)
        } else {
            Err(Error::PushIntoFull)
        }
    }

    fn reset(&mut self) {
        self.len = 0;
        //self.atomic_len.store(0, SeqCst);
    }

    fn read(&self) -> ReadBuffer {
        //let len = self.atomic_len.load(SeqCst);
        let len = self.len;
        let mut data = Vec::new();
        let mut ts = [0u64; 256];
        let inner: &Inner<V> = &self.inner;
        ts[..len].copy_from_slice(&inner.ts[..len]);
        for v in inner.data.iter() {
            data.extend_from_slice(&v[..len]);
        }
        ReadBuffer { len, ts, data }
    }

    fn to_flush(&self) -> Option<FullSegment> {
        //let len = self.atomic_len.load(SeqCst);
        let len = self.len;
        let inner: &Inner<V> = &self.inner;
        if len > 0 {
            Some(FullSegment {
                len,
                nvars: V,
                ts: &inner.ts,
                data: &inner.data[..],
            })
        } else {
            None
        }
    }
}

#[repr(C)]
pub struct Buffer<const V: usize> {
    inner: WpLock<InnerBuffer<V>>,
}

impl<const V: usize> Buffer<V> {
    pub fn new() -> Self {
        Self {
            inner: WpLock::new(InnerBuffer::new()),
        }
    }

    pub fn push_item(
        &mut self,
        ts: u64,
        item: [[u8; 8]; V],
    ) -> Result<InnerPushStatus, Error> {
        // Safe because the push method does not race with another method in buffer
        unsafe { self.inner.get_mut_ref().push_item(ts, item) }
    }

    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
        // Safe because the push method does not race with another method in buffer
        unsafe { self.inner.get_mut_ref().push(ts, item) }
    }

    pub fn push_univariate(&mut self, ts: u64, item: [u8; 8]) -> Result<InnerPushStatus, Error> {
        // Safe because the push method does not race with another method in buffer
        unsafe { self.inner.get_mut_ref().push_univariate(ts, item) }
    }

    pub fn reset(&mut self) {
        self.inner.write().reset()
    }

    pub fn read(&self) -> Option<ReadBuffer> {
        // Safety: Safe because the inner buffer's contents are not dropped by any write operation
        let read_guard = unsafe { self.inner.read() };
        let read_result = read_guard.read();
        match read_guard.release() {
            Ok(_) => Some(read_result),
            Err(_) => None,
        }
    }

    pub fn to_flush(&self) -> Option<FullSegment> {
        // Safe because the to_flush method does not race with another method requiring mutable
        // access. Uses ref because we can't use the wp lock guard as the lifetime
        unsafe { self.inner.get_ref().to_flush() }
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
