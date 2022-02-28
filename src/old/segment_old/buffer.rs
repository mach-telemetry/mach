use crate::constants::*;
use crate::segment::{full_segment::FullSegment, Error};
use crate::utils::wp_lock::*;
use crate::sample::Bytes;
use crate::runtime::RUNTIME;
use std::cell::UnsafeCell;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering::SeqCst}};
use std::convert::TryInto;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use std::sync::mpsc::{channel, Sender, Receiver, sync_channel, SyncSender};
use lazy_static::*;

pub type Column = [[u8; 8]; SEGSZ];
//pub type ColumnSet = [Column];

fn vector_allocator(heaps: usize, sender: SyncSender<Vec<[u8; 8]>>) {
    loop {
        match sender.send(Vec::with_capacity(256 * heaps * FREE_INTERVAL)) {
            Ok(_) => {},
            Err(_) => break,
        }
    }
}

fn vector_freer(receiver: Receiver<Vec<[u8; 8]>>) {
    let mut last = std::time::Instant::now();
    let mut counter = 0;
    while let Ok(data) = receiver.recv() {
        counter += data.len();
        //let mut now = std::time::Instant::now();
        //for entry in data {
        //    let d = unsafe { Bytes::from_sample_entry(entry) };
        //    drop(d); // drop bytes to free heap
        //}
        //last = std::time::Instant::now();
    }
}

static FREE_INTERVAL: usize = 4;

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
    //heap_pointers: [bool; V],
    //heap: Vec<Vec<u8>>,
    //last_reset: std::time::Instant,
}

impl<const V: usize> InnerBuffer<V> {
    fn new(heap_pointers: &[bool]) -> Self {

        //let heaps: usize= heap_pointers.iter().map(|x| *x as usize).sum();

        //let mut heap = Vec::new();
        //for _ in 0..heaps {
        //    heap.push(Vec::with_capacity(2_000_000));
        //}

        //let (tx, pointers_alloc) = sync_channel(5);
        //std::thread::spawn(move || {
        //    vector_allocator(heaps, tx)
        //});
        //let (pointers_free, rx) = channel();
        //std::thread::spawn(move || {
        //    vector_freer(rx)
        //});
        //let pointers = pointers_alloc.recv().unwrap();

        InnerBuffer {
            len: 0,
            inner: Inner {
                ts: [0u64; SEGSZ],
                data: [[[0u8; 8]; 256]; V],
            },
            atomic_len: AtomicUsize::new(0),
            //heap_pointers: heap_pointers.try_into().unwrap(),
            //heap,
            //heap_len: 0,
            //pointers,
            //last_reset: std::time::Instant::now(),
            //pointers_alloc,
            //pointers_free,
        }
    }

    fn push_item(&mut self, ts: u64, item: [[u8; 8]; V]) -> Result<InnerPushStatus, Error> {
        let len = self.len;
        if len < SEGSZ - 1 {
            self.inner.ts[len] = ts;
            for i in 0..V {
                self.inner.data[i][len] = item[i];
            }
            self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            Ok(InnerPushStatus::Done)
        } else if len == SEGSZ - 1 {
            self.inner.ts[len] = ts;
            for i in 0..V {
                self.inner.data[i][len] = item[i];
            }
            self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
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
            self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            //self.len += 1;
            Ok(InnerPushStatus::Done)
        } else if len == SEGSZ - 1 {
            self.inner.ts[len] = ts;
            for i in 0..V {
                self.inner.data[i][len] = item[i];
            }
            self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            //self.len += 1;
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
            self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            //self.len += 1;
            Ok(InnerPushStatus::Done)
        } else if len == SEGSZ - 1 {
            self.inner.ts[len] = ts;
            self.inner.data[0][len] = item;
            self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            //self.len += 1;
            Ok(InnerPushStatus::Flush)
        } else {
            Err(Error::PushIntoFull)
        }
    }

    fn reset(&mut self) {

        //let len = self.atomic_len.load(SeqCst);
        //for (v, heap) in self.heap_pointers.iter().enumerate() {
        //    if *heap {
        //        let mut data: [[u8; 8]; 256] = self.inner.data[v];
        //        std::thread::spawn(move || {
        //            for entry in &data[..len] {
        //                let d = unsafe { Bytes::from_sample_entry(*entry) };
        //                drop(d); // drop bytes to free heap
        //            }
        //        });
        //    }
        //}
        // Drop heap allocated pointers if a column is pointers to the heap
        //if self.pointers.len() == self.pointers.capacity() {
        //    let now = std::time::Instant::now();
        //    let mut data = self.pointers_alloc.recv().unwrap();
        //    std::mem::swap(&mut data, &mut self.pointers);
        //    let elapsed1 = now.elapsed();
        //    //println!("take vector: {:?}", elapsed1);
        //    let now = std::time::Instant::now();
        //    //self.pointers_free.send(data).unwrap();
        //    std::thread::spawn(move || {
        //        for entry in data {
        //            let d = unsafe { Bytes::from_sample_entry(entry) };
        //            drop(d); // drop bytes to free heap
        //        }
        //    });
        //    let elapsed2 = now.elapsed();
        //    //println!("spawn time: {:?}", elapsed2);
        //    //println!("total time: {:?}", elapsed1 + elapsed2);
        //}

        ////println!("last-reset elapsed {:?}", self.last_reset.elapsed());
        //self.last_reset = std::time::Instant::now();
        self.atomic_len.store(0, SeqCst);
        self.len = 0;
        //self.heap_len = 0;
    }

    fn read(&self) -> ReadBuffer {
        let len = self.atomic_len.load(SeqCst);
        //let len = self.len;
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
        let len = self.atomic_len.load(SeqCst);
        //let len = self.len;
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

/// SAFETY: Inner buffer doesn't deallocate memory in any of its API (except Drop)
unsafe impl<const V: usize> NoDealloc for InnerBuffer<V> {}

#[repr(C)]
pub struct Buffer<const V: usize> {
    inner: WpLock<InnerBuffer<V>>,
}

impl<const V: usize> Buffer<V> {
    pub fn new(heap_pointers: &[bool]) -> Self {
        Self {
            inner: WpLock::new(InnerBuffer::new(heap_pointers)),
        }
    }

    pub fn push_item(&mut self, ts: u64, item: [[u8; 8]; V]) -> Result<InnerPushStatus, Error> {
        // Safe because the push method does not race with another method in buffer
        unsafe { self.inner.unprotected_write().push_item(ts, item) }
    }

    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
        // Safe because the push method does not race with another method in buffer
        unsafe { self.inner.unprotected_write().push(ts, item) }
    }

    pub fn push_univariate(&mut self, ts: u64, item: [u8; 8]) -> Result<InnerPushStatus, Error> {
        // Safe because the push method does not race with another method in buffer
        unsafe { self.inner.unprotected_write().push_univariate(ts, item) }
    }

    pub fn reset(&mut self) {
        self.inner.protected_write().reset()
    }

    pub fn read(&self) -> Option<ReadBuffer> {
        let read_guard = self.inner.protected_read();
        let read_result = read_guard.read();
        match read_guard.release() {
            Ok(_) => Some(read_result),
            Err(_) => None,
        }
    }

    pub fn to_flush(&self) -> Option<FullSegment> {
        // Safe because the to_flush method does not race with another method requiring mutable
        // access. Uses ref because we can't use the wp lock guard as the lifetime
        unsafe { self.inner.unprotected_read().to_flush() }
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
