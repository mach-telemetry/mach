use crate::constants::*;
//use crate::segment::{full_segment::FullSegment, Error};
use crate::reader;
use crate::runtime::RUNTIME;
use crate::sample::{Type, Bytes};
use crate::segment::Error;
use crate::utils::wp_lock::*;
//use crate::reader::SampleIterator;
use lazy_static::*;
use serde::*;
use std::cell::UnsafeCell;
use std::convert::TryInto;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

const HEAP_SZ: usize = 1_000_000;
const HEAP_TH: usize = 3 * (HEAP_SZ / 4);

#[derive(Eq, PartialEq, Debug)]
pub enum InnerPushStatus {
    Done,
    Flush,
}

struct InnerBuffer {
    is_full: bool,
    atomic_len: AtomicUsize,
    len: usize,
    ts: [u64; SEGSZ],
    data: Vec<[[u8; 8]; SEGSZ]>,
    heap: Vec<Option<Vec<u8>>>,
    heap_flags: Vec<bool>,
}

impl InnerBuffer {
    fn new(heap_pointers: &[bool]) -> Self {
        println!("Heap pointers: {:?}", heap_pointers);
        let nvars = heap_pointers.len();
        //let heap_count = heap_pointers.iter().map(|x| *x as usize).sum();

        // Heap
        let mut heap = Vec::new();
        for in_heap in heap_pointers {
            if *in_heap {
                heap.push(Some(Vec::with_capacity(HEAP_SZ)));
            } else {
                heap.push(None);
            }
        }

        let mut data = Vec::new();
        for _ in 0..nvars {
            data.push([[0u8; 8]; SEGSZ]);
        }

        // Flag
        let heap_flags = heap_pointers.into();

        InnerBuffer {
            is_full: false,
            atomic_len: AtomicUsize::new(0),
            len: 0,
            ts: [0u64; SEGSZ],
            heap,
            data,
            heap_flags,
        }
    }

    fn push_item(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
        if self.is_full {
            return Err(Error::PushIntoFull);
        }
        let len = self.len;
        self.ts[len] = ts;
        let mut heap_offset = 0;
        for (i, heap) in self.heap_flags.iter().enumerate() {
            let mut item = item[i];
            if *heap {
                let b = unsafe { Bytes::from_sample_entry(item) };
                let bytes = b.as_raw_bytes();
                let heap = self.heap[heap_offset].as_mut().unwrap();
                let cur_len = heap.len();
                let len_after = bytes.len() + heap.len();
                heap.extend_from_slice(b.as_raw_bytes());
                if len_after > HEAP_TH {
                    self.is_full = true;
                }
                b.into_raw();
                heap_offset += 1;
                item = ((&heap[cur_len..]).as_ptr() as u64).to_be_bytes();
            }
            self.data[i][len] = item;
        }
        self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
        if self.len == SEGSZ {
            self.is_full = true;
        }
        if self.is_full {
            Ok(InnerPushStatus::Flush)
        } else {
            Ok(InnerPushStatus::Done)
        }
    }

    fn push_type(&mut self, ts: u64, items: &[Type]) -> Result<InnerPushStatus, Error> {
        if self.is_full {
            return Err(Error::PushIntoFull);
        }
        let len = self.len;
        self.ts[len] = ts;
        for (i, item) in items.iter().enumerate() {
            match item {
                Type::U64(x) => { self.data[i][len] = x.to_be_bytes(); },
                Type::F64(x) => { self.data[i][len] = x.to_be_bytes(); },
                Type::Bytes(b) => {
                    //let b = unsafe { Bytes::from_raw(*x) };
                    let bytes = b.as_raw_bytes();
                    let heap = self.heap[i].as_mut().unwrap();
                    let cur_len = heap.len();
                    let len_after = bytes.len() + heap.len();
                    heap.extend_from_slice(b.as_raw_bytes());
                    if len_after > HEAP_TH {
                        self.is_full = true;
                    }
                    let item = ((&heap[cur_len..]).as_ptr() as u64).to_be_bytes();
                    self.data[i][len] = item;
                }
            }
        }
        self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
        if self.len == SEGSZ {
            self.is_full = true;
        }
        if self.is_full {
            Ok(InnerPushStatus::Flush)
        } else {
            Ok(InnerPushStatus::Done)
        }
    }


    pub fn is_full(&self) -> bool {
        self.is_full
    }

    fn reset(&mut self) {
        self.atomic_len.store(0, SeqCst);
        self.len = 0;
        self.is_full = false;
        for h in self.heap.iter_mut() {
            match h {
                Some(v) => v.clear(),
                None => {}
            }
        }
    }

    fn read(&self) -> ReadBuffer {
        let len = self.atomic_len.load(SeqCst);
        ReadBuffer {
            len,
            ts: self.ts.into(),
            data: self.data.iter().map(|x| x[..].into()).collect(),
            heap: self.heap.clone(),
            heap_flags: self.heap_flags.clone(),
        }
    }

    fn to_flush(&self) -> Option<FlushBuffer> {
        //println!("In to_flush in Buffer");
        let len = self.atomic_len.load(SeqCst);
        if len > 0 {
            Some(FlushBuffer { len, inner: self })
        } else {
            //println!("Buffer: NONE");
            None
        }
    }
}

/// SAFETY: Inner buffer doesn't deallocate memory in any of its API (except Drop)
unsafe impl NoDealloc for InnerBuffer {}

pub struct Buffer {
    inner: WpLock<InnerBuffer>,
}

impl Buffer {
    pub fn new(heap_pointers: &[bool]) -> Self {
        Self {
            inner: WpLock::new(InnerBuffer::new(heap_pointers)),
        }
    }

    pub fn is_full(&self) -> bool {
        // Safe because the is_full method does not race with another method in buffer
        unsafe { self.inner.unprotected_read().is_full() }
    }

    pub fn push_item(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
        // Safe because the push method does not race with another method in buffer
        unsafe { self.inner.unprotected_write().push_item(ts, item) }
    }

    pub fn push_type(&mut self, ts: u64, items: &[Type]) -> Result<InnerPushStatus, Error> {
        // Safe because the push method does not race with another method in buffer
        unsafe { self.inner.unprotected_write().push_type(ts, items) }
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

    pub fn to_flush(&self) -> Option<FlushBuffer> {
        // Safe because the to_flush method does not race with another method requiring mutable
        // access. Uses ref because we can't use the wp lock guard as the lifetime
        unsafe { self.inner.unprotected_read().to_flush() }
    }
}

pub enum Variable<'a> {
    Var(&'a [[u8; 8]]),
    Heap(&'a [u8]),
}

pub struct FlushBuffer<'a> {
    len: usize,
    inner: &'a InnerBuffer,
}

impl<'a> FlushBuffer<'a> {
    pub fn variable(&self, i: usize) -> &[[u8; 8]] {
        &self.inner.data[i][..self.len]
    }

    pub fn get_variable(&self, i: usize) -> Variable {
        match &self.inner.heap[i] {
            Some(x) => Variable::Heap(x.as_slice()),
            None => Variable::Var(self.variable(i)),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn nvars(&self) -> usize {
        self.inner.heap_flags.len()
    }

    pub fn timestamps(&self) -> &[u64] {
        &self.inner.ts[..self.len]
    }
}

pub type BufferSnapshot = ReadBuffer;

#[derive(Serialize, Deserialize)]
pub struct ReadBuffer {
    len: usize,
    ts: Vec<u64>,
    data: Vec<Vec<[u8; 8]>>,
    heap: Vec<Option<Vec<u8>>>,
    heap_flags: Vec<bool>,
}

impl ReadBuffer {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn variable(&self, i: usize) -> &[[u8; 8]] {
        &self.data[i][..self.len]
    }

    pub fn get_timestamp_at(&self, i: usize) -> u64 {
        let i = self.len - i - 1;
        self.ts[i]
    }

    pub fn get_value_at(&self, var: usize, i: usize) -> [u8; 8] {
        let i = self.len - i - 1;
        self.variable(var)[i]
    }

    pub fn timestamps(&self) -> &[u64] {
        &self.ts[..self.len]
    }

    //pub fn as_segment(&self) -> reader::Segment {
    //    let mut ts = self.ts[..self.len].into();
    //    let mut data = Vec::new();
    //    let mut heap = Vec::new();

    //    for i in 0..self.data.len() {
    //        if self.heap_flags[i] {
    //            for j in 0..self.len {
    //                // Copy each bytes location into the heap
    //                let b = unsafe { Bytes::from_sample_entry(self.data[i][j]) };
    //                let l = heap.len();
    //                heap.extend_from_slice(b.as_raw_bytes());
    //                let ptr: *const u8 = (&heap[l..heap.len()]).as_ptr();
    //                data.push((ptr as u64).to_be_bytes());
    //                b.into_raw(); // prevent freeing
    //            }
    //        } else {
    //            data.extend_from_slice(&self.data[i][..self.len]);
    //        }
    //    }
    //    reader::Segment::new(ts, data, heap)
    //}

    //pub fn reader(&self) -> reader::Segment {
    //    let mut ts = Vec::new();
    //    let mut data = Vec::new();
    //    let nvars = self.heap_flags.len();

    //    for i in (0..self.len).rev() {
    //        ts.push(self.ts[i]);
    //        for col in self.data.iter() {
    //            data.push(col[i]);
    //        }
    //    }

    //    reader::Segment {
    //        ts,
    //        data,
    //        heap: self.heap.clone(),
    //        heap_flags: self.heap_flags.clone(),
    //    }
    //}
}

//impl From<ReadBuffer> for reader::Segment {
//    fn from(buf: ReadBuffer) -> reader::Segment {
//        buf.reader()
//    }
//}