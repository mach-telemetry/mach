use crate::{
    //segment::{Segment, SegmentIterator, SegmentLike},
    types::{Sample},
    //utils::overlaps,
};
use seq_macro::seq;
use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, AtomicPtr, Ordering::{self, Acquire, Release, SeqCst}, fence},
        Arc,
    },
    mem,
    cell::UnsafeCell,
};
use nolock::queues::{
    mpmc::unbounded::{Sender, Receiver},
    DequeueError,
};

pub enum Error {
    Full
}

pub const SEGSZ: usize = 256;

struct Inner {
    inner_q: Arc<Sender<* mut Inner>>,
    strong_count: AtomicUsize,
    atomic_len: AtomicUsize,
    len: usize,
    ts: UnsafeCell<[u64; SEGSZ]>,
    data: UnsafeCell<Vec<u8>>,
}

impl Inner {
    unsafe fn push(&self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
        if self.len == SEGSZ {
            Err(Error::Full)
        } else {
            let item = unsafe {
                mem::transmute::<&[[u8; 8]], &[u8]>(item)
            };
            self.ts.get().as_mut().unwrap()[self.len] = ts;
            let v: &mut Vec<u8> = self.data.get().as_mut().unwrap();
            v.extend_from_slice(item);
            Ok(())
        }
    }

    fn new(inner_q: Arc<Sender<* mut Inner>>) -> Self {
        Inner {
            inner_q,
            strong_count: AtomicUsize::new(1),
            ts: UnsafeCell::new([0u64; SEGSZ]),
            data: UnsafeCell::new(Vec::with_capacity(8 * 256)),
            atomic_len: AtomicUsize::new(0),
            len: 0,
        }
    }

    fn new_ptr(inner_q: Arc<Sender<* mut Inner>>) -> * mut Self {
        Box::into_raw(box Inner::new(inner_q))
    }

    // ptr should have come from new_ptr
    unsafe fn from_ptr(ptr: * mut Self) -> Box<Inner> {
        Box::from_raw(ptr)
    }

    // ptr should ahve come from new_ptr
    unsafe fn incr_cnt(ptr: * mut Inner) -> usize {
        ptr.as_ref().unwrap().strong_count.fetch_add(1, SeqCst)
    }

    // ptr should have come from new_ptr
    unsafe fn decr_cnt_drop(ptr: * mut Inner) {
        let x = &ptr.as_ref().unwrap().strong_count;
        if x.fetch_sub(1, SeqCst) != 1 {
            return
        }
        let this = Inner::from_ptr(ptr);
        this.inner_q.enqueue(ptr).unwrap();
        let raw = Box::into_raw(this); // prevent destruction
    }
}

#[derive(Clone)]
pub struct ActiveSegment {
    sender: Arc<Sender<* mut Inner>>,
    receiver: Arc<Receiver<* mut Inner>>,
    inner: Arc<AtomicPtr<Inner>>,
    writers: Arc<AtomicUsize>,
}

impl Drop for ActiveSegment {
    fn drop(&mut self) {
        if self.writers.load(SeqCst) > 0 {
            panic!("there are live writers");
        }
    }
}

impl ActiveSegment {

    fn new(sender: Arc<Sender<* mut Inner>>, receiver: Arc<Receiver<* mut Inner>>) -> Self {
        let inner: * mut Inner = match receiver.try_dequeue() {
            Ok(inner) => {
                if Inner::incr_cnt(inner) > 0 {
                    panic!("Got an item where there are still references");
                };
                inner
            },
            Err(DequeueError::Empty) => Inner::new_ptr(self.sender.clone())
            Err(DequeueError::Closed) => panic!("Closed segment queue!!"),
        };
        let inner = Arc::new(AtomicPtr::new(inner));
        ActiveSegment {
            sender,
            receiver,
            inner,
            writers: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn writer(&self) -> ActiveSegmentWriter {
        if self.writers.fetch_add(1, SeqCst) > 0 {
            panic!("Multiple writers for ActiveSegment");
        }

        let inner = self.inner.clone();
        let ptr = inner.load(SeqCst);
        unsafe {
            assert!(Inner::incr_cnt(ptr) > 0); // This active segment has a reference to inner
        }

        ActiveSegmentWriter {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            ptr,
            inner,
            writers: self.writers.clone()
        }
    }

    pub fn snapshot(&self) -> Option<ActiveSegmentReader> {

        let ptr = self.inner.load(SeqCst);

        if unsafe { Inner::incr_cnt(ptr) == 0 } {
            // We know that ptr's count was decremented to zero and is being put or is in the
            // queue. Need to call snapshot again
            None
        } else {
            // We know that at the time of the increment, ptr's count was not zero. This means
            // the counter is now > 1. Any decr_cnt after the incr_cnt will not result in the
            // inner ptr being put in the queue
            let len = unsafe { ptr.as_ref().unwrap() }.atomic_len.load(SeqCst);
            ActiveSegmentReader {
                ptr,
                len,
            }
        }
    }
}

pub struct ActiveSegmentWriter {
    sender: Arc<Sender<* mut Inner>>,
    receiver: Arc<Receiver<* mut Inner>>,
    ptr: * mut Inner, // Assumes a ptr from Inner::new_ptr()
    inner: Arc<AtomicPtr<Inner>>,
    writers: Arc<AtomicUsize>
}

impl ActiveSegmentWriter {

    fn replace_segment(&mut self) {
        // Generate new ptr
        let new_inner: * mut Inner = match self.receiver.try_dequeue() {
            Ok(inner) => {
                // Increment twice, one for the Active Segment, and another for the writer
                if Inner::incr_cnt(inner) > 0 {
                    panic!("Got an item where there are still references");
                }
                Inner::incr_cnt(inner);
                inner
            },
            Err(DequeueError::Empty) => Inner::new_ptr(self.sender.clone()),
            Err(DequeueError::Closed) => panic!("Closed segment queue!!"),
        };

        // Swap.
        let ptr = self.ptr;
        self.ptr = new_inner;
        self.inner.swap(new_inner, SeqCst);

        // Handle old ptr
        unsafe {
            // Decrement twice, once for this writer, and once for the ActiveSegment
            Inner::decr_cnt_drop(ptr);
            Inner::decr_cnt_drop(ptr);
        };
    }

    fn yield_segment(&self) -> ActiveSegmentBuffer {
        let ptr = self.inner.load(SeqCst);
        unsafe {
            // Yielding should always be on an Inner that has at least one reference (e.g. the
            // writer that has not swapped it out)
            assert!(Inner::incr_cnt(ptr) > 1);
        }

        let len = unsafe { ptr.as_ref().unwrap() }.atomic_len.load(SeqCst);
        ActiveSegmentBuffer {
            ptr,
            len
        }
    }

    #[inline]
    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
        // Safety:
        // -> There's only one writer so no race condition on writes
        // -> On reads, snapshots use atomic len in inner as a boundary
        unsafe { self.ptr.as_ref().unwrap().push(ts, item) }
    }
}

impl Drop for ActiveSegmentWriter {
    fn drop(&mut self) {
        unsafe {
            Inner::decr_cnt_drop(self.ptr);
        }
    }
}

pub struct ActiveSegmentReader {
    ptr: * mut Inner, // Assumes a ptr from Box::into_raw(Box::new(Inner::new()))
    len: usize,
}

impl Drop for ActiveSegmentReader {
    fn drop(&mut self) {
        unsafe {
            Inner::decr_cnt_drop(self.ptr);
        }
    }
}

pub struct ActiveSegmentBuffer {
    ptr: * mut Inner, // Assumes a ptr from Box::into_raw(Box::new(Inner::new()))
    len: usize,
}

impl Drop for ActiveSegmentBuffer {
    fn drop(&mut self) {
        unsafe {
            Inner::decr_cnt_drop(self.ptr);
        }
    }
}


//impl<const V: usize> ActiveSegment<V> {
//
//    pub fn snapshot(&self) -> ActiveSegmentReader<V> {
//        let inner_ptr = self.inner.load(SeqCst);
//        let inner = unsafe {
//            let arced = Arc::from_raw(inner_ptr);
//            let cloned = arced.clone();
//            Arc::into_raw(arced); // avoid decrementing the Arc so need to into_raw again
//            cloned
//        };
//        let len = inner.atomic_len.load(SeqCst);
//        ActiveSegmentReader {
//            inner,
//            len,
//        }
//    }
//
//    pub fn writer(&self) -> ActiveSegmentWriter<V> {
//        if self.writer_count.fetch_add(1, SeqCst) > 0 {
//            panic!("Multiple writers for ActiveSegment");
//        }
//        ActiveSegmentWriter {
//            ptr: self.inner.load(SeqCst),
//            inner: self.inner.clone(),
//            writer_count: self.writer_count.clone(),
//            capacity: SEGSZ,
//        }
//    }
//}
//
//impl<const V: usize> ActiveSegmentWriter<V> {
//
//
//    // Unsafe because could race with the push method
//    pub fn replace(&mut self) {
//        let arc_inner = Arc::new(InnerData::<V>::new());
//        self.ptr = Arc::as_ptr(&arc_inner) as *mut _;
//        let old_ptr = self.inner.swap(Arc::into_raw(arc_inner) as *mut _, SeqCst);
//        let _old_inner = unsafe { Arc::from_raw(old_ptr as * const InnerData<V>) }; // drop the Arc to allow deallocation
//    }
//
//    pub fn yield_segment(&self) -> ActiveSegmentBuffer<V> {
//        let ptr = self.inner.load(SeqCst);
//        let data = {
//            let owned = unsafe { Arc::from_raw(ptr as * const InnerData<V>) };
//            let cloned = owned.clone();
//            let _decons = Arc::into_raw(owned); // decons again to prevent dealloc
//            cloned
//        };
//        let len = data.atomic_len.load(SeqCst);
//        ActiveSegmentBuffer {
//            data,
//            len
//        }
//    }
//}
//
//impl<const V: usize> Drop for ActiveSegmentWriter<V> {
//    fn drop(&mut self) {
//        self.writer_count.fetch_sub(1, SeqCst);
//        let ptr = self.inner.load(SeqCst);
//        let to_be_dropped = unsafe {
//            Arc::from_raw(ptr as * const InnerData<V>)
//        }; // drop the last Arc to allow dealloc
//    }
//}
//

/*
impl SegmentLike for ActiveSegmentReader {
    fn timestamps(&self) -> &[Dt] {
        &self.inner.ts[..self.len]
    }

    fn variable(&self, id: usize) -> &[Fl] {
        &self.inner.values[id][..self.len]
    }

    fn value(&self, varid: usize, idx: usize) -> Fl {
        assert!(idx < self.len);
        self.inner.values[varid][idx]
    }

    fn row(&self, _idx: usize) -> &[Fl] {
        unimplemented!()
    }

    fn nvars(&self) -> usize {
        self.inner.values.len()
    }
}

impl SegmentIterator for ActiveSegmentReader {
    fn set_range(&mut self, mint: Dt, maxt: Dt) {
        self.mint = mint;
        self.maxt = maxt;
    }

    fn next_segment(&mut self) -> Option<Segment> {
        let contains = {
            let ts = self.timestamps();
            !ts.is_empty() && overlaps(self.mint, self.maxt, ts[0], *ts.last().unwrap())
        };
        if !self.yielded && contains {
            self.yielded = true;
            let ts = self.timestamps();
            let mut seg = Segment::new();
            let mut start = 0;
            let mut end = 0;
            for t in ts {
                if *t < self.mint {
                    start += 1;
                }
                if *t < self.maxt {
                    end += 1;
                } else {
                    break;
                }
            }
            seg.timestamps.extend_from_slice(&ts[start..end]);
            for v in &self.inner.values {
                seg.values.extend_from_slice(&v[start..end]);
            }
            Some(seg)
        } else {
            None
        }
    }
}

//pub fn get_segment(&mut self, mint: Dt, maxt: Dt, buf &mut Segment) -> Option<()> {
//    if len == 0 {
//        None
//    } else if {
//        let ts = self.timestamps();
//        if overlaps(mint, maxt, ts[0], *ts.last().unwrap()) {
//            let mut start = 0;
//            let mut end = 0;
//            for t in ts {
//                if t < mint {
//                    start += 1;
//                }
//                if t < maxt {
//                    end += 1;
//                } else {
//                    break
//                }
//            }
//            buf.extend_timestamps(&self.timestamps()[start..end]);
//            for i in
//            buf.extend_values
//        }
//    }
//}

//impl SegmentIterator<ActiveSegmentBuffer> for ActiveSegmentReader {
//    fn next_segment(&mut self, mint: Dt, maxt: Dt) -> Option<ActiveSegmentBuffer> {
//        let ts = &self.inner.ts[..self.len];
//        let overlaps = self.len > 0 && overlaps(ts[0], *ts.last().unwrap(), mint, maxt);
//        let result = if self.yielded || !overlaps {
//            None
//        } else {
//            self.yielded = true;
//            let buf = ActiveSegmentBuffer {
//                inner: self.inner.clone(),
//                len: self.len,
//            };
//            Some(buf)
//        };
//        self.yielded = true;
//        result
//    }
//
//    fn reset(&mut self) {
//        self.yielded = false;
//    }
//}

*/

//#[cfg(test)]
//mod test {
//    use super::*;
//    use rand::prelude::*;
//
//    #[test]
//    fn test_write() {
//        let mut rng = thread_rng();
//        let segment = ActiveSegment::new(2);
//        let mut writer = segment.writer();
//
//        let mut samples = Vec::new();
//        for dt in 0..3 {
//            let s = Sample {
//                ts: dt,
//                values: Box::new([rng.gen(), rng.gen()]),
//            };
//            samples.push(s);
//        }
//
//        let mut v0 = Vec::new();
//        let mut v1 = Vec::new();
//        for s in samples {
//            v0.push(s.values[0]);
//            v1.push(s.values[1]);
//            writer._push(s);
//        }
//
//        assert_eq!(segment.inner.len, 3);
//        assert_eq!(&segment.inner.values[0][..3], v0.as_slice());
//        assert_eq!(&segment.inner.values[1][..3], v1.as_slice());
//    }
//
//    #[test]
//    #[should_panic]
//    fn test_multiple_write_panic() {
//        let segment = ActiveSegment::new(2);
//        let _writer = segment.writer();
//        let _writer = segment.writer();
//    }
//
//    #[test]
//    fn test_multiple_writers_drop() {
//        let segment = ActiveSegment::new(2);
//        let writer = segment.writer();
//        drop(writer);
//        let _writer = segment.writer();
//    }
//
//    #[test]
//    fn test_yield_replace() {
//        let mut rng = thread_rng();
//        let segment = ActiveSegment::new(2);
//        let mut writer = segment.writer();
//
//        let mut samples = Vec::new();
//        for dt in 0..3 {
//            let s = Sample {
//                ts: dt,
//                values: Box::new([rng.gen(), rng.gen()]),
//            };
//            samples.push(s);
//        }
//
//        let mut v0 = Vec::new();
//        let mut v1 = Vec::new();
//        for s in samples {
//            v0.push(s.values[0]);
//            v1.push(s.values[1]);
//            writer._push(s);
//        }
//        let buf = writer.yield_replace();
//
//        assert_eq!(buf.len(), 3);
//        assert_eq!(buf.variable(0), v0.as_slice());
//        assert_eq!(buf.variable(1), v1.as_slice());
//        assert_eq!(segment.inner.len, 0);
//    }
//
//    #[test]
//    fn test_segment_reader() {
//        let mut rng = thread_rng();
//        let segment = ActiveSegment::new(2);
//        let mut writer = segment.writer();
//
//        let mut samples = Vec::new();
//        for dt in 0..3 {
//            let s = Sample {
//                ts: dt,
//                values: Box::new([rng.gen(), rng.gen()]),
//            };
//            samples.push(s);
//        }
//
//        for s in samples {
//            writer._push(s);
//        }
//
//        //let mut reader = segment.snapshot();
//        //assert!(reader.next_segment(0, 1).is_some());
//        //assert!(reader.next_segment(0, 1).is_none());
//        //reader.reset();
//        //assert!(reader.next_segment(1, 4).is_some());
//        //assert!(reader.next_segment(1, 4).is_none());
//        //reader.reset();
//        //assert!(reader.next_segment(4, 5).is_none());
//        //assert!(reader.next_segment(1, 4).is_none());
//    }
//
//    //#[test]
//    //fn test_read() {
//    //}
//}
