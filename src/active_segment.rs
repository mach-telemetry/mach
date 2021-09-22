use crate::{
    segment::{Segment, SegmentIterator, SegmentLike},
    tsdb::{Dt, Fl, Sample},
    utils::overlaps,
};
use seq_macro::seq;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};

pub const SEGSZ: usize = 256;

struct GenericInner<T: ?Sized> {
    ts: [Dt; SEGSZ],
    len: AtomicUsize,
    values: T,
}

pub struct ActiveSegmentBuffer {
    inner: Arc<InnerSegment>,
    len: usize,
}

impl SegmentLike for ActiveSegmentBuffer {
    fn timestamps(&self) -> &[Dt] {
        &self.inner.ts[..self.len]
    }

    fn variable(&self, id: usize) -> &[Fl] {
        &self.inner.values[id][..self.len]
    }

    fn value(&self, varid: usize, idx: usize) -> Fl {
        self.inner.values[varid][idx]
    }

    fn row(&self, _: usize) -> &[Fl] {
        unimplemented!()
    }

    fn nvars(&self) -> usize {
        self.inner.values.len()
    }

    fn len(&self) -> usize {
        self.len
    }
}

type InnerSegment = GenericInner<[[f64; SEGSZ]]>;

seq!(NVARS in 1..=50 {
    fn inner_segment#NVARS() -> Arc<InnerSegment> {
        Arc::new(GenericInner {
            ts: [0; SEGSZ],
            len: AtomicUsize::new(0),
            values: [[0.; SEGSZ]; NVARS],
        })
    }
});

impl InnerSegment {
    fn arc_new(nvars: usize) -> Arc<Self> {
        seq!(NVARS in 1..=50 {
            match nvars {
                #(
                    NVARS => inner_segment#NVARS(),
                )*
                _ => panic!("Unsupported nvars"),
            }
        })
    }

    fn push(&mut self, item: Sample) -> usize {
        let len = self.len();
        self.ts[len] = item.ts;
        for (var, val) in item.values.iter().enumerate() {
            self.values.as_mut()[var][len] = *val;
        }
        self.len.fetch_add(1, SeqCst) + 1
    }

    fn len(&self) -> usize {
        self.len.load(SeqCst)
    }
}

#[derive(Clone)]
#[allow(clippy::redundant_allocation)]
pub struct ActiveSegment {
    inner: Arc<Arc<InnerSegment>>,
    writer_count: Arc<AtomicUsize>,
    nvars: usize,
}

impl ActiveSegment {
    pub fn new(nvars: usize) -> Self {
        Self {
            inner: Arc::new(InnerSegment::arc_new(nvars)),
            writer_count: Arc::new(AtomicUsize::new(0)),
            nvars,
        }
    }

    pub fn snapshot(&self) -> ActiveSegmentReader {
        let len = self.inner.len();
        ActiveSegmentReader {
            inner: (*self.inner).clone(),
            len,
            mint: Dt::MIN,
            maxt: Dt::MAX,
            yielded: false,
        }
    }

    pub fn writer(&self) -> ActiveSegmentWriter {
        if self.writer_count.fetch_add(1, SeqCst) > 0 {
            panic!("Multiple writers for ActiveSegment");
        }

        ActiveSegmentWriter {
            ptr: Arc::as_ptr(&*self.inner) as *mut InnerSegment,
            arc: self.inner.clone(),
            writer_count: self.writer_count.clone(),
            nvars: self.nvars,
            capacity: SEGSZ,
        }
    }
}

#[allow(clippy::redundant_allocation)]
pub struct ActiveSegmentWriter {
    ptr: *mut InnerSegment,
    arc: Arc<Arc<InnerSegment>>,
    writer_count: Arc<AtomicUsize>,
    nvars: usize,
    capacity: usize,
}

impl ActiveSegmentWriter {
    pub fn push(&mut self, item: Sample) -> usize {
        unsafe { self.ptr.as_mut().unwrap().push(item) }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn set_capacity(&mut self, cap: usize) {
        self.capacity = cap;
    }

    pub fn yield_replace(&mut self) -> ActiveSegmentBuffer {
        let mut new = InnerSegment::arc_new(self.nvars);
        unsafe {
            std::mem::swap(&mut *Arc::get_mut_unchecked(&mut self.arc), &mut new);
        }
        self.ptr = Arc::as_ptr(&*self.arc) as *mut InnerSegment;

        let len = new.len.load(SeqCst);
        ActiveSegmentBuffer { inner: new, len }
    }
}

impl Drop for ActiveSegmentWriter {
    fn drop(&mut self) {
        self.writer_count.fetch_sub(1, SeqCst);
    }
}

pub struct ActiveSegmentReader {
    inner: Arc<InnerSegment>,
    len: usize,
    mint: Dt,
    maxt: Dt,
    yielded: bool,
}

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
            ts.len() > 0 && overlaps(self.mint, self.maxt, ts[0], *ts.last().unwrap())
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

#[cfg(test)]
mod test {
    use super::*;
    use rand::prelude::*;

    #[test]
    fn test_write() {
        let mut rng = thread_rng();
        let segment = ActiveSegment::new(2);
        let mut writer = segment.writer();

        let mut samples = Vec::new();
        for dt in 0..3 {
            let s = Sample {
                ts: dt,
                values: Box::new([rng.gen(), rng.gen()]),
            };
            samples.push(s);
        }

        let mut v0 = Vec::new();
        let mut v1 = Vec::new();
        for s in samples {
            v0.push(s.values[0]);
            v1.push(s.values[1]);
            writer.push(s);
        }

        assert_eq!(segment.inner.len(), 3);
        assert_eq!(&segment.inner.values[0][..3], v0.as_slice());
        assert_eq!(&segment.inner.values[1][..3], v1.as_slice());
    }

    #[test]
    #[should_panic]
    fn test_multiple_write_panic() {
        let segment = ActiveSegment::new(2);
        let _writer = segment.writer();
        let _writer = segment.writer();
    }

    #[test]
    fn test_multiple_writers_drop() {
        let segment = ActiveSegment::new(2);
        let writer = segment.writer();
        drop(writer);
        let _writer = segment.writer();
    }

    #[test]
    fn test_yield_replace() {
        let mut rng = thread_rng();
        let segment = ActiveSegment::new(2);
        let mut writer = segment.writer();

        let mut samples = Vec::new();
        for dt in 0..3 {
            let s = Sample {
                ts: dt,
                values: Box::new([rng.gen(), rng.gen()]),
            };
            samples.push(s);
        }

        let mut v0 = Vec::new();
        let mut v1 = Vec::new();
        for s in samples {
            v0.push(s.values[0]);
            v1.push(s.values[1]);
            writer.push(s);
        }
        let buf = writer.yield_replace();

        assert_eq!(buf.len(), 3);
        assert_eq!(buf.variable(0), v0.as_slice());
        assert_eq!(buf.variable(1), v1.as_slice());
        assert_eq!(segment.inner.len(), 0);
    }

    #[test]
    fn test_segment_reader() {
        let mut rng = thread_rng();
        let segment = ActiveSegment::new(2);
        let mut writer = segment.writer();

        let mut samples = Vec::new();
        for dt in 0..3 {
            let s = Sample {
                ts: dt,
                values: Box::new([rng.gen(), rng.gen()]),
            };
            samples.push(s);
        }

        for s in samples {
            writer.push(s);
        }

        //let mut reader = segment.snapshot();
        //assert!(reader.next_segment(0, 1).is_some());
        //assert!(reader.next_segment(0, 1).is_none());
        //reader.reset();
        //assert!(reader.next_segment(1, 4).is_some());
        //assert!(reader.next_segment(1, 4).is_none());
        //reader.reset();
        //assert!(reader.next_segment(4, 5).is_none());
        //assert!(reader.next_segment(1, 4).is_none());
    }

    //#[test]
    //fn test_read() {
    //}
}
