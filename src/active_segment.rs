use crate::{
    tsdb::{Fl, Dt, Sample},
    segment::SegmentLike,
};
use seq_macro::seq;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};

const SECTSZ: usize = 256;

struct GenericInner<T: ?Sized> {
    ts: [Dt; SECTSZ],
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

type InnerSegment = GenericInner<[[f64; SECTSZ]]>;

impl InnerSegment {
    fn arc_new(nvars: usize) -> Arc<Self> {
        seq!(V in 1..=50 {
            match nvars {
                #(
                    V => {
                        Arc::new(GenericInner {
                            ts: [0; SECTSZ],
                            len: AtomicUsize::new(0),
                            values: [[0.; SECTSZ]; V]
                        })
                    },
                )*
                    _ => panic!("Unsupported nvars"),
            }
        })
    }

    fn push(&mut self, item: Sample) -> bool {
        let len = self.len();
        self.ts[len] = item.ts;
        for (var, val) in item.values.iter().enumerate() {
            self.values.as_mut()[var][len] = *val;
        }
        self.len.fetch_add(1, SeqCst) == SECTSZ
    }

    fn len(&self) -> usize {
        self.len.load(SeqCst)
    }
}

#[derive(Clone)]
pub struct ActiveSegment {
    inner: Arc<Arc<InnerSegment>>,
    nvars: usize
}

impl ActiveSegment {
    pub fn new(nvars: usize) -> Self {
        Self {
            inner: Arc::new(InnerSegment::arc_new(nvars)),
            nvars,
        }
    }

    pub fn snapshot(&self) -> ActiveSegmentReader {
        ActiveSegmentReader {
            _inner: (*self.inner).clone(),
            _len: self.inner.len(),
        }
    }

    pub fn writer(&self) -> ActiveSegmentWriter {
        ActiveSegmentWriter {
            ptr: Arc::as_ptr(&*self.inner) as *mut InnerSegment,
            arc: self.inner.clone(),
            nvars: self.nvars,
        }
    }
}

pub struct ActiveSegmentWriter {
    ptr: *mut InnerSegment,
    arc: Arc<Arc<InnerSegment>>,
    nvars: usize,
}

impl ActiveSegmentWriter {
    pub fn push(&mut self, item: Sample) -> bool {
        unsafe { self.ptr.as_mut().unwrap().push(item) }
    }

    pub fn yield_replace(&mut self) -> ActiveSegmentBuffer {
        let mut new = InnerSegment::arc_new(self.nvars);
        unsafe {
            std::mem::swap(&mut *Arc::get_mut_unchecked(&mut self.arc), &mut new);
        }
        self.ptr = Arc::as_ptr(&*self.arc) as *mut InnerSegment;

        let len = new.len.load(SeqCst);
        ActiveSegmentBuffer {
            inner: new,
            len
        }
    }
}

pub struct ActiveSegmentReader {
    _inner: Arc<InnerSegment>,
    _len: usize,
}
