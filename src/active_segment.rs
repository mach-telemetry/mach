use crate::tsdb::Sample;
use seq_macro::seq;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};

const SECTSZ: usize = 256;

struct GenericInner<T: ?Sized> {
    ts: [u64; SECTSZ],
    len: AtomicUsize,
    values: T,
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
}

impl ActiveSegment {
    pub fn new(nvars: usize) -> Self {
        Self {
            inner: Arc::new(InnerSegment::arc_new(nvars)),
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
            _arc: (*self.inner).clone(),
        }
    }
}

pub struct ActiveSegmentWriter {
    ptr: *mut InnerSegment,
    _arc: Arc<InnerSegment>,
}

impl ActiveSegmentWriter {
    pub fn push(&mut self, item: Sample) -> bool {
        unsafe { self.ptr.as_mut().unwrap().push(item) }
    }
}

pub struct ActiveSegmentReader {
    _inner: Arc<InnerSegment>,
    _len: usize,
}
