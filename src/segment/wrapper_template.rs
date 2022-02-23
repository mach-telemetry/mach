use crate::segment::{buffer::*, segment, Error, FullSegment, InnerPushStatus};
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};

macro_rules! cast {
    ($seg: ident, $b:expr, $v: expr) => {
        ($seg.inner as *mut segment::Segment<$b, $v>)
            .as_mut()
            .unwrap()
    };
}

macro_rules! new {
    ($b:expr, $v: expr, $h: expr) => {
        // TODO: Note - using Box::new results in stack overflow because reasons
        // https://github.com/rust-lang/rust/issues/53827
        Box::into_raw(box segment::Segment::<$b, $v>::new($h)) as *mut u8
    };
}

macro_rules! drop_box {
    ($ptr: ident, $b:expr, $v: expr) => {
        drop(Box::from_raw($ptr as *mut segment::Segment<$b, $v>))
    };
}

pub struct Segment {
    inner: *mut u8,
    refcount: Arc<AtomicUsize>,
    buffers: usize,
    vars: usize,
}

impl Drop for Segment {
    fn drop(&mut self) {
        let cnt = self.refcount.fetch_sub(1, SeqCst);
        if cnt == 1 {
            // Safety: This is the last reference held, with exclusive ownership, so can drop the
            // box safely.
            unsafe { self.drop_ptr() }
        }
    }
}

impl Clone for Segment {
    fn clone(&self) -> Self {
        self.refcount.fetch_add(1, SeqCst);
        Segment {
            inner: self.inner,
            buffers: self.buffers,
            vars: self.vars,
            refcount: self.refcount.clone(),
        }
    }
}

impl Segment {
    unsafe fn drop_ptr(&self) {
        let ptr = self.inner;

        match (self.buffers, self.vars) {
[[drop]]
(_, _) => unimplemented!(),
        }
    }

    pub fn new(buffers: usize, vars: usize, heap_pointer: &[bool]) -> Self {
        let boxed = match (buffers, vars) {
[[new]]
(_, _) => unimplemented!(),
        };

        let inner = boxed;
        let segment = Segment {
            inner,
            buffers,
            vars,
            refcount: Arc::new(AtomicUsize::new(1)),
        };
        segment
    }

    pub unsafe fn push_item<const I: usize>(&mut self, ts: u64, item: [[u8; 8]; I]) -> Result<InnerPushStatus, Error> {
        match (self.buffers, self.vars) {
[[push_item]]
(_, _) => unimplemented!(),
        }
    }

    pub unsafe fn push_univariate(&mut self, ts: u64, item: [u8; 8]) -> Result<InnerPushStatus, Error> {
        match (self.buffers, self.vars) {
[[push_univariate]]
(_, _) => unimplemented!(),
        }
    }

    pub unsafe fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
        match (self.buffers, self.vars) {
[[push]]
(_, _) => unimplemented!(),
        }
    }

    pub unsafe fn to_flush(&self) -> Option<FullSegment> {
        match (self.buffers, self.vars) {
[[to_flush]]
(_, _) => unimplemented!(),
        }
    }

    pub fn flushed(&self) {
        unsafe {
            match (self.buffers, self.vars) {
[[flushed]]
(_, _) => unimplemented!(),
            }
        }
    }

    pub unsafe fn read(&self) -> Result<Vec<ReadBuffer>, Error> {
        match (self.buffers, self.vars) {
            [[read]]
            (_, _) => unimplemented!(),
        }
    }
}
