use crossbeam_queue::SegQueue;
use std::{
    mem,
    ptr::NonNull,
    sync::{
        atomic::{AtomicIsize, AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
};

const SEGSZ: usize = 256;

pub enum Error {
    Full,
}

struct Inner {
    atomic_len: AtomicUsize,
    len: usize,
    ts: [u64; SEGSZ],
    data: Vec<u8>,
}

impl Inner {
    fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
        if self.len == SEGSZ {
            Err(Error::Full)
        } else {
            let item = unsafe { mem::transmute::<&[[u8; 8]], &[u8]>(item) };

            self.ts[self.len] = ts;
            self.data.extend_from_slice(item);
            self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            Ok(())
        }
    }

    fn new() -> Self {
        Inner {
            ts: [0u64; SEGSZ],
            data: Vec::with_capacity(8 * 256),
            len: 0,
            atomic_len: AtomicUsize::new(0),
        }
    }

    fn clear(&mut self) {
        self.atomic_len.store(0, SeqCst);
        self.len = 0;
    }
}

//pub struct BufferMut<'buffer> {
//    inner: &'buffer mut Inner,
//}
//
//impl<'buffer> BufferMut<'buffer> {
//    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
//        self.inner.push(ts, item)
//    }
//}
//
//pub struct BufferRef<'buffer> {
//    inner: &'buffer Inner,
//    len: usize,
//}

pub struct Buffer {
    inner: NonNull<Inner>,
    allocator: InnerQ,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        unsafe {
            self.inner.as_mut().clear();
        }
        self.allocator.push(self.inner);
    }
}

//impl Buffer {
//    pub fn as_mut(&mut self) -> BufferMut {
//        unsafe {
//            BufferMut {
//                inner: self.inner.as_mut(),
//            }
//        }
//    }
//
//    pub fn as_ref(&self) -> BufferRef {
//        let r = unsafe { self.inner.as_ref() };
//        let len = r.atomic_len.load(SeqCst);
//        BufferRef { inner: r, len }
//    }
//}

type InnerQ = Arc<SegQueue<NonNull<Inner>>>;

#[derive(Clone)]
pub struct BufferAllocator {
    inner: InnerQ,
}

impl BufferAllocator {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SegQueue::new()),
        }
    }

    pub fn take_or_add(&self) -> Buffer {
        let ptr: NonNull<Inner> = match self.inner.pop() {
            Some(x) => x,
            None => {
                let b = box Inner::new();
                Box::leak(b).into()
            }
        };
        Buffer {
            inner: ptr,
            allocator: self.inner.clone(),
        }
    }
}

struct SwappableBuffer {
    buffer: Mutex<Arc<Buffer>>,
    allocator: BufferAllocator,
}

impl SwappableBuffer {
    fn new(allocator: BufferAllocator) -> Self {
        let buffer = Mutex::new(Arc::new(allocator.take_or_add()));

        Self { buffer, allocator }
    }

    fn clone_buffer(&self) -> Arc<Buffer> {
        self.buffer.lock().unwrap().clone()
    }

    fn swap_buffer(&self) -> Arc<Buffer> {
        let mut buf = Arc::new(self.allocator.take_or_add());
        mem::swap(&mut *self.buffer.lock().unwrap(), &mut buf);
        buf
    }

    fn get_inner(&mut self) -> NonNull<Inner> {
        self.buffer.get_mut().unwrap().inner
    }
}

pub struct InnerActiveBuffer {
    current: NonNull<Inner>,
    buffers: Vec<SwappableBuffer>,
    h: isize,
    head: Arc<AtomicIsize>,
    cleared: isize,
    flushed: Arc<AtomicIsize>,
    worker: Arc<SegQueue<(Arc<AtomicIsize>, Arc<Buffer>)>>,
}

impl InnerActiveBuffer {
    pub fn new(c: usize, worker: Arc<SegQueue<(Arc<AtomicIsize>, Arc<Buffer>)>>) -> Self {
        let allocator = BufferAllocator::new();
        let mut buffers: Vec<SwappableBuffer> = (0..c)
            .map(|_| SwappableBuffer::new(allocator.clone()))
            .collect();
        let current = buffers[0].get_inner();

        InnerActiveBuffer {
            current,
            buffers,
            h: 0,
            head: Arc::new(AtomicIsize::new(0)),
            cleared: -1,
            flushed: Arc::new(AtomicIsize::new(-1)),
            worker,
        }
    }

    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
        let buf = unsafe { self.current.as_mut() };

        if buf.push(ts, item).is_ok() {
            return Ok(());
        }

        let c = self.buffers.len();
        let flushed = self.flushed.load(SeqCst);

        if self.h >= flushed + c as isize {
            Err(Error::Full)
        } else {
            // Swap out everything that's been flushed
            let flushed = self.flushed.load(SeqCst);
            while self.cleared < flushed {
                self.cleared += 1;
                self.buffers[self.cleared as usize % c].swap_buffer(); // drop buffer to return it to queue
            }

            // Get the current buffer, send to worker
            let buf = self.buffers[self.h as usize % c].clone_buffer();
            self.worker.push((self.flushed.clone(), buf));

            // Move to next buffer
            self.h += 1;
            self.current = self.buffers[self.h as usize % c].get_inner();
            self.head.fetch_add(1, SeqCst);

            // push into new buf
            unsafe { self.current.as_mut() }.push(ts, item)
        }
    }
}

//pub struct ActiveBuffers {
//    inner: Arc<InnerActiveBuffer>
//}
//
//impl ActiveBuffers {
//    fn new(n: usize) -> Self {
//        Self {
//            inner: Arc::new(InnerActiveBuffer::new(n))
//        }
//    }
//}
//
//pub struct Writer {
//    current_buffer: NonNull<Inner>,
//    inner: Arc<InnerActiveBuffer>
//}
//
//impl Writer {
//    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
//        let buf = unsafe {
//            self.current_buffer.as_mut()
//        };
//        if buf.push(ts, item).is_err() {
//
//        }
//        Ok(())
//    }
//}
