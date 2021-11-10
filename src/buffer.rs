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
    id: isize,
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

    fn len(&self) -> usize {
        self.atomic_len.load(SeqCst)
    }

    fn new() -> Self {
        Inner {
            id: -1,
            ts: [0u64; SEGSZ],
            data: Vec::with_capacity(8 * 256),
            len: 0,
            atomic_len: AtomicUsize::new(0),
        }
    }

    fn clear(&mut self) {
        self.id = -1;
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
        let mut ptr: NonNull<Inner> = match self.inner.pop() {
            Some(x) => x,
            None => {
                let b = box Inner::new();
                Box::leak(b).into()
            }
        };
        unsafe {
            ptr.as_mut().clear();
        }
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

    fn snapshot(&self) -> SnapshotBuffer {
        let buffer = self.clone_buffer();
        let len = unsafe { buffer.inner.as_ref().len() };
        SnapshotBuffer {
            _buffer: buffer,
            _len: len,
        }
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

pub struct SnapshotBuffer {
    _buffer: Arc<Buffer>,
    _len: usize,
}

pub struct InnerActiveBuffer {
    current: NonNull<Inner>,
    buffers: Vec<SwappableBuffer>,
    head: isize,
    cleared: isize,
    flushed: Arc<AtomicIsize>,
    worker: Arc<SegQueue<(Arc<AtomicIsize>, Arc<Buffer>)>>,
}

impl InnerActiveBuffer {
    pub fn new(c: usize, worker: Arc<SegQueue<(Arc<AtomicIsize>, Arc<Buffer>)>>) -> Self {
        // Initialize buffers and allocators
        let allocator = BufferAllocator::new();
        let mut buffers: Vec<SwappableBuffer> = (0..c)
            .map(|_| SwappableBuffer::new(allocator.clone()))
            .collect();

        // Get the current inner buffer and assign it a an ID
        let mut current = buffers[0].get_inner();
        unsafe { current.as_mut().id = 0 };

        InnerActiveBuffer {
            current,
            buffers,
            head: 0,
            cleared: -1,
            flushed: Arc::new(AtomicIsize::new(-1)),
            worker,
        }
    }

    pub fn snapshot(&self) -> Vec<SnapshotBuffer> {
        self.buffers.iter().map(|x| x.snapshot()).collect()
    }

    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
        let buf = unsafe { self.current.as_mut() };

        // Try pushing into current buffer. If that works, then we're done
        if buf.push(ts, item).is_ok() {
            return Ok(());
        }

        let c = self.buffers.len();
        let flushed = self.flushed.load(SeqCst);

        // Otherwise the current buffer is full.
        // If the last flushed index is too far behind, return an error If c = 3 (i.e. [0, 1, 2])
        // and head = 2, then flushed should be at least 0 (head < flushed + c). Otherwise, all
        // buffers prior to head have not been flushed and we must return an error.
        if self.head >= flushed + c as isize {
            Err(Error::Full)
        }
        // If head = 2, buffers prior to head (at least buffer 0) has been flushed so we can move
        // to the next buffer.
        else {
            // Get the current buffer, send to worker
            let buf = self.buffers[self.head as usize % c].clone_buffer();
            self.worker.push((self.flushed.clone(), buf));

            // If head >= c, then we're cycling over the circular buffer. We need to swap out the
            // flushed buffer (could be that more than just this buffer was flushed, that's fine,
            // we don't want to do any more work than necessary)
            if self.head >= c as isize {
                let to_clear = (self.cleared + 1) as usize;
                let flushed_buffer = self.buffers[to_clear % c].swap_buffer();
                mem::drop(flushed_buffer); // drop the swapped out buffer to return it to queue
                self.cleared = to_clear as isize;
            }

            // Now that there's an available buffer, move to next buffer
            self.head += 1;
            self.current = self.buffers[self.head as usize % c].get_inner();
            unsafe {
                self.current.as_mut().id = self.head;
            }

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
