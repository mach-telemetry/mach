use crossbeam_queue::SegQueue;
use std::{
    mem,
    ptr::NonNull,
    sync::{
        atomic::{AtomicIsize, AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
};
use seq_macro::seq;

const SEGSZ: usize = 256;

pub enum Error {
    Full,
}

type Bytes = [[[u8; 8]; SEGSZ]];

struct Inner<T: ?Sized> {
    id: isize,
    atomic_len: AtomicUsize,
    len: usize,
    ts: [u64; SEGSZ],
    data: T,
}

impl Inner<Bytes> {
    fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
        if self.len == SEGSZ {
            Err(Error::Full)
        } else {
            let nvars = self.data.len();

            self.ts[self.len] = ts;
            for (i, b) in (&item[..nvars]).iter().enumerate() {
                self.data[i][self.len] = *b;
            }
            self.len = self.atomic_len.fetch_add(1, SeqCst) + 1;
            Ok(())
        }
    }

    fn len(&self) -> usize {
        self.atomic_len.load(SeqCst)
    }

    fn clear(&mut self) {
        self.id = -1;
        self.atomic_len.store(0, SeqCst);
        self.len = 0;
    }
}

seq!(NVARS in 1..2 {
    fn inner#NVARS() -> NonNull<Inner<Bytes>> {
        let b = Box::new(Inner {
            id: -1,
            ts: [0u64; SEGSZ],
            len: 0,
            atomic_len: AtomicUsize::new(0),
            data: [[[0u8; 8]; SEGSZ]; NVARS],
        });
        NonNull::new(Box::into_raw(b)).unwrap()
    }
});

type InnerAllocator = fn() -> NonNull<Inner<Bytes>>;

fn inner_allocator(nvars: usize) -> InnerAllocator {
    let func = seq!(NVARS in 1..2 {
        match nvars {
             #(
                    NVARS => inner#NVARS,
              )*
              _ => panic!("Unsupported nvars"),
        }
    });
    func
}

pub struct Buffer {
    inner: NonNull<Inner<Bytes>>,
    allocator: InnerQ,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.allocator.push(self.inner);
    }
}

type InnerQ = Arc<SegQueue<NonNull<Inner<Bytes>>>>;

#[derive(Clone)]
pub struct BufferAllocator {
    inner: InnerQ,
    alloc: InnerAllocator,
}

impl BufferAllocator {
    pub fn new(nvars: usize) -> Self {
        Self {
            inner: Arc::new(SegQueue::new()),
            alloc: inner_allocator(nvars)
        }
    }

    pub fn take_or_add(&self) -> Buffer {
        let mut ptr: NonNull<Inner<Bytes>> = match self.inner.pop() {
            Some(x) => x,
            None => (self.alloc)()
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

    fn get_inner(&self) -> NonNull<Inner<Bytes>> {
        self.buffer.lock().unwrap().inner
    }
}

pub struct SnapshotBuffer {
    _buffer: Arc<Buffer>,
    _len: usize,
}

pub struct FlushBuffer {
    _buffer: Arc<Buffer>,
    inner: Arc<InnerActiveSegment>,
}

impl FlushBuffer {
    pub fn mark_flushed(&self) {
        self.inner.flushed.fetch_add(1, SeqCst);
    }
}

struct InnerActiveSegment {
    buffers: Vec<SwappableBuffer>,
    head: AtomicIsize,
    cleared: AtomicIsize,
    flushed: AtomicIsize,
}

impl InnerActiveSegment {
    fn new(nvars: usize, c: usize) -> Self {

        // Initialize buffers and allocators
        let allocator = BufferAllocator::new(nvars);
        let buffers: Vec<SwappableBuffer> = (0..c)
            .map(|_| SwappableBuffer::new(allocator.clone()))
            .collect();

        InnerActiveSegment {
            buffers,
            head: AtomicIsize::new(0),
            cleared: AtomicIsize::new(-1),
            flushed: AtomicIsize::new(-1),
        }
    }
}

pub struct ActiveSegment {
    inner: Arc<InnerActiveSegment>,
    writers: Arc<AtomicUsize>
}

impl ActiveSegment {
    pub fn new(nvars: usize, c: usize) -> Self {
        Self {
            inner: Arc::new(InnerActiveSegment::new(nvars, c)),
            writers: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn snapshot(&self) -> Vec<SnapshotBuffer> {
        self.inner.buffers.iter().map(|x| x.snapshot()).collect()
    }

    pub fn writer(&self, worker: Arc<SegQueue<FlushBuffer>>) -> ActiveSegmentWriter {
        if self.writers.fetch_add(1, SeqCst) >= 1 {
            panic!("Multiple writers!");
        }

        let head = self.inner.head.load(SeqCst);
        let cleared = self.inner.cleared.load(SeqCst);
        ActiveSegmentWriter {
            current: self.inner.buffers[head as usize].get_inner(),
            inner: self.inner.clone(),
            head,
            cleared,
            worker,
        }
    }
}

pub struct ActiveSegmentWriter {
    current: NonNull<Inner<Bytes>>,
    head: isize,
    cleared: isize,
    inner: Arc<InnerActiveSegment>,
    worker: Arc<SegQueue<FlushBuffer>>,
}

impl ActiveSegmentWriter {

    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
        let buf = unsafe { self.current.as_mut() };

        // Try pushing into current buffer. If that works, then we're done
        if buf.push(ts, item).is_ok() {
            return Ok(());
        }

        let c = self.inner.buffers.len();
        let flushed = self.inner.flushed.load(SeqCst);

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
            let buffer = self.inner.buffers[self.head as usize % c].clone_buffer();
            let flush_buffer = FlushBuffer {
                _buffer: buffer,
                inner: self.inner.clone()
            };
            self.worker.push(flush_buffer);

            // If head >= c, then we're cycling over the circular buffer. We need to swap out the
            // flushed buffer (could be that more than just this buffer was flushed, that's fine,
            // we don't want to do any more work than necessary)
            if self.head >= c as isize {
                let to_clear = (self.cleared + 1) as usize;
                let flushed_buffer = self.inner.buffers[to_clear % c].swap_buffer();
                mem::drop(flushed_buffer); // drop the swapped out buffer to return it to queue
                self.cleared = to_clear as isize;
            }

            // Now that there's an available buffer, move to next buffer
            self.head += 1;
            self.current = self.inner.buffers[self.head as usize % c].get_inner();
            unsafe {
                self.current.as_mut().id = self.head;
            }

            // push into new buf
            unsafe { self.current.as_mut() }.push(ts, item)
        }
    }
}

