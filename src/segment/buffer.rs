use crate::segment::Error;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

const SEGSZ: usize = 256;

pub type Column = [[u8; 8]; 256];
pub type ColumnSet = [Column];

pub struct Flushable<'a> {
    len: usize,
    nvars: usize,
    ts: &'a [u64; SEGSZ],
    data: &'a [Column],
}

#[repr(C)]
pub struct Buffer<const V: usize> {
    pub id: AtomicUsize,
    pub atomic_len: AtomicUsize,
    pub len: usize,
    pub ts: [u64; SEGSZ],
    pub data: [Column; V],
}

impl<const V: usize> Buffer<V> {
    pub fn new() -> Self {
        Buffer {
            id: AtomicUsize::new(0),
            ts: [0u64; SEGSZ],
            len: 0,
            atomic_len: AtomicUsize::new(0),
            data: [[[0u8; 8]; 256]; V],
        }
    }

    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
        if self.len == SEGSZ {
            Err(Error::PushIntoFull)
        } else {
            let nvars = self.data.len();

            self.ts[self.len] = ts;
            for (i, b) in (&item[..nvars]).iter().enumerate() {
                self.data[i][self.len] = *b;
            }
            self.len += 1;
            self.atomic_len.store(self.len, SeqCst);
            Ok(())
        }
    }

    pub fn to_flush(&self) -> Flushable {
        Flushable {
            len: self.len,
            nvars: V,
            ts: &self.ts,
            data: &self.data[..],
        }
    }

    fn len(&self) -> usize {
        self.atomic_len.load(SeqCst)
    }

    pub fn reuse(&mut self, id: usize) {
        self.id.store(id, SeqCst);
        self.atomic_len.store(0, SeqCst);
        self.len = 0;
    }

    pub fn read(&self) -> Result<ReadBuffer, Error> {
        let id = self.id.load(SeqCst);
        let len = self.atomic_len.load(SeqCst);
        let mut data = Vec::new();

        // Note: In case this buffer is being recycled, this would race. That's fine because we can
        // just treat the data as junk and return an error
        for v in self.data.iter() {
            data.extend_from_slice(&v[..len]);
        }
        if self.id.load(SeqCst) == id {
            Ok(ReadBuffer { len, data })
        } else {
            Err(Error::InconsistentCopy)
        }
    }
}

pub struct ReadBuffer {
    len: usize,
    data: Vec<[u8; 8]>,
}

//pub struct WriteBuffer<'segment, const B: usize, const V: usize> {
//    buffer: NonNull<InnerBuffer<B, V>>,
//    inner: NonNull<ActiveSegment<B, V>>,
//    _lifetime: PhantomData<&'segment ActiveSegment<B, V>>,
//}
//
//pub struct ActiveSegment<const B: usize, const V: usize>  {
//    has_writer: AtomicBool,
//    inner: InnerSegment<B, V>,
//}
//
//impl<const B: usize, const V: usize> ActiveSegment<B, V> {
//    pub fn writer(&self) -> WriteBuffer {
//        if let Ok(x) = self.has_writer.compare_exchange(false, true, SeqCst, SeqCst) {
//            let inner = NonNull::new(self.inner.current_buffer() as *const _ as *mut _).unwrap();
//            let from = NonNull::new(self as *const _ as *mut _).unwrap();
//            Ok(WriteBuffer {
//                inner,
//                from,
//                _lifetime: PhantomData,
//            })
//        } else {
//            Err(Error::MultipleWriters)
//        }
//    }
//}
//
//enum EnumType {
//    V1B1(InnerSegment<1, 1>,
//}

//seq!(B in 1..2 {
//    seq!(V in 1..2 {
//        fn downcast_mut_#B_#V(item: &mut Box<dyn InnerSegmentTrait>) -> &mut InnerSegment<B, V> {
//            unsafe { &mut *(self as *mut dyn Any as *mut InnerSegment<B, V>) }
//        }
//    });
//});

//struct WriteBuffer<'segment> {
//    inner: NonNull<InnerBuffer>,
//    from: NonNull<ActiveSegment>,
//    _lifetime: PhantomData<&'segment ActiveSegment>,
//}
//
//impl<'segment> WriteBuffer<'segment> {
//    fn push(&mut self, ts: u64, item: &[[u8; 8]], sync: bool) -> Result<(), Error> {
//        unsafe {
//            self.inner.as_mut().push(ts, item, sync)
//        }
//    }
//
//    fn move_to_next_buffer(&mut self) {
//        unsafe {
//            self.from.as_mut().inner.move_to_next_buffer();
//        }
//    }
//}
//
//pub struct ActiveSegment {
//    has_writer: AtomicBool,
//    inner: InnerSegment,
//}
//
//impl ActiveSegment {
//
//    fn new(nbuf: usize, nvar: usize) {
//        let x = segment!(3, 3);
//    }
//    //fn new(n_buffers: usize, nvars: usize) -> Result<Box<Self>, Error> {
//    //    let buffer_allocator = inner_buffer_allocator(nvars)?;
//    //    let segment_allocator = inner_segment_allocator(n_buffers)?;
//
//    //    Box::new(ActiveSegment {
//    //        has_writer: AtomicBool::new(false),
//    //        inner: segment_allocator(buffer_allocator),
//    //    })
//    //}
//
//    fn writer(&self) -> Result<WriteBuffer, Error> {
//        if let Ok(x) = self.has_writer.compare_exchange(false, true, SeqCst, SeqCst) {
//            let inner  = NonNull::new(self.inner.current_buffer() as *const _ as *mut _).unwrap();
//            let from = NonNull::new(self as *const _ as *mut _).unwrap();
//            Ok(WriteBuffer {
//                inner,
//                from,
//                _lifetime: PhantomData,
//            })
//        } else {
//            Err(Error::MultipleWriters)
//        }
//    }
//
//    fn read(&self) -> Result<Vec<ReadBuffer>, Error> {
//        let mut copies = Vec::new();
//        for buf in self.inner.buffers.iter() {
//            let mut try_counter = 0;
//            loop {
//                if let Ok(x) = buf.read() {
//                    copies.push(x);
//                    break;
//                } else {
//                    try_counter += 1;
//                    if try_counter == 3 {
//                        return Err(Error::InconsistentCopy)
//                    }
//                }
//            }
//        }
//        Ok(copies)
//    }
//}

//type InnerAllocator = fn() -> NonNull<Inner<Bytes>>;
//
//fn inner_allocator(nvars: usize) -> InnerAllocator {
//    let func = seq!(NVARS in 1..2 {
//        match nvars {
//             #(
//                    NVARS => inner#NVARS,
//              )*
//              _ => panic!("Unsupported nvars"),
//        }
//    });
//    func
//}
//
//pub struct Buffer {
//    inner: NonNull<Inner<Bytes>>,
//    allocator: InnerQ,
//}
//
//impl Drop for Buffer {
//    fn drop(&mut self) {
//        self.allocator.push(self.inner);
//    }
//}
//
//type InnerQ = Arc<SegQueue<NonNull<Inner<Bytes>>>>;
//
//#[derive(Clone)]
//pub struct BufferAllocator {
//    inner: InnerQ,
//    alloc: InnerAllocator,
//}
//
//impl BufferAllocator {
//    pub fn new(nvars: usize) -> Self {
//        Self {
//            inner: Arc::new(SegQueue::new()),
//            alloc: inner_allocator(nvars)
//        }
//    }
//
//    pub fn take_or_add(&self) -> Buffer {
//        let mut ptr: NonNull<Inner<Bytes>> = match self.inner.pop() {
//            Some(x) => x,
//            None => (self.alloc)()
//        };
//        unsafe {
//            ptr.as_mut().clear();
//        }
//        Buffer {
//            inner: ptr,
//            allocator: self.inner.clone(),
//        }
//    }
//}
//
//struct SwappableBuffer {
//    buffer: Mutex<Arc<Buffer>>,
//    allocator: BufferAllocator,
//}
//
//impl SwappableBuffer {
//    fn new(allocator: BufferAllocator) -> Self {
//        let buffer = Mutex::new(Arc::new(allocator.take_or_add()));
//
//        Self { buffer, allocator }
//    }
//
//    fn clone_buffer(&self) -> Arc<Buffer> {
//        self.buffer.lock().unwrap().clone()
//    }
//
//    fn snapshot(&self) -> SnapshotBuffer {
//        let buffer = self.clone_buffer();
//        let len = unsafe { buffer.inner.as_ref().len() };
//        SnapshotBuffer {
//            _buffer: buffer,
//            _len: len,
//        }
//    }
//
//    fn swap_buffer(&self) -> Arc<Buffer> {
//        let mut buf = Arc::new(self.allocator.take_or_add());
//        mem::swap(&mut *self.buffer.lock().unwrap(), &mut buf);
//        buf
//    }
//
//    fn get_inner(&self) -> NonNull<Inner<Bytes>> {
//        self.buffer.lock().unwrap().inner
//    }
//}
//
//pub struct SnapshotBuffer {
//    _buffer: Arc<Buffer>,
//    _len: usize,
//}
//
//pub struct FlushBuffer {
//    _buffer: Arc<Buffer>,
//    inner: Arc<InnerActiveSegment>,
//}
//
//impl FlushBuffer {
//    pub fn mark_flushed(&self) {
//        self.inner.flushed.fetch_add(1, SeqCst);
//    }
//}
//
//struct InnerActiveSegment {
//    buffers: Vec<SwappableBuffer>,
//    head: AtomicIsize,
//    cleared: AtomicIsize,
//    flushed: AtomicIsize,
//}
//
//impl InnerActiveSegment {
//    fn new(nvars: usize, c: usize) -> Self {
//
//        // Initialize buffers and allocators
//        let allocator = BufferAllocator::new(nvars);
//        let buffers: Vec<SwappableBuffer> = (0..c)
//            .map(|_| SwappableBuffer::new(allocator.clone()))
//            .collect();
//
//        InnerActiveSegment {
//            buffers,
//            head: AtomicIsize::new(0),
//            cleared: AtomicIsize::new(-1),
//            flushed: AtomicIsize::new(-1),
//        }
//    }
//}
//
//pub struct ActiveSegment {
//    inner: Arc<InnerActiveSegment>,
//    writers: Arc<AtomicUsize>
//}
//
//impl ActiveSegment {
//    pub fn new(nvars: usize, c: usize) -> Self {
//        Self {
//            inner: Arc::new(InnerActiveSegment::new(nvars, c)),
//            writers: Arc::new(AtomicUsize::new(0)),
//        }
//    }
//
//    pub fn snapshot(&self) -> Vec<SnapshotBuffer> {
//        self.inner.buffers.iter().map(|x| x.snapshot()).collect()
//    }
//
//    pub fn writer(&self, worker: Arc<SegQueue<FlushBuffer>>) -> ActiveSegmentWriter {
//        if self.writers.fetch_add(1, SeqCst) >= 1 {
//            panic!("Multiple writers!");
//        }
//
//        let head = self.inner.head.load(SeqCst);
//        let cleared = self.inner.cleared.load(SeqCst);
//        ActiveSegmentWriter {
//            current: self.inner.buffers[head as usize].get_inner(),
//            inner: self.inner.clone(),
//            head,
//            cleared,
//            worker,
//        }
//    }
//}
//
//pub struct ActiveSegmentWriter {
//    current: NonNull<Inner<Bytes>>,
//    head: isize,
//    cleared: isize,
//    inner: Arc<InnerActiveSegment>,
//    worker: Arc<SegQueue<FlushBuffer>>,
//}
//
//impl ActiveSegmentWriter {
//
//    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
//        let buf = unsafe { self.current.as_mut() };
//
//        // Try pushing into current buffer. If that works, then we're done
//        if buf.push(ts, item).is_ok() {
//            return Ok(());
//        }
//
//        let c = self.inner.buffers.len();
//        let flushed = self.inner.flushed.load(SeqCst);
//
//        // Otherwise the current buffer is full.
//        // If the last flushed index is too far behind, return an error If c = 3 (i.e. [0, 1, 2])
//        // and head = 2, then flushed should be at least 0 (head < flushed + c). Otherwise, all
//        // buffers prior to head have not been flushed and we must return an error.
//        if self.head >= flushed + c as isize {
//            Err(Error::Full)
//        }
//        // If head = 2, buffers prior to head (at least buffer 0) has been flushed so we can move
//        // to the next buffer.
//        else {
//            // Get the current buffer, send to worker
//            let buffer = self.inner.buffers[self.head as usize % c].clone_buffer();
//            let flush_buffer = FlushBuffer {
//                _buffer: buffer,
//                inner: self.inner.clone()
//            };
//            self.worker.push(flush_buffer);
//
//            // If head >= c, then we're cycling over the circular buffer. We need to swap out the
//            // flushed buffer (could be that more than just this buffer was flushed, that's fine,
//            // we don't want to do any more work than necessary)
//            if self.head >= c as isize {
//                let to_clear = (self.cleared + 1) as usize;
//                let flushed_buffer = self.inner.buffers[to_clear % c].swap_buffer();
//                mem::drop(flushed_buffer); // drop the swapped out buffer to return it to queue
//                self.cleared = to_clear as isize;
//            }
//
//            // Now that there's an available buffer, move to next buffer
//            self.head += 1;
//            self.current = self.inner.buffers[self.head as usize % c].get_inner();
//            unsafe {
//                self.current.as_mut().id = self.head;
//            }
//
//            // push into new buf
//            unsafe { self.current.as_mut() }.push(ts, item)
//        }
//    }
//}
//
