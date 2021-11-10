use crate::{
    buffer::{self, Buffer, BufferAllocator},
};
use std::{
    sync::{
        Arc,
        Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    mem::{self, MaybeUninit},
    ptr::NonNull,
};

struct ActiveBuffer {
    current_buffer: NonNull<Buffer>
    buffer: Mutex<Arc<Buffer>>,
    allocator: BufferAllocator,
}

impl ActiveBuffer {
    fn new(allocator: BufferAllocator) -> Self {
        let buffer = Mutex::new(Arc::new(allocator.take_or_add()));

        Self {
            buffer,
            allocator
        }
    }

    fn clone_buffer(&self) -> Arc<Buffer> {
        self.buffer.lock().unwrap().clone()
    }

    fn swap_buffer(&self) -> Arc<Buffer> {
        let mut buf = Arc::new(self.allocator.take_or_add());
        mem::swap(&mut *self.buffer.lock().unwrap(), &mut buf);
        buf
    }
}

pub struct ActiveBuffers {
    buffers: Vec<ActiveBuffers>,
    buff_cnt: AtomicUsize,
}

//struct InnerBuffers {
//    buffers: Vec<SwappableBuffer>,
//    buff_cnt: AtomicUsize,
//    has_writer: AtomicUsize,
//}
//
//impl InnerBuffers {
//    fn new(size: usize) -> Self {
//        let manager: Manager<Buffer> = Manager::new();
//        let buff_cnt = AtomicUsize::new(0);
//        let has_writer = AtomicUsize::new(0);
//        let mut buffers = Vec::new();
//        for _ in 0..size {
//            buffers.push(SwappableBuffer::new(manager.clone()));
//        }
//        InnerBuffers {
//            buffers,
//            buff_cnt,
//            has_writer
//        }
//    }
//
//    fn curr_ptr(&self) -> * const Buffer {
//        self.buffers[self.buff_cnt.load(SeqCst)].curr_ptr
//    }
//}
//
//pub struct ActiveBuffer {
//    inner: Arc<InnerBuffers>
//}
//
//impl ActiveBuffer {
//    pub fn new(size: usize) -> Self {
//        ActiveBuffer {
//            inner: Arc::new(InnerBuffers::new(size))
//        }
//    }
//
//    pub fn writer(&self) -> ActiveBufferWriter {
//        if self.inner.has_writer.fetch_add(1, SeqCst) > 1 {
//            panic!("Multiple writers detected");
//        }
//        let ptr = unsafe { self.inner.curr_ptr().as_ref().unwrap() }.into();
//
//        ActiveBufferWriter {
//            ptr,
//            inner: self.inner.clone()
//        }
//    }
//
//    //pub fn snapshot(&self) -> ActiveBufferReader {
//    //    let buffer = self.inner.clone_buffer();
//    //    let len = buffer.len();
//    //    ActiveBufferReader {
//    //        buffer,
//    //        len
//    //    }
//    //}
//}
//
//pub struct ActiveBufferWriter {
//    ptr: NonNull<Buffer>,
//    inner: Arc<InnerBuffers>
//}
//
//impl ActiveBufferWriter {
//    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), buffer::Error> {
//        // Safety: There's only one writer so no multiple mutable access to Buffer. There could be
//        // concurrent snapshots. See ActiveBuffer.snapshot() which describes safety of snapshots
//        unsafe {
//            self.ptr.as_mut().push(ts, item)
//        }
//    }
//}

//impl ActiveBufferWriter {
//
//    pub fn raw_ptr(&self) -> * const Buffer {
//        // Safety: there's only one writer so no races involved in swap. Inner.swap is also
//        // guaranteed to not race with any method in Inner struct except for mut_inner. Because
//        // push and swap are are &mut self, these two calls can't race.
//        unsafe {
//            self.inner.raw_ptr()
//        }
//    }
//
//    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), buffer::Error> {
//        // Safety: there's only one writer so no races involved in swap. Inner.swap is also
//        // guaranteed to not race with any method in Inner struct except for mut_inner. Because
//        // push and swap are are &mut self, these two calls can't race.
//        unsafe {
//            self.inner.raw_ptr().as_ref().unwrap().get_mut().push(ts, item)
//        }
//    }
//
//    pub fn swap_buffer(&mut self) -> ManagedPtr<Buffer> {
//        // Safety: there's only one writer so no races involved in swap. Inner.swap is also
//        // guaranteed to not race with any method in Inner struct except for mut_inner. Because
//        // push and swap are are &mut self, these two calls can't race.
//        unsafe {
//            (Arc::as_ptr(&self.inner) as * mut SwappableBuffer).as_mut().unwrap().swap_buffer()
//        }
//    }
//
//    pub fn yield_buffer(&self) -> ManagedPtr<Buffer> {
//        self.inner.clone_buffer()
//    }
//}

//pub struct ActiveBufferReader {
//    buffer: ManagedPtr<Buffer>,
//    len: usize,
//}
//
//impl ActiveBufferReader {
//    pub fn warn_supressor(&self) {
//        let _ = &self.buffer;
//        let _ = self.len;
//    }
//}


