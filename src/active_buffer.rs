use crate::{
    buffer::{self, Buffer},
    managed::{Manager, ManagedPtr},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    mem::{self, MaybeUninit},
    ops::{Deref, DerefMut},
};

struct SwappableBuffer {
    idx: AtomicUsize,
    curr_cnt: usize,
    curr_ptr: * const Buffer,
    buffers: [MaybeUninit<ManagedPtr<Buffer>>; 2],
    manager: Manager<Buffer>,
}

impl SwappableBuffer {

    unsafe fn mut_inner(&self) -> &mut Buffer {
        (self.curr_ptr as * mut Buffer).as_mut().unwrap()
    }

    fn new(manager: Manager<Buffer>) -> Self {
        let buffer = manager.take_or_add(Buffer::new);
        let curr_ptr = ManagedPtr::raw_ptr(&buffer);

        let idx = AtomicUsize::new(0);
        let curr_cnt = 0;
        let buffers = [MaybeUninit::new(buffer), MaybeUninit::uninit()];

        Self {
            idx,
            curr_cnt,
            curr_ptr,
            buffers,
            manager,
        }
    }

    fn clone_buffer(&self) -> ManagedPtr<Buffer> {
        // Safety
        // buffers[0] always initted.
        // self.id == 1 iff swap has occured therefore buffers[1] is also initted
        unsafe {
            self.buffers[self.idx.load(SeqCst)].assume_init_ref().clone()
        }
    }

    // Safety:
    // This function should never race with clone_buffer
    fn swap_buffer(&mut self) -> ManagedPtr<Buffer> {
        let buf = self.manager.take_or_add(Buffer::new);
        let ptr = ManagedPtr::raw_ptr(&buf);
        let mut buf = MaybeUninit::new(buf);

        self.curr_ptr = ptr;
        self.curr_cnt += 1;
        let next_idx = self.curr_cnt % 2;

        // Write to the next index. All concurrent cloners (if any) will be cloning from
        // self.id's atomic idx so no race here
        mem::swap(&mut self.buffers[next_idx], &mut buf);

        // Atomic switch to the new buffer
        self.idx.store(next_idx, SeqCst);

        // Safety: buffers[0] was initted at creation, subsequent buffers are only initted by
        // calling self.swap function
        unsafe { buf.assume_init() }
    }
}

struct Inner {
    buffer: SwappableBuffer,
    writer_cnt: AtomicUsize,
}

pub struct ActiveBuffer {
    inner: Arc<Inner>,
}

impl ActiveBuffer {
    fn writer(&self) -> ActiveBufferWriter {
        if self.inner.writer_cnt.fetch_add(1, SeqCst) == 1 {
            panic!("More than one writer");
        }
        ActiveBufferWriter {
            inner: self.inner.clone()
        }
    }

    fn snapshot(&self) -> ActiveBufferReader {
        let buffer = self.inner.buffer.clone_buffer();
        let len = buffer.len();
        ActiveBufferReader {
            buffer,
            len
        }
    }
}

pub struct ActiveBufferWriter {
    inner: Arc<Inner>
}

impl Drop for ActiveBufferWriter {
    fn drop(&mut self) {
        self.inner.writer_cnt.fetch_sub(1, SeqCst);
    }
}

impl ActiveBufferWriter {
    fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), buffer::Error> {
        // Safety: there's only one writer so no races involved in swap. Inner.swap is also
        // guaranteed to not race with any method in Inner struct except for mut_inner. Because
        // push and swap are are &mut self, these two calls can't race.
        unsafe {
            self.inner.buffer.mut_inner().get_mut().push(ts, item)
        }
    }

    fn swap(&mut self) -> ManagedPtr<Buffer> {
        // Safety: there's only one writer so no races involved in swap. Inner.swap is also
        // guaranteed to not race with any method in Inner struct except for mut_inner. Because
        // push and swap are are &mut self, these two calls can't race.
        unsafe {
            (Arc::as_ptr(&self.inner) as * mut Inner).as_mut().unwrap().buffer.swap_buffer()
        }
    }
}

pub struct ActiveBufferReader {
    buffer: ManagedPtr<Buffer>,
    len: usize,
}


