use crate::{
    buffer::Buffer,
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

    fn swap_buffer(&mut self) -> ManagedPtr<Buffer> {
        let mut buf = MaybeUninit::new(self.manager.take_or_add(Buffer::new));

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
    fn push(&mut self) {
        //fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
    }

    fn swap(&mut self) {
    }
}

pub struct ActiveBufferReader {
    buffer: ManagedPtr<Buffer>,
    len: usize,
}


