use std::{
    sync::atomic::{AtomicUsize, Ordering::SeqCst},
    cell::UnsafeCell,
    mem,
    ops::{Deref, DerefMut},
};

const SEGSZ: usize = 256;

pub enum Error {
    Full
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

pub struct BufferMut<'buffer> {
    inner: &'buffer mut Inner
}

impl<'buffer> BufferMut<'buffer> {
    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<(), Error> {
        self.inner.push(ts, item)
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

pub struct Buffer {
    inner: UnsafeCell<Inner>,
}

impl Buffer {
    pub unsafe fn get_mut(&self) -> BufferMut {
        BufferMut {
            inner: self.inner.get().as_mut().unwrap()
        }
    }

    pub fn new() -> Self {
        Buffer {
            inner: UnsafeCell::new(Inner::new())
        }
    }

    fn inner(&self) -> &Inner {
        unsafe {
            self.inner.get().as_ref().unwrap()
        }
    }

    pub fn len(&self) -> usize {
        self.inner().atomic_len.load(SeqCst)
    }
}
