use crate::{block::BLOCKSZ, tsdb::Dt};

use std::{
    convert::TryFrom,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

pub struct ActiveBlockBuffer {
    inner: Arc<InnerActiveBlock>,
}

impl ActiveBlockBuffer {
    pub fn slice(&self) -> &[u8] {
        &self.inner.block[..]
    }

    pub fn mint(&self) -> Dt {
        self.inner.mint
    }

    pub fn maxt(&self) -> Dt {
        self.inner.maxt
    }
}

struct InnerActiveBlock {
    block: [u8; BLOCKSZ],
    offset: AtomicUsize,
    mint: Dt,
    maxt: Dt,
}

impl InnerActiveBlock {
    fn new() -> Self {
        Self {
            block: [0u8; BLOCKSZ],
            offset: AtomicUsize::new(2), // HEADER of size 2
            mint: Dt::MAX,
            maxt: Dt::MIN,
        }
    }

    fn push_segment(&mut self, mint: Dt, maxt: Dt, slice: &[u8]) {
        let len = self.offset.load(SeqCst);

        if BLOCKSZ - len < slice.len() {
            panic!("Not enough space in active block");
        }

        let len = self.offset.load(SeqCst);
        self.mint = self.mint.min(mint);
        self.maxt = maxt;
        self.block[len..len + slice.len()].clone_from_slice(slice);
        let old_size = self.offset.fetch_add(slice.len(), SeqCst);
        let new_size = old_size + slice.len();
        let sz = <u16>::try_from(new_size).unwrap().to_le_bytes();
        self.block[..2].copy_from_slice(&sz);
    }

    fn remaining(&self) -> usize {
        BLOCKSZ - self.offset.load(SeqCst)
    }
}

#[derive(Clone)]
#[allow(clippy::redundant_allocation)]
pub struct ActiveBlock {
    inner: Arc<Arc<InnerActiveBlock>>,
}

impl ActiveBlock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Arc::new(InnerActiveBlock::new())),
        }
    }
    pub fn writer(&self) -> ActiveBlockWriter {
        ActiveBlockWriter {
            ptr: Arc::as_ptr(&*self.inner) as *mut InnerActiveBlock,
            arc: self.inner.clone(),
        }
    }

    // Snapshotting this from multiple threads assumes that there is a lock held
    pub fn snapshot(&self) -> ActiveBlockReader {
        ActiveBlockReader {
            _len: (*self.inner).offset.load(SeqCst),
            _inner: (*self.inner).clone(),
        }
    }
}

#[allow(clippy::redundant_allocation)]
pub struct ActiveBlockWriter {
    ptr: *mut InnerActiveBlock,
    arc: Arc<Arc<InnerActiveBlock>>,
}

impl ActiveBlockWriter {
    pub fn push_section(&mut self, mint: Dt, maxt: Dt, slice: &[u8]) {
        unsafe {
            self.ptr
                .as_mut()
                .expect("inactive block ptr is null")
                .push_segment(mint, maxt, slice);
        }
    }

    pub fn remaining(&self) -> usize {
        unsafe { self.ptr.as_ref().unwrap().remaining() }
    }

    pub fn yield_replace(&mut self) -> ActiveBlockBuffer {
        let mut inner = Arc::new(InnerActiveBlock::new());
        unsafe {
            std::mem::swap(&mut *Arc::get_mut_unchecked(&mut self.arc), &mut inner);
        }
        self.ptr = Arc::as_ptr(&*self.arc) as *mut InnerActiveBlock;

        ActiveBlockBuffer { inner }
    }
}

pub struct ActiveBlockReader {
    _inner: Arc<InnerActiveBlock>,
    _len: usize,
}
