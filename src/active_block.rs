use crate::{block::BLOCKSZ, tsdb::Dt};

use std::{
    convert::TryFrom,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

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

    fn _remaining(&self) -> usize {
        BLOCKSZ - self.offset.load(SeqCst)
    }
}

#[derive(Clone)]
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
            _arc: (*self.inner).clone(),
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

pub struct ActiveBlockWriter {
    ptr: *mut InnerActiveBlock,
    _arc: Arc<InnerActiveBlock>,
}

impl ActiveBlockWriter {
    pub fn _push_section(&mut self, mint: Dt, maxt: Dt, slice: &[u8]) {
        unsafe {
            self.ptr
                .as_mut()
                .expect("inactive block ptr is null")
                .push_segment(mint, maxt, slice);
        }
    }

    pub fn _remaining(&self) -> usize {
        self._arc._remaining()
    }
}

pub struct ActiveBlockReader {
    _inner: Arc<InnerActiveBlock>,
    _len: usize,
}
