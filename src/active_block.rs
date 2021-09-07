use crate::{
    block::BLOCKSZ,
    tsdb::Dt,
};

use std::{
    sync::{Arc, atomic::{AtomicUsize, Ordering::SeqCst}},
    convert::TryFrom,
};

struct InnerActiveBlock {
    block: [u8; BLOCKSZ],
    offset: usize,
    mint: Dt,
    maxt: Dt,
}

impl InnerActiveBlock {
    fn new() -> Self {
        Self {
            block: [0u8; BLOCKSZ],
            offset: 2, // HEADER of size 2
            mint: Dt::MAX,
            maxt: Dt::MIN,
        }
    }

    fn push_segment(&mut self, mint: Dt, maxt: Dt, slice: &[u8]) {
        if self.remaining() < slice.len() {
            panic!("Not enough space in active block");
        }

        let len = self.offset;
        self.mint = self.mint.min(mint);
        self.maxt = maxt;
        self.block[len..len + slice.len()].clone_from_slice(slice);
        self.offset += slice.len();
        self.write_size();
    }

    fn write_size(&mut self) {
        let sz = <u16>::try_from(self.offset).unwrap().to_le_bytes();
        self.block[..2].copy_from_slice(&sz);
    }

    fn remaining(&self) -> usize {
        BLOCKSZ - self.offset
    }
}

#[derive(Clone)]
pub struct ActiveBlock {
    inner: Arc<InnerActiveBlock>,
}

impl ActiveBlock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InnerActiveBlock::new())
        }
    }
    pub fn writer(&self) -> ActiveBlockWriter {
        ActiveBlockWriter {
            ptr: Arc::as_ptr(&self.inner) as *mut InnerActiveBlock,
            _arc: self.inner.clone()
        }
    }

    pub fn snapshot(&self) -> ActiveBlockReader {
        ActiveBlockReader {
            _len: self.inner.offset,
            _inner: self.inner.clone(),
        }
    }
}

pub struct ActiveBlockWriter {
    ptr: *mut InnerActiveBlock,
    _arc: Arc<InnerActiveBlock>
}

impl ActiveBlockWriter {
    pub fn push_section(&mut self, mint: Dt, maxt: Dt, slice: &[u8]) {
        unsafe { self.ptr.as_mut().unwrap().push_segment(mint, maxt, slice); }
    }

    pub fn _remaining(&self) -> usize {
        self._arc.remaining()
    }
}

struct ActiveBlockReader {
    _inner: Arc<InnerActiveBlock>,
    _len: usize,
}


