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
    writer_count: Arc<AtomicUsize>,
}

impl ActiveBlock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Arc::new(InnerActiveBlock::new())),
            writer_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn writer(&self) -> ActiveBlockWriter {
        if self.writer_count.fetch_add(1, SeqCst) > 0 {
            panic!("Multiple writers for ActiveBlock");
        }
        ActiveBlockWriter {
            ptr: Arc::as_ptr(&*self.inner) as *mut InnerActiveBlock,
            arc: self.inner.clone(),
            writer_count: self.writer_count.clone(),
        }
    }

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
    writer_count: Arc<AtomicUsize>,
}

impl ActiveBlockWriter {
    pub fn push_segment(&mut self, mint: Dt, maxt: Dt, slice: &[u8]) {
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

impl Drop for ActiveBlockWriter {
    fn drop(&mut self) {
        self.writer_count.fetch_sub(1, SeqCst);
    }
}

pub struct ActiveBlockReader {
    _inner: Arc<InnerActiveBlock>,
    _len: usize,
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::prelude::*;
    use std::convert::TryInto;

    #[test]
    #[should_panic]
    fn test_multiple_write_panic() {
        let block = ActiveBlock::new();
        let writer = block.writer();
        let writer = block.writer();
    }

    #[test]
    fn test_multiple_writers_drop() {
        let block = ActiveBlock::new();
        let writer = block.writer();
        drop(writer);
        let writer = block.writer();
    }

    #[test]
    fn test_write_block() {
        let mut rng = thread_rng();
        let block = ActiveBlock::new();
        let mut writer = block.writer();

        let mut v = vec![0u8; 123];
        rng.try_fill(&mut v[..]).unwrap();
        writer.push_segment(0, 3, v.as_slice());

        assert_eq!(
            u16::from_le_bytes(block.inner.block[..2].try_into().unwrap()),
            125
        );
        assert_eq!(&block.inner.block[2..125], &v[..]);
        assert_eq!(block.inner.offset.load(SeqCst), 125);
        assert_eq!(block.inner.mint, 0);
        assert_eq!(block.inner.maxt, 3);
    }

    #[test]
    fn test_yield_replace() {
        let mut rng = thread_rng();
        let block = ActiveBlock::new();
        let mut writer = block.writer();

        let mut v1 = vec![0u8; 123];
        rng.try_fill(&mut v1[..]).unwrap();
        writer.push_segment(0, 3, v1.as_slice());

        let mut v2 = vec![0u8; 456];
        rng.try_fill(&mut v2[..]).unwrap();
        writer.push_segment(4, 10, v2.as_slice());

        let buf = writer.yield_replace();

        let total_bytes: usize = 2 + 123 + 456;
        assert_eq!(
            u16::from_le_bytes(buf.inner.block[..2].try_into().unwrap()),
            total_bytes as u16
        );
        assert_eq!(&buf.inner.block[2..125], &v1[..]);
        assert_eq!(&buf.inner.block[125..456 + 125], &v2[..]);
        assert_eq!(buf.inner.offset.load(SeqCst), total_bytes);
        assert_eq!(buf.inner.mint, 0);
        assert_eq!(buf.inner.maxt, 10);

        assert_eq!(block.inner.offset.load(SeqCst), 2);
        assert_eq!(block.inner.mint, Dt::MAX);
        assert_eq!(block.inner.maxt, Dt::MIN);
    }
}
