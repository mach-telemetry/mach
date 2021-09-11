use crate::{block::BLOCKSZ, tsdb::Dt, utils::overlaps};

use std::{
    convert::{TryFrom, TryInto},
    mem::size_of,
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
    len: AtomicUsize,
    idx_offset: usize,
    tail_offset: usize,
    mint: Dt,
    maxt: Dt,
}

impl InnerActiveBlock {
    fn new() -> Self {
        Self {
            block: [0u8; BLOCKSZ],
            len: AtomicUsize::new(0),
            idx_offset: 2, // First two bytes are for
            tail_offset: BLOCKSZ,
            mint: Dt::MAX,
            maxt: Dt::MIN,
        }
    }

    // Segment format:
    // 0: Number of segments (2 bytes)
    // 2: First segment mint (8 bytes)
    // 10: First segment maxt (8 bytes)
    // 18: First segment offset (2 bytes)
    // 20: Second segment mint (8 bytes)
    // ...
    // Random lengths: segments in the tail
    // BLOCKSZ: End of the block
    fn push_segment(&mut self, mint: Dt, maxt: Dt, slice: &[u8]) {
        if self.remaining() < size_of::<Dt>() * 2 + size_of::<u16>() + slice.len() {
            panic!("Not enough space in active block");
        }

        let mut s = self.idx_offset;
        let t = self.tail_offset - slice.len();

        // Write the mint and maxt of this block
        let sz = size_of::<Dt>();

        self.block[s..s + sz].copy_from_slice(&mint.to_be_bytes()[..]);
        s += sz;

        self.block[s..s + sz].copy_from_slice(&maxt.to_be_bytes()[..]);
        s += sz;

        // Write the offset to the tail portion of this block
        let sz = size_of::<u16>();
        self.block[s..s + sz].copy_from_slice(&t.to_be_bytes()[..]);
        s += sz;

        // Set new idx offset
        self.idx_offset = s;

        // Write slice information
        self.block[t..t + slice.len()].copy_from_slice(slice);
        self.tail_offset = t;

        // Increment new information
        self.mint = self.mint.min(mint);
        self.maxt = maxt;
        self.len.fetch_add(1, SeqCst);
    }

    fn remaining(&self) -> usize {
        self.tail_offset - self.idx_offset
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
            len: (*self.inner).len.load(SeqCst),
            inner: (*self.inner).clone(),
            mint: self.inner.mint,
            maxt: self.inner.maxt,
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

struct IdxEntry {
    mint: Dt,
    maxt: Dt,
    offt: usize,
}

pub struct ActiveBlockReader {
    inner: Arc<InnerActiveBlock>,
    len: usize,
    mint: Dt,
    maxt: Dt,
}

impl ActiveBlockReader {
    fn index(&self) -> Vec<IdxEntry> {
        let block = &self.inner.block[..];
        let mut offset = 2;
        let mut v = Vec::new();
        for _ in 0..self.len {
            let mint = &block[offset..offset + size_of::<Dt>()];
            offset += size_of::<Dt>();

            let maxt = &block[offset..offset + size_of::<Dt>()];
            offset += size_of::<Dt>();

            let offt = &block[offset..offset + size_of::<u16>()];
            offset += size_of::<u16>();

            v.push(IdxEntry {
                mint: Dt::from_be_bytes(mint.try_into().unwrap()),
                maxt: Dt::from_be_bytes(maxt.try_into().unwrap()),
                offt: u16::from_be_bytes(offt.try_into().unwrap()) as usize,
            });
        }
        v
    }

    fn iter(&self, mint: Dt, maxt: Dt) -> ActiveBlockIterator {
        let index = self.index();
        let mut offset = 0;
        for entry in index.iter() {
            if !overlaps(entry.mint, entry.maxt, mint, maxt) {
                offset += 1;
            } else {
                break;
            }
        }
        ActiveBlockIterator {
            reader: self,
            idx: index,
            offset,
            mint,
            maxt,
        }
    }
}

pub struct ActiveBlockIterator<'a> {
    reader: &'a ActiveBlockReader,
    idx: Vec<IdxEntry>,
    offset: usize,
    mint: Dt,
    maxt: Dt,
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
