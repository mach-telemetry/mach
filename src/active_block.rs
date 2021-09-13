use crate::{
    block::BLOCKSZ,
    compression::Compression,
    segment::{Segment, SegmentIterator},
    tsdb::Dt,
    utils::overlaps,
};

use std::{
    convert::{TryFrom, TryInto},
    mem::size_of,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

pub struct MemBlock {
    block: [u8; BLOCKSZ],
    index: Option<Vec<IdxEntry>>,
    offset: usize,
    qmint: Dt,
    qmaxt: Dt,
}

impl Deref for MemBlock {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.block[..]
    }
}

impl DerefMut for MemBlock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.block[..]
    }
}

impl MemBlock {
    pub fn new() -> Self {
        Self {
            block: [0u8; BLOCKSZ],
            index: None,
            offset: 0,
            qmint: Dt::MIN,
            qmaxt: Dt::MAX,
        }
    }

    pub fn load_index(&mut self) {
        let len = <u16>::from_be_bytes(self.block[..2].try_into().unwrap()) as usize;
        self.index = Some(block_index(&self.block[..], len));
    }

    fn next_compressed_segment(&mut self) -> Option<&[u8]> {
        match &self.index {
            None => panic!("Index not loaded, call self.load_index()"),
            Some(index) => {
                if self.offset == index.len() {
                    None
                } else {
                    let entry = index[self.offset];
                    self.offset += 1;
                    Some(&self.block[entry.offt..entry.offt + entry.len])
                }
            }
        }
    }
}

impl SegmentIterator for MemBlock {
    fn set_range(&mut self, mint: Dt, maxt: Dt) {
        match &self.index {
            None => panic!("Index not loaded, call self.load_index()"),
            Some(index) => {
                self.offset = 0;
                self.qmint = mint;
                self.qmaxt = maxt;
                for entry in index.iter() {
                    if !overlaps(entry.mint, entry.maxt, self.qmint, self.qmaxt) {
                        self.offset += 1;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn next_segment(&mut self) -> Option<Segment> {
        let compressed = self.next_compressed_segment()?;
        let mut seg = Segment::new();
        Compression::decompress(compressed, &mut seg);
        Some(seg)
    }
}

fn block_index(data: &[u8], len: usize) -> Vec<IdxEntry> {
    let mut offset = 2;
    let mut v = Vec::new();
    for _ in 0..len {
        let mint = &data[offset..offset + size_of::<Dt>()];
        offset += size_of::<Dt>();

        let maxt = &data[offset..offset + size_of::<Dt>()];
        offset += size_of::<Dt>();

        let offt = &data[offset..offset + size_of::<u16>()];
        offset += size_of::<u16>();

        let len = &data[offset..offset + size_of::<u16>()];
        offset += size_of::<u16>();

        v.push(IdxEntry {
            mint: Dt::from_be_bytes(mint.try_into().unwrap()),
            maxt: Dt::from_be_bytes(maxt.try_into().unwrap()),
            offt: u16::from_be_bytes(offt.try_into().unwrap()) as usize,
            len: u16::from_be_bytes(len.try_into().unwrap()) as usize,
        });
    }
    v
}

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
    // 20: First segment len (2 bytes) // TODO: Can be elided
    // 22: Second segment mint (8 bytes)
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
        self.block[s..s + sz].copy_from_slice(&(<u16>::try_from(t).unwrap()).to_be_bytes()[..]);
        s += sz;

        // Write the len of this segment
        let l = <u16>::try_from(slice.len()).unwrap();
        self.block[s..s + sz].copy_from_slice(&l.to_be_bytes()[..]);
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
        let len = (*self.inner).len.load(SeqCst);
        let index = block_index(&self.inner.block[..], len);
        ActiveBlockReader {
            _inner: (*self.inner).clone(),
            block: (&self.inner.block[..]).as_ptr(),
            index,
            offset: 0,
            qmint: Dt::MIN,
            qmaxt: Dt::MAX,
            //len,
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

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct IdxEntry {
    mint: Dt,
    maxt: Dt,
    offt: usize,
    len: usize,
}

pub struct ActiveBlockReader {
    _inner: Arc<InnerActiveBlock>,
    block: *const u8,
    //len: usize,
    index: Vec<IdxEntry>,
    offset: usize,
    qmint: Dt,
    qmaxt: Dt,
}

impl ActiveBlockReader {
    fn next_compressed_segment(&mut self) -> Option<&[u8]> {
        let data = unsafe { std::slice::from_raw_parts(self.block, BLOCKSZ) };
        if self.offset == self.index.len() {
            None
        } else {
            let entry = &self.index[self.offset];
            self.offset += 1;
            Some(&data[entry.offt..entry.offt + entry.len])
        }
    }
}

impl SegmentIterator for ActiveBlockReader {
    fn set_range(&mut self, mint: Dt, maxt: Dt) {
        self.offset = 0;
        self.qmint = mint;
        self.qmaxt = maxt;
        for entry in self.index.iter() {
            if !overlaps(entry.mint, entry.maxt, self.qmint, self.qmaxt) {
                self.offset += 1;
            } else {
                break;
            }
        }
    }

    fn next_segment(&mut self) -> Option<Segment> {
        let compressed = self.next_compressed_segment()?;
        let mut seg = Segment::new();
        Compression::decompress(compressed, &mut seg);
        Some(seg)
    }
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

        let mut v1 = vec![0u8; 123];
        rng.try_fill(&mut v1[..]).unwrap();
        writer.push_segment(0, 3, v1.as_slice());

        let mut v2 = vec![0u8; 456];
        rng.try_fill(&mut v2[..]).unwrap();
        writer.push_segment(4, 10, v2.as_slice());

        let mut v3 = vec![0u8; 234];
        rng.try_fill(&mut v3[..]).unwrap();
        writer.push_segment(11, 15, v3.as_slice());

        let mut reader = block.snapshot();
        let index = &reader.index;

        assert_eq!(index.len(), 3);

        assert_eq!(
            index[0],
            IdxEntry {
                mint: 0,
                maxt: 3,
                offt: BLOCKSZ - 123,
                len: 123,
            }
        );

        assert_eq!(
            index[1],
            IdxEntry {
                mint: 4,
                maxt: 10,
                offt: BLOCKSZ - 123 - 456,
                len: 456,
            }
        );

        assert_eq!(
            index[2],
            IdxEntry {
                mint: 11,
                maxt: 15,
                offt: BLOCKSZ - 123 - 456 - 234,
                len: 234,
            }
        );

        reader.set_range(8, 12);
        assert_eq!(reader.next_compressed_segment().unwrap(), v2.as_slice());
        assert_eq!(reader.next_compressed_segment().unwrap(), v3.as_slice());
        assert!(reader.next_compressed_segment().is_none());
    }

    #[test]
    #[should_panic]
    fn test_fill_block() {
        let mut rng = thread_rng();
        let block = ActiveBlock::new();
        let mut writer = block.writer();

        // 1522 bytes
        let mut v = vec![0u8; 1500];
        rng.try_fill(&mut v[..]).unwrap();
        writer.push_segment(0, 3, v.as_slice());

        let mut v = vec![0u8; 1500];
        rng.try_fill(&mut v[..]).unwrap();
        writer.push_segment(3, 9, v.as_slice());

        let mut v = vec![0u8; 1500];
        rng.try_fill(&mut v[..]).unwrap();
        writer.push_segment(10, 15, v.as_slice());

        let mut v = vec![0u8; 1500];
        rng.try_fill(&mut v[..]).unwrap();
        writer.push_segment(16, 23, v.as_slice());

        let mut v = vec![0u8; 1500];
        rng.try_fill(&mut v[..]).unwrap();
        writer.push_segment(25, 26, v.as_slice());

        assert_eq!(writer.remaining(), 582);

        let mut v = vec![0u8; 1500];
        rng.try_fill(&mut v[..]).unwrap();
        writer.push_segment(27, 30, v.as_slice());
    }

    #[test]
    fn test_get_many() {
        let mut rng = thread_rng();
        let block = ActiveBlock::new();
        let mut writer = block.writer();

        let bytes = (0..5)
            .map(|_| {
                let mut v = vec![0u8; 1500];
                rng.try_fill(&mut v[..]).unwrap();
                v
            })
            .collect::<Vec<Vec<u8>>>();

        let mut next = 0;
        for b in bytes.iter() {
            writer.push_segment(next, next + 5, b);
            next += 6;
        }

        let mut reader = block.snapshot();
        let index = &reader.index;
        let times = [(0, 5), (6, 11), (12, 17), (18, 23), (24, 29)];
        for (e, r) in index.iter().zip(times.iter()) {
            assert_eq!(e.mint, r.0);
            assert_eq!(e.maxt, r.1);
        }

        let mut idx = 0;
        reader.set_range(Dt::MIN, 6);
        while let Some(segment) = reader.next_compressed_segment() {
            assert_eq!(segment, bytes[idx].as_slice());
            idx += 1;
        }

        let mut idx = 2;
        reader.set_range(12, Dt::MAX);
        while let Some(segment) = reader.next_compressed_segment() {
            assert_eq!(segment, bytes[idx].as_slice());
            idx += 1;
        }
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
        assert_eq!(block.inner.len.load(SeqCst), 0);
        assert_eq!(block.inner.mint, Dt::MAX);
        assert_eq!(block.inner.maxt, Dt::MIN);
    }
}
