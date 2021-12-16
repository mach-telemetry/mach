use std::{
    sync::atomic::{AtomicUsize, Ordering::SeqCst},
};
use crossbeam_skiplist::SkipMap;

const BLOCKSZ: usize = 8192;

struct InnerBlock {
    blocks: SkipMap<usize, [u8; 8192]>,
    offsets: [usize; 256],
    timestamps: [(u64, u64); 256],
    len: AtomicUsize,
    mint: u64,
    maxt: u64,
}

//impl InnerActiveBlock {
//    fn new() -> Self {
//        Self {
//            block: Vec::with_capacity(BLOCKSZ),
//            offsets: Vec::new(),
//            idx: Vec::new(),
//            len: AtomicUsize::new(0),
//            mint: u64::MAX,
//            maxt: u64::MIN,
//        }
//    }
//
//    fn push_segment(&mut self, mint: u64, maxt: u64, slice: &[u8]) {
//        self.offsets.push(self.block.len());
//        self.block.extend_with_slice(slice);
//        self.idx.push((mint, maxt));
//        self.mint = self.mint.min(mint);
//        self.maxt = maxt;
//        self.len.fetch_add(1 SeqCst) + 1
//    }
//}

//struct InnerActiveBlock {
//    block: [u8; BLOCKSZ],
//    len: AtomicUsize,
//    idx_offset: usize,
//    tail_offset: usize,
//    mint: u64,
//    maxt: u64,
//}
//
//impl InnerActiveBlock {
//    fn new() -> Self {
//        Self {
//            block: [0u8; BLOCKSZ],
//            len: AtomicUsize::new(0),
//            idx_offset: 2, // First two bytes are for
//            tail_offset: BLOCKSZ,
//            mint: u64::MAX,
//            maxt: u64::MIN,
//        }
//    }
//
//    // Segment format:
//    // 0: Number of segments (2 bytes)
//    // 2: First segment mint (8 bytes)
//    // 10: First segment maxt (8 bytes)
//    // 18: First segment offset (2 bytes)
//    // 20: First segment len (2 bytes) // TODO: Can be elided
//    // 22: Second segment mint (8 bytes)
//    // ...
//    // Random lengths: segments in the tail
//    // BLOCKSZ: End of the block
//    fn push_segment(&mut self, mint: u64, maxt: u64, slice: &[u8]) {
//        if self.remaining() < slice.len() {
//            panic!("Not enough space in active block");
//        }
//
//        let mut s = self.idx_offset;
//        let t = self.tail_offset - slice.len();
//
//        // Write the mint and maxt of this block
//        let sz = size_of::<u64>();
//
//        self.block[s..s + sz].copy_from_slice(&mint.to_be_bytes()[..]);
//        s += sz;
//
//        self.block[s..s + sz].copy_from_slice(&maxt.to_be_bytes()[..]);
//        s += sz;
//
//        // Write the offset to the tail portion of this block
//        let sz = size_of::<u16>();
//        self.block[s..s + sz].copy_from_slice(&(<u16>::try_from(t).unwrap()).to_be_bytes()[..]);
//        s += sz;
//
//        // Write the len of this segment
//        let l = <u16>::try_from(slice.len()).unwrap();
//        self.block[s..s + sz].copy_from_slice(&l.to_be_bytes()[..]);
//        s += sz;
//
//        // Set new idx offset
//        self.idx_offset = s;
//
//        // Write slice information
//        self.block[t..t + slice.len()].copy_from_slice(slice);
//        self.tail_offset = t;
//
//        // Increment new information
//        self.mint = self.mint.min(mint);
//        self.maxt = maxt;
//        self.len.fetch_add(1, SeqCst);
//    }
//
//    fn write_len(&mut self) {
//        let len: u16 = self.len.load(SeqCst).try_into().unwrap();
//        self.block[..2].copy_from_slice(&len.to_be_bytes()[..]);
//    }
//
//    fn remaining(&self) -> usize {
//        assert!(self.tail_offset >= self.idx_offset);
//        let idx_entry_sz = size_of::<u64>() * 2 + size_of::<u16>() * 2;
//        let diff = self.tail_offset - self.idx_offset;
//        if diff > idx_entry_sz {
//            diff - idx_entry_sz
//        } else {
//            0
//        }
//    }
//}
//
//#[derive(Clone)]
//#[allow(clippy::redundant_allocation)]
//pub struct ActiveBlock {
//    inner: Arc<Arc<InnerActiveBlock>>,
//    writer_count: Arc<AtomicUsize>,
//}
//
//impl ActiveBlock {
//    pub fn new() -> Self {
//        Self {
//            inner: Arc::new(Arc::new(InnerActiveBlock::new())),
//            writer_count: Arc::new(AtomicUsize::new(0)),
//        }
//    }
//
//    pub fn writer(&self) -> ActiveBlockWriter {
//        if self.writer_count.fetch_add(1, SeqCst) > 0 {
//            panic!("Multiple writers for ActiveBlock");
//        }
//        ActiveBlockWriter {
//            ptr: Arc::as_ptr(&*self.inner) as *mut InnerActiveBlock,
//            arc: self.inner.clone(),
//            writer_count: self.writer_count.clone(),
//        }
//    }
//
//    pub fn snapshot(&self) -> ActiveBlockReader {
//        let len = (*self.inner).len.load(SeqCst);
//        let index = block_index(&self.inner.block[..], len);
//        ActiveBlockReader {
//            _inner: (*self.inner).clone(),
//            block: (&self.inner.block[..]).as_ptr(),
//            index,
//            offset: 0,
//            qmint: u64::MIN,
//            qmaxt: u64::MAX,
//            //len,
//        }
//    }
//}
//
//#[allow(clippy::redundant_allocation)]
//pub struct ActiveBlockWriter {
//    ptr: *mut InnerActiveBlock,
//    arc: Arc<Arc<InnerActiveBlock>>,
//    writer_count: Arc<AtomicUsize>,
//}
//
//impl ActiveBlockWriter {
//    pub fn push_segment(&mut self, mint: u64, maxt: u64, slice: &[u8]) {
//        unsafe {
//            self.ptr
//                .as_mut()
//                .expect("active block ptr is null")
//                .push_segment(mint, maxt, slice);
//        }
//    }
//
//    pub fn remaining(&self) -> usize {
//        unsafe { self.ptr.as_ref().unwrap().remaining() }
//    }
//
//    pub fn yield_replace(&mut self) -> ActiveBlockBuffer {
//        unsafe {
//            self.ptr
//                .as_mut()
//                .expect("active block ptr is null")
//                .write_len();
//        }
//
//        let mut inner = Arc::new(InnerActiveBlock::new());
//        unsafe {
//            std::mem::swap(&mut *Arc::get_mut_unchecked(&mut self.arc), &mut inner);
//        }
//        self.ptr = Arc::as_ptr(&*self.arc) as *mut InnerActiveBlock;
//
//        ActiveBlockBuffer { inner }
//    }
//}
//
//impl Drop for ActiveBlockWriter {
//    fn drop(&mut self) {
//        self.writer_count.fetch_sub(1, SeqCst);
//    }
//}
//
//#[derive(Copy, Clone, Eq, PartialEq, Debug)]
//struct IdxEntry {
//    mint: u64,
//    maxt: u64,
//    offt: usize,
//    len: usize,
//}
//
//pub struct ActiveBlockReader {
//    _inner: Arc<InnerActiveBlock>,
//    block: *const u8,
//    index: Vec<IdxEntry>,
//    offset: usize,
//    qmint: u64,
//    qmaxt: u64,
//}
//
//impl ActiveBlockReader {
//    fn next_compressed_segment(&mut self) -> Option<&[u8]> {
//        let data = unsafe { std::slice::from_raw_parts(self.block, BLOCKSZ) };
//        if self.offset == self.index.len() {
//            None
//        } else {
//            let entry = &self.index[self.offset];
//            self.offset += 1;
//            Some(&data[entry.offt..entry.offt + entry.len])
//        }
//    }
//}
//
//impl SegmentIterator for ActiveBlockReader {
//    fn set_range(&mut self, mint: u64, maxt: u64) {
//        self.offset = 0;
//        self.qmint = mint;
//        self.qmaxt = maxt;
//        for entry in self.index.iter() {
//            if !overlaps(entry.mint, entry.maxt, self.qmint, self.qmaxt) {
//                self.offset += 1;
//            } else {
//                break;
//            }
//        }
//    }
//
//    fn next_segment(&mut self) -> Option<Segment> {
//        let compressed = self.next_compressed_segment()?;
//        let mut seg = Segment::new();
//        Compression::decompress(compressed, &mut seg);
//        Some(seg)
//    }
//}
//
//
