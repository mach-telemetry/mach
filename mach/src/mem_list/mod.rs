use crate::{
    compression::Compression,
    id::SeriesId,
    segment::FlushSegment,
    utils::wp_lock::{WpLock, NoDealloc},
    utils::byte_buffer::ByteBuffer,
};
use std::sync::{Arc, RwLock, Mutex, atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst}};
use std::mem;
use dashmap::DashMap;

pub struct List {
    head: AtomicU64,
}

pub struct BlockList {
    head_block: WpLock<Block>,
    tail: AtomicU64,
    block_map: DashMap<u64, Arc<ReadOnlyBlock>>,
}

impl BlockList {
    //pub fn new() -> Self {
    //    BlockList {
    //        head: AtomicU64::new(u64::MAX),
    //        tail: AtomicU64::new(u64::MAX),
    //        map: DashMap::new(),
    //    }
    //}

    //pub fn push(&self, key: u64, block: Block) {
    //    let key = self.head.store(key) + 1;
    //    self.map.insert(key, block);
    //    self.head.store(key, SeqCst);
    //}

    pub fn push(
        &self,
        list: &List,
        series_id: SeriesId,
        segment: &FlushSegment,
        compression: &Compression,
    ) {

        // Safety: id() is atomic
        let block_id = unsafe { self.head_block.unprotected_read().id.load(SeqCst) };
        let head = list.head.load(SeqCst);

        // Safety: unprotected write because Block push only appends data and increments an atomic
        // read is bounded by the atomic
        let is_full = unsafe {
            self.head_block.unprotected_write().push(head, series_id, segment, compression)
        };

        if head != block_id {
            list.head.store(block_id, SeqCst);
        }

        if is_full {
            // Safety: there should be only one writer and it should be doing this push
            let copy = unsafe { self.head_block.unprotected_read().as_read_only() };
            self.block_map.insert(block_id, Arc::new(copy));

            // Mark current block as cleared
            self.head_block.protected_write().reset();
        }
    }
}

struct Bytes(Box<[u8]>);

impl std::ops::Deref for Bytes {
    type Target = Box<[u8]>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Safety:
/// for internal use only. see use in Block struct
unsafe impl NoDealloc for Block{}

struct Block {
    id: AtomicU64,
    bytes: Bytes,
    len: AtomicUsize,
}

impl Block {
    //fn new(id: u64) -> Self {
    //    Self {
    //        bytes: Bytes(vec![0u8; 2_000_000].into_boxed_slice()),
    //        len: AtomicUsize::new(0),
    //    }
    //}

    fn push(
        &mut self,
        last_block_id: u64,
        series_id: SeriesId,
        segment: &FlushSegment,
        compression: &Compression,
    ) -> bool {
        let mut offset = self.len.load(SeqCst);
        let u64sz = mem::size_of::<u64>();

        // Write SeriesId
        let end = offset + u64sz;
        self.bytes[offset..end].copy_from_slice(&series_id.0.to_be_bytes());
        offset = end;

        // Write where we can find the next chunk for this SeriesId
        let end = offset + u64sz;
        self.bytes[offset..end].copy_from_slice(&last_block_id.to_be_bytes());
        offset = end;

        // Reserve space for the size of the chunk
        let end = offset + u64sz;
        self.bytes[offset..end].copy_from_slice(&0u64.to_be_bytes());
        let size_offset = offset;
        offset = end;

        // Compress the data into the buffer
        offset += {
            let mut byte_buffer = ByteBuffer::new(&mut self.bytes[offset..]);
            compression.compress(&segment.to_flush().unwrap(), &mut byte_buffer);
            byte_buffer.len()
        };

        // Write the chunk
        let size = (offset - size_offset) as u64;
        self.bytes[size_offset..size_offset + u64sz].copy_from_slice(&size.to_be_bytes());

        // update length
        self.len.store(offset, SeqCst);

        // return true if full
        offset > 1_000_000
    }

    fn reset(&mut self) {
        let u64sz = mem::size_of::<u64>();
        let new_id = self.id.fetch_add(1, SeqCst) + 1;
        self.bytes[..u64sz].copy_from_slice(&new_id.to_be_bytes());
        self.len.store(u64sz, SeqCst);
    }

    fn as_read_only(&self) -> ReadOnlyBlock {
        let len = self.len.load(SeqCst);
        let bytes = self.bytes[..len].into();
        ReadOnlyBlock {
            bytes,
        }
    }
}

pub struct ReadOnlyBlock {
    bytes: Box<[u8]>,
}
