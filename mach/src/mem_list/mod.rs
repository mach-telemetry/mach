use crate::{
    compression::Compression,
    id::SeriesId,
    segment::FlushSegment,
    utils::wp_lock::{WpLock, NoDealloc},
    utils::byte_buffer::ByteBuffer,
};
use serde::*;
use std::sync::{Arc, RwLock, Mutex, atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst}};
use std::mem;

pub struct Error {
}

pub struct MemList {
    head: Arc<RwLock<Node>>,
    tail: Arc<RwLock<Node>>,
}

impl MemList {
    pub fn new(bytes: Box<[u8]>, len: usize) -> Self {
        let data = Data { bytes, len };
        let node = Node {
            persistent_offset: Arc::new(AtomicU64::new(u64::MAX)),
            data: Arc::new(RwLock::new(data)),
            prev: Arc::new(RwLock::new(None)),
            next: Arc::new(RwLock::new(None)),
        };
        let head = Arc::new(RwLock::new(node));
        let tail = head.clone();

        Self {
            head,
            tail,
        }
    }

    pub fn push(&self, bytes: Box<[u8]>, len: usize) {
        let data = Data { bytes, len };
        let next = self.head.read().unwrap().clone();
        let node = Node {
            persistent_offset: Arc::new(AtomicU64::new(u64::MAX)),
            data: Arc::new(RwLock::new(data)),
            prev: Arc::new(RwLock::new(None)),
            next: Arc::new(RwLock::new(Some(next.clone()))),
        };
        next.set_prev(node.clone());
        self.set_head(node);
    }

    fn set_head(&self, node: Node) {
        let _ = mem::replace(&mut *self.head.write().unwrap(), node);
    }

    fn set_tail(&self, node: Node) {
        let _ = mem::replace(&mut *self.tail.write().unwrap(), node);
    }
}

struct Data {
    len: usize,
    bytes: Box<[u8]>,
}

#[derive(Clone)]
struct Node {
    persistent_offset: Arc<AtomicU64>,
    data: Arc<RwLock<Data>>,
    prev: Arc<RwLock<Option<Node>>>,
    next: Arc<RwLock<Option<Node>>>,
}

impl Node {
    fn set_prev(&self, node: Node) {
        assert!(mem::replace(&mut *self.prev.write().unwrap(), Some(node)).is_none());
    }

    fn set_next(&self, node: Node) {
        assert!(mem::replace(&mut *self.next.write().unwrap(), Some(node)).is_none());
    }

    fn set_persistent_offset(&self, val: u64) {
        self.persistent_offset.store(val, SeqCst);
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
unsafe impl NoDealloc for Bytes{}

pub struct Block {
    bytes: WpLock<Bytes>,
    len: AtomicUsize,
}

impl Block {
    pub fn new() -> Self {
        Self {
            bytes: WpLock::new(Bytes(vec![0u8; 2_000_000].into_boxed_slice())),
            len: AtomicUsize::new(0),
        }
    }

    pub fn push(
        &self,
        series_id: SeriesId,
        segment: &FlushSegment,
        compression: &Compression,
    ) -> bool {

        let mut offset = self.len.load(SeqCst);
        let end = offset + mem::size_of::<u64>();

        // Safety: Read is bounded by self.len.load(), these data won't be dealloced because self
        // is held by caller
        let bytes = unsafe { self.bytes.unprotected_write() };

        bytes[offset..end].copy_from_slice(&series_id.0.to_be_bytes());
        offset = end;

        // Compress the data into the buffer
        offset += {
            let mut byte_buffer = ByteBuffer::new(&mut bytes[offset..]);
            compression.compress(&segment.to_flush().unwrap(), &mut byte_buffer);
            byte_buffer.len()
        };
        self.len.store(offset, SeqCst);
        offset > 1_000_000
    }

    pub fn read(&self) -> &[u8] {
        let len = self.len.load(SeqCst);
        unsafe {
            &self.bytes.unprotected_read()[..len]
        }
    }
}

//
//
