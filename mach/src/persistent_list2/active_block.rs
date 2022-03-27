use crate::{
    compression::{Compression, DecompressBuffer},
    constants::BUFSZ,
    id::SeriesId,
    segment::FullSegment,
    tags::Tags,
    utils::{
        byte_buffer::ByteBuffer,
        wp_lock::{NoDealloc, WpLock},
    },
    durable_queue::{self, DurableQueueWriter},
};
use std::{
    sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst}},
    ops::{Deref, DerefMut},
    mem::size_of,
    collections::HashMap,
    convert::TryInto,
};
use serde::*;

pub enum Error {
    FlushError(durable_queue::Error),
    InconsistentRead,
}

impl From<durable_queue::Error> for Error {
    fn from(item: durable_queue::Error) -> Self {
        Error::FlushError(item)
    }
}

#[derive(Clone)]
pub struct ActiveNode {
    queue_offset: Arc<AtomicU64>,
    offset: usize,
    size: usize,
    block_version: usize,
}

unsafe impl NoDealloc for ActiveNode {}

impl ActiveNode {
    fn new() -> Self {
        Self {
            queue_offset: Arc::new(AtomicU64::new(u64::MAX)),
            offset: usize::MAX,
            size: usize::MAX,
        }
    }

    pub fn static_node(&self) -> StaticNode {
        StaticNode {
            queue_offset: self.queue_offset.load(SeqCst),
            offset: self.offset,
            size: self.size,
        }
    }
}

#[derive(Clone)]
pub struct StaticNode {
    pub queue_offset: u64,
    pub offset: usize,
    pub size: usize,
    pub block_version: usize,
}

struct Bytes<T> {
    len: AtomicUsize,
    bytes: T,
}

impl<T: AsRef<[u8]>> Deref for Bytes<T> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.bytes.as_ref()[..]
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]>> DerefMut for Bytes<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes.as_mut()[..]
    }
}

impl<T: AsRef<[u8]>> Bytes<T> {
    fn copy_buffer(&self) -> Box<[u8]> {
        let len = self.len.load(SeqCst);
        self.bytes.as_ref()[..len].into()
    }

    fn read(&self, last_offset: usize, last_size: usize) -> (Vec<Box<[u8]>>, StaticNode) {
        let mut queue_offset = u64::MAX;
        let mut offset = last_offset;
        let mut size = last_size;
        let mut copies = Vec::new();

        while queue_offset == u64::MAX && offset != usize::MAX {
            let bytes = &self[offset..offset + size];
            //let _series_id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
            queue_offset = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
            offset = usize::from_be_bytes(bytes[16..24].try_into().unwrap());
            size = usize::from_be_bytes(bytes[24..32].try_into().unwrap());
            copies.push(bytes[32..].into());
        }

        let node = StaticNode {
            queue_offset,
            offset,
            size,
        };

        (copies, node)
    }

    fn new(bytes: T) -> Self {
        Self {
            len: AtomicUsize::new(8),
            bytes,
        }
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]>> Bytes<T> {
    fn set_tail<D: Serialize>(&mut self, data: &D) {
        let len = self.len.load(SeqCst);
        self[0..8].copy_from_slice(&len.to_be_bytes());
        let mut byte_buffer = ByteBuffer::new(&mut self[len..]);
        let len = byte_buffer.len();
        bincode::serialize_into(&mut byte_buffer, data).unwrap();
        self.len.fetch_add(len, SeqCst);
    }

    fn reset(&mut self) {
        self.len.store(8, SeqCst);
    }

    fn push(
        &mut self,
        series_id: SeriesId,
        segment: &FullSegment,
        compression: &Compression,
        prev_node: StaticNode
    ) -> (usize, usize) {
        let len = self.len.load(SeqCst);
        let mut offset = len;

        let end = offset + size_of::<u64>();
        self[offset..end].copy_from_slice(&series_id.0.to_be_bytes());
        offset = end;

        let end = offset + size_of::<u64>();
        self[offset..end].copy_from_slice(&prev_node.queue_offset.to_be_bytes());
        offset = end;

        let end = offset + size_of::<usize>();
        self[offset..end].copy_from_slice(&prev_node.offset.to_be_bytes());
        offset = end;

        let end = offset + size_of::<usize>();
        self[offset..end].copy_from_slice(&prev_node.size.to_be_bytes());
        offset = end;

        // Compress the data into the buffer
        offset += {
            let mut byte_buffer = ByteBuffer::new(&mut self[offset..]);
            compression.compress(segment, &mut byte_buffer);
            byte_buffer.len()
        };

        let result = (len, offset - len);
        self.len.store(offset, SeqCst);
        result
    }
}

pub struct ActiveBlock {
    queue_offset: Arc<AtomicU64>,
    block_version: AtomicUsize,
    bytes: Bytes<Box<[u8]>>,
    series: HashMap<SeriesId, (usize, usize)>,
    flush_sz: usize,
}

unsafe impl NoDealloc for ActiveBlock {}

impl ActiveBlock {
    pub fn is_full(&self) -> bool {
        self.flush_sz <= self.bytes.len.load(SeqCst)
    }

    pub fn new(flush_sz: usize) -> Self {
        Self {
            bytes: Bytes::new(vec![0u8; flush_sz * 2].into_boxed_slice()),
            queue_offset: Arc::new(AtomicU64::new(u64::MAX)),
            series: HashMap::new(),
            flush_sz,
            block_version: AtomicUsize::new(0),
        }
    }

    pub fn push(
        &mut self,
        id: SeriesId,
        segment: &FullSegment,
        compression: &Compression,
        prev_node: StaticNode,
    ) -> ActiveNode {
        let (offset, size) =
            self.bytes
                .push(id, segment, compression, prev_node);

        self.series.insert(id, (offset, size));

        ActiveNode {
            queue_offset: self.queue_offset.clone(),
            offset,
            size,
        }
    }

    pub fn flush(&mut self, flusher: &mut DurableQueueWriter) -> Result<(), Error> {
        self.bytes.set_tail(&self.series);
        let queue_offset = flusher
            .write(&self.bytes[..self.bytes.len.load(SeqCst)])?;
        // Boradcast the queue offset to everyone who pushed
        self.queue_offset.store(queue_offset, SeqCst);
        Ok(())
    }

    pub fn reset(&mut self) {
        self.block_version.fetch_add(1, SeqCst);
        self.series.clear();
        self.bytes.reset();
        self.queue_offset = Arc::new(AtomicU64::new(u64::MAX));
    }

    pub fn copy_buffer(&self) -> Box<[u8]> {
        self.bytes.copy_buffer()
    }

    pub fn read(
        &self,
        last_offset: usize,
        last_size: usize,
    ) -> (Vec<Box<[u8]>>, StaticNode) {
        self.bytes.read(last_offset, last_size)
    }
}

