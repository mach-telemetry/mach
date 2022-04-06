use crate::{
    compression::Compression,
    durable_queue::{self, DurableQueueReader, DurableQueueWriter},
    id::SeriesId,
    segment::FullSegment,
    utils::{byte_buffer::ByteBuffer, wp_lock::NoDealloc},
};
use serde::*;
use std::{
    collections::HashMap,
    convert::TryInto,
    mem::size_of,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

#[derive(Debug)]
pub enum Error {
    DurableQueue(durable_queue::Error),
    BlockVersion,
    EndOfQueue,
}

impl From<durable_queue::Error> for Error {
    fn from(item: durable_queue::Error) -> Self {
        Error::DurableQueue(item)
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
    pub fn new() -> Self {
        Self {
            queue_offset: Arc::new(AtomicU64::new(u64::MAX)),
            offset: usize::MAX,
            size: usize::MAX,
            block_version: usize::MAX,
        }
    }

    pub fn static_node(&self) -> StaticNode {
        StaticNode {
            queue_offset: self.queue_offset.load(SeqCst),
            offset: self.offset,
            size: self.size,
            block_version: self.block_version,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct StaticNode {
    pub queue_offset: u64,
    pub offset: usize,
    pub size: usize,
    pub block_version: usize,
}

impl Default for StaticNode {
    fn default() -> Self {
        StaticNode {
            queue_offset: u64::MAX,
            offset: usize::MAX,
            size: usize::MAX,
            block_version: usize::MAX,
        }
    }
}

impl StaticNode {
    fn to_bytes(self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..8].copy_from_slice(&self.queue_offset.to_be_bytes());
        buf[8..16].copy_from_slice(&self.offset.to_be_bytes());
        buf[16..24].copy_from_slice(&self.size.to_be_bytes());
        buf[24..32].copy_from_slice(&self.block_version.to_be_bytes());
        buf
    }

    pub fn from_bytes(data: [u8; 32]) -> Self {
        let queue_offset = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let offset = usize::from_be_bytes(data[8..16].try_into().unwrap());
        let size = usize::from_be_bytes(data[16..24].try_into().unwrap());
        let block_version = usize::from_be_bytes(data[24..32].try_into().unwrap());
        StaticNode {
            queue_offset,
            offset,
            size,
            block_version,
        }
    }

    pub fn read_from_queue(
        &self,
        queue: &mut DurableQueueReader,
    ) -> Result<(BlockEntries, StaticNode), Error> {
        if self.queue_offset == u64::MAX {
            Err(Error::EndOfQueue)
        } else {
            Ok(Bytes::new(queue.read(self.queue_offset)?).read(self.offset, self.size))
        }
    }
}

struct Bytes<T> {
    len: AtomicUsize,
    bytes: T,
}

impl<T: AsRef<[u8]>> Deref for Bytes<T> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &*self.bytes.as_ref()
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]>> DerefMut for Bytes<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.bytes.as_mut()
    }
}

impl<T: AsRef<[u8]>> Bytes<T> {
    fn copy_buffer(&self) -> Box<[u8]> {
        let len = self.len.load(SeqCst);
        self.bytes.as_ref()[..len].into()
    }

    fn read(&self, last_offset: usize, last_size: usize) -> (BlockEntries, StaticNode) {
        let mut node = StaticNode {
            queue_offset: u64::MAX,
            offset: last_offset,
            size: last_size,
            block_version: usize::MAX,
        };
        let mut copies = Vec::new();

        while node.queue_offset == u64::MAX && node.offset != usize::MAX {
            let bytes = &self[node.offset..node.offset + node.size];
            //let _series_id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
            node = StaticNode::from_bytes(bytes[8..40].try_into().unwrap());
            copies.push(bytes[40..].into());
        }
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
        prev_node: StaticNode,
    ) -> (usize, usize) {
        let len = self.len.load(SeqCst);
        let mut offset = len;

        let end = offset + size_of::<u64>();
        self[offset..end].copy_from_slice(&series_id.0.to_be_bytes());
        offset = end;

        let end = offset + 32;
        self[offset..end].copy_from_slice(&prev_node.to_bytes());
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

pub type BlockEntries = Vec<Box<[u8]>>;

pub struct StaticBlock<T> {
    bytes: Bytes<T>,
}

impl<T: AsRef<[u8]>> StaticBlock<T> {
    pub fn new(bytes: T) -> Self {
        StaticBlock {
            bytes: Bytes::new(bytes),
        }
    }

    pub fn read(&self, node: StaticNode) -> Result<(BlockEntries, StaticNode), Error> {
        Ok(self.bytes.read(node.offset, node.size))
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
        let (offset, size) = self.bytes.push(id, segment, compression, prev_node);

        self.series.insert(id, (offset, size));

        ActiveNode {
            queue_offset: self.queue_offset.clone(),
            offset,
            size,
            block_version: self.block_version.load(SeqCst),
        }
    }

    pub async fn flush(&mut self, flusher: &mut DurableQueueWriter) -> Result<(), Error> {
        //println!("FLUSHING ACTIVE BLOCK");
        self.bytes.set_tail(&self.series);
        let queue_offset = flusher
            .write(&self.bytes[..self.bytes.len.load(SeqCst)])
            .await?;
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

    pub fn read(&self, node: StaticNode) -> Result<(BlockEntries, StaticNode), Error> {
        // Need to keep track of own version because of argument. StaticNode can be for another
        // version of the node. Without this argument, WpLock would have been enough but alas, no
        // dice
        let block_version = self.block_version.load(SeqCst);
        if block_version == node.block_version {
            let result = self.bytes.read(node.offset, node.size);
            let block_version_check = self.block_version.load(SeqCst);
            if block_version == block_version_check {
                return Ok(result);
            }
        }
        Err(Error::BlockVersion)
    }
}
