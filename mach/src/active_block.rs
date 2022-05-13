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

    pub fn get_tail(&self) -> &[u8] {
        let b = self.bytes.as_ref();
        let offset = usize::from_be_bytes(b[0..8].try_into().unwrap());
        &b[offset..]
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]>> Bytes<T> {
    fn set_tail<D: Serialize>(&mut self, data: &D) {
        let len = self.len.load(SeqCst);
        self[0..8].copy_from_slice(&len.to_be_bytes());
        let mut byte_buffer = ByteBuffer::new(&mut self[len..]);
        bincode::serialize_into(&mut byte_buffer, data).unwrap();
        let len = byte_buffer.len();
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
    tail: BlockTail,
}

impl<T: AsRef<[u8]>> StaticBlock<T> {
    pub fn new(bytes: T) -> Self {
        let bytes = Bytes::new(bytes);
        let tail = bincode::deserialize(bytes.get_tail()).unwrap();
        StaticBlock {
            bytes,
            tail
        }
    }

    pub fn read(&self, node: StaticNode) -> Result<(BlockEntries, StaticNode), Error> {
        Ok(self.bytes.read(node.offset, node.size))
    }

    pub fn samples(&self) -> usize {
        self.tail.samples
    }

    pub fn min_timestamp(&self) -> u64 {
        self.tail.min_ts
    }

    pub fn max_timestamp(&self) -> u64 {
        self.tail.max_ts
    }

    pub fn chunks(&self) -> &HashMap<SeriesId, Vec<(usize, usize, u64)>> {
        &self.tail.chunks
    }
}

#[derive(Serialize, Deserialize)]
pub struct BlockTail {
    samples: usize,
    min_ts: u64,
    max_ts: u64,
    chunks: HashMap<SeriesId, Vec<(usize, usize, u64)>>,
}

impl BlockTail {
    fn new() -> Self {
        Self {
            samples: 0,
            min_ts: u64::MAX,
            max_ts: 0,
            chunks: HashMap::new(),
        }
    }

    fn clear(&mut self) {
        self.chunks.clear();
        self.samples = 0;
    }

    fn insert(&mut self, id: SeriesId, meta: (usize, usize, u64), samples: usize) {
        self.samples += samples;
        self.min_ts = self.min_ts.min(meta.2);
        self.max_ts = self.max_ts.max(meta.2);
        self.chunks.entry(id).or_insert_with(|| {
            Vec::new()
        }).push(meta);
    }
}

pub struct ActiveBlock {
    queue_offset: Arc<AtomicU64>,
    block_version: AtomicUsize,
    bytes: Bytes<Box<[u8]>>,

    // Safety note: this is a struct that does not meet the guarantees of WpLock. It should not be
    // accessed in a read without extra precaution
    tail: BlockTail,
    flush_sz: usize,
}

/// Safety
///
/// This is safe because ActiveBlock.tail is never accessed by the read() function
unsafe impl NoDealloc for ActiveBlock {}

impl ActiveBlock {
    pub fn is_full(&self) -> bool {
        self.flush_sz <= self.bytes.len.load(SeqCst)
    }

    pub fn new(flush_sz: usize) -> Self {
        Self {
            bytes: Bytes::new(vec![0u8; flush_sz * 2].into_boxed_slice()),
            queue_offset: Arc::new(AtomicU64::new(u64::MAX)),
            tail: BlockTail::new(),
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
        let ts = *segment.timestamps().last().unwrap();
        let (offset, size) = self.bytes.push(id, segment, compression, prev_node);

        self.tail.insert(id, (offset, size, ts), segment.len());

        ActiveNode {
            queue_offset: self.queue_offset.clone(),
            offset,
            size,
            block_version: self.block_version.load(SeqCst),
        }
    }

    pub async fn flush(&mut self, flusher: &mut DurableQueueWriter) -> Result<(), Error> {
        //println!("FLUSHING ACTIVE BLOCK");
        self.bytes.set_tail(&self.tail);
        let queue_offset = flusher
            .write(&self.bytes[..self.bytes.len.load(SeqCst)])
            .await?;
        // Boradcast the queue offset to everyone who pushed
        self.queue_offset.store(queue_offset, SeqCst);
        Ok(())
    }

    pub fn reset(&mut self) {
        self.block_version.fetch_add(1, SeqCst);
        self.tail.clear();
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

#[cfg(test)]
mod bytes_tests {
    use super::*;
    use crate::compression::CompressFn;
    use crate::sample::Type;
    use crate::segment::Buffer;
    use crate::series::Types;
    use crate::test_utils::*;

    #[test]
    fn test_set_tail() {
        // set-tail should change Bytes's first 8 bytes to point to tail,
        // and increment Bytes's len by the tail's size.

        let data = (*MULTIVARIATE_DATA[0].1).clone();

        let mut b = Bytes::new(vec![0xCC; 2048].into_boxed_slice());
        let mut meta = HashMap::new();

        let series_id = SeriesId(333);
        let nvars = data[0].values.len();
        let types = vec![Types::F64; nvars];
        let mut buf = Buffer::new(types.as_slice());
        for (_, item) in data[..255].iter().enumerate() {
            let mut vals = Vec::new();
            item.values.iter().for_each(|x| vals.push(Type::F64(*x)));
            buf.push_type(item.ts, vals.as_slice()).unwrap();
        }

        let segment_info = b.push(
            series_id,
            &buf.to_flush().unwrap(),
            &Compression::from(vec![CompressFn::Decimal(3); nvars]),
            ActiveNode::new().static_node(),
        );
        meta.insert(series_id, segment_info);
        let len_meta = {
            let mut buf = vec![0; 1024].into_boxed_slice();
            let mut byte_buffer = ByteBuffer::new(&mut buf);
            bincode::serialize_into(&mut byte_buffer, &meta).unwrap();
            byte_buffer.len()
        };

        let len_before_insert = b.len.load(SeqCst);
        b.set_tail(&meta);
        let len_after_insert = b.len.load(SeqCst);

        let tail_offset = usize::from_be_bytes(b[0..8].try_into().unwrap());
        assert!(len_after_insert == len_before_insert + len_meta);
        assert!(len_after_insert == tail_offset + len_meta);
    }
}
