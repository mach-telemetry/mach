use crate::{
    compression::{Compression, DecompressBuffer},
    constants::BUFSZ,
    id::SeriesId,
    persistent_list::Error,
    segment::FullSegment,
    tags::Tags,
    utils::{
        byte_buffer::ByteBuffer,
        wp_lock::{NoDealloc, WpLock},
    },
};
use serde::*;
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    convert::{AsMut, AsRef, TryInto},
    mem::size_of,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

//pub trait ChunkMeta: Sync + Send {
//    fn update(&mut self, tags: &HashMap<Tags, ChunkMetadata>, chunk_id: u64) -> Result<(), Error>;
//    //fn get_meta(&mut self, tag: &Tag) -> Result<(u64, usize), Error>;
//}

pub trait ChunkWriter: Sync + Send {
    fn write(&mut self, bytes: &[u8]) -> Result<u64, Error>;
}

pub trait ChunkReader {
    fn read(&mut self, chunk_id: u64) -> Result<&[u8], Error>;
}

struct InnerList {
    head: WpLock<Node>,
    has_writer: AtomicBool,
}

impl InnerList {
    pub fn new() -> Self {
        Self {
            head: WpLock::new(Node::new()),
            has_writer: AtomicBool::new(false),
        }
    }
}

#[derive(Clone)]
pub struct ListBuffer(Arc<WpLock<InnerBuffer>>);

pub type Buffer = ListBuffer;

impl ListBuffer {
    pub fn new(flush_sz: usize) -> Self {
        ListBuffer(Arc::new(WpLock::new(InnerBuffer::new(flush_sz))))
    }
}

impl Deref for ListBuffer {
    type Target = Arc<WpLock<InnerBuffer>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub struct List {
    inner_list: Arc<InnerList>,
    buffer: ListBuffer,
}

impl List {
    pub fn new(buffer: ListBuffer) -> Self {
        Self {
            inner_list: Arc::new(InnerList::new()),
            buffer,
        }
    }

    pub fn writer(&self) -> ListWriter {
        assert!(!self.inner_list.has_writer.swap(true, SeqCst));
        ListWriter {
            inner_list: self.inner_list.clone(),
            buffer: self.buffer.clone(),
        }
    }

    fn inner_get_head(&self, tries: usize) -> Result<Node, Error> {
        let head = self.inner_list.head.protected_read();
        let cloned = head.clone();
        match (head.release(), tries) {
            (Err(_), 0..=2) => self.inner_get_head(tries + 1),
            (Err(_), 3) => Err(Error::InconsistentRead),
            (Ok(_), _) => Ok(cloned),
            (Err(_), _) => unimplemented!(),
        }
    }

    fn get_head(&self) -> Result<Node, Error> {
        self.inner_get_head(1)
    }

    pub fn read(&self) -> Result<ListSnapshot, Error> {
        let head = self.get_head()?;
        let chunk_id = head.chunk_id.load(SeqCst);

        // This means the buffer_id has not been updated and this list has not yet written to the
        // buffer. There is no data for this list
        if head.buffer_id == usize::MAX {
            Ok(ListSnapshot::new(Vec::new(), ReadNode::new()))
        }
        // This means that the buffer that this head was pointing has been written to storage.
        else if chunk_id != u64::MAX {
            Ok(ListSnapshot::new(Vec::new(), head.read_node()))
        }
        // Otherwise the buffer has not yet been written to storage...
        else {
            let buf = self.buffer.protected_read();
            match buf.read(head.buffer_id, head.offset, head.size) {
                // And the relevant buffer items were copied successfully
                Ok((buffer_copy, persistent)) => {
                    // Can release the read guard without checking since the check is done
                    // internally by the buffer
                    match buf.release() {
                        _ => {}
                    }
                    Ok(ListSnapshot::new(buffer_copy, persistent))
                }

                // And the relevant buffer items were not copied successfully (by implication
                // flushed to storage)
                Err(_) => {
                    // Can release the read guard without checking since the check is done
                    // internally by the buffer
                    match buf.release() {
                        _ => {}
                    }
                    // chunk_id must have been updated since the last check
                    assert!(head.chunk_id.load(SeqCst) != u64::MAX);
                    Ok(ListSnapshot::new(Vec::new(), head.read_node()))
                }
            }
        }
    }
}

pub struct ListWriter {
    inner_list: Arc<InnerList>,
    buffer: ListBuffer,
}

impl Drop for ListWriter {
    fn drop(&mut self) {
        assert!(self.inner_list.has_writer.swap(false, SeqCst))
    }
}

impl ListWriter {
    pub fn push_segment<W: ChunkWriter>(
        &mut self,
        id: SeriesId,
        segment: &FullSegment,
        compression: &Compression,
        w: &mut W,
    ) {
        // SAFETY: Holding a read reference to head here doesn't race with a concurrent ListReader
        let head = unsafe { self.inner_list.head.unprotected_read() };
        let chunk_id = head.chunk_id.load(SeqCst);

        // SAFETY: push_bytes doesn't race with a concurrent ListReader.
        let (new_head, to_flush) = unsafe {
            let buf = self.buffer.unprotected_write();
            let new_head =
                buf.push_segment(id, segment, compression, chunk_id, head.offset, head.size);
            let to_flush = buf.is_full();
            (new_head, to_flush)
        };
        drop(head);

        //let to_flush = new_head.offset + new_head.size > BUFSZ;

        // Need to guard the writes now since we're updating the head
        let mut guard = self.inner_list.head.protected_write();
        *guard = new_head;
        drop(guard);

        // Then now we check if we should flush. A concurrent reader can safely access the new
        // head. When making a copy of the buffer information, will fail if the buffer reset
        // concurrently.
        if to_flush {
            let mut guard = self.buffer.protected_write();
            guard.flush(w);
            guard.reset();
        }
    }

    pub fn push_bytes<W: ChunkWriter>(&mut self, bytes: &[u8], w: &mut W) {
        // SAFETY: Holding a read reference to head here doesn't race with a concurrent ListReader
        let head = unsafe { self.inner_list.head.unprotected_read() };
        let chunk_id = head.chunk_id.load(SeqCst);

        // SAFETY: push_bytes doesn't race with a concurrent ListReader.
        let (new_head, to_flush) = unsafe {
            let buf = self.buffer.unprotected_write();
            let new_head = buf.push_bytes(bytes, chunk_id, head.offset, head.size);
            let to_flush = buf.is_full();
            (new_head, to_flush)
        };
        drop(head);

        // Need to guard the writes now since we're updating the head
        let mut guard = self.inner_list.head.protected_write();
        *guard = new_head;
        drop(guard);

        // Then now we check if we should flush. A concurrent reader can safely access the new
        // head. When making a copy of the buffer information, will fail if the buffer reset
        // concurrently.
        if to_flush {
            let mut guard = self.buffer.protected_write();
            guard.flush(w);
            guard.reset();
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ListSnapshot {
    buffer_copy: Vec<Box<[u8]>>,
    persistent: ReadNode,
}

impl ListSnapshot {
    fn new(buffer_copy: Vec<Box<[u8]>>, persistent: ReadNode) -> Self {
        Self {
            buffer_copy,
            persistent,
        }
    }
}

pub struct ListReader {
    buffer_copy: Vec<Box<[u8]>>,
    persistent: ReadNode,
    idx: usize,
    local_buffer: Vec<u8>,
    decompress_buf: DecompressBuffer,
}

impl ListReader {
    fn new(buffer_copy: Vec<Box<[u8]>>, persistent: ReadNode) -> Self {
        Self {
            buffer_copy,
            persistent,
            idx: 0,
            local_buffer: Vec::new(),
            decompress_buf: DecompressBuffer::new(),
        }
    }

    pub fn next_segment<R: ChunkReader>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<&DecompressBuffer>, Error> {
        let bytes = self.next_bytes(reader)?;
        match bytes {
            None => Ok(None),
            Some(bytes) => {
                // bytes is a self reference. drop the reference here and get a local ref so that
                // we don't make borrow checker sad
                drop(bytes);
                let bytes = &self.buffer_copy[self.idx - 1][..];

                // get the compressed size and move to compressed data offset
                Compression::decompress(bytes, &mut self.decompress_buf).unwrap();

                Ok(Some(&self.decompress_buf))
            }
        }
    }

    pub fn next_bytes<R: ChunkReader>(&mut self, reader: &mut R) -> Result<Option<&[u8]>, Error> {
        if self.idx == self.buffer_copy.len() {
            let chunk_id = self.persistent.chunk_id;
            if chunk_id == u64::MAX {
                return Ok(None);
            } else {
                let buf = Bytes::new(reader.read(chunk_id)?);
                let (copies, node) = buf.read(self.persistent.offset, self.persistent.size);
                self.buffer_copy = copies;
                self.persistent = node;
                self.idx = 0;
            }
        }
        let idx = self.idx;
        self.idx += 1;
        Ok(Some(&self.buffer_copy[idx][..]))
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct ReadNode {
    chunk_id: u64,
    offset: usize,
    size: usize,
    buffer_id: usize,
}

impl ReadNode {
    fn new() -> Self {
        Self {
            chunk_id: u64::MAX,
            offset: usize::MAX,
            size: usize::MAX,
            buffer_id: usize::MAX,
        }
    }
}

#[derive(Clone)]
struct Node {
    chunk_id: Arc<AtomicU64>,
    offset: usize,
    size: usize,
    buffer_id: usize,
}

unsafe impl NoDealloc for Node {}

impl Node {
    fn new() -> Self {
        Self {
            chunk_id: Arc::new(AtomicU64::new(u64::MAX)),
            offset: usize::MAX,
            size: usize::MAX,
            buffer_id: usize::MAX,
        }
    }

    fn read_node(&self) -> ReadNode {
        ReadNode {
            chunk_id: self.chunk_id.load(SeqCst),
            offset: self.offset,
            size: self.size,
            buffer_id: self.buffer_id,
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
        &self.bytes.as_ref()[..]
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]>> DerefMut for Bytes<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes.as_mut()[..]
    }
}

impl<T: AsRef<[u8]>> Bytes<T> {
    fn copy(&self) -> Box<[u8]> {
        let len = self.len.load(SeqCst);
        self.bytes.as_ref()[..len].into()
    }

    fn read(&self, last_offset: usize, last_size: usize) -> (Vec<Box<[u8]>>, ReadNode) {
        let mut chunk_id = u64::MAX;
        let mut offset = last_offset;
        let mut size = last_size;
        let mut copies = Vec::new();

        while chunk_id == u64::MAX && offset != usize::MAX {
            let bytes = &self[offset..offset + size];
            //let _series_id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
            chunk_id = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
            offset = usize::from_be_bytes(bytes[16..24].try_into().unwrap());
            size = usize::from_be_bytes(bytes[24..32].try_into().unwrap());
            copies.push(bytes[32..].into());
        }

        let node = ReadNode {
            chunk_id,
            offset,
            size,
            buffer_id: usize::MAX,
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

    fn push_segment(
        &mut self,
        series_id: SeriesId,
        segment: &FullSegment,
        compression: &Compression,
        chunk_id: u64,
        last_offset: usize,
        last_size: usize,
    ) -> (usize, usize) {
        let len = self.len.load(SeqCst);
        let mut offset = len;

        let end = offset + size_of::<u64>();
        self[offset..end].copy_from_slice(&series_id.0.to_be_bytes());
        offset = end;

        let end = offset + size_of::<u64>();
        self[offset..end].copy_from_slice(&chunk_id.to_be_bytes());
        offset = end;

        let end = offset + size_of::<usize>();
        self[offset..end].copy_from_slice(&last_offset.to_be_bytes());
        offset = end;

        let end = offset + size_of::<usize>();
        self[offset..end].copy_from_slice(&last_size.to_be_bytes());
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

    fn push_bytes(
        &mut self,
        bytes: &[u8],
        chunk_id: u64,
        last_offset: usize,
        last_size: usize,
    ) -> (usize, usize) {
        let len = self.len.load(SeqCst);
        let mut offset = len;

        let end = offset + size_of::<u64>();
        self[offset..end].copy_from_slice(&0u64.to_be_bytes());
        offset = end;

        let end = offset + size_of::<u64>();
        self[offset..end].copy_from_slice(&chunk_id.to_be_bytes());
        offset = end;

        let end = offset + size_of::<usize>();
        self[offset..end].copy_from_slice(&last_offset.to_be_bytes());
        offset = end;

        let end = offset + size_of::<usize>();
        self[offset..end].copy_from_slice(&last_size.to_be_bytes());
        offset = end;

        let end = offset + bytes.len();
        self[offset..end].copy_from_slice(bytes);
        offset = end;

        let result = (len, offset - len);
        self.len.store(offset, SeqCst);
        result
    }
}

pub struct InnerBuffer {
    chunk_id: Arc<AtomicU64>,
    bytes: Bytes<Box<[u8]>>,
    series: HashMap<SeriesId, (usize, usize)>,
    flush_sz: usize,
    buffer_id: AtomicUsize,
}

unsafe impl NoDealloc for InnerBuffer {}

impl InnerBuffer {
    fn is_full(&self) -> bool {
        self.flush_sz <= self.bytes.len.load(SeqCst)
    }

    pub fn new(flush_sz: usize) -> Self {
        Self {
            bytes: Bytes::new(vec![0u8; flush_sz * 2].into_boxed_slice()),
            chunk_id: Arc::new(AtomicU64::new(u64::MAX)),
            series: HashMap::new(),
            flush_sz,
            buffer_id: AtomicUsize::new(0),
        }
    }

    fn push_segment(
        &mut self,
        id: SeriesId,
        segment: &FullSegment,
        compression: &Compression,
        chunk_id: u64,
        last_offset: usize,
        last_size: usize,
    ) -> Node {
        let (offset, size) =
            self.bytes
                .push_segment(id, segment, compression, chunk_id, last_offset, last_size);

        self.series.insert(id, (offset, size));

        Node {
            chunk_id: self.chunk_id.clone(),
            offset,
            size,
            buffer_id: self.buffer_id.load(SeqCst),
        }
    }

    fn push_bytes(
        &mut self,
        bytes: &[u8],
        chunk_id: u64,
        last_offset: usize,
        last_size: usize,
    ) -> Node {
        let (offset, size) = self
            .bytes
            .push_bytes(bytes, chunk_id, last_offset, last_size);
        Node {
            chunk_id: self.chunk_id.clone(),
            offset,
            size,
            buffer_id: self.buffer_id.load(SeqCst),
        }
    }

    fn flush<W: ChunkWriter>(&mut self, flusher: &mut W) {
        self.bytes.set_tail(&self.series);
        let chunk_id = flusher
            .write(&self.bytes[..self.bytes.len.load(SeqCst)])
            .unwrap();
        self.chunk_id.store(chunk_id, SeqCst);
    }

    fn reset(&mut self) {
        let buffer_id = self.buffer_id.fetch_add(1, SeqCst) + 1;
        self.series.clear();
        self.bytes.reset();
        self.chunk_id = Arc::new(AtomicU64::new(u64::MAX));
    }

    fn copy(&self) -> Result<Box<[u8]>, Error> {
        let buffer_id = self.buffer_id.load(SeqCst);
        let copy = self.bytes.copy();
        if buffer_id != self.buffer_id.load(SeqCst) {
            return Err(Error::InconsistentRead);
        }
        Ok(copy)
    }

    fn read(
        &self,
        buffer_id: usize,
        last_offset: usize,
        last_size: usize,
    ) -> Result<(Vec<Box<[u8]>>, ReadNode), Error> {
        // We check buffer id internally because wrapping in WPLock does not have enough logic to
        // detect that the buffer was actually flushed. (e.g. the buffer version is wrong)

        // Buffer was flushed before this happened
        if buffer_id != self.buffer_id.load(SeqCst) {
            return Err(Error::InconsistentRead);
        }

        let (copies, node) = self.bytes.read(last_offset, last_size);

        // Buffer was flushed during read
        if buffer_id != self.buffer_id.load(SeqCst) {
            Err(Error::InconsistentRead)
        } else {
            Ok((copies, node))
        }
    }
}
