use crate::{
    compression::{Compression, DecompressBuffer},
    constants::BUFSZ,
    persistent_list::Error,
    segment::FullSegment,
    tags::Tags,
    utils::{
        byte_buffer::ByteBuffer,
        wp_lock::{NoDealloc, WpLock},
    },
};
use std::{
    cell::UnsafeCell,
    convert::{AsMut, AsRef, TryInto},
    mem::size_of,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

pub trait ChunkWriter: Sync + Send {
    fn write(&mut self, bytes: &[u8]) -> Result<u64, Error>;
}

pub trait ChunkReader {
    fn read(&mut self, p_meta: u64) -> Result<&[u8], Error>;
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

pub struct List {
    inner_list: Arc<InnerList>,
    buffer: Arc<WpLock<Buffer>>,
}

impl List {
    pub fn new(buffer: Arc<WpLock<Buffer>>) -> Self {
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

    pub fn read(&self) -> Result<ListReader, Error> {
        let head = self.get_head()?;
        let p_meta = head.p_meta.load(SeqCst);

        // This means the buffer_id has not been updated and this list has not yet written to the
        // buffer. There is no data for this list
        if head.buffer_id == usize::MAX {
            Ok(ListReader::new(Vec::new(), Node::new()))
        }
        // This means that the buffer that this head was pointing has been written to storage.
        else if p_meta != u64::MAX {
            Ok(ListReader::new(Vec::new(), head))
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
                    Ok(ListReader::new(buffer_copy, persistent))
                }

                // And the relevant buffer items were not copied successfully (by implication
                // flushed to storage)
                Err(_) => {
                    // Can release the read guard without checking since the check is done
                    // internally by the buffer
                    match buf.release() {
                        _ => {}
                    }
                    // p_meta must have been updated since the last check
                    assert!(head.p_meta.load(SeqCst) != u64::MAX);
                    Ok(ListReader::new(Vec::new(), head))
                }
            }
        }
    }
}

pub struct ListWriter {
    inner_list: Arc<InnerList>,
    buffer: Arc<WpLock<Buffer>>,
}

impl Drop for ListWriter {
    fn drop(&mut self) {
        assert!(self.inner_list.has_writer.swap(false, SeqCst))
    }
}

impl ListWriter {
    pub fn push_segment<W: ChunkWriter>(
        &mut self,
        segment: &FullSegment,
        tags: &Tags,
        compression: &Compression,
        w: &mut W,
    ) {
        // SAFETY: Holding a read reference to head here doesn't race with a concurrent ListReader
        let head = unsafe { self.inner_list.head.unprotected_read() };
        let p_meta = head.p_meta.load(SeqCst);

        // SAFETY: push_bytes doesn't race with a concurrent ListReader.
        let (new_head, to_flush) = unsafe {
            let buf = self.buffer.unprotected_write();
            let new_head =
                buf.push_segment(segment, tags, compression, p_meta, head.offset, head.size);
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
            //SAFETY: flushing does not race with the reader
            unsafe { self.buffer.unprotected_read().flush(w) };

            // The reset does race with creating a reader
            self.buffer.protected_write().reset();
        }
    }

    pub fn push_bytes<W: ChunkWriter>(&mut self, bytes: &[u8], w: &mut W) {
        // SAFETY: Holding a read reference to head here doesn't race with a concurrent ListReader
        let head = unsafe { self.inner_list.head.unprotected_read() };
        let p_meta = head.p_meta.load(SeqCst);

        // SAFETY: push_bytes doesn't race with a concurrent ListReader.
        let (new_head, to_flush) = unsafe {
            let buf = self.buffer.unprotected_write();
            let new_head = buf.push_bytes(bytes, p_meta, head.offset, head.size);
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
            //SAFETY: flushing does not race with the reader
            unsafe { self.buffer.unprotected_read().flush(w) };

            // The reset does race with creating a reader
            self.buffer.protected_write().reset();
        }
    }
}

pub struct ListReader {
    buffer_copy: Vec<Box<[u8]>>,
    persistent: Node,
    idx: usize,
    local_buffer: Vec<u8>,
    decompress_buf: DecompressBuffer,
}

impl ListReader {
    fn new(buffer_copy: Vec<Box<[u8]>>, persistent: Node) -> Self {
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
                let bytes = &self.buffer_copy[self.idx-1][..];

                // get the tag size and skip the tags
                let mut offset = 0;
                let end = offset + size_of::<u64>();
                let tag_sz =
                    u64::from_be_bytes(bytes[offset..end].try_into().unwrap()) as usize;
                offset = end + tag_sz;

                // get the compressed size and move to compressed data offset
                Compression::decompress(&bytes[offset..], &mut self.decompress_buf)
                    .unwrap();

                Ok(Some(&self.decompress_buf))
            }
        }
    }

    pub fn next_bytes<R: ChunkReader>(&mut self, reader: &mut R) -> Result<Option<&[u8]>, Error> {
        if self.idx == self.buffer_copy.len() {
            let p_meta = self.persistent.p_meta.load(SeqCst);
            if p_meta == u64::MAX {
                return Ok(None);
            } else {
                let buf = InnerBuf(reader.read(p_meta)?);
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

#[derive(Clone)]
struct Node {
    p_meta: Arc<AtomicU64>,
    offset: usize,
    size: usize,
    buffer_id: usize,
}

unsafe impl NoDealloc for Node {}

impl Node {
    fn new() -> Self {
        Self {
            p_meta: Arc::new(AtomicU64::new(u64::MAX)),
            offset: usize::MAX,
            size: usize::MAX,
            buffer_id: usize::MAX,
        }
    }
}

struct InnerBuf<T>(T);

impl<T: AsRef<[u8]>> Deref for InnerBuf<T> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0.as_ref()[..]
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]>> DerefMut for InnerBuf<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.as_mut()[..]
    }
}

impl<T: AsRef<[u8]>> InnerBuf<T> {
    fn read(&self, last_offset: usize, last_size: usize) -> (Vec<Box<[u8]>>, Node) {
        let mut p_meta = u64::MAX;
        let mut offset = last_offset;
        let mut size = last_size;
        let mut copies = Vec::new();

        while p_meta == u64::MAX && offset != usize::MAX {
            let bytes = &self[offset..offset + size];
            p_meta = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
            offset = usize::from_be_bytes(bytes[8..16].try_into().unwrap());
            size = usize::from_be_bytes(bytes[16..24].try_into().unwrap());
            copies.push(bytes[24..].into());
        }

        let node = Node {
            p_meta: Arc::new(AtomicU64::new(p_meta)),
            offset,
            size,
            buffer_id: usize::MAX,
        };

        (copies, node)
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]>> InnerBuf<T> {
    fn push_segment(
        &mut self,
        segment: &FullSegment,
        tags: &Tags,
        compression: &Compression,
        p_meta: u64,
        last_offset: usize,
        last_size: usize,
        offset: usize,
    ) -> usize {
        let start = offset;
        let mut offset = offset;
        let end = offset + size_of::<u64>();
        self[offset..end].copy_from_slice(&p_meta.to_be_bytes());
        offset = end;

        let end = offset + size_of::<usize>();
        self[offset..end].copy_from_slice(&last_offset.to_be_bytes());
        offset = end;

        let end = offset + size_of::<usize>();
        self[offset..end].copy_from_slice(&last_size.to_be_bytes());
        offset = end;

        // Write tags
        offset += {
            let sz_offset = offset;
            let tags_offset = sz_offset + size_of::<u64>();
            let tags_sz = {
                let mut byte_buffer = ByteBuffer::new(&mut self[tags_offset..]);
                tags.serialize_into(&mut byte_buffer);
                byte_buffer.len()
            };
            self[sz_offset..tags_offset].copy_from_slice(&tags_sz.to_be_bytes()[..]);
            tags_sz + size_of::<u64>()
        };

        // Compress the data into the buffer
        offset += {
            let mut byte_buffer = ByteBuffer::new(&mut self[offset..]);
            compression.compress(segment, &mut byte_buffer);
            byte_buffer.len()
        };

        offset - start
    }

    fn push_bytes(
        &mut self,
        bytes: &[u8],
        p_meta: u64,
        last_offset: usize,
        last_size: usize,
        offset: usize,
    ) -> usize {
        let start = offset;
        let mut offset = start;

        let end = offset + size_of::<u64>();
        self[offset..end].copy_from_slice(&p_meta.to_be_bytes());
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

        offset - start
    }
}

pub struct Buffer {
    p_meta: Arc<AtomicU64>,
    bytes: InnerBuf<Box<[u8]>>,
    flush_sz: usize,
    len: usize,
    buffer_id: AtomicUsize,
}

unsafe impl NoDealloc for Buffer {}

impl Buffer {
    fn is_full(&self) -> bool {
        self.flush_sz <= self.len
    }

    pub fn new(flush_sz: usize) -> Self {
        Self {
            bytes: InnerBuf(vec![0u8; flush_sz * 2].into_boxed_slice()),
            p_meta: Arc::new(AtomicU64::new(u64::MAX)),
            flush_sz,
            len: 0,
            buffer_id: AtomicUsize::new(0),
        }
    }

    fn push_segment(
        &mut self,
        segment: &FullSegment,
        tags: &Tags,
        compression: &Compression,
        p_meta: u64,
        last_offset: usize,
        last_size: usize,
    ) -> Node {
        let size = self.bytes.push_segment(
            segment,
            tags,
            compression,
            p_meta,
            last_offset,
            last_size,
            self.len,
        );
        let offset = self.len;
        self.len += size;

        Node {
            p_meta: self.p_meta.clone(),
            offset,
            size,
            buffer_id: self.buffer_id.load(SeqCst),
        }
    }

    fn push_bytes(
        &mut self,
        bytes: &[u8],
        p_meta: u64,
        last_offset: usize,
        last_size: usize,
    ) -> Node {
        let size = self
            .bytes
            .push_bytes(bytes, p_meta, last_offset, last_size, self.len);
        let offset = self.len;
        self.len += size;

        Node {
            p_meta: self.p_meta.clone(),
            offset,
            size,
            buffer_id: self.buffer_id.load(SeqCst),
        }
    }

    fn flush<W: ChunkWriter>(&self, flusher: &mut W) {
        let p_meta = flusher.write(&self.bytes[..self.len]).unwrap();
        self.p_meta.store(p_meta, SeqCst);
    }

    fn reset(&mut self) {
        let buffer_id = self.buffer_id.fetch_add(1, SeqCst) + 1;
        self.len = 0;
        self.p_meta = Arc::new(AtomicU64::new(u64::MAX));
    }

    fn read(
        &self,
        buffer_id: usize,
        last_offset: usize,
        last_size: usize,
    ) -> Result<(Vec<Box<[u8]>>, Node), Error> {
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
