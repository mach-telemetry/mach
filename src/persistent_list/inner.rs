use crate::{
    compression::{Compression, DecompressBuffer},
    persistent_list::{Error, FLUSH_THRESHOLD},
    segment::FullSegment,
    tags::Tags,
    utils::{byte_buffer::ByteBuffer, wp_lock::WpLock},
};
use std::{
    cell::UnsafeCell,
    convert::TryInto,
    marker::PhantomData,
    mem,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
};

pub trait ChunkWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error>;
}

pub trait ChunkReader {
    fn read(&mut self, persistent: PersistentHead, local: BufferHead) -> Result<&[u8], Error>;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct PersistentHead {
    pub partition: usize,
    pub offset: usize,
}

impl PersistentHead {
    fn new() -> Self {
        PersistentHead {
            partition: usize::MAX,
            offset: usize::MAX,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { mem::transmute::<&Self, &[u8; mem::size_of::<Self>()]>(self) }
    }

    fn from_bytes(bytes: [u8; Self::size()]) -> Self {
        unsafe { mem::transmute::<[u8; Self::size()], Self>(bytes) }
    }

    const fn size() -> usize {
        mem::size_of::<Self>()
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct BufferHead {
    pub offset: usize,
    pub size: usize,
}

impl BufferHead {
    fn new() -> Self {
        BufferHead {
            offset: usize::MAX,
            size: usize::MAX,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { mem::transmute::<&Self, &[u8; mem::size_of::<Self>()]>(self) }
    }

    fn from_bytes(bytes: [u8; Self::size()]) -> Self {
        unsafe { mem::transmute::<[u8; Self::size()], Self>(bytes) }
    }

    const fn size() -> usize {
        mem::size_of::<Self>()
    }
}

#[derive(Clone)]
pub struct Head {
    persistent: Arc<WpLock<PersistentHead>>,
    buffer: BufferHead,
}

impl Head {
    fn new() -> Self {
        Self {
            persistent: Arc::new(WpLock::new(PersistentHead::new())),
            buffer: BufferHead::new(),
        }
    }

    fn bytes(&self) -> [u8; Self::size()] {
        let persistent = *self.persistent.write();
        let mut bytes = [0u8; Self::size()];
        bytes[..PersistentHead::size()].copy_from_slice(persistent.as_bytes());
        bytes[BufferHead::size()..].copy_from_slice(self.buffer.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8; Self::size()]) -> Self {
        let ps = PersistentHead::size();
        let bs = BufferHead::size();
        let persistent = PersistentHead::from_bytes(bytes[..ps].try_into().unwrap());
        let buffer = BufferHead::from_bytes(bytes[ps..ps + bs].try_into().unwrap());
        Self {
            persistent: Arc::new(WpLock::new(persistent)),
            buffer,
        }
    }

    const fn size() -> usize {
        PersistentHead::size() + BufferHead::size()
    }
}

struct InnerBuffer {
    persistent_head: Arc<WpLock<PersistentHead>>,
    len: AtomicUsize,
    buffer: Box<[u8]>,
}

impl InnerBuffer {
    fn new() -> Self {
        Self {
            persistent_head: Arc::new(WpLock::new(PersistentHead::new())),
            len: AtomicUsize::new(0),
            buffer: vec![0u8; FLUSH_THRESHOLD * 2].into_boxed_slice(),
        }
    }

    fn push_bytes(&mut self, bytes: &[u8], last_head: &Head) -> Head {
        let len = self.len.load(SeqCst);
        let mut offset = len;

        let end = offset + Head::size();
        self.buffer[offset..end].copy_from_slice(&last_head.bytes());
        offset = end;

        let end = offset + bytes.len();
        self.buffer[offset..end].copy_from_slice(bytes);
        offset = end;

        self.len.store(offset, SeqCst);

        Head {
            persistent: self.persistent_head.clone(),
            buffer: BufferHead {
                offset: len,
                size: offset - len,
            },
        }
    }

    fn push_segment(
        &mut self,
        segment: &FullSegment,
        tags: &Tags,
        compression: Compression,
        last_head: &Head,
    ) -> Head {
        let len = self.len.load(SeqCst);
        let mut offset = len;

        // Write head
        let end = offset + Head::size();
        self.buffer[offset..end].copy_from_slice(&last_head.bytes());
        offset = end;

        // Write tags
        offset += {
            let sz_offset = offset;
            let tags_offset = sz_offset + mem::size_of::<u64>();
            let tags_sz = {
                let mut byte_buffer = ByteBuffer::new(&mut self.buffer[tags_offset..]);
                tags.serialize_into(&mut byte_buffer);
                byte_buffer.len()
            };
            self.buffer[sz_offset..tags_offset].copy_from_slice(&tags_sz.to_be_bytes()[..]);
            tags_sz + mem::size_of::<u64>()
        };

        // Compress the data into the buffer
        offset += {
            let mut byte_buffer = ByteBuffer::new(&mut self.buffer[offset..]);
            compression.compress(segment, &mut byte_buffer);
            byte_buffer.len()
        };

        self.len.store(offset, SeqCst);

        Head {
            persistent: self.persistent_head.clone(),
            buffer: BufferHead {
                offset: len,
                size: offset - len,
            },
        }
    }

    fn flush<W: ChunkWriter>(&mut self, flusher: &mut W) {
        let head = flusher
            .write(&self.buffer[..self.len.load(SeqCst)])
            .unwrap();
        let mut guard = self.persistent_head.write();
        *guard = head;
        drop(guard);
        self.persistent_head = Arc::new(WpLock::new(PersistentHead::new()));
    }

    fn reset(&self) {
        self.len.store(0, SeqCst);
    }

    fn read(&self, offset: usize, bytes: usize) -> Box<[u8]> {
        self.buffer[offset..offset + bytes].into()
    }
}

#[derive(Clone)]
pub struct Buffer {
    inner: Arc<WpLock<InnerBuffer>>,
}

impl Buffer {
    pub fn new() -> Self {
        Buffer {
            inner: Arc::new(WpLock::new(InnerBuffer::new())),
        }
    }

    fn read(&self, offset: usize, bytes: usize) -> Result<Box<[u8]>, Error> {
        let guard = unsafe { self.inner.read() };
        let res = guard.read(offset, bytes);
        match guard.release() {
            Ok(()) => Ok(res),
            Err(_) => Err(Error::InconsistentRead),
        }
    }

    fn push_bytes<W: ChunkWriter>(&mut self, bytes: &[u8], last_head: &Head, w: &mut W) -> Head {
        // Safety: The push_bytes function does not race with readers
        let head = unsafe { self.inner.get_mut_ref().push_bytes(bytes, last_head) };
        if head.buffer.offset + head.buffer.size > FLUSH_THRESHOLD {
            //println!("FLUSHING");
            // Need to guard here because reset will conflict with concurrent readers
            let mut guard = self.inner.write();
            guard.flush(w);
            guard.reset();
        }
        head
    }

    fn push_segment<W: ChunkWriter>(
        &mut self,
        segment: &FullSegment,
        tags: &Tags,
        compression: Compression,
        last_head: &Head,
        w: &mut W
    ) -> Head {
        // Safety: The push_bytes function does not race with readers
        let head = unsafe {
            self.inner
                .get_mut_ref()
                .push_segment(segment, tags, compression, last_head)
        };
        if head.buffer.offset + head.buffer.size > FLUSH_THRESHOLD {
            // Need to guard here because reset will conflict with concurrent readers
            let mut guard = self.inner.write();
            guard.flush(w);
            guard.reset();
        }
        head
    }
}

struct InnerList {
    head: Head,
    buffer: Buffer,
}

impl InnerList {
    fn new(buffer: Buffer) -> Self {
        let head = Head::new();
        Self { head, buffer }
    }

    fn push_bytes<W: ChunkWriter>(&mut self, bytes: &[u8], w: &mut W) {
        self.head = self.buffer.push_bytes(bytes, &self.head, w);
    }

    fn push_segment<W: ChunkWriter>(
        &mut self,
        segment: &FullSegment,
        tags: &Tags,
        compression: Compression,
        last_head: &Head,
        w: &mut W
    ) {
        self.head = self.buffer.push_segment(segment, tags, compression, last_head, w);
    }
}

pub struct List {
    inner: Arc<WpLock<InnerList>>,
}

impl List {
    pub fn new(buffer: Buffer) -> Self {
        List {
            inner: Arc::new(WpLock::new(InnerList::new(buffer))),
        }
    }

    pub fn push_bytes<W: ChunkWriter>(&self, bytes: &[u8], w: &mut W) {
        self.inner.write().push_bytes(bytes, w);
    }

    pub fn push_segment<W: ChunkWriter>(&self, bytes: &[u8], w: &mut W) {
        self.inner.write().push_bytes(bytes, w);
    }

    pub fn reader(&self) -> Result<ListReader, Error> {
        // Safety: safe because none of the write operations dealloc memory. Protect the list from
        // concurrent writers
        let guard = unsafe { self.inner.read() };
        let head = guard.head.clone();
        let buff = guard.buffer.clone();
        match guard.release() {
            Ok(()) => Ok(ListReader {
                head,
                buff,
                local_buf: Vec::new(),
                last_persistent: None,
                decompress_buf: DecompressBuffer::new(),
            }),
            Err(_) => Err(Error::InconsistentRead),
        }
    }
}

pub struct ListReader {
    head: Head,
    buff: Buffer,
    last_persistent: Option<PersistentHead>,
    local_buf: Vec<u8>,
    decompress_buf: DecompressBuffer,
}

impl ListReader {
    pub fn next_segment<R: ChunkReader>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<&DecompressBuffer>, Error> {
        let bytes = self.next_bytes(reader)?;
        match bytes {
            None => Ok(None),
            Some(_) => {
                let mut offset = Head::size();

                // get the tag size and skip the tags
                let end = offset + mem::size_of::<u64>();
                let tag_sz =
                    u64::from_be_bytes(self.local_buf[offset..end].try_into().unwrap()) as usize;
                offset = end + tag_sz;

                // get the compressed size and move to compressed data offset
                Compression::decompress(&self.local_buf[offset..], &mut self.decompress_buf)
                    .unwrap();

                Ok(Some(&self.decompress_buf))
            }
        }
    }

    pub fn next_bytes<R: ChunkReader>(&mut self, reader: &mut R) -> Result<Option<&[u8]>, Error> {
        if self.head.buffer.offset == usize::MAX {
            return Ok(None);
        }
        self.local_buf.clear();

        // Try to copy the persistent head
        let persistent = unsafe {
            let guard = self.head.persistent.read();
            let persistent = *guard;
            match guard.release() {
                Ok(()) => Ok(persistent),
                Err(_) => Err(Error::InconsistentRead),
            }
        }?;

        // last persistent is unset, try to copy from the local buffer
        if self.last_persistent.is_none() && persistent.offset == usize::MAX {
            let offset = self.head.buffer.offset;
            let bytes = self.head.buffer.size;
            self.local_buf
                .extend_from_slice(&*self.buff.read(offset, bytes)?);
        }
        // last persistent is set and the persistent data is unset, then must be in the same
        // partition as the last persistent
        else if self.last_persistent.is_some() && persistent.offset == usize::MAX {
            let persistent = *self.last_persistent.as_ref().unwrap();
            self.local_buf
                .extend_from_slice(reader.read(persistent, self.head.buffer)?)
        }

        // persistent offset has a value, means need to get the next persistent
        else if persistent.offset != usize::MAX {
            self.last_persistent = Some(persistent);
            self.local_buf
                .extend_from_slice(reader.read(persistent, self.head.buffer)?)
        }

        // get the next head
        self.head = Head::from_bytes(self.local_buf[..Head::size()].try_into().unwrap());

        // return item
        Ok(Some(&self.local_buf[Head::size()..]))
    }
}
