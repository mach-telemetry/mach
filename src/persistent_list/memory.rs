use crate::{
    persistent_list::{
        chunk_trait::*,
        FLUSH_THRESHOLD,
        Error,
    },
    utils::wp_lock::{WpLock, ReadGuard},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, AtomicBool, Ordering::SeqCst},
    },
    mem,
    cell::UnsafeCell,
};

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct Header {
    partition: usize,
    offset: usize,
    offset: usize,
    size: usize,
    epoch: usize,
}

impl Header {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            mem::transmute::<&Self, &[u8; mem::size_of::<Self>()]>(self)
        }
    }

    const fn size() -> usize {
        mem::size_of::<Self>()
    }
}

struct InnerBuffer {
    has_writer: AtomicBool,
    len: AtomicUsize,
    epoch: AtomicUsize,
    buffer: UnsafeCell<Box<[u8]>>,
    chunker_header_sz: usize,
}

impl InnerBuffer {
    fn new(chunker_header_sz: usize) -> Self {
        Self {
            has_writer: AtomicBool::new(false),
            len: AtomicUsize::new(chunker_header_sz),
            epoch: AtomicUsize::new(0),
            buffer: UnsafeCell::new(vec![0u8; FLUSH_THRESHOLD * 2].into_boxed_slice()),
            chunker_header_sz,
        }
    }

    // Safety: this is unsafe because there should only ever be one writer calling this
    unsafe fn push_bytes(&self, bytes: &[u8], last_head: Header) -> Header {

        let len = self.len.load(SeqCst);

        let mut offset = len;
        let buf = self.buffer.get().as_mut().unwrap();

        let end = offset + Header::size();
        buf[offset..end].copy_from_slice(last_head.as_bytes());
        offset = end;

        let end = offset + bytes.len();
        buf[offset..end].copy_from_slice(bytes);
        offset = end;

        self.len.store(offset, SeqCst);

        Header {
            offset: len,
            size: offset - len,
            epoch: self.epoch.load(SeqCst),
        }
    }

    unsafe fn set_header(&self, chunker_header: &[u8]) {
        let buf = unsafe { self.buffer.get().as_mut().unwrap() };
        buf[..self.chunker_header_sz].copy_from_slice(chunker_header)
    }

    fn read(&self, header: Header) -> Result<&[u8], Error> {
        if header.epoch == self.epoch.load(SeqCst) {
            let buf = unsafe { self.buffer.get().as_mut().unwrap() };
            Ok(&buf[header.offset..header.offset + header.size])
        } else {
            Err(Error::BufferEpochReadError)
        }
    }

    fn bytes(&self) -> &[u8] {
        let buf = unsafe { self.buffer.get().as_mut().unwrap() };
        &buf[..]
    }

    fn reset(&mut self) {
        self.epoch.fetch_add(1, SeqCst);
        self.len.store(self.chunker_header_sz, SeqCst);
    }

    fn set_writer(&self) {
        assert!(!self.has_writer.swap(true, SeqCst));
    }

    fn unset_writer(&self) {
        self.has_writer.swap(false, SeqCst);
    }
}

#[derive(Clone)]
pub struct Buffer {
    inner: Arc<WpLock<InnerBuffer>>,
}

impl Buffer {
    pub fn new(header_sz: usize) -> Self {
        Self {
            inner: Arc::new(WpLock::new(InnerBuffer::new(header_sz))),
        }
    }

    pub fn writer(&self) -> BufferWriter {
        let writer = self.inner.clone();
        unsafe { (&*writer).as_ref().set_writer() };
        BufferWriter {
            inner: writer,
        }
    }

    //pub fn read(&self, header: Header) -> BufferReader {
    //    // Safety: write operations in buffer don't deallocate memory
    //    let inner = unsafe { self.inner.read() };
    //    let mut data = Vec::new();
    //    let mut header = header;
    //    BufferReader {
    //        inner,
    //        header,
    //    }
    //}
}

pub struct BufferReader {
    inner: Vec<Box<[u8]>>,
}

pub struct BufferWriter {
    inner: Arc<WpLock<InnerBuffer>>,
}

impl BufferWriter {
    pub fn push_bytes(&mut self, bytes: &[u8], last_head: Header) -> Header {
        // Safety: There is only one writer so this should be fine
        unsafe { (&*self.inner).as_ref().push_bytes(bytes, last_head) }
    }

    pub fn flush_bytes<W: ChunkWriter>(&mut self, flusher: &mut W) -> Result<(), Error> {
        // Safety: There is only one writer.
        let buf = unsafe {
            let buf = (*self.inner).as_ref();
            buf.set_header(flusher.header());
            buf
        };
        flusher.write(buf.bytes())
    }

    pub fn reset(&mut self) {
        // Lock to prevent the Buffer object from concurrent reads
        self.inner.write().reset()
    }
}
