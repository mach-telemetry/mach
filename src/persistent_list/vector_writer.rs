use crate::{
    utils::wp_lock::WpLock,
    persistent_list::{
        chunk_trait::{Chunker, ChunkWriter, ChunkReader},
        Error,
    },
};
use std::{
    convert::TryInto,
    sync::{Arc, Mutex},
    mem,
};

pub const HEADERSZ: usize = Header::size();

type Inner = Vec<Box<[u8]>>;

#[derive(Copy, Clone, Debug)]
#[repr(C)]
struct Header {
    offset: usize,
}

impl Header {
    fn new() -> Self {
        Self { 
            offset: usize::MAX,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe {
            mem::transmute::<&Self, &[u8; mem::size_of::<Self>()]>(self)
        }
    }

    fn from_bytes(bytes: [u8; Self::size()]) -> Self {
        unsafe {
            mem::transmute::<[u8; Self::size()], Self>(bytes)
        }
    }

    const fn size() -> usize {
        mem::size_of::<Self>()
    }
}

pub struct Vector {
    header: Arc<WpLock<Header>>,
    inner: Arc<Mutex<Inner>>,
}

impl Chunker<VectorWriter, VectorReader> for Vector {
    fn writer(&self) -> Result<VectorWriter, Error> {
        VectorWriter::new(self.header.clone(), self.inner.clone())
    }

    fn reader(&self) -> Result<VectorReader, Error> {
        // Safe because no header operation deallocates this memory location
        let header = unsafe { *self.header.read() };
        VectorReader::new(header, self.inner.clone())
    }
}

impl Vector {
    pub fn new(inner: Arc<Mutex<Inner>>) -> Self {
        Vector {
            header: Arc::new(WpLock::new(Header::new())),
            inner,
        }
    }
}

pub struct VectorWriter {
    header: Arc<WpLock<Header>>,
    inner: Arc<Mutex<Inner>>,
}

impl VectorWriter {
    fn new(header: Arc<WpLock<Header>>, inner: Arc<Mutex<Inner>>) -> Result<Self, Error> {
        Ok(Self {
            header,
            inner,
        })
    }
}

impl ChunkWriter for VectorWriter {

    fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let mut guard = self.inner.lock().unwrap();
        let len = guard.len();
        guard.push(bytes.into());
        drop(guard);

        *self.header.write() = Header {
            offset: len,
        };
        Ok(())
    }

    fn header(&self) -> &[u8] {
        // Safety: There should be no concurrent writers enforced by &mut
        unsafe {
            (*self.header).as_ref().as_bytes()
        }
    }
}

pub struct VectorReader {
    inner: Arc<Mutex<Inner>>,
    header: Header,
    buffer: Vec<u8>,
}

impl VectorReader {
    fn new(header: Header, inner: Arc<Mutex<Inner>>) -> Result<Self, Error> {
        Ok(Self {
            header,
            inner,
            buffer: Vec::new(),
        })
    }

}

impl ChunkReader for VectorReader {
    fn read_next(&mut self) -> Option<&[u8]> {
        if self.header.offset == usize::MAX {
            None
        } else {
            self.buffer.clear();
            self.buffer.extend_from_slice(&*self.inner.lock().unwrap()[self.header.offset]);
            self.header = Header::from_bytes(self.buffer[..Header::size()].try_into().unwrap());
            Some(&self.buffer[..])
        }
    }
}
