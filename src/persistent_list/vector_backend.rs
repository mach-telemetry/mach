use crate::{
    id::SeriesId,
    //metadata::METADATA,
    persistent_list::{inner2, Error, PersistentListBackend},
    tags::Tags,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub struct VectorReader {
    inner: Arc<Mutex<Vec<Box<[u8]>>>>,
    local_copy: Vec<u8>,
    offset: usize,
}

impl VectorReader {
    pub fn new(inner: Arc<Mutex<Vec<Box<[u8]>>>>) -> Self {
        Self {
            inner,
            local_copy: Vec::new(),
            offset: usize::MAX,
        }
    }
}

impl inner2::ChunkReader for VectorReader {
    fn read(&mut self, offset: u64) -> Result<&[u8], Error> {
        let offset = offset as usize;
        if self.offset == usize::MAX || self.offset != offset {
            self.offset = offset;
            self.local_copy.clear();
            let guard = self.inner.lock().unwrap();
            self.local_copy.extend_from_slice(&*guard[offset]);
        }
        Ok(self.local_copy.as_slice())
    }
}

//impl ChunkReader for VectorReader {
//    fn read(&mut self, persistent: PersistentHead, local: BufferHead) -> Result<&[u8], Error> {
//        if self.offset == usize::MAX || self.offset != persistent.offset {
//            self.offset = persistent.offset;
//            self.local_copy.clear();
//            let guard = self.inner.lock().unwrap();
//            self.local_copy
//                .extend_from_slice(&*guard[persistent.offset]);
//        }
//        Ok(&self.local_copy[local.offset..local.offset + local.size])
//    }
//}

#[derive(Clone)]
pub struct VectorWriter {
    inner: Arc<Mutex<Vec<Box<[u8]>>>>,
}

impl VectorWriter {
    pub fn new(inner: Arc<Mutex<Vec<Box<[u8]>>>>) -> Self {
        VectorWriter { inner }
    }
}

impl inner2::ChunkWriter for VectorWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        let mut guard = self.inner.lock().unwrap();
        //let sz = bytes.len();
        let offset = guard.len();
        guard.push(bytes.into());
        drop(guard);
        Ok(offset as u64)
    }
}

//impl ChunkWriter for VectorWriter {
//    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error> {
//        //println!("Since last flush: {:?}", self.last_flush.elapsed());
//        //let now = std::time::Instant::now();
//        let mut guard = self.inner.lock().unwrap();
//        let sz = bytes.len();
//        let offset = guard.len();
//        guard.push(bytes.into());
//        //let spin = Instant::now();
//        //loop {
//        //    if spin.elapsed() > Duration::from_millis(8) {
//        //        break;
//        //    }
//        //}
//        //println!("Duration: {:?}", now.elapsed());
//        //self.last_flush = Instant::now();
//        let head = PersistentHead {
//            partition: usize::MAX,
//            offset,
//            sz,
//        };
//        Ok(head)
//    }
//}

pub struct VectorBackend {
    data: Arc<Mutex<Vec<Box<[u8]>>>>,
}

impl VectorBackend {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl PersistentListBackend for VectorBackend {
    type Writer = VectorWriter;
    type Reader = VectorReader;

    fn id(&self) -> &str {
        &"VectorBackend provides no ID"
    }

    fn default_backend() -> Result<Self, Error> {
        Ok(Self::new())
    }
    fn writer(&self) -> Result<Self::Writer, Error> {
        Ok(VectorWriter::new(self.data.clone()))
    }
    fn reader(&self) -> Result<Self::Reader, Error> {
        Ok(VectorReader::new(self.data.clone()))
    }
}

//impl BackendOld for VectorBackend {
//    type Writer = VectorWriter;
//    type Reader = VectorReader;
//    fn make_backend(&mut self) -> Result<(VectorWriter, VectorReader), Error> {
//        Ok(self.make_read_write())
//    }
//}
