use crate::persistent_list::{inner::*, Error};
use std::sync::{Arc, Mutex};

pub struct VectorReader {
    inner: Arc<Mutex<Vec<Box<[u8]>>>>,
    local_copy: Vec<u8>,
    current_head: Option<PersistentHead>,
}

impl VectorReader {
    pub fn new(inner: Arc<Mutex<Vec<Box<[u8]>>>>) -> Self {
        Self {
            inner,
            local_copy: Vec::new(),
            current_head: None,
        }
    }
}

impl ChunkReader for VectorReader {
    fn read(&mut self, persistent: PersistentHead, local: BufferHead) -> Result<&[u8], Error> {
        if self.current_head.is_none() || *self.current_head.as_ref().unwrap() != persistent {
            self.current_head = Some(persistent);
            self.local_copy.clear();
            let mut guard = self.inner.lock().unwrap();
            self.local_copy
                .extend_from_slice(&*guard[persistent.offset]);
        }
        Ok(&self.local_copy[local.offset..local.offset + local.size])
    }
}

#[derive(Clone)]
pub struct VectorWriter {
    inner: Arc<Mutex<Vec<Box<[u8]>>>>,
}

impl VectorWriter {
    pub fn new(inner: Arc<Mutex<Vec<Box<[u8]>>>>) -> Self {
        VectorWriter { inner }
    }
}

impl ChunkWriter for VectorWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error> {
        let mut guard = self.inner.lock().unwrap();
        let sz = bytes.len();
        let offset = guard.len();
        guard.push(bytes.into());
        let head = PersistentHead {
            partition: usize::MAX,
            offset,
            sz,
        };
        Ok(head)
    }
}
