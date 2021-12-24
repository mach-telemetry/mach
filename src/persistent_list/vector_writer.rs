use crate::persistent_list::{ChunkReader, ChunkWriter, Error};
use std::{
    convert::TryInto,
    sync::{Arc, Mutex},
};

pub struct VectorWriter {
    producer: Arc<Mutex<Vec<Box<[u8]>>>>,
}

impl ChunkWriter for VectorWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<(i32, i64), Error> {
        let mut guard = self.producer.lock().unwrap();
        let len = guard.len();
        guard.push(bytes.into());
        Ok((i32::MAX, len as i64))
    }
}

impl VectorWriter {
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            producer: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub fn reader(&self) -> VectorReader {
        VectorReader::new(self.producer.clone()).unwrap()
    }
}

pub struct VectorReader {
    reader: Arc<Mutex<Vec<Box<[u8]>>>>,
    local_buffer: Vec<u8>,
}

impl VectorReader {
    pub fn new(reader: Arc<Mutex<Vec<Box<[u8]>>>>) -> Result<Self, Error> {
        Ok(Self { reader })
    }
}

impl ChunkReader for VectorReader {
    fn read(
        &self,
        _: usize,
        offset: usize,
        chunk_offset: usize,
        bytes: usize,
        buf: &mut [u8],
    ) -> Result<(), Error> {
        let mut guard = self.reader.lock().unwrap();
        if offset != usize::MAX {
            self.local_buffer.clear();
            self.local_buffer.extend_with_slice(&*guard[offset]);
        }
        buf[..bytes].copy_from_slice(&self.local_buffer[chunk_offset..chunk_offset + bytes]);
        //buf[..len].copy_from_slice(&data[..]);
        Ok(())
    }
}
