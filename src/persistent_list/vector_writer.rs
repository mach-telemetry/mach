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
    pub fn new(producer: Arc<Mutex<Vec<Box<[u8]>>>>) -> Result<Self, Error> {
        Ok(Self { producer })
    }
}

pub struct VectorReader {
    reader: Arc<Mutex<Vec<Box<[u8]>>>>,
}

impl VectorReader {
    pub fn new(reader: Arc<Mutex<Vec<Box<[u8]>>>>) -> Result<Self, Error> {
        Ok(Self { reader })
    }
}

impl ChunkReader for VectorReader {
    fn read(&self, partition: i32, offset: i64, buf: &mut [u8]) -> Result<(), Error> {
        let mut guard = self.reader.lock().unwrap();
        let offset: usize = offset.try_into().unwrap();
        let data: &Box<[u8]> = &guard[offset];
        let len = data.len();
        buf[..len].copy_from_slice(&data[..]);
        Ok(())
    }
}
