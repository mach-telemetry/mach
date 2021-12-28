use crate::persistent_list::{inner::*, Error};
use std::{
    convert::AsRef,
    fs::{File, OpenOptions},
    io::{prelude::*, SeekFrom},
    path::Path,
};

pub struct FileReader {
    file: File,
    local_copy: Vec<u8>,
    current_head: Option<PersistentHead>,
}

impl FileReader {
    pub fn new<F: AsRef<Path>>(filename: &F) -> Result<Self, Error> {
        let file = OpenOptions::new().read(true).open(filename)?;
        Ok(Self {
            file,
            local_copy: Vec::new(),
            current_head: None,
        })
    }
}

impl ChunkReader for FileReader {
    fn read(&mut self, persistent: PersistentHead, local: BufferHead) -> Result<&[u8], Error> {
        if self.current_head.is_none() || *self.current_head.as_ref().unwrap() != persistent {
            self.current_head = Some(persistent);
            self.local_copy.resize(persistent.sz, 0);
            self.file.seek(SeekFrom::Start(persistent.offset as u64))?;
            self.file.read(self.local_copy.as_mut_slice())?;
        }
        Ok(&self.local_copy[local.offset..local.offset + local.size])
    }
}

//#[derive(Clone)]
pub struct FileWriter {
    file: File,
    current_offset: usize,
}

impl FileWriter {
    pub fn new<F: AsRef<Path>>(filename: &F) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(filename)
            .unwrap();
        Ok(Self {
            file,
            current_offset: 0,
        })
    }
}

impl ChunkWriter for FileWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error> {
        let offset = self.current_offset;
        let sz = bytes.len();
        self.current_offset += self.file.write(bytes)?;
        self.file.sync_all()?;
        let head = PersistentHead {
            partition: usize::MAX,
            offset,
            sz,
        };
        Ok(head)
    }
}
