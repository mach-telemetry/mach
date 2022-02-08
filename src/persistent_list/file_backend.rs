use crate::persistent_list::{inner::*, inner2, Error, PersistentListBackend};
use std::{
    convert::AsRef,
    convert::TryInto,
    fs::{File, OpenOptions},
    io::{prelude::*, SeekFrom},
    path::{Path, PathBuf},
};

pub struct FileReader {
    file: File,
    local_copy: Vec<u8>,
    offset: usize,
}

impl FileReader {
    pub fn new<F: AsRef<Path>>(filename: &F) -> Result<Self, Error> {
        let file = OpenOptions::new().read(true).open(filename)?;
        Ok(Self {
            file,
            local_copy: Vec::new(),
            offset: usize::MAX,
        })
    }
}

impl inner2::ChunkReader for FileReader {
    fn read(&mut self, offset: u64) -> Result<&[u8], Error> {
        let offset = offset as usize;
        if self.offset == usize::MAX || self.offset != offset {
            self.file.seek(SeekFrom::Start(offset as u64))?;
            let mut sz_bytes = [0u8; 8];
            self.file.read_exact(&mut sz_bytes[..])?;
            self.local_copy.resize(usize::from_be_bytes(sz_bytes), 0);
            self.file.read_exact(self.local_copy.as_mut_slice())?;
            self.offset = offset;
        }
        Ok(&self.local_copy[..])
    }
}

impl ChunkReader for FileReader {
    fn read(&mut self, persistent: PersistentHead, local: BufferHead) -> Result<&[u8], Error> {
        if self.offset == usize::MAX || self.offset != persistent.offset {
            self.offset = persistent.offset;
            self.local_copy.resize(persistent.sz, 0);
            self.file.seek(SeekFrom::Start(persistent.offset as u64))?;
            self.file.read_exact(self.local_copy.as_mut_slice())?;
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
    pub fn new<F: AsRef<Path>>(filename: F) -> Result<Self, Error> {
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

impl inner2::ChunkWriter for FileWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        let offset = self.current_offset;
        let sz = bytes.len();
        self.current_offset += self.file.write(&sz.to_be_bytes())?;
        self.current_offset += self.file.write(bytes)?;
        self.file.sync_all()?;
        Ok(offset as u64)
    }
}

impl ChunkWriter for FileWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error> {
        let offset = self.current_offset;
        let sz = bytes.len();
        //let now = std::time::Instant::now();
        self.current_offset += self.file.write(bytes)?;
        //self.file.sync_all()?;
        //println!("Duration: {:?}", now.elapsed());
        let head = PersistentHead {
            partition: usize::MAX,
            offset,
            sz,
        };
        Ok(head)
    }
}

pub struct FileBackend {
    dir: PathBuf,
    id: u32,
}

impl FileBackend {
    pub fn new(dir: PathBuf, id: u32) -> Self {
        FileBackend { dir, id }
    }
}

impl PersistentListBackend for FileBackend {
    type Writer = FileWriter;
    type Reader = FileReader;

    fn writer(&self) -> Result<FileWriter, Error> {
        let file = self.dir.join(format!("data_{}", self.id));
        FileWriter::new(&file)
    }

    fn reader(&self) -> Result<FileReader, Error> {
        let file = self.dir.join(format!("data_{}", self.id));
        FileReader::new(&file)
    }
}
