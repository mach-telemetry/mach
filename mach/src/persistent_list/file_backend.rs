use crate::id::SeriesId;
use crate::persistent_list::{inner, Error, PersistentListBackend};
use crate::utils::random_id;
use crate::constants::*;
//use crate::metadata::METADATA;
use std::{
    convert::AsRef,
    convert::TryInto,
    fs::{create_dir_all, File, OpenOptions},
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

impl inner::ChunkReader for FileReader {
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

impl inner::ChunkWriter for FileWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        let offset = self.current_offset;
        let sz = bytes.len();
        self.current_offset += self.file.write(&sz.to_be_bytes())?;
        self.current_offset += self.file.write(bytes)?;
        self.file.sync_all()?;
        Ok(offset as u64)
    }
}

pub struct FileBackend {
    dir: PathBuf,
    id: String,
}

impl FileBackend {
    pub fn new(dir: PathBuf, id: String) -> Result<Self, Error> {
        create_dir_all(&dir)?;
        Ok(FileBackend { dir, id })
    }
}

impl PersistentListBackend for FileBackend {
    type Writer = FileWriter;
    type Reader = FileReader;

    fn id(&self) -> &str {
        self.id.as_str()
    }

    fn default_backend() -> Result<Self, Error> {
        let dir = PathBuf::from(FILEBACKEND_DIR);
        let id = random_id();
        Self::new(dir, id)
    }

    fn writer(&self) -> Result<FileWriter, Error> {
        let file = self.dir.join(self.id.as_str());
        FileWriter::new(&file)
    }

    fn reader(&self) -> Result<FileReader, Error> {
        let file = self.dir.join(self.id.as_str());
        FileReader::new(&file)
    }
}
