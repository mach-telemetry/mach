use std::{
    sync::{Arc, atomic::{AtomicU64, AtomicBool, AtomicUsize, Ordering::SeqCst}},
    fs::{OpenOptions, File},
    path::{PathBuf, Path},
    io::{self, prelude::*, SeekFrom},
    convert::TryInto,
};
use lazy_static::*;

lazy_static! {
    pub static ref DATADIR: PathBuf = PathBuf::from("/nvme/fsolleza/output");
}

#[derive(Debug)]
pub enum Error {
    IO(io::Error),
    ReadVersion,
    MultipleWriters,
}

impl From<io::Error> for Error {
    fn from(item: io::Error) -> Self {
        Error::IO(item)
    }
}

pub struct FileList {
    inner: Arc<InnerFileList>,
    has_writer: Arc<AtomicBool>,
}

impl FileList {
    pub fn new() -> Self {
        FileList {
            inner: Arc::new(InnerFileList::new()),
            has_writer: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn writer(&self) -> Result<FileListWriter, Error> {
        if self.has_writer.swap(true, SeqCst) {
            Err(Error::MultipleWriters)
        } else {
            Ok(FileListWriter {
                inner: self.inner.clone(),
                has_writer: self.has_writer.clone(),
            })
        }
    }

    pub fn read(&self) -> Result<FileListIterator, Error> {
        Ok(FileListIterator {
            inner: self.inner.read()?,
            file: None,
        })
    }
}

pub struct FileListWriter {
    inner: Arc<InnerFileList>,
    has_writer: Arc<AtomicBool>,
}

impl FileListWriter {
}

impl Drop for FileListWriter {
    fn drop(&mut self) {
        self.has_writer.swap(false, SeqCst);
    }
}

pub struct FileListIterator {
    inner: InnerFileMetadata,
    file: Option<File>,
}

struct InnerFileList {
    head_metadata: InnerFileMetadata,
    version: AtomicUsize,
    buf: Vec<u8>,
}

impl InnerFileList {
    fn new() -> Self {
        InnerFileList {
            head_metadata: InnerFileMetadata::new(),
            version: AtomicUsize::new(0),
        }
    }

    fn push(&mut self, file: &mut FileMetadata, bytes: &[u8]) -> Result<InnerFileMetadata, Error> {

        buf.clear();
        buf.



        file.file.seek(SeekFrom::Start(metadata.offset))?;
        file.file.write(&self.head_metadata.to_bytes()[..])?;
        file.file.write(bytes.len().to_be_bytes())?;
        file.file.write(bytes)?;
        let metadata = InnerWriteMetadata {
            offset: file.offset,
            file_id: file.local_id,
            ts_id: self.head_metadata.ts_id,
            bytes: bytes.len() + InnerFileMetadata::size(),
        };
        self.head_metadata = metadata;
        self.version.fetch_add(1, SeqCst);
        Ok(metadata)
    }

    fn read(&self) -> Result<InnerFileMetadata, Error> {
        let v = self.version.load(SeqCst);
        let meta = self.head_metadata;
        if v == self.version.load(SeqCst) {
            Ok(meta)
        } else {
            Err(Error::ReadVersion)
        }
    }
}

struct FileMetadata {
    offset: u64,
    local_id: u64,
    shared_id: Arc<AtomicUsize>,
    file: File,
}

#[derive(Copy, Clone)]
struct InnerWriteMetadata {
    offset: u64,
    file_id: u64,
    ts_id: u64,
    bytes: u64,
}

impl InnerWriteMetadata {

    fn new() -> Self {
        InnerWriteMetadata {
            offset: u64::MAX,
            file_id: u64::MAX,
            ts_id: u64::MAX,
            bytes: u64::MAX,
        }
    }

    fn to_bytes(self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&self.offset.to_be_bytes()[..]);
        bytes[8..16].copy_from_slice(&self.ts_id.to_be_bytes()[..]);
        bytes[16..24].copy_from_slice(&self.bytes.to_be_bytes()[..]);
        bytes[24..32].copy_from_slice(&self.file_id.to_be_bytes()[..]);
        bytes
    }

    fn from_bytes(data: &[u8; 32]) -> Self {
        let offset = u64::from_be_bytes(data[..8].try_into().unwrap());
        let ts_id = u64::from_be_bytes(data[8..16].try_into().unwrap());
        let bytes = u64::from_be_bytes(data[16..24].try_into().unwrap());
        let file_id = u64::from_be_bytes(data[24..32].try_into().unwrap());
        Self {
            offset,
            ts_id,
            file_id,
            bytes
        }
    }

    fn size() -> usize {
        std::mem::size_of::<Self>() as usize
    }
}
