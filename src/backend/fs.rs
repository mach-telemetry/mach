use std::{
    sync::{Arc, atomic::{AtomicU64, AtomicBool, AtomicUsize, Ordering::SeqCst}},
    fs::{OpenOptions, File},
    path::{PathBuf, Path},
    io::{self, prelude::*, SeekFrom},
    convert::TryInto,
};
use crate::tags::Tags;
use lazy_static::*;

const MAGICSTR: &str = "filebackend";

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

pub struct FileWriter {
    offset: u64,
    local_id: u64,
    shared_id: Arc<AtomicU64>,
    file: File,
}

impl FileWriter {
    pub fn new(shared_id: Arc<AtomicU64>) -> Result<Self, Error> {
        let local_id = shared_id.load(SeqCst);
        let file_name = format!("data-{}", local_id);
        let file = OpenOptions::new().write(true).append(true).open(DATADIR.join(file_name))?;
        let offset = 0;
        Ok(Self {
            offset,
            local_id,
            shared_id,
            file,
        })
    }

    pub fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.file.write_all(bytes)?;
        self.offset += bytes.len() as u64;
        Ok(())
    }
}

pub struct FileListIterator {
    next_offset: u64,
    next_file_id: u64,
    next_bytes: u64,
    next_mint: u64,
    next_maxt: u64,
    file: Option<File>,
}

pub struct FileList {
    inner: Arc<InnerFileList>,
    has_writer: Arc<AtomicBool>,
}

impl FileList {
    pub fn new(tags: &Tags) -> Self {
        Self {
            inner: Arc::new(InnerFileList::new(tags)),
            has_writer: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn reader(&self) -> Result<FileListIterator, Error> {
        self.inner.read()
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
}

pub struct FileListWriter {
    inner: Arc<InnerFileList>,
    has_writer: Arc<AtomicBool>,
}

impl FileListWriter {
    pub fn push(&mut self, file: &mut FileWriter, mint: u64, maxt: u64, bytes: &[u8]) -> Result<(), Error> {

        // Safety: Safe because there's only one writer and concurrent readers check versions
        // during each read.
        unsafe {
            Arc::get_mut_unchecked(&mut self.inner).push(file, mint, maxt, bytes)
        }
    }
}

impl Drop for FileListWriter {
    fn drop(&mut self) {
        self.has_writer.swap(false, SeqCst);
    }
}

struct InnerFileList {
    last_offset: u64,
    last_file_id: u64,
    last_bytes: u64,
    last_mint: u64,
    last_maxt: u64,
    tags: Vec<u8>,
    version: Arc<AtomicU64>,
    init_buf_len: usize,
    buf: Vec<u8>,
}

impl InnerFileList {
    fn new(tags: &Tags) -> Self {
        let mut buf = Vec::new();
        let tags = tags.serialize();

        buf.extend_from_slice(&MAGICSTR.as_bytes());
        buf.extend_from_slice(&tags.len().to_be_bytes()[..]);
        buf.extend_from_slice(&tags[..]);

        Self {
            last_offset: u64::MAX,
            last_file_id: u64::MAX,
            last_bytes: u64::MAX,
            last_mint: u64::MAX,
            last_maxt: u64::MAX,
            tags,
            version: Arc::new(AtomicU64::new(0)),
            init_buf_len: buf.len(),
            buf,
        }
    }

    fn push(&mut self, file: &mut FileWriter, mint: u64, maxt: u64, bytes: &[u8]) -> Result<(), Error> {
        self.buf.truncate(self.init_buf_len);

        // Write data about the current set of bytes
        self.buf.extend_from_slice(&mint.to_be_bytes()[..]);
        self.buf.extend_from_slice(&maxt.to_be_bytes()[..]);

        // Write data about the last set of bytes
        self.buf.extend_from_slice(&self.last_offset.to_be_bytes()[..]);
        self.buf.extend_from_slice(&self.last_file_id.to_be_bytes()[..]);
        self.buf.extend_from_slice(&self.last_mint.to_be_bytes()[..]);
        self.buf.extend_from_slice(&self.last_maxt.to_be_bytes()[..]);
        self.buf.extend_from_slice(&self.last_bytes.to_be_bytes()[..]);

        // Write the bytes
        self.buf.extend_from_slice(bytes);
        self.buf.extend_from_slice(&bytes.len().to_be_bytes()[..]);

        // Write all of it into the file
        file.write(self.buf.as_slice())?;

        // Then update the metadata
        self.last_offset = file.offset;
        self.last_file_id = file.local_id;
        self.last_bytes = self.buf.len() as u64;
        self.last_mint = mint;
        self.last_maxt = maxt;

        // Update versioning for concurrent readers
        self.version.fetch_add(1, SeqCst);

        Ok(())
    }

    fn read(&self) -> Result<FileListIterator, Error> {
        let v = self.version.load(SeqCst);
        let iterator = FileListIterator {
            next_offset: self.last_offset,
            next_file_id: self.last_file_id,
            next_bytes: self.last_bytes,
            next_mint: self.last_mint,
            next_maxt: self.last_maxt,
            file: None,
        };
        if v == self.version.load(SeqCst) {
            Ok(iterator)
        } else {
            Err(Error::ReadVersion)
        }
    }
}
