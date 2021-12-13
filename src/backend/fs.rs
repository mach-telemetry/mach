use crate::{
    backend::{ByteEntry, FlushEntry},
    flush_buffer::{FlushBuffer, FrozenBuffer},
    tags::{self, Tags},
};
use lazy_static::*;
use serde::*;
use std::{
    convert::TryInto,
    fs::{File, OpenOptions},
    io::{self, prelude::*, SeekFrom},
    mem::size_of,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

const MAGICSTR: [u8; 11] = *b"filebackend";
pub const TAILSZ: usize = 8;
pub const HEADERSZ: usize = size_of::<Header>();

pub type FileBuffer = FlushBuffer<HEADERSZ, TAILSZ>;
pub type FileFrozenBuffer<'buffer> = FrozenBuffer<'buffer, HEADERSZ, TAILSZ>;
pub type FileEntry<'buffer> = FlushEntry<'buffer, HEADERSZ, TAILSZ>;

#[derive(Debug, Serialize, Deserialize)]
struct Header {
    magic: [u8; 11],
    mint: u64,
    maxt: u64,
    last_offset: u64,
    last_file_id: u64,
    last_bytes: u64,
    last_mint: u64,
    last_maxt: u64,
}

impl Header {
    fn new() -> Self {
        Self {
            magic: MAGICSTR,
            mint: u64::MAX,
            maxt: u64::MAX,
            last_offset: u64::MAX,
            last_file_id: u64::MAX,
            last_bytes: u64::MAX,
            last_mint: u64::MAX,
            last_maxt: u64::MAX,
        }
    }
}

#[cfg(test)]
lazy_static! {
    pub static ref TMPDIR: tempfile::TempDir =
        tempfile::TempDir::new_in(crate::test_utils::TEST_DATA_PATH.as_path()).unwrap();
    pub static ref DATADIR: PathBuf = PathBuf::from(TMPDIR.path());
}

#[cfg(not(test))]
lazy_static! {
    pub static ref DATADIR: PathBuf = PathBuf::from("/nvme/fsolleza/output");
}

#[derive(Debug)]
pub enum Error {
    IO(io::Error),
    Bincode(bincode::Error),
    ReadVersion,
    MultipleWriters,
    InvalidMagic,
    Tags(crate::tags::Error),
}

impl From<io::Error> for Error {
    fn from(item: io::Error) -> Self {
        Error::IO(item)
    }
}

impl From<tags::Error> for Error {
    fn from(item: tags::Error) -> Self {
        Error::Tags(item)
    }
}

impl From<bincode::Error> for Error {
    fn from(item: bincode::Error) -> Self {
        Error::Bincode(item)
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
        //let path = PathBuf::from(path.as_ref());
        let local_id = shared_id.fetch_add(1, SeqCst);
        let file_name = format!("data-{}", local_id);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(DATADIR.join(file_name))?;
        let offset = 0;
        Ok(Self {
            offset,
            local_id,
            shared_id,
            file,
        })
    }

    pub fn reset(&mut self) -> Result<(), Error> {
        let local_id = self.shared_id.fetch_add(1, SeqCst);
        let file_name = format!("data-{}", local_id);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(DATADIR.join(file_name))?;
        self.offset = 0;
        self.local_id = local_id;
        self.file = file;
        Ok(())
    }

    pub fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let w = self.file.write_all(bytes)?;
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
    buf: Vec<u8>,
}

impl FileListIterator {
    pub fn next_item(&mut self) -> Result<Option<ByteEntry>, Error> {
        if self.next_offset == u64::MAX {
            return Ok(None);
        } else if self.file.is_none() {
            let file_name = format!("data-{}", self.next_file_id);
            self.file = Some(
                OpenOptions::new()
                    .read(true)
                    .open(DATADIR.join(file_name))?,
            );
        }

        let mut file = self.file.as_mut().unwrap();
        let meta = file.metadata().unwrap();
        file.seek(SeekFrom::Start(self.next_offset))?;
        let next_bytes = self.next_bytes as usize;
        self.buf.resize(next_bytes, 0u8);
        let bytes = file.read(&mut self.buf[..next_bytes])?;
        assert_eq!(bytes, next_bytes);

        let mut off = 0;

        // Get header
        let header: Header = bincode::deserialize(&self.buf[off..off + HEADERSZ])?;
        if header.magic != MAGICSTR {
            return Err(Error::InvalidMagic);
        }
        off += HEADERSZ;

        // Get the offsets for the chunk of bytes
        let end = self.buf.len() - TAILSZ;

        let f = ByteEntry {
            mint: header.mint,
            maxt: header.maxt,
            bytes: &self.buf[off..end],
        };

        // Update iterator state
        self.next_offset = header.last_offset;
        self.next_mint = header.last_mint;
        self.next_maxt = header.last_maxt;
        self.next_bytes = header.last_bytes;
        if self.next_file_id != header.last_file_id {
            self.file = None;
            self.next_file_id = header.last_file_id;
        }
        Ok(Some(f))
    }
}

#[derive(Clone)]
pub struct FileList {
    inner: Arc<InnerFileList>,
    has_writer: Arc<AtomicBool>,
}

impl FileList {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InnerFileList::new()),
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
    pub fn push(&mut self, file: &mut FileWriter, mut entry: FileEntry) -> Result<(), Error> {
        // Safety: Safe because there's only one writer and concurrent readers check versions
        // during each read.
        unsafe {
            Arc::get_mut_unchecked(&mut self.inner).push(
                file,
                entry.mint,
                entry.maxt,
                &mut entry.buffer,
            )
        }
    }
}

impl Drop for FileListWriter {
    fn drop(&mut self) {
        assert!(self.has_writer.swap(false, SeqCst));
    }
}

struct InnerFileList {
    header: Header,
    version: Arc<AtomicU64>,
}

impl InnerFileList {
    fn new() -> Self {
        Self {
            header: Header::new(),
            version: Arc::new(AtomicU64::new(0)),
        }
    }

    fn push(
        &mut self,
        file: &mut FileWriter,
        mint: u64,
        maxt: u64,
        buf: &mut FileFrozenBuffer,
    ) -> Result<(), Error> {

        // Wher to access these bytes
        let last_offset = file.offset;
        let last_file_id = file.local_id;

        self.header.mint = mint;
        self.header.maxt = maxt;

        // Write header and tail
        bincode::serialize_into(buf.header_mut(), &self.header)?;
        buf.write_tail(buf.len().to_be_bytes());

        let bytes = buf.bytes();
        let len = bytes.len();
        file.write(bytes)?;

        // Then update the metadata
        self.header.last_offset = last_offset;
        self.header.last_file_id = last_file_id;
        self.header.last_bytes = len as u64;
        self.header.last_mint = mint;
        self.header.last_maxt = maxt;

        // Update versioning for concurrent readers
        self.version.fetch_add(1, SeqCst);

        Ok(())
    }

    fn read(&self) -> Result<FileListIterator, Error> {
        let v = self.version.load(SeqCst);
        let iterator = FileListIterator {
            next_offset: self.header.last_offset,
            next_file_id: self.header.last_file_id,
            next_bytes: self.header.last_bytes,
            next_mint: self.header.last_mint,
            next_maxt: self.header.last_maxt,
            file: None,
            buf: Vec::new(),
        };
        if v == self.version.load(SeqCst) {
            Ok(iterator)
        } else {
            Err(Error::ReadVersion)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;
    use rand::{thread_rng, Rng};

    #[test]
    fn run_test() {
        std::fs::remove_dir_all(DATADIR.as_path()).unwrap();
        std::fs::create_dir_all(DATADIR.as_path()).unwrap();
        let shared_id = SHARED_FILE_ID.clone();
        let mut file = FileWriter::new(shared_id.clone()).unwrap();
        let mut tags = Tags::new();
        tags.insert(("A".to_string(), "B".to_string()));
        tags.insert(("C".to_string(), "D".to_string()));
        let mut file_list = FileList::new();

        let data = (0..3)
            .map(|i| {
                let mut v = vec![i + 1 as u8; 512];
                //thread_rng().fill(&mut v[..]);
                v
            })
            .collect::<Vec<Vec<u8>>>();

        let mut writer = file_list.writer().unwrap();
        let mut buf = FileBuffer::new();
        buf.push_bytes(data[0].as_slice()).unwrap();
        writer
            .push(&mut file, FileEntry::new(0, 5, buf.freeze()))
            .unwrap();
        buf.truncate(0);
        buf.push_bytes(data[1].as_slice()).unwrap();
        writer
            .push(&mut file, FileEntry::new(6, 11, buf.freeze()))
            .unwrap();
        buf.truncate(0);
        buf.push_bytes(data[2].as_slice()).unwrap();
        writer
            .push(&mut file, FileEntry::new(12, 13, buf.freeze()))
            .unwrap();
        file.file.sync_all().unwrap();

        let mut reader = file_list.reader().unwrap();
        let chunk = reader.next_item().unwrap().unwrap();
        assert_eq!(chunk.bytes, data[2].as_slice());
        assert_eq!(chunk.mint, 12);
        assert_eq!(chunk.maxt, 13);

        let chunk = reader.next_item().unwrap().unwrap();
        assert_eq!(chunk.bytes, data[1].as_slice());
        assert_eq!(chunk.mint, 6);
        assert_eq!(chunk.maxt, 11);

        let chunk = reader.next_item().unwrap().unwrap();
        assert_eq!(chunk.bytes, data[0].as_slice());
        assert_eq!(chunk.mint, 0);
        assert_eq!(chunk.maxt, 5);
    }
}
