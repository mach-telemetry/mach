use crate::{
    backend::ByteEntry,
    tags::{self, Tags},
};
use lazy_static::*;
use std::{
    convert::TryInto,
    fs::{File, OpenOptions},
    io::{self, prelude::*, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

const MAGICSTR: &str = "filebackend";

#[cfg(test)]
lazy_static! {
    pub static ref TMPDIR: tempfile::TempDir =
        tempfile::TempDir::new_in(crate::test_utils::TEST_DATA_PATH.as_path()).unwrap();
    pub static ref DATADIR: PathBuf = PathBuf::from(TMPDIR.path());
    pub static ref SHARED_ID: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
}

#[cfg(not(test))]
lazy_static! {
    pub static ref DATADIR: PathBuf = PathBuf::from("/nvme/fsolleza/output");
}

#[derive(Debug)]
pub enum Error {
    IO(io::Error),
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

pub struct FileWriter {
    offset: u64,
    local_id: u64,
    shared_id: Arc<AtomicU64>,
    file: File,
}

impl FileWriter {
    pub fn new(shared_id: Arc<AtomicU64>) -> Result<Self, Error> {
        //let path = PathBuf::from(path.as_ref());
        //println!("path: {:?}", path);
        let local_id = shared_id.load(SeqCst);
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
        let local_id = self.shared_id.load(SeqCst);
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
    pub fn next_item(&mut self) -> Result<Option<FileChunk>, Error> {
        println!("next offset: {}", self.next_offset);
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

        // Parse magic
        {
            let end = MAGICSTR.as_bytes().len();
            let magic = std::str::from_utf8(&self.buf[off..end]);
            if magic.is_err() || magic.unwrap() != MAGICSTR {
                return Err(Error::InvalidMagic)
            }
            off = end;
        }

        // Get mint and maxt
        let mint = u64::from_be_bytes(self.buf[off..off + 8].try_into().unwrap());
        off += 8;
        let maxt = u64::from_be_bytes(self.buf[off..off + 8].try_into().unwrap());
        off += 8;

        // Get information for next item
        let next_offset = u64::from_be_bytes(self.buf[off..off + 8].try_into().unwrap());
        off += 8;
        let next_file_id = u64::from_be_bytes(self.buf[off..off + 8].try_into().unwrap());
        off += 8;
        let next_mint = u64::from_be_bytes(self.buf[off..off + 8].try_into().unwrap());
        off += 8;
        let next_maxt = u64::from_be_bytes(self.buf[off..off + 8].try_into().unwrap());
        off += 8;
        let next_bytes = u64::from_be_bytes(self.buf[off..off + 8].try_into().unwrap());
        off += 8;

        // Get the offsets for the chunk of bytes
        let end = self.buf.len() - 8;

        let f = FileChunk {
            mint,
            maxt,
            bytes: &self.buf[off..end],
        };

        // Update iterator state
        self.next_offset = next_offset;
        self.next_mint = next_mint;
        self.next_maxt = next_maxt;
        self.next_bytes = next_bytes;
        if self.next_file_id != next_file_id {
            self.file = None;
            self.next_file_id = next_file_id;
        }
        Ok(Some(f))
    }
}

pub struct FileChunk<'a> {
    pub mint: u64,
    pub maxt: u64,
    pub bytes: &'a [u8],
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
    pub fn push(
        &mut self,
        file: &mut FileWriter,
        byte_entry: ByteEntry,
    ) -> Result<(), Error> {
        // Safety: Safe because there's only one writer and concurrent readers check versions
        // during each read.
        unsafe { Arc::get_mut_unchecked(&mut self.inner).push(file, byte_entry.mint, byte_entry.maxt, byte_entry.bytes) }
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
    version: Arc<AtomicU64>,
    init_buf_len: usize,
    buf: Vec<u8>,
}

impl InnerFileList {
    fn new(tags: &Tags) -> Self {
        let mut buf = Vec::new();
        let tags = tags.serialize();

        buf.extend_from_slice(&MAGICSTR.as_bytes());

        Self {
            last_offset: u64::MAX,
            last_file_id: u64::MAX,
            last_bytes: u64::MAX,
            last_mint: u64::MAX,
            last_maxt: u64::MAX,
            version: Arc::new(AtomicU64::new(0)),
            init_buf_len: buf.len(),
            buf,
        }
    }

    fn push(
        &mut self,
        file: &mut FileWriter,
        mint: u64,
        maxt: u64,
        bytes: &[u8],
    ) -> Result<(), Error> {
        // Get the information of where the bytes will be written
        let last_offset = file.offset;
        let last_file_id = file.local_id;

        // Truncate data here to just the header information
        self.buf.truncate(self.init_buf_len);

        // Write data about the current set of bytes
        self.buf.extend_from_slice(&mint.to_be_bytes()[..]);
        self.buf.extend_from_slice(&maxt.to_be_bytes()[..]);

        // Write data about the last set of bytes
        self.buf
            .extend_from_slice(&self.last_offset.to_be_bytes()[..]);
        self.buf
            .extend_from_slice(&self.last_file_id.to_be_bytes()[..]);
        self.buf
            .extend_from_slice(&self.last_mint.to_be_bytes()[..]);
        self.buf
            .extend_from_slice(&self.last_maxt.to_be_bytes()[..]);
        self.buf
            .extend_from_slice(&self.last_bytes.to_be_bytes()[..]);

        // Write the bytes
        self.buf.extend_from_slice(bytes);
        self.buf.extend_from_slice(&bytes.len().to_be_bytes()[..]);

        // Write all of it into the file
        file.write(self.buf.as_slice())?;

        // Then update the metadata
        self.last_offset = last_offset;
        self.last_file_id = last_file_id;
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
        let shared_id = SHARED_ID.clone();
        let mut file = FileWriter::new(shared_id.clone()).unwrap();
        let mut tags = Tags::new();
        tags.insert(("A".to_string(), "B".to_string()));
        tags.insert(("C".to_string(), "D".to_string()));
        let mut file_list = FileList::new(&tags);

        let data = (0..3)
            .map(|i| {
                let mut v = vec![i + 1 as u8; 512];
                //thread_rng().fill(&mut v[..]);
                v
            })
            .collect::<Vec<Vec<u8>>>();

        let mut writer = file_list.writer().unwrap();
        writer.push(&mut file, 0, 5, data[0].as_slice()).unwrap();
        writer.push(&mut file, 6, 11, data[1].as_slice()).unwrap();
        writer.push(&mut file, 12, 13, data[2].as_slice()).unwrap();
        file.file.sync_all().unwrap();

        let mut reader = file_list.reader().unwrap();
        let chunk = reader.next_item().unwrap().unwrap();
        assert_eq!(chunk.bytes, data[2].as_slice());
        assert_eq!(chunk.mint, 12);
        assert_eq!(chunk.maxt, 13);
        assert_eq!(chunk.tags, tags);

        let chunk = reader.next_item().unwrap().unwrap();
        assert_eq!(chunk.bytes, data[1].as_slice());
        assert_eq!(chunk.mint, 6);
        assert_eq!(chunk.maxt, 11);
        assert_eq!(chunk.tags, tags);

        let chunk = reader.next_item().unwrap().unwrap();
        assert_eq!(chunk.bytes, data[0].as_slice());
        assert_eq!(chunk.mint, 0);
        assert_eq!(chunk.maxt, 5);
        assert_eq!(chunk.tags, tags);
    }
}
