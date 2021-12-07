use std::{
    sync::{Arc, atomic::{AtomicU64, AtomicBool, AtomicUsize, Ordering::SeqCst}},
    fs::{OpenOptions, File},
    path::{PathBuf, Path},
    io::{self, prelude::*, SeekFrom},
    convert::TryInto,
};
use crate::tags::{self, Tags};
use lazy_static::*;

const MAGICSTR: &str = "filebackend";

#[cfg(test)]
lazy_static! {
    pub static ref DATADIR: PathBuf = {
        let path = crate::test_utils::TEST_DATA_PATH.join("tmp");
        fs::create_dir_all(path.as_path());
        path
    };

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
        let file = OpenOptions::new().create(true).write(true).append(true).open(DATADIR.join(file_name))?;
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
        let file = OpenOptions::new().create(true).write(true).append(true).open(DATADIR.join(file_name))?;
        self.offset = 0;
        self.local_id = local_id;
        self.file = file;
        Ok(())
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
    buf: Vec<u8>,
}

impl FileListIterator {
    pub fn next_item(&mut self) -> Result<Option<FileChunk>, Error> {
        if self.next_offset == u64::MAX {
            return Ok(None)
        }

        else if self.file.is_none() {
            let file_name = format!("data-{}", self.next_file_id);
            self.file = Some(OpenOptions::new().create(true).write(true).append(true).open(DATADIR.join(file_name))?);
        }

        let mut file = self.file.as_mut().unwrap();
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
            let fail = match magic {
                Ok(x) => if x != MAGICSTR { true } else { false },
                Err(_) => true,
            };
            if fail {
                return Err(Error::InvalidMagic)
            }
            off = end;
        }

        // Get tags
        let tags_len = u64::from_be_bytes(self.buf[off..off + 8].try_into().unwrap()) as usize;
        off += 8;
        let tags = Tags::from_bytes(&self.buf[off..off+tags_len])?;
        off += tags_len;

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
            tags,
            bytes: &self.buf[off..end]
        };
        Ok(Some(f))
    }
}

pub struct FileChunk<'a> {
    pub mint: u64,
    pub maxt: u64,
    pub tags: Tags,
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

        // Truncate data here to just the header information
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
    use rand::{thread_rng, Rng};
    use crate::test_utils::*;

    #[test]
    fn run_test() {
        //let dir = tempfile::TempDir::new_in(TEST_DATA_PATH.as_path()).unwrap();
        //std::fs::create_dir_all(dir.path()).unwrap();
        //println!("dir: {:?}", dir);
        let shared_id = Arc::new(AtomicU64::new(0));
        let mut file = FileWriter::new(shared_id.clone()).unwrap();
        let mut tags = Tags::new();
        tags.insert(("A".to_string(),"B".to_string()));
        tags.insert(("C".to_string(),"D".to_string()));
        let mut file_list = FileList::new(&tags);

        let data = (0..3).map(|_| {
            let mut v = vec![0u8; 512];
            thread_rng().fill(&mut v[..]);
            v
        }).collect::<Vec<Vec<u8>>>();

        let mut writer = file_list.writer().unwrap();
        writer.push(&mut file, 0, 5, data[0].as_slice()).unwrap();
        writer.push(&mut file, 6, 11, data[1].as_slice()).unwrap();
        writer.push(&mut file, 12, 13, data[2].as_slice()).unwrap();
    }
}
