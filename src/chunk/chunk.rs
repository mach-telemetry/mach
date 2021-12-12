use crate::{
    backend::fs,
    chunk::WriteFileChunk,
    compression::Compression,
    flush_buffer::{FlushBuffer, FlushEntry, FrozenBuffer},
    segment::FullSegment,
    tags::Tags,
};
use bincode;
use serde::*;
use std::{
    convert::TryInto,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

const MAGIC: &[u8] = b"CHUNKMAGIC";
//const CHUNK_THRESHOLD_SIZE: usize = 8192;
const THRESH: usize = 16;

#[derive(Debug)]
pub enum Error {
    InconsistentRead,
    MultipleWriters,
    InvalidMagic,
    Bincode(bincode::Error),
}

impl From<bincode::Error> for Error {
    fn from(item: bincode::Error) -> Self {
        Error::Bincode(item)
    }
}

// Chunk Format
//
// > magic
// magic: [0..10]
//
// tags size
// tag bytes
// mint (8)
// maxt (8)
// segment count (1)
//
// > Segment metadata information
// segment offset (8)
// segment mint: (8)
// segment maxt: (8)
// ... repeated for the segment count
//
// > Segment data information
// segments: [header bytes + segment count * 8 * 3 ..]

#[derive(Copy, Clone, Serialize, Deserialize)]
struct SegmentMeta {
    offset: u64,
    mint: u64,
    maxt: u64,
}

struct Inner<const H: usize, const T: usize> {
    flush_buffer: FlushBuffer<H, T>,
    counter: AtomicUsize,
    version: AtomicUsize,
    reset_flag: AtomicBool,
    local_counter: usize,
    compression: Compression,
    header_len: usize,
    metadata_offset: usize,
    data_offset: usize,
    mint: u64,
    maxt: u64,
    segment_meta: [SegmentMeta; THRESH],
    has_writer: AtomicBool,
}

impl<const H: usize, const T: usize> Inner<H, T> {
    fn new(tags: &Tags, compression: Compression) -> Self {
        let mut flush_buffer = FlushBuffer::new();

        // Magic
        flush_buffer.push_bytes(MAGIC).unwrap();

        // Filler for the length
        // data.extend_from_slice(&[0u8; 8][..]);

        // Tags
        flush_buffer
            .push_bytes(&bincode::serialized_size(tags).unwrap().to_be_bytes()[..])
            .unwrap();
        bincode::serialize_into(flush_buffer.pushable_vec().unwrap(), tags).unwrap();

        // Fixed header information done
        let header_len = flush_buffer.len();

        // Chunk mint, maxt, len
        flush_buffer.push_bytes(&[0u8; 8 + 8 + 1]).unwrap();

        // Filler for mint (8), maxt (8), segment count (1) , and segment information 16 * 3 * 8,
        let metadata_offset = flush_buffer.len();
        flush_buffer.push_bytes(&[0u8; THRESH * 3 * 8]).unwrap();
        let data_offset = flush_buffer.len();

        Inner {
            flush_buffer,
            compression,
            counter: AtomicUsize::new(0),
            version: AtomicUsize::new(0),
            reset_flag: AtomicBool::new(false),
            local_counter: 0,
            metadata_offset,
            data_offset,
            header_len,
            mint: u64::MAX,
            maxt: u64::MAX,
            segment_meta: [SegmentMeta {
                offset: 0,
                mint: 0,
                maxt: 0,
            }; THRESH],
            has_writer: AtomicBool::new(false),
        }
    }

    fn flush_buffer(&mut self) -> FlushEntry<H, T> {
        let data = self.flush_buffer.data_mut();

        // Mint, Maxt, count
        let mut off = self.header_len;
        data[off..off + 8].copy_from_slice(&self.mint.to_be_bytes()[..]);
        off += 8;
        data[off..off + 8].copy_from_slice(&self.maxt.to_be_bytes()[..]);
        off += 8;
        data[off] = self.local_counter as u8;

        // Segment Metadata
        off = self.metadata_offset;
        for i in 0..self.local_counter {
            let m = self.segment_meta[i];
            data[off..off + 8].copy_from_slice(&m.offset.to_be_bytes()[..]);
            off += 8;
            data[off..off + 8].copy_from_slice(&m.mint.to_be_bytes()[..]);
            off += 8;
            data[off..off + 8].copy_from_slice(&m.maxt.to_be_bytes()[..]);
            off += 8;
        }
        drop(data);

        FlushEntry {
            mint: self.mint,
            maxt: self.maxt,
            buffer: self.flush_buffer.freeze(),
        }
    }

    fn read(&self) -> Result<ReadChunk, Error> {
        let v = self.version.load(SeqCst);

        // Prevent entry while there is a concurrent reset
        while self.reset_flag.load(SeqCst) {}

        // Make the copy
        let counter = self.counter.load(SeqCst);
        let r = ReadChunk {
            data: self.flush_buffer.data().into(),
            segment_meta: Box::new(self.segment_meta.clone()),
            counter,
        };

        // Prevent exit while there is a concurrent reset
        while self.reset_flag.load(SeqCst) {}

        // If there was a concurrent reset, the version would be incorrect
        if v == self.version.load(SeqCst) {
            Ok(r)
        } else {
            Err(Error::InconsistentRead)
        }
    }

    fn reset(&mut self) {
        // Prevent readers from entering or exiting
        self.reset_flag.store(true, SeqCst);

        // Readers already inside must have loaded this or prior version. They can't exit
        // without seeing this version increment
        self.version.fetch_add(1, SeqCst);

        self.flush_buffer.truncate(self.metadata_offset);
        self.local_counter = 0;
        self.mint = u64::MAX;
        self.maxt = u64::MAX;
        self.counter.store(0, SeqCst);

        // Allow new readers to enter or concurrent readers to exit (and load the version)
        self.reset_flag.store(false, SeqCst);
    }

    fn push(&mut self, segment: &FullSegment) -> Option<FlushEntry<H, T>> {
        let (mint, maxt) = {
            let ts = segment.timestamps();
            let mint = ts[0];
            let maxt = ts[ts.len() - 1];
            (mint, maxt)
        };
        if self.mint == u64::MAX {
            self.mint = mint;
        }
        self.maxt = maxt;

        let offset = self.flush_buffer.len() as u64;
        self.compression
            .compress(segment, self.flush_buffer.pushable_vec().unwrap());
        self.segment_meta[self.local_counter] = SegmentMeta { offset, mint, maxt };
        self.local_counter += 1;
        self.counter.swap(self.local_counter, SeqCst);

        if self.local_counter == THRESH {
            Some(self.flush_buffer())
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub struct Chunk<const H: usize, const T: usize> {
    inner: Arc<Inner<H, T>>,
}

impl<const H: usize, const T: usize> Chunk<H, T> {
    pub fn new(tags: &Tags, compression: Compression) -> Self {
        Self {
            inner: Arc::new(Inner::new(tags, compression)),
        }
    }

    pub fn writer(&self) -> Result<WriteChunk<H, T>, Error> {
        if self.inner.has_writer.swap(true, SeqCst) {
            Err(Error::MultipleWriters)
        } else {
            Ok(WriteChunk {
                inner: self.inner.clone(),
            })
        }
    }

    pub fn read(&self) -> Result<ReadChunk, Error> {
        self.inner.read()
    }
}

pub struct WriteChunk<const H: usize, const T: usize> {
    inner: Arc<Inner<H, T>>,
}

impl<const H: usize, const T: usize> WriteChunk<H, T> {
    pub fn push(&mut self, segment: &FullSegment) -> Option<FlushEntry<H, T>> {
        // Safety: There can only be one writer. Concurrent readers on the Chunk struct and the
        // writer push is coordianted using the atomic counter
        unsafe { Arc::get_mut_unchecked(&mut self.inner).push(segment) }
    }

    pub fn reset(&mut self) {
        // Safety: There can only be one reseter. Concurrent readers on the Chunk struct and the
        // writer push is coordianted by causing the reader to spin and versioning. See the read
        // and reset functions
        unsafe { Arc::get_mut_unchecked(&mut self.inner).reset() }
    }

    pub fn flush_buffer(&mut self) -> FlushEntry<H, T> {
        // Safety: Only the writer can access the inner method. The inner method modifies inner
        // state but does not modify data accessed by concurrent readers
        unsafe { Arc::get_mut_unchecked(&mut self.inner).flush_buffer() }
    }
}

pub struct ReadChunk {
    data: Vec<u8>,
    counter: usize,
    segment_meta: Box<[SegmentMeta; THRESH]>,
}

impl ReadChunk {
    pub fn get_segment_time_range(&self, id: usize) -> (u64, u64) {
        let seg = &self.segment_meta[id];
        (seg.mint, seg.maxt)
    }

    pub fn get_segment_bytes(&self, id: usize) -> &[u8] {
        let offset = self.segment_meta[id].offset as usize;
        let end = if id == THRESH - 1 {
            self.data.len()
        } else {
            self.segment_meta[id + 1].offset as usize
        };
        &self.data[offset..end]
    }
}

pub struct SerializedChunk<'a> {
    data: &'a [u8],
    counter: usize,
    mint: u64,
    maxt: u64,
    tags: Tags,
    segment_meta: Box<[SegmentMeta; THRESH]>,
}

impl<'a> SerializedChunk<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self, Error> {
        if MAGIC != &data[..MAGIC.len()] {
            return Err(Error::InvalidMagic);
        }

        let mut off = MAGIC.len();
        let tag_len = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;

        let tags: Tags = bincode::deserialize(&data[off..off + tag_len])?;
        off += tag_len;

        let mint = u64::from_be_bytes(data[off..off + 8].try_into().unwrap());
        off += 8;

        let maxt = u64::from_be_bytes(data[off..off + 8].try_into().unwrap());
        off += 8;

        let counter: u8 = data[off];
        off += 1;

        let mut segment_meta = Box::new(
            [SegmentMeta {
                offset: 0,
                mint: 0,
                maxt: 0,
            }; THRESH],
        );
        for i in 0..counter as usize {
            let offset = u64::from_be_bytes(data[off..off + 8].try_into().unwrap());
            off += 8;
            let mint = u64::from_be_bytes(data[off..off + 8].try_into().unwrap());
            off += 8;
            let maxt = u64::from_be_bytes(data[off..off + 8].try_into().unwrap());
            off += 8;
            segment_meta[i] = SegmentMeta { offset, mint, maxt };
        }

        Ok(SerializedChunk {
            data,
            counter: counter.into(),
            segment_meta,
            mint,
            maxt,
            tags,
        })
    }

    pub fn get_segment_time_range(&self, id: usize) -> (u64, u64) {
        let seg = &self.segment_meta[id];
        (seg.mint, seg.maxt)
    }

    pub fn get_segment_bytes(&self, id: usize) -> &[u8] {
        let offset = self.segment_meta[id].offset as usize;
        let end = if id == THRESH - 1 {
            self.data.len()
        } else {
            self.segment_meta[id + 1].offset as usize
        };
        &self.data[offset..end]
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::compression::DecompressBuffer;
    use crate::segment::{self, Segment};
    use crate::test_utils::*;

    #[test]
    fn test_check_data() {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let mut tags = Tags::new();
        tags.insert(("A".to_string(), "B".to_string()));
        tags.insert(("C".to_string(), "D".to_string()));

        let segment = Segment::new(3, nvars);
        let mut writer = segment.writer().unwrap();
        let mut chunk = Chunk::<5, 6>::new(&tags, Compression::LZ4(1));
        let mut chunk_writer = chunk.writer().unwrap();

        let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
            let mut values = vec![[0u8; 8]; nvars];
            for (i, v) in items.iter().enumerate() {
                values[i] = v.to_be_bytes();
            }
            values
        };

        let mut exp_ts = Vec::new();
        let mut exp_values = Vec::new();
        for _ in 0..nvars {
            exp_values.push(Vec::new());
        }

        for item in &data[..256 * (THRESH - 1)] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            for i in 0..nvars {
                exp_values[i].push(v[i]);
            }
            match writer.push(item.ts, &v[..]) {
                Ok(segment::PushStatus::Done) => {}
                Ok(segment::PushStatus::Flush(flusher)) => {
                    let seg = flusher.to_flush().unwrap();
                    assert!(chunk_writer.push(&seg).is_none());
                    flusher.flushed();
                }
                Err(_) => unimplemented!(),
            }
        }
        let start = 256 * (THRESH - 1);
        for item in &data[start..start + 256] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            for i in 0..nvars {
                exp_values[i].push(v[i]);
            }
            match writer.push(item.ts, &v[..]) {
                Ok(segment::PushStatus::Done) => {}
                Ok(segment::PushStatus::Flush(flusher)) => {
                    let seg = flusher.to_flush().unwrap();
                    assert!(chunk_writer.push(&seg).is_some());
                    flusher.flushed();
                }
                Err(_) => unimplemented!(),
            }
        }

        assert_eq!(chunk_writer.inner.counter.load(SeqCst), THRESH);

        let reader = chunk.read().unwrap();
        let bytes = reader.get_segment_bytes(0);
        let mut decompressed = DecompressBuffer::new();
        let bytes_read = Compression::decompress(bytes, &mut decompressed).unwrap();

        //assert_eq!(bytes_read, bytes.len());
        assert_eq!(decompressed.timestamps(), &exp_ts[..256]);
        for i in 0..nvars {
            assert_eq!(decompressed.variable(i), &exp_values[i][..256]);
        }

        let bytes_read =
            Compression::decompress(reader.get_segment_bytes(1), &mut decompressed).unwrap();
        assert_eq!(decompressed.timestamps(), &exp_ts[..512]);
        for i in 0..nvars {
            assert_eq!(decompressed.variable(i), &exp_values[i][..512]);
        }
    }

    #[test]
    fn serialized() {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let mut tags = Tags::new();
        tags.insert(("A".to_string(), "B".to_string()));
        tags.insert(("C".to_string(), "D".to_string()));

        let segment = Segment::new(3, nvars);
        let mut writer = segment.writer().unwrap();
        let mut chunk = Chunk::<5, 6>::new(&tags, Compression::LZ4(1));
        let mut chunk_writer = chunk.writer().unwrap();

        let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
            let mut values = vec![[0u8; 8]; nvars];
            for (i, v) in items.iter().enumerate() {
                values[i] = v.to_be_bytes();
            }
            values
        };

        let mut exp_ts = Vec::new();
        let mut exp_values = Vec::new();
        for _ in 0..nvars {
            exp_values.push(Vec::new());
        }

        for item in &data[..256 * (THRESH - 1)] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            for i in 0..nvars {
                exp_values[i].push(v[i]);
            }
            match writer.push(item.ts, &v[..]) {
                Ok(segment::PushStatus::Done) => {}
                Ok(segment::PushStatus::Flush(flusher)) => {
                    let seg = flusher.to_flush().unwrap();
                    assert!(chunk_writer.push(&seg).is_none());
                    flusher.flushed();
                }
                Err(_) => unimplemented!(),
            }
        }
        let start = 256 * (THRESH - 1);
        for item in &data[start..start + 256] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            for i in 0..nvars {
                exp_values[i].push(v[i]);
            }
            match writer.push(item.ts, &v[..]) {
                Ok(segment::PushStatus::Done) => {}
                Ok(segment::PushStatus::Flush(flusher)) => {
                    let seg = flusher.to_flush().unwrap();
                    assert!(chunk_writer.push(&seg).is_some());
                    flusher.flushed();
                }
                Err(_) => unimplemented!(),
            }
        }

        let buf = chunk_writer.flush_buffer();
        let bytes = buf.buffer.data();
        let mut v: Vec<u8> = bytes.try_into().unwrap();
        let serialized_chunk = SerializedChunk::new(&v[..]).unwrap();

        let bytes = serialized_chunk.get_segment_bytes(0);
        let mut decompressed = DecompressBuffer::new();
        let bytes_read = Compression::decompress(bytes, &mut decompressed).unwrap();

        assert_eq!(decompressed.timestamps(), &exp_ts[..256]);
        for i in 0..nvars {
            assert_eq!(decompressed.variable(i), &exp_values[i][..256]);
        }

        let bytes_read =
            Compression::decompress(serialized_chunk.get_segment_bytes(1), &mut decompressed)
                .unwrap();
        assert_eq!(decompressed.timestamps(), &exp_ts[..512]);
        for i in 0..nvars {
            assert_eq!(decompressed.variable(i), &exp_values[i][..512]);
        }
    }
}
