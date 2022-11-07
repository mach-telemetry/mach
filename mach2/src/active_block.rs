use crate::{
    byte_buffer::ByteBuffer,
    compression::Compression,
    constants::*,
    id::SourceId,
    mem_list::{data_block::DataBlock, metadata_list::MetadataListWriter},
    segment::Segment,
    segment::SegmentRef,
};
use dashmap::DashMap;
use log::*;
use serde::*;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};

enum PushStatus {
    Ok,
    Full,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct BlockMetadata {
    pub source_id: SourceId,
    offset: usize,
    pub min_ts: u64,
    pub max_ts: u64,
}

impl BlockMetadata {
    //fn source_id(&self) -> SourceId {
    //    self.source_id
    //}

    //fn time_range(&self) -> (u64, u64) {
    //    (self.min_ts, self.max_ts)
    //}
}

impl BlockMetadata {
    fn new() -> Self {
        Self {
            source_id: SourceId(u64::MAX),
            offset: usize::MAX,
            min_ts: u64::MAX,
            max_ts: u64::MAX,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOnlyBlock {
    data: Vec<u8>,
    offsets: Vec<BlockMetadata>,
}

impl ReadOnlyBlock {
    pub fn from_closed_bytes(bytes: &[u8]) -> Self {
        let l = bytes.len();
        let offsets_len = usize::from_be_bytes(bytes[l - 8..l].try_into().unwrap());
        let offsets_start = l - 8 - offsets_len;
        let offsets =
            bincode::deserialize(&bytes[offsets_start..offsets_start + offsets_len]).unwrap();
        let data = bytes[..offsets_start].into();
        Self { data, offsets }
    }

    pub fn get_metadata(&self) -> &[BlockMetadata] {
        self.offsets.as_slice()
    }

    pub fn get_segment(&self, meta: BlockMetadata) -> Segment {
        let mut o = meta.offset;

        let e = o + 8;
        let _source_id = u64::from_be_bytes(self.data[o..e].try_into().unwrap());
        o = e;

        let e = o + 8;
        let _min_ts = u64::from_be_bytes(self.data[o..e].try_into().unwrap());
        o = e;

        let e = o + 8;
        let _max_ts = u64::from_be_bytes(self.data[o..e].try_into().unwrap());
        o = e;

        let e = o + 8;
        let compressed_sz = usize::from_be_bytes(self.data[o..e].try_into().unwrap());
        o = e;

        // Compress and record size of compressed chunk
        let e = o + compressed_sz;
        let mut segment = Segment::new_empty();
        Compression::decompress_segment(&self.data[o..e], &mut segment);
        segment
    }
}

pub struct ActiveBlockWriter {
    metadata_lists: Arc<DashMap<SourceId, MetadataListWriter>>,
    inner: Arc<InnerActiveBlock>,
}

// Safety: ActiveBlockwriter write methods require &mut which means it's the only one accessing
// data. Concurrent readers are coordinated internally.
unsafe impl Sync for ActiveBlockWriter {}
unsafe impl Send for ActiveBlockWriter {}

impl ActiveBlockWriter {
    pub fn push(
        &mut self,
        source_id: SourceId,
        active_segment: SegmentRef,
        compression: &Compression,
    ) {
        match self.inner.push(source_id, active_segment, compression) {
            PushStatus::Ok => {}
            PushStatus::Full => {
                debug!("Active Block Filled");
                // Close the block first to get the data block and metadata for this block
                let (block, meta) = self.inner.close();

                // Get the sources and their respsective time ranges
                let mut map: HashMap<SourceId, (u64, u64)> = HashMap::new();
                meta.iter().for_each(|x| {
                    let key = x.source_id;
                    let min_ts = x.min_ts;
                    let max_ts = x.max_ts;
                    let range = map.entry(key).or_insert((min_ts, max_ts));
                    range.1 = range.1.max(max_ts);
                });

                // Insert the block into individual metadata lists
                for (k, v) in map.drain() {
                    self.metadata_lists
                        .get(&k)
                        .unwrap()
                        .push(block.clone(), v.0, v.1);
                }

                // Now that the blocks are queriable, can reset the active block
                self.inner.reset();
            }
        }
    }
}

#[derive(Clone)]
pub struct ActiveBlock {
    metadata_lists: Arc<DashMap<SourceId, MetadataListWriter>>,
    inner: Arc<InnerActiveBlock>,
}

impl ActiveBlock {
    pub fn add_source(&self, source: SourceId, metadata_list: MetadataListWriter) {
        self.metadata_lists.insert(source, metadata_list);
    }

    pub fn new() -> (ActiveBlock, ActiveBlockWriter) {
        let inner = Arc::new(InnerActiveBlock {
            inner: UnsafeCell::new(Inner::new()),
            version: AtomicUsize::new(0),
        });
        let metadata_lists = Arc::new(DashMap::new());
        let active_block = Self {
            metadata_lists: metadata_lists.clone(),
            inner: inner.clone(),
        };
        let writer = ActiveBlockWriter {
            metadata_lists,
            inner,
        };
        (active_block, writer)
    }

    pub fn read_only(&self) -> Result<ReadOnlyBlock, &'static str> {
        self.inner.read_only()
    }
}

// Safety: It is safe to share ActiveBlock with multiple threads because of the sync mechansims
// implemented in Inner and the restricted methods of ActiveBlock. See Above.
unsafe impl Sync for ActiveBlock {}
unsafe impl Send for ActiveBlock {}

struct InnerActiveBlock {
    version: AtomicUsize,
    inner: UnsafeCell<Inner>,
}

impl InnerActiveBlock {
    fn push(
        &self,
        source_id: SourceId,
        active_segment: SegmentRef,
        compression: &Compression,
    ) -> PushStatus {
        // Safety This function does not race with any other function
        unsafe { (*self.inner.get()).push(source_id, active_segment, compression) }
    }

    fn close(&self) -> (DataBlock, Vec<BlockMetadata>) {
        // Safety This function does not race with any other function
        unsafe { (*self.inner.get()).close() }
    }

    fn reset(&self) {
        self.version.fetch_add(1, SeqCst);
        unsafe {
            (*self.inner.get()).reset();
        }
        self.version.fetch_add(1, SeqCst);
        debug!("Active Block Reset");
    }

    fn read_only(&self) -> Result<ReadOnlyBlock, &'static str> {
        let version = self.version.load(SeqCst);
        // Safety: versioning ensures that if read_only races with reset, will return error
        let result = unsafe { (*self.inner.get()).read_only() };
        if version != self.version.load(SeqCst) {
            Err("Failed to snapshot active block")
        } else {
            Ok(result)
        }
    }
}

struct Inner {
    data: Box<[u8; BLOCK_SZ * 2]>,
    data_len: AtomicUsize,
    offsets: Box<[BlockMetadata; BLOCK_SZ]>,
    offsets_len: AtomicUsize,
}

impl Inner {
    fn new() -> Self {
        Inner {
            data: box [0u8; BLOCK_SZ * 2],
            data_len: AtomicUsize::new(0),
            offsets: box [BlockMetadata::new(); BLOCK_SZ],
            offsets_len: AtomicUsize::new(0),
        }
    }
    fn push(
        &mut self,
        source_id: SourceId,
        active_segment: SegmentRef,
        compression: &Compression,
    ) -> PushStatus {
        let offset = self.data_len.load(SeqCst);
        let mut byte_buffer = ByteBuffer::new(offset, &mut self.data[..]);

        let len = active_segment.len;
        let min_ts = active_segment.timestamps[0];
        let max_ts = active_segment.timestamps[len - 1];

        byte_buffer.extend_from_slice(&source_id.0.to_be_bytes());
        byte_buffer.extend_from_slice(&min_ts.to_be_bytes());
        byte_buffer.extend_from_slice(&max_ts.to_be_bytes());

        // place holder for chunk size
        let chunk_sz_offset = byte_buffer.len();
        byte_buffer.extend_from_slice(&0u64.to_be_bytes());

        // Compress and record size of compressed chunk
        let compress_start = byte_buffer.len();
        compression.compress_segment(&active_segment, &mut byte_buffer);
        let chunk_sz = byte_buffer.len() - compress_start;
        byte_buffer.as_mut_slice()[chunk_sz_offset..chunk_sz_offset + 8]
            .copy_from_slice(&chunk_sz.to_be_bytes());

        let new_len = byte_buffer.len();

        let offsets_len = self.offsets_len.load(SeqCst);
        let metadata = BlockMetadata {
            source_id,
            offset,
            min_ts,
            max_ts,
        };
        self.offsets[offsets_len] = metadata;
        self.data_len.store(new_len, SeqCst);
        self.offsets_len.fetch_add(1, SeqCst);

        if new_len > BLOCK_SZ || offsets_len + 1 == BLOCK_SZ {
            PushStatus::Full
        } else {
            PushStatus::Ok
        }
    }

    fn close(&mut self) -> (DataBlock, Vec<BlockMetadata>) {
        let data_len = self.data_len.load(SeqCst);
        let offsets_len = self.offsets_len.load(SeqCst);
        let block_metadata: Vec<BlockMetadata> = self.offsets[..offsets_len].into();
        let new_len = {
            let mut byte_buffer = ByteBuffer::new(data_len, &mut self.data[..]);

            // Write in offsets
            let offsets_start = byte_buffer.len();
            bincode::serialize_into(&mut byte_buffer, &block_metadata).unwrap();
            let offsets_sz = byte_buffer.len() - offsets_start;

            // Write in sz of offsets
            byte_buffer.extend_from_slice(&offsets_sz.to_be_bytes());
            byte_buffer.len()
        };

        (DataBlock::new(&self.data[..new_len]), block_metadata)
    }

    fn reset(&mut self) {
        self.offsets_len.store(0, SeqCst);
        self.data_len.store(0, SeqCst);
    }

    fn read_only(&self) -> ReadOnlyBlock {
        let data_len = self.data_len.load(SeqCst);
        let offsets_len = self.offsets_len.load(SeqCst);
        let data = self.data[..data_len].into();
        let offsets = self.offsets[..offsets_len].into();
        ReadOnlyBlock { data, offsets }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::active_segment::ActiveSegment;
    use crate::compression::*;
    use crate::field_type::FieldType;
    use crate::id::SourceId;
    use crate::sample::SampleType;
    use crate::test_utils::*;

    #[test]
    fn test_active_block() {
        let types = &[FieldType::Bytes, FieldType::F64];
        let compression = Compression::new(vec![
            CompressionScheme::delta_of_delta(),
            CompressionScheme::lz4(),
        ]);

        let samples = random_samples(types, SEG_SZ, 16..1024);
        let (_active_segment, mut active_segment_writer) = ActiveSegment::new(types);
        let (active_block, mut active_block_writer) = ActiveBlock::new();

        assert_eq!(
            fill_active_segment(&samples, &mut active_segment_writer),
            SEG_SZ
        );
        {
            let segment_reference = active_segment_writer.as_segment_ref();
            active_block_writer.push(SourceId(1234), segment_reference, &compression);
        }

        active_segment_writer.reset();

        assert_eq!(
            fill_active_segment(&samples, &mut active_segment_writer),
            SEG_SZ
        );
        {
            let segment_reference = active_segment_writer.as_segment_ref();
            active_block_writer.push(SourceId(5678), segment_reference, &compression);
        }

        active_segment_writer.reset();

        assert_eq!(
            fill_active_segment(&samples, &mut active_segment_writer),
            SEG_SZ
        );
        {
            let segment_reference = active_segment_writer.as_segment_ref();
            active_block_writer.push(SourceId(1234), segment_reference, &compression);
        }

        let read_only_block = active_block.read_only().unwrap();
        let meta = read_only_block.get_metadata();
        println!("META: {:?}", read_only_block.get_metadata());

        let segment = read_only_block.get_segment(meta[2]);
        assert_eq!(segment.timestamps[0], meta[2].min_ts);
        assert_eq!(*segment.timestamps.last().unwrap(), meta[2].max_ts);
        let strings: Vec<SampleType> = (0..SEG_SZ).map(|x| segment.field_idx(0, x)).collect();
        assert_eq!(&strings[..], samples[0].as_slice());
        let strings: Vec<SampleType> = (0..SEG_SZ).map(|x| segment.field_idx(0, x)).collect();
        assert_eq!(&strings[..], samples[0].as_slice());
        let floats: Vec<SampleType> = (0..SEG_SZ).map(|x| segment.field_idx(1, x)).collect();
        assert_eq!(&floats[..], samples[1].as_slice());
    }

    #[test]
    fn test_list() {}
}
