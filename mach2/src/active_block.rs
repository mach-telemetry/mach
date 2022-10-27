use crate::{
    active_segment::ActiveSegmentRef, byte_buffer::ByteBuffer, compression::Compression,
    constants::*, id::SourceId, mem_list::data_block::DataBlock,
};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering::SeqCst}};
use serde::*;
use std::cell::UnsafeCell;

enum PushStatus {
    Ok,
    Full,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct BlockMetadata {
    source_id: SourceId,
    offset: usize,
    min_ts: u64,
    max_ts: u64,
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


pub struct ReadOnlyBlock {
    data: Vec<u8>,
    offsets: Vec<BlockMetadata>
}

pub struct ActiveBlockWriter {
    inner: Arc<InnerActiveBlock>
}

impl ActiveBlockWriter {
    //pub fn push(
    //    &mut self,
    //    source_id: SourceId,
    //    active_segment: ActiveSegmentRef,
    //    compression: &Compression
    //) -> {
    //}
}

pub struct ActiveBlock {
    inner: Arc<InnerActiveBlock>
}

impl ActiveBlock {
    pub fn new() -> (ActiveBlock, ActiveBlockWriter) {
        let boxed = box InnerActiveBlock {
            inner: UnsafeCell::new(Inner {
                data: [0u8; BLOCK_SZ * 2],
                data_len: AtomicUsize::new(0),
                offsets: [BlockMetadata::new(); BLOCK_SZ],
                offsets_len: AtomicUsize::new(0),
            }),
            version: AtomicUsize::new(0)
        };
        let inner: Arc<InnerActiveBlock> = boxed.into();
        let active_block = Self {
            inner: inner.clone()
        };
        let writer = ActiveBlockWriter {
            inner,
        };
        (active_block, writer)
    }

    pub fn read_only(&self) -> Result<ReadOnlyBlock, &'static str> {
        self.inner.read_only()
    }
}

struct InnerActiveBlock {
    version: AtomicUsize,
    inner: UnsafeCell<Inner>
}

impl InnerActiveBlock {
    fn push(&self,
        source_id: SourceId,
        active_segment: ActiveSegmentRef,
        compression: &Compression
    ) -> PushStatus {

        // Safety This function does not race with any other function
        unsafe {
            (*self.inner.get()).push(source_id, active_segment, compression)
        }
    }

    fn reset(&self) -> DataBlock {
        self.version.fetch_add(1, SeqCst);
        let ptr = self.inner.get();
        // Safety: Close does not race with other functions. Reset does (with read_only). Version
        // makes sure read_only will return an error if they race.
        let data_block = unsafe {
            let data_block = (*ptr).close();
            (*ptr).reset();
            data_block
        };
        self.version.fetch_add(1, SeqCst);
        data_block
    }

    fn read_only(&self) -> Result<ReadOnlyBlock, &'static str> {
        let version = self.version.load(SeqCst);
        // Safety: versioning ensures that if read_only races with reset, will return error
        let result = unsafe {
            (*self.inner.get()).read_only()
        };
        if version != self.version.load(SeqCst) {
            Err("Failed to snapshot active block")
        } else {
            Ok(result)
        }
    }
}

struct Inner {
    data: [u8; BLOCK_SZ * 2],
    data_len: AtomicUsize,
    offsets: [BlockMetadata; BLOCK_SZ],
    offsets_len: AtomicUsize,
}

impl Inner {
    fn push(
        &mut self,
        source_id: SourceId,
        active_segment: ActiveSegmentRef,
        compression: &Compression,
    ) -> PushStatus {
        let offset = self.data_len.load(SeqCst);
        let mut byte_buffer = ByteBuffer::new(offset, &mut self.data);

        let min_ts = active_segment.ts[0];
        let max_ts = *active_segment.ts.last().unwrap();

        byte_buffer.extend_from_slice(&source_id.0.to_be_bytes());
        byte_buffer.extend_from_slice(&min_ts.to_be_bytes());
        byte_buffer.extend_from_slice(&max_ts.to_be_bytes());

        // place holder for chunk size
        let chunk_sz_offset = byte_buffer.len();
        byte_buffer.extend_from_slice(&0u64.to_be_bytes());

        // Compress and record size of compressed chunk
        let compress_start = byte_buffer.len();
        compression.compress(
            active_segment.len,
            active_segment.heap_len,
            active_segment.ts,
            active_segment.heap,
            active_segment.data.as_slice(),
            active_segment.types,
            &mut byte_buffer,
        );
        let chunk_sz = byte_buffer.len() - compress_start;
        byte_buffer.as_mut_slice()[chunk_sz_offset..chunk_sz_offset + 8]
            .copy_from_slice(&chunk_sz.to_be_bytes());

        let new_len = byte_buffer.len();

        let offsets_len = self.offsets_len.load(SeqCst);
        let metadata = BlockMetadata {
            source_id,
            offset,
            min_ts,
            max_ts
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

    fn close(&mut self) -> DataBlock {
        let data_len = self.data_len.load(SeqCst);
        let offsets_len = self.offsets_len.load(SeqCst);
        let new_len = {
            let mut byte_buffer = ByteBuffer::new(data_len, &mut self.data);

            // Write in offsets
            let offsets_start = byte_buffer.len();
            bincode::serialize_into(&mut byte_buffer, &self.offsets[..offsets_len]).unwrap();
            let offsets_sz = byte_buffer.len() - offsets_start;

            // Write in sz of offsets
            byte_buffer.extend_from_slice(&offsets_sz.to_be_bytes());
            byte_buffer.len()
        };

        DataBlock::new(&self.data[..new_len])
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
        ReadOnlyBlock {
            data,
            offsets
        }
    }
}
