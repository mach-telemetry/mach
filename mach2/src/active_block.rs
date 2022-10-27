use crate::{
    active_segment::ActiveSegmentRef, byte_buffer::ByteBuffer, compression::Compression,
    constants::*, id::SourceId, mem_list::data_block::DataBlock,
};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
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

pub struct ReadOnlyBlock {
    data: Vec<u8>,
    offsets: Vec<BlockMetadata>
}

struct InnerActiveBlock {
    version: AtomicUsize,
    inner: UnsafeCell<Inner>
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
        self.data_len.store(new_len, SeqCst);

        let offsets_len = self.offsets_len.load(SeqCst);
        let metadata = BlockMetadata {
            source_id,
            offset,
            min_ts,
            max_ts
        };
        self.offsets[offsets_len] = metadata;
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
