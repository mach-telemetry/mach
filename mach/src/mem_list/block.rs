use crate::{
    compression::Compression,
    id::SeriesId,
    segment::FlushSegment,
    utils::wp_lock::{WpLock, NoDealloc},
    utils::byte_buffer::ByteBuffer,
};
use std::sync::{Arc, RwLock, Mutex, atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst}};
use std::mem;

struct Bytes(Box<[u8]>);
impl std::ops::Deref for Bytes {
    type Target = Box<[u8]>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Safety:
/// for internal use only. see use in Block struct
unsafe impl NoDealloc for Bytes{}

pub struct Block {
    bytes: WpLock<Bytes>,
    len: AtomicUsize,
}

impl Block {
    pub fn new() -> Self {
        Self {
            bytes: WpLock::new(Bytes(vec![0u8; 2_000_000].into_boxed_slice())),
            len: AtomicUsize::new(0),
        }
    }

    pub fn push(
        &self,
        series_id: SeriesId,
        segment: &FlushSegment,
        compression: &Compression,
    ) -> bool {

        let mut offset = self.len.load(SeqCst);
        let end = offset + mem::size_of::<u64>();

        // Safety: Read is bounded by self.len.load(), these data won't be dealloced because self
        // is held by caller
        let bytes = unsafe { self.bytes.unprotected_write() };

        bytes[offset..end].copy_from_slice(&series_id.0.to_be_bytes());
        offset = end;

        // Compress the data into the buffer
        offset += {
            let mut byte_buffer = ByteBuffer::new(&mut bytes[offset..]);
            compression.compress(&segment.to_flush().unwrap(), &mut byte_buffer);
            byte_buffer.len()
        };
        self.len.store(offset, SeqCst);
        offset > 1_000_000
    }

    pub fn read(&self) -> &[u8] {
        let len = self.len.load(SeqCst);
        unsafe {
            &self.bytes.unprotected_read()[..len]
        }
    }
}


