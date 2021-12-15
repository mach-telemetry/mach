use crate::{backend, compression, segment, tags};
use std::sync::atomic::AtomicU64;

pub struct SeriesMetadata {
    pub thread_id: AtomicU64,
    pub segment: segment::Segment,
    pub backend: backend::Backend,
}

impl SeriesMetadata {
    pub fn with_file_backend(
        thread_id: u64,
        nvars: usize,
        buffers: usize,
        tags: &tags::Tags,
        compression: compression::Compression,
    ) -> Self {
        let backend = backend::Backend::new_file_backed(tags, compression);
        let thread_id = AtomicU64::new(thread_id);
        let segment = segment::Segment::new(buffers, nvars);
        SeriesMetadata {
            thread_id,
            segment,
            backend,
        }
    }

    pub fn with_kafka_backend(
        thread_id: u64,
        nvars: usize,
        buffers: usize,
        tags: &tags::Tags,
        compression: compression::Compression,
    ) -> Self {
        let backend = backend::Backend::new_kafka_backed(tags, compression);
        let thread_id = AtomicU64::new(thread_id);
        let segment = segment::Segment::new(buffers, nvars);
        SeriesMetadata {
            thread_id,
            segment,
            backend,
        }
    }
}
