#![deny(unused_must_use)]
#![feature(get_mut_unchecked)]
#![feature(is_sorted)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
#![feature(cell_update)]
#![feature(box_syntax)]
#![feature(thread_id_value)]
#![allow(clippy::new_without_default)]
#![allow(clippy::len_without_is_empty)]
#![allow(warnings)]

//mod active_block;
//mod active_segment;
//mod types;
//mod block;
//mod compression;
//mod read_set;
//mod segment;
//mod tsdb;
//mod tsdb2;
//mod ids;
//pub mod managed;
//pub mod memseries;
//pub mod active_buffer;
//pub mod active_segment;

mod backend;
mod chunk;
mod compression;
mod flush_buffer;
mod segment;
mod tags;
mod utils;
mod writer;

use std::sync::atomic::AtomicU64;

// TODO: Need to store SeriesMetadata with different backends into a single data structure (e.g.
// Hashmap. This is a work around.

#[derive(Clone)]
struct FileBackend {
    pub chunk: chunk::FileChunk,
    pub list: backend::fs::FileList,
}

impl FileBackend {
    fn new(tags: &tags::Tags, compression: compression::Compression) -> Self {
        let chunk = chunk::FileChunk::new(tags, compression);
        let list = backend::fs::FileList::new();
        Self { chunk, list }
    }
}

#[derive(Clone)]
struct KafkaBackend {
    pub chunk: chunk::KafkaChunk,
    pub list: backend::kafka::KafkaList,
}

impl KafkaBackend {
    fn new(tags: &tags::Tags, compression: compression::Compression) -> Self {
        let chunk = chunk::KafkaChunk::new(tags, compression);
        let list = backend::kafka::KafkaList::new();
        Self { chunk, list }
    }
}

enum Backend {
    File(FileBackend),
    Kafka(KafkaBackend),
}

impl Backend {
    fn new_file_backed(tags: &tags::Tags, compression: compression::Compression) -> Self {
        Backend::File(FileBackend::new(tags, compression))
    }

    fn file_backend(&self) -> &FileBackend {
        match self {
            Self::File(x) => &x,
            _ => unimplemented!(),
        }
    }

    fn new_kafka_backed(tags: &tags::Tags, compression: compression::Compression) -> Self {
        Backend::Kafka(KafkaBackend::new(tags, compression))
    }

    fn kafka_backend(&self) -> &KafkaBackend {
        match self {
            Self::Kafka(x) => &x,
            _ => unimplemented!(),
        }
    }
}

struct SeriesMetadata {
    pub thread_id: AtomicU64,
    pub segment: segment::Segment,
    pub backend: Backend,
}

impl SeriesMetadata {
    fn with_file_backend(
        thread_id: u64,
        nvars: usize,
        buffers: usize,
        tags: &tags::Tags,
        compression: compression::Compression,
    ) -> Self {
        let backend = Backend::new_file_backed(tags, compression);
        let thread_id = AtomicU64::new(thread_id);
        let segment = segment::Segment::new(buffers, nvars);
        SeriesMetadata {
            thread_id,
            segment,
            backend,
        }
    }

    fn with_kafka_backend(
        thread_id: u64,
        nvars: usize,
        buffers: usize,
        tags: &tags::Tags,
        compression: compression::Compression,
    ) -> Self {
        let backend = Backend::new_kafka_backed(tags, compression);
        let thread_id = AtomicU64::new(thread_id);
        let segment = segment::Segment::new(buffers, nvars);
        SeriesMetadata {
            thread_id,
            segment,
            backend,
        }
    }
}

//struct KafkaSeriesMetadata {
//    pub thread_id: AtomicU64,
//    pub segment: segment::Segment,
//    pub chunk: chunk::KafkaChunk,
//    pub list: backend::kafka::KafkaList,
//}

//mod read_set;
//mod active_block;

//pub mod buffer;
//pub mod active_block;
//pub mod ids;

//mod utils;

//pub use block::file::{FileBlockLoader, FileStore, ThreadFileWriter};
//pub use read_set::SeriesReadSet;
//pub use segment::SegmentLike;
//pub use tsdb::{
//    Db, RefId, Sample, SeriesId, SeriesMetadata, SeriesOptions, Writer, WriterMetadata,
//};

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod serial_memseries;
