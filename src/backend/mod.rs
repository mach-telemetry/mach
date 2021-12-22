pub mod fs;
pub mod kafka;
//pub mod kafka_new;

use crate::{chunk, compression, flush_buffer::FlushEntry, tags};

// Re-export flushentry
//use crate::flush_buffer::FlushEntry;

pub struct ByteEntry<'a> {
    pub mint: u64,
    pub maxt: u64,
    pub bytes: &'a [u8],
}

impl<'a> ByteEntry<'a> {
    pub fn new(mint: u64, maxt: u64, bytes: &'a [u8]) -> Self {
        Self { mint, maxt, bytes }
    }
}

// TODO: Need to store SeriesMetadata with different backends into a single data structure (e.g.
// Hashmap. This is a work around.

#[derive(Clone)]
pub struct FileBackend {
    pub chunk: chunk::FileChunk,
    pub list: fs::FileList,
}

impl FileBackend {
    pub fn new(tags: &tags::Tags, compression: compression::Compression) -> Self {
        let chunk = chunk::FileChunk::new(tags, compression);
        let list = fs::FileList::new();
        Self { chunk, list }
    }
}

#[derive(Clone)]
pub struct KafkaBackend {
    pub chunk: chunk::KafkaChunk,
    pub list: kafka::KafkaList,
}

impl KafkaBackend {
    pub fn new(tags: &tags::Tags, compression: compression::Compression) -> Self {
        let chunk = chunk::KafkaChunk::new(tags, compression);
        let list = kafka::KafkaList::new();
        Self { chunk, list }
    }
}

pub enum Backend {
    File(FileBackend),
    Kafka(KafkaBackend),
}

impl Backend {
    pub fn new_file_backed(tags: &tags::Tags, compression: compression::Compression) -> Self {
        Backend::File(FileBackend::new(tags, compression))
    }

    pub fn file_backend(&self) -> &FileBackend {
        match self {
            Self::File(x) => &x,
            _ => unimplemented!(),
        }
    }

    pub fn new_kafka_backed(tags: &tags::Tags, compression: compression::Compression) -> Self {
        Backend::Kafka(KafkaBackend::new(tags, compression))
    }

    pub fn kafka_backend(&self) -> &KafkaBackend {
        match self {
            Self::Kafka(x) => &x,
            _ => unimplemented!(),
        }
    }
}

//pub enum PushMetadata {
//    FS({
//        offset: u64,
//        file_id: u64,
//        ts_id: u64,
//    }),
//}
