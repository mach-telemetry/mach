mod chunk;

use crate::backend::{
    fs::{FILE_HEADER_SZ, FILE_TAIL_SZ},
    kafka::{KAFKA_HEADER_SZ, KAFKA_TAIL_SZ},
};

pub use chunk::*;

pub type FileChunk = Chunk<FILE_HEADER_SZ, FILE_TAIL_SZ>;
pub type WriteFileChunk = WriteChunk<FILE_HEADER_SZ, FILE_TAIL_SZ>;

pub type KafkaChunk = Chunk<KAFKA_HEADER_SZ, KAFKA_TAIL_SZ>;
pub type WriteKafkaChunk = WriteChunk<KAFKA_HEADER_SZ, KAFKA_TAIL_SZ>;
