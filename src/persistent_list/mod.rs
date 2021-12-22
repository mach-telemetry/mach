mod inner;
mod kafka_writer;
mod vector_writer;

use inner::*;
use kafka_writer::*;

use crate::{
    compression::{ByteBuffer, Compression, DecompressBuffer},
    segment::FullSegment,
    utils::wp_lock::*,
};
use rdkafka::error::KafkaError;
use std::sync::Arc;
//use bincode::*;
//use serde::*;

#[derive(Debug)]
pub enum Error {
    Kafka(KafkaError),
    InconsistentRead,
    KafkaPollTimeout,
}

impl From<KafkaError> for Error {
    fn from(item: KafkaError) -> Self {
        Error::Kafka(item)
    }
}

pub const FLUSH_THRESHOLD: usize = 1_000_000; // 1MB threshold

pub struct ReadSegment {
    inner: InnerReadNode,
    decompress_buf: DecompressBuffer,
}

impl ReadSegment {
    fn next_segment<R: ChunkReader>(self, reader: &R) -> Result<Option<Self>, Error> {
        let inner = self.inner.next(reader);
        match inner {
            Ok(Some(inner)) => Ok(Some(ReadSegment { inner, ..self })),
            Ok(None) => Ok(None),
            Err(x) => Err(x),
        }
    }
}

pub struct Head {
    inner: Arc<WpLock<InnerHead>>,
    compression: Compression,
}

impl Head {
    pub fn push_segment<W: ChunkWriter>(&self, segment: &FullSegment, writer: &mut W) {
        let mut write_guard = self.inner.write();
        write_guard.push_segment(segment, self.compression, writer);
    }

    pub fn read<R: ChunkReader>(&self, reader: &R) -> Result<Option<ReadSegment>, Error> {
        let read_guard = unsafe { self.inner.read() };
        let res = match read_guard.read_segment(reader) {
            Ok(Some(inner)) => Ok(Some(ReadSegment {
                inner,
                decompress_buf: DecompressBuffer::new(),
            })),
            Ok(None) => Ok(None),
            Err(Error::InconsistentRead) => Err(Error::InconsistentRead),
            _ => unimplemented!(),
        };
        match read_guard.release() {
            Ok(_) => res,
            Err(_) => Err(Error::InconsistentRead),
        }
    }
}

trait ChunkWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<(i32, i64), Error>;
}

trait ChunkReader {
    fn read(&self, partition: i32, offset: i64, buf: &mut [u8]) -> Result<(), Error>;
}
