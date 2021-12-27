//mod inner;
//mod kafka_writer;
//mod vector_writer;
//mod memory;
//mod chunk_trait;
//mod vector_list;
mod list_head;

//use vector_writer::*;

#[cfg(not(test))]
pub const FLUSH_THRESHOLD: usize = 1_000_000; // 1MB threshold

#[cfg(test)]
pub const FLUSH_THRESHOLD: usize = 100; // 1MB threshold

#[derive(Debug)]
pub enum Error {
    InconsistentRead,
}

//trait ChunkWriter {
//    fn write(&mut self, bytes: &[u8]) -> Result<(i32, i64), Error>;
//}
//
//trait ChunkReader {
//    fn read(
//        &self,
//        partition: usize,
//        offset: usize,
//        buf: &mut [u8],
//    ) -> Result<(), Error>;
//}

//use inner::*;
////use kafka_writer::*;
//
//use crate::{
//    compression::{ByteBuffer, Compression, DecompressBuffer},
//    segment::FullSegment,
//    utils::wp_lock::*,
//};
//use rdkafka::error::KafkaError;
//use std::sync::Arc;
////use bincode::*;
////use serde::*;
//
//
//impl From<KafkaError> for Error {
//    fn from(item: KafkaError) -> Self {
//        Error::Kafka(item)
//    }
//}
//
//
//pub struct ReadSegment {
//    inner: InnerReadNode,
//    decompress_buf: DecompressBuffer,
//}
//
//impl ReadSegment {
//    fn next_segment<R: ChunkReader>(self, reader: &R) -> Result<Option<Self>, Error> {
//        let inner = self.inner.next(reader);
//        match inner {
//            Ok(Some(inner)) => Ok(Some(ReadSegment { inner, ..self })),
//            Ok(None) => Ok(None),
//            Err(x) => Err(x),
//        }
//    }
//}
//
//pub struct Head {
//    inner: Arc<WpLock<InnerHead>>,
//    compression: Compression,
//}
//
//impl Head {
//    pub fn new(compression: Compression) -> Self {
//        Head {
//            inner: Arc::new(WpLock::new(InnerHead::new())),
//            compression,
//        }
//    }
//
//    fn push_bytes<W: ChunkWriter>(&self, bytes: &[u8], writer: &mut W) {
//        let mut write_guard = self.inner.write();
//        write_guard.push_bytes(bytes, writer);
//    }
//
//    pub fn push_segment<W: ChunkWriter>(&self, segment: &FullSegment, writer: &mut W) {
//        let mut write_guard = self.inner.write();
//        write_guard.push_segment(segment, self.compression, writer);
//    }
//
//    pub fn read<R: ChunkReader>(&self, reader: &R) -> Result<Option<ReadSegment>, Error> {
//        let read_guard = unsafe { self.inner.read() };
//        let res = match read_guard.read_segment(reader) {
//            Ok(Some(inner)) => Ok(Some(ReadSegment {
//                inner,
//                decompress_buf: DecompressBuffer::new(),
//            })),
//            Ok(None) => Ok(None),
//            Err(Error::InconsistentRead) => Err(Error::InconsistentRead),
//            _ => unimplemented!(),
//        };
//        match read_guard.release() {
//            Ok(_) => res,
//            Err(_) => Err(Error::InconsistentRead),
//        }
//    }
//}
//
//
//#[cfg(test)]
//mod test {
//    use super::vector_writer::VectorWriter;
//    use super::*;
//
//    #[test]
//    fn test_push_read() {
//        let mut vec_writer = VectorWriter::new().unwrap();
//        let head = Head::new(Compression::LZ4(1));
//
//        let data: Vec<Vec<u8>> = (0..5).map(|x| vec![x; 15]).collect();
//        println!("data: {:?}", data);
//        data.iter()
//            .for_each(|x| head.push_bytes(x.as_slice(), &mut vec_writer));
//
//        let vec_reader = vec_writer.reader();
//        let read_segment = head.read(&vec_reader).unwrap().unwrap();
//        println!("read result: {:?}", read_segment.inner.bytes());
//        let read_segment = read_segment.next_segment(&vec_reader).unwrap().unwrap();
//        println!("{:?}", read_segment.inner.bytes());
//        let read_segment = read_segment.next_segment(&vec_reader).unwrap().unwrap();
//        println!("{:?}", read_segment.inner.bytes());
//        //let read_segment = read_segment.next_segment(&vec_reader).unwrap().unwrap();
//        //println!("{:?}", read_segment.inner.bytes());
//        //let read_segment = read_segment.next_segment(&vec_reader).unwrap().unwrap();
//        //println!("{:?}", read_segment.inner.bytes());
//    }
//}
