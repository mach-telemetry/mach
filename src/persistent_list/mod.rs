mod file_backend;
mod inner;
pub mod inner2;
#[cfg(feature="kafka-backend")]
mod kafka2_backend;
#[cfg(feature="redis-backend")]
mod redis_backend;
mod vector_backend;

#[cfg(any(feature="redis-backend", feature="kafka-backend"))]
use rdkafka::error::KafkaError;

#[cfg(feature="redis-backend")]
use redis::RedisError;

#[derive(Debug)]
pub enum Error {
    InconsistentRead,

    #[cfg(any(feature="redis-backend", feature="kafka-backend"))]
    Kafka(KafkaError),

    #[cfg(feature="redis-backend")]
    Redis(RedisError),
    IO(std::io::Error),
    FactoryError,
    MultipleWriters,
}

#[cfg(any(feature="redis-backend", feature="kafka-backend"))]
impl From<KafkaError> for Error {
    fn from(item: KafkaError) -> Self {
        Error::Kafka(item)
    }
}

#[cfg(feature="redis-backend")]
impl From<RedisError> for Error {
    fn from(item: RedisError) -> Self {
        Error::Redis(item)
    }
}


impl From<std::io::Error> for Error {
    fn from(item: std::io::Error) -> Self {
        Error::IO(item)
    }
}

pub use file_backend::{FileBackend, FileReader, FileWriter};
pub use vector_backend::{VectorBackend, VectorReader, VectorWriter};
#[cfg(feature="redis-backend")]
pub use redis_backend::{RedisBackend, RedisReader, RedisWriter};
#[cfg(feature="kafka-backend")]
pub use kafka2_backend::{KafkaBackend, KafkaReader, KafkaWriter};

pub use inner2::{ListBuffer, Buffer, ChunkReader, ChunkWriter, List, ListReader, ListWriter};

pub trait BackendOld {
    type Writer: ChunkWriter + 'static;
    type Reader: ChunkReader + 'static;
    fn make_backend(&mut self) -> Result<(Self::Writer, Self::Reader), Error>;
}

// One Backend per writer thread. Single writer, multiple readers
pub trait PersistentListBackend: Sized {
    type Writer: inner2::ChunkWriter + 'static;
    type Reader: inner2::ChunkReader + 'static;
    fn writer(&self) -> Result<Self::Writer, Error>;
    fn reader(&self) -> Result<Self::Reader, Error>;
}

pub struct Backend<T: PersistentListBackend> {
    backend: T,
    writer: Option<T::Writer>,
}

impl<T: PersistentListBackend> Backend<T> {
    pub fn new(backend: T) -> Result<Self, Error> {
        let writer = Some(backend.writer()?);
        Ok(Self { backend, writer })
    }

    pub fn writer(&mut self) -> Result<T::Writer, Error> {
        self.writer.take().ok_or(Error::MultipleWriters)
    }

    pub fn reader(&self) -> Result<T::Reader, Error> {
        self.backend.reader()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        compression::*,
        constants::*,
        persistent_list::{inner::*, vector_backend::*},
        segment::*,
        tags::*,
        test_utils::*,
        utils::wp_lock::WpLock,
    };
    use std::env;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;
    use dashmap::DashMap;

    fn test_multiple<R: inner2::ChunkReader, W: inner2::ChunkWriter>(
        mut chunk_reader: R,
        mut chunk_writer: W,
    ) {
        let buffer = inner2::ListBuffer::new(100);
        let list1 = inner2::List::new(buffer.clone());
        let list2 = inner2::List::new(buffer.clone());

        let mut list1_writer = list1.writer();
        let mut list2_writer = list2.writer();

        let data1: Vec<Vec<u8>> = (0..5).map(|x| vec![x; 15]).collect();
        let data2: Vec<Vec<u8>> = (5..10).map(|x| vec![x; 15]).collect();

        list1_writer.push_bytes(data1[0].as_slice(), &mut chunk_writer);
        list1_writer.push_bytes(data1[1].as_slice(), &mut chunk_writer);
        list2_writer.push_bytes(data2[0].as_slice(), &mut chunk_writer);
        // Should have flushed in the last push
        list1_writer.push_bytes(data1[2].as_slice(), &mut chunk_writer);
        list2_writer.push_bytes(data2[1].as_slice(), &mut chunk_writer);
        list2_writer.push_bytes(data2[2].as_slice(), &mut chunk_writer);
        // Should have flushed in the last push
        list1_writer.push_bytes(data1[3].as_slice(), &mut chunk_writer);
        list1_writer.push_bytes(data1[4].as_slice(), &mut chunk_writer);
        list2_writer.push_bytes(data2[3].as_slice(), &mut chunk_writer);
        list2_writer.push_bytes(data2[4].as_slice(), &mut chunk_writer);

        let mut reader = list1.read().unwrap();
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data1[4], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data1[3], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data1[2], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data1[1], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data1[0], res);

        // Guarantee that reads are available on the source (e.g. redis has async flush)
        std::thread::sleep(std::time::Duration::from_millis(500));
        let mut reader = list2.read().unwrap();
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data2[4], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data2[3], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data2[2], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data2[1], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data2[0], res);
    }

    fn test_single<R: inner2::ChunkReader, W: inner2::ChunkWriter>(
        mut chunk_reader: R,
        mut chunk_writer: W,
    ) {
        let buffer = inner2::ListBuffer::new(100);
        let list = inner2::List::new(buffer.clone());
        let mut writer = list.writer();
        let data: Vec<Vec<u8>> = (0..5).map(|x| vec![x; 15]).collect();
        data.iter()
            .for_each(|x| writer.push_bytes(x.as_slice(), &mut chunk_writer));

        let mut reader = list.read().unwrap();
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data[4], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data[3], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data[2], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data[1], res);
        let res = reader.next_bytes(&mut chunk_reader).unwrap().unwrap();
        assert_eq!(data[0], res);
    }

    fn test_sample_data<R: inner2::ChunkReader, W: inner2::ChunkWriter>(
        mut persistent_reader: R,
        mut persistent_writer: W,
    ) {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();

        let mut tags = Tags::new();
        tags.insert((String::from("A"), String::from("1")));
        tags.insert((String::from("B"), String::from("2")));

        let compression = Compression::LZ4(1);

        let buffer = inner2::ListBuffer::new(6000);
        let l = inner2::List::new(buffer.clone());
        let mut list = l.writer();

        let segment = Segment::new(1, nvars);
        let mut writer = segment.writer().unwrap();

        let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
            let mut values = vec![[0u8; 8]; nvars];
            for (i, v) in items.iter().enumerate() {
                values[i] = v.to_be_bytes();
            }
            values
        };

        // This set of pushes stays in the buffer
        for item in &data[..256] {
            let v = to_values(&item.values[..]);
            assert!(writer.push(item.ts, &v[..]).is_ok());
        }
        let flusher = writer.flush();
        let seg: FullSegment = flusher.to_flush().unwrap();
        list.push_segment(&seg, &tags, &compression, &mut persistent_writer);
        flusher.flushed();

        // This next set of pushes **should** result in a flush
        for item in &data[256..512] {
            let v = to_values(&item.values[..]);
            assert!(writer.push(item.ts, &v[..]).is_ok());
        }
        let flusher = writer.flush();
        let seg: FullSegment = flusher.to_flush().unwrap();
        list.push_segment(&seg, &tags, &compression, &mut persistent_writer);
        flusher.flushed();

        // This next set of pushes won't result in a flush
        for item in &data[512..768] {
            let v = to_values(&item.values[..]);
            assert!(writer.push(item.ts, &v[..]).is_ok());
        }

        let flusher = writer.flush();
        let seg: FullSegment = flusher.to_flush().unwrap();
        list.push_segment(&seg, &tags, &compression, &mut persistent_writer);
        flusher.flushed();

        let mut exp_ts: Vec<u64> = Vec::new();
        let mut exp_values: Vec<Vec<[u8; 8]>> = Vec::new();
        for _ in 0..nvars {
            exp_values.push(Vec::new());
        }

        let mut reader = l.read().unwrap();
        let res: &DecompressBuffer = reader
            .next_segment(&mut persistent_reader)
            .unwrap()
            .unwrap();
        for item in &data[512..768] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            v.iter()
                .zip(exp_values.iter_mut())
                .for_each(|(v, e)| e.push(*v));
        }
        assert_eq!(res.timestamps(), exp_ts.as_slice());
        exp_values
            .iter()
            .enumerate()
            .for_each(|(i, v)| assert_eq!(res.variable(i), v));
        exp_ts.clear();
        exp_values.iter_mut().for_each(|e| e.clear());

        let res: &DecompressBuffer = reader
            .next_segment(&mut persistent_reader)
            .unwrap()
            .unwrap();
        for item in &data[256..512] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            v.iter()
                .zip(exp_values.iter_mut())
                .for_each(|(v, e)| e.push(*v));
        }
        assert_eq!(res.timestamps(), exp_ts.as_slice());
        exp_values
            .iter()
            .enumerate()
            .for_each(|(i, v)| assert_eq!(res.variable(i), v));
        exp_ts.clear();
        exp_values.iter_mut().for_each(|e| e.clear());

        let res: &DecompressBuffer = reader
            .next_segment(&mut persistent_reader)
            .unwrap()
            .unwrap();
        for item in &data[0..256] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            v.iter()
                .zip(exp_values.iter_mut())
                .for_each(|(v, e)| e.push(*v));
        }
        assert_eq!(res.timestamps(), exp_ts.as_slice());
        exp_values
            .iter()
            .enumerate()
            .for_each(|(i, v)| assert_eq!(res.variable(i), v));
        exp_ts.clear();
        exp_values.iter_mut().for_each(|e| e.clear());
    }

    #[cfg(features="kafka-backend")]
    #[test]
    fn test_kafka_bytes() {
        let kafka_writer = kafka2_backend::KafkaWriter::new(KAFKA_BOOTSTRAP).unwrap();
        let kafka_reader = kafka2_backend::KafkaReader::new(KAFKA_BOOTSTRAP).unwrap();
        test_multiple(kafka_reader, kafka_writer);
    }

    #[test]
    fn test_vec_simple() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut persistent_writer = VectorWriter::new(vec.clone());
        let mut persistent_reader = VectorReader::new(vec.clone());
        test_single(persistent_reader, persistent_writer);
    }

    #[cfg(features="redis-backend")]
    #[test]
    fn test_redis_simple() {
        let client = redis::Client::open(REDIS_ADDR).unwrap();
        let map = Arc::new(DashMap::new());
        let mut con = client.get_connection().unwrap();
        let mut persistent_writer = RedisWriter::new(con, map.clone());

        let client = redis::Client::open(REDIS_ADDR).unwrap();
        let mut con = client.get_connection().unwrap();
        let mut persistent_reader = RedisReader::new(con, map.clone());
        test_single(persistent_reader, persistent_writer);
    }

    #[cfg(features="redis-backend")]
    #[test]
    fn test_redis_multiple() {
        let client = redis::Client::open(REDIS_ADDR).unwrap();
        let map = Arc::new(DashMap::new());
        let mut con = client.get_connection().unwrap();
        let mut persistent_writer = RedisWriter::new(con, map.clone());

        let client = redis::Client::open(REDIS_ADDR).unwrap();
        let mut con = client.get_connection().unwrap();
        let mut persistent_reader = RedisReader::new(con, map.clone());
        test_multiple(persistent_reader, persistent_writer);
    }

    #[test]
    fn test_vec_multiple() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut persistent_writer = VectorWriter::new(vec.clone());
        let mut persistent_reader = VectorReader::new(vec.clone());
        test_multiple(persistent_reader, persistent_writer);
    }

    #[test]
    fn test_file_multiple() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_path");
        let mut persistent_writer = FileWriter::new(&file_path).unwrap();
        let mut persistent_reader = FileReader::new(&file_path).unwrap();
        test_multiple(persistent_reader, persistent_writer);
    }

    #[test]
    fn test_vec_data() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut persistent_writer = VectorWriter::new(vec.clone());
        let mut persistent_reader = VectorReader::new(vec.clone());
        test_sample_data(persistent_reader, persistent_writer);
    }

    #[test]
    fn test_file_data() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_path");
        let mut persistent_writer = FileWriter::new(&file_path).unwrap();
        let mut persistent_reader = FileReader::new(&file_path).unwrap();
        test_sample_data(persistent_reader, persistent_writer);
    }

    #[cfg(features="redis-backend")]
    #[test]
    fn test_redis_data() {
        let client = redis::Client::open(REDIS_ADDR).unwrap();
        let map = Arc::new(DashMap::new());
        let mut con = client.get_connection().unwrap();
        let mut persistent_writer = RedisWriter::new(con, map.clone());

        let client = redis::Client::open(REDIS_ADDR).unwrap();
        let mut con = client.get_connection().unwrap();
        let mut persistent_reader = RedisReader::new(con, map.clone());
        test_sample_data(persistent_reader, persistent_writer);
    }

    #[cfg(features="kafka-backend")]
    #[test]
    fn test_kafka_data() {
        let mut persistent_writer = KafkaWriter::new(0).unwrap();
        let mut persistent_reader = KafkaReader::new().unwrap();
        test_sample_data(persistent_reader, persistent_writer);
    }
}
