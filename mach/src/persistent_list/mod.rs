mod file_backend;
mod inner;
mod kafka_backend;
mod vector_backend;
use rdkafka::{error::KafkaError, types::RDKafkaErrorCode};
use std::path::PathBuf;

#[derive(Default, Debug, Clone)]
pub struct Config {
    kafka_bootstrap: Option<String>,
    directory: Option<PathBuf>,
}

impl Config {
    pub fn with_kafka_bootstrap(mut self, bootstrap: String) -> Self {
        self.kafka_bootstrap = Some(bootstrap);
        self
    }

    pub fn with_directory(mut self, dir: PathBuf) -> Self {
        self.directory = Some(dir);
        self
    }

    pub fn kafka_bootstrap(&self) -> &Option<String> {
        &self.kafka_bootstrap
    }

    pub fn directory(&self) -> &Option<PathBuf> {
        &self.directory
    }
}

#[derive(Debug)]
pub enum Error {
    InconsistentRead,
    Kafka(KafkaError),
    KafkaErrorCode((String, RDKafkaErrorCode)),
    IO(std::io::Error),
    FactoryError,
    MultipleWriters,
    InvalidConfig(Config),
}

impl From<KafkaError> for Error {
    fn from(item: KafkaError) -> Self {
        Error::Kafka(item)
    }
}

impl From<std::io::Error> for Error {
    fn from(item: std::io::Error) -> Self {
        Error::IO(item)
    }
}

pub use file_backend::{FileBackend, FileReader, FileWriter};
pub use inner::{
    ChunkReader, ChunkWriter, List, ListBuffer, ListSnapshot, ListSnapshotReader, ListWriter,
};
pub use kafka_backend::{KafkaBackend, KafkaReader, KafkaWriter};
pub use vector_backend::{VectorBackend, VectorReader, VectorWriter};

// One Backend per writer thread. Single writer, multiple readers
pub trait PersistentListBackend: Sized {
    type Writer: inner::ChunkWriter + 'static;
    type Reader: inner::ChunkReader + 'static;
    fn id(&self) -> &str;
    fn default_backend() -> Result<Self, Error>;
    fn with_config(conf: Config) -> Result<Self, Error>;
    fn writer(&self) -> Result<Self::Writer, Error>;
    fn reader(&self) -> Result<Self::Reader, Error>;
}

pub trait ListBackend = PersistentListBackend;

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        compression::*, constants::*, id::SeriesId, persistent_list::vector_backend::*, segment::*,
        tags::*, test_utils::*, utils::wp_lock::WpLock, series::Types,
    };
    use dashmap::DashMap;
    use rand::prelude::*;
    use std::collections::HashMap;
    use std::env;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn test_multiple<R: inner::ChunkReader, W: inner::ChunkWriter>(
        mut chunk_reader: R,
        mut chunk_writer: W,
    ) {
        let buffer = inner::ListBuffer::new(100);
        let list1 = inner::List::new(buffer.clone());
        let list2 = inner::List::new(buffer.clone());

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

        let snapshot = list1.read().unwrap();
        let mut reader = ListSnapshotReader::new(snapshot);
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
        let snapshot = list2.read().unwrap();
        let mut reader = ListSnapshotReader::new(snapshot);
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

    fn test_single<R: inner::ChunkReader, W: inner::ChunkWriter>(
        mut chunk_reader: R,
        mut chunk_writer: W,
    ) {
        let buffer = inner::ListBuffer::new(100);
        let list = inner::List::new(buffer.clone());
        let mut writer = list.writer();
        let data: Vec<Vec<u8>> = (0..5).map(|x| vec![x; 15]).collect();
        data.iter()
            .for_each(|x| writer.push_bytes(x.as_slice(), &mut chunk_writer));

        let snapshot = list.read().unwrap();
        let mut reader = ListSnapshotReader::new(snapshot);
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

    fn test_sample_data<R: inner::ChunkReader, W: inner::ChunkWriter>(
        mut persistent_reader: R,
        mut persistent_writer: W,
    ) {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();

        let id = SeriesId(thread_rng().gen());

        let mut compression = Vec::new();
        for _ in 0..nvars {
            compression.push(CompressFn::XOR);
        }
        let compression = Compression::from(compression);

        let buffer = inner::ListBuffer::new(6000);
        let l = inner::List::new(buffer.clone());
        let mut list = l.writer();

        let segment = Segment::new(1, nvars, vec![Types::F64; nvars].as_slice());
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
        list.push_segment(id, &seg, &compression, &mut persistent_writer);
        flusher.flushed();

        // This next set of pushes **should** result in a flush
        for item in &data[256..512] {
            let v = to_values(&item.values[..]);
            assert!(writer.push(item.ts, &v[..]).is_ok());
        }
        let flusher = writer.flush();
        let seg: FullSegment = flusher.to_flush().unwrap();
        list.push_segment(id, &seg, &compression, &mut persistent_writer);
        flusher.flushed();

        // This next set of pushes won't result in a flush
        for item in &data[512..768] {
            let v = to_values(&item.values[..]);
            assert!(writer.push(item.ts, &v[..]).is_ok());
        }

        let flusher = writer.flush();
        let seg: FullSegment = flusher.to_flush().unwrap();
        list.push_segment(id, &seg, &compression, &mut persistent_writer);
        flusher.flushed();

        let mut exp_ts: Vec<u64> = Vec::new();
        let mut exp_values: Vec<Vec<[u8; 8]>> = Vec::new();
        for _ in 0..nvars {
            exp_values.push(Vec::new());
        }

        let snapshot = l.read().unwrap();
        let mut reader = ListSnapshotReader::new(snapshot);
        //let mut reader = l.read().unwrap();
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
            .for_each(|(i, v)| assert_eq!(res.variable(i), (Types::F64, v.as_slice())));
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
            .for_each(|(i, v)| assert_eq!(res.variable(i), (Types::F64, v.as_slice())));
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
            .for_each(|(i, v)| assert_eq!(res.variable(i), (Types::F64, v.as_slice())));
        exp_ts.clear();
        exp_values.iter_mut().for_each(|e| e.clear());
    }

    #[test]
    #[cfg_attr(not(feature = "kafka-backend"), ignore)]
    fn test_kafka_bytes() {
        let topic = format!("test-{}", thread_rng().gen::<usize>());
        let kafka = kafka_backend::KafkaBackend::new(KAFKA_BOOTSTRAP, &topic).unwrap();
        let kafka_writer = kafka.writer().unwrap();
        let kafka_reader = kafka.reader().unwrap();
        test_multiple(kafka_reader, kafka_writer);
    }

    #[test]
    fn test_vec_simple() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut persistent_writer = VectorWriter::new(vec.clone());
        let mut persistent_reader = VectorReader::new(vec.clone());
        test_single(persistent_reader, persistent_writer);
    }

    #[test]
    #[cfg_attr(not(feature = "kafka-backend"), ignore)]
    fn test_kafka_simple() {
        let topic = format!("test-{}", thread_rng().gen::<usize>());
        let kafka = kafka_backend::KafkaBackend::new(KAFKA_BOOTSTRAP, &topic).unwrap();
        let kafka_writer = kafka.writer().unwrap();
        let kafka_reader = kafka.reader().unwrap();
        test_single(kafka_reader, kafka_writer);
    }

    //#[test]
    //#[cfg_attr(not(feature="redis-backend"), ignore)]
    //fn test_redis_simple() {
    //    let client = redis::Client::open(REDIS_ADDR).unwrap();
    //    let map = Arc::new(DashMap::new());
    //    let mut con = client.get_connection().unwrap();
    //    let mut persistent_writer = RedisWriter::new(con, map.clone());

    //    let client = redis::Client::open(REDIS_ADDR).unwrap();
    //    let mut con = client.get_connection().unwrap();
    //    let mut persistent_reader = RedisReader::new(con, map.clone());
    //    test_single(persistent_reader, persistent_writer);
    //}

    //#[test]
    //#[cfg_attr(not(feature="redis-backend"), ignore)]
    //fn test_redis_multiple() {
    //    let client = redis::Client::open(REDIS_ADDR).unwrap();
    //    let map = Arc::new(DashMap::new());
    //    let mut con = client.get_connection().unwrap();
    //    let mut persistent_writer = RedisWriter::new(con, map.clone());

    //    let client = redis::Client::open(REDIS_ADDR).unwrap();
    //    let mut con = client.get_connection().unwrap();
    //    let mut persistent_reader = RedisReader::new(con, map.clone());
    //    test_multiple(persistent_reader, persistent_writer);
    //}

    //#[test]
    //#[cfg_attr(not(feature = "redis-kafka-backend"), ignore)]
    //fn test_redis_kafka_multiple() {
    //    let b = RedisKafkaBackend::new(REDIS_ADDR, KAFKA_BOOTSTRAP);
    //    let mut persistent_writer = b.writer().unwrap();
    //    let mut persistent_reader = b.reader().unwrap();
    //    test_multiple(persistent_reader, persistent_writer);
    //}

    #[test]
    fn test_vec_multiple() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut persistent_writer = VectorWriter::new(vec.clone());
        let mut persistent_reader = VectorReader::new(vec.clone());
        test_multiple(persistent_reader, persistent_writer);
    }

    //#[test]
    //fn test_file_multiple() {
    //    let dir = tempdir().unwrap();
    //    let file_path = dir.path().join("test_path");
    //    let mut persistent_writer = FileWriter::new(&file_path).unwrap();
    //    let mut persistent_reader = FileReader::new(&file_path).unwrap();
    //    test_multiple(persistent_reader, persistent_writer);
    //}

    #[test]
    fn test_vec_data() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut persistent_writer = VectorWriter::new(vec.clone());
        let mut persistent_reader = VectorReader::new(vec.clone());
        test_sample_data(persistent_reader, persistent_writer);
    }

    //#[test]
    //fn test_file_data() {
    //    let dir = tempdir().unwrap();
    //    let file_path = dir.path().join("test_path");
    //    let mut persistent_writer = FileWriter::new(&file_path).unwrap();
    //    let mut persistent_reader = FileReader::new(&file_path).unwrap();
    //    test_sample_data(persistent_reader, persistent_writer);
    //}

    //#[test]
    //#[cfg_attr(not(feature="redis-backend"), ignore)]
    //fn test_redis_data() {
    //    let client = redis::Client::open(REDIS_ADDR).unwrap();
    //    let map = Arc::new(DashMap::new());
    //    let mut con = client.get_connection().unwrap();
    //    let mut persistent_writer = RedisWriter::new(con, map.clone());

    //    let client = redis::Client::open(REDIS_ADDR).unwrap();
    //    let mut con = client.get_connection().unwrap();
    //    let mut persistent_reader = RedisReader::new(con, map.clone());
    //    test_sample_data(persistent_reader, persistent_writer);
    //}

    #[test]
    #[cfg_attr(not(feature = "kafka-backend"), ignore)]
    fn test_kafka_data() {
        let topic = format!("test-{}", thread_rng().gen::<usize>());
        let kafka = kafka_backend::KafkaBackend::new(KAFKA_BOOTSTRAP, &topic).unwrap();
        let kafka_writer = kafka.writer().unwrap();
        let kafka_reader = kafka.reader().unwrap();
        test_sample_data(kafka_reader, kafka_writer);
    }

    //#[test]
    //#[cfg_attr(not(feature = "redis-kafka-backend"), ignore)]
    //fn test_redis_kafka_data() {
    //    let b = RedisKafkaBackend::new(REDIS_ADDR, KAFKA_BOOTSTRAP);
    //    let mut persistent_writer = b.writer().unwrap();
    //    let mut persistent_reader = b.reader().unwrap();
    //    test_sample_data(persistent_reader, persistent_writer);
    //}
}
