mod file_backend;
mod inner;
mod kafka_backend;
mod vector_backend;
//mod kafka_hybrid_backend;

use rdkafka::error::KafkaError;

#[derive(Debug)]
pub enum Error {
    InconsistentRead,
    Kafka(KafkaError),
    IO(std::io::Error),
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

pub use file_backend::{FileReader, FileWriter};
pub use inner::{Buffer, ChunkReader, ChunkWriter, List, ListReader};
pub use kafka_backend::{KafkaReader, KafkaWriter};
pub use vector_backend::{VectorReader, VectorWriter};

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        compression::*,
        persistent_list::{inner::*, vector_backend::*},
        segment::*,
        tags::*,
        test_utils::*,
    };
    use std::env;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn test_multiple<R: ChunkReader, W: ChunkWriter>(mut chunk_reader: R, mut chunk_writer: W) {
        let buffer = Buffer::new(100);
        let list1 = List::new(buffer.clone());
        let list2 = List::new(buffer.clone());

        let data1: Vec<Vec<u8>> = (0..5).map(|x| vec![x; 15]).collect();
        let data2: Vec<Vec<u8>> = (5..10).map(|x| vec![x; 15]).collect();

        list1.push_bytes(data1[0].as_slice(), &mut chunk_writer);
        list1.push_bytes(data1[1].as_slice(), &mut chunk_writer);
        list2.push_bytes(data2[0].as_slice(), &mut chunk_writer);
        // Should have flushed in the last push
        list1.push_bytes(data1[2].as_slice(), &mut chunk_writer);
        list2.push_bytes(data2[1].as_slice(), &mut chunk_writer);
        list2.push_bytes(data2[2].as_slice(), &mut chunk_writer);
        // Should have flushed in the last push
        list1.push_bytes(data1[3].as_slice(), &mut chunk_writer);
        list1.push_bytes(data1[4].as_slice(), &mut chunk_writer);
        list2.push_bytes(data2[3].as_slice(), &mut chunk_writer);
        list2.push_bytes(data2[4].as_slice(), &mut chunk_writer);

        let mut reader = list1.reader().unwrap();
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

        let mut reader = list2.reader().unwrap();
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

    fn test_single<R: ChunkReader, W: ChunkWriter>(mut chunk_reader: R, mut chunk_writer: W) {
        let buffer = Buffer::new(100);
        let list = List::new(buffer.clone());
        let data: Vec<Vec<u8>> = (0..5).map(|x| vec![x; 15]).collect();
        data.iter()
            .for_each(|x| list.push_bytes(x.as_slice(), &mut chunk_writer));

        let mut reader = list.reader().unwrap();
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

    fn test_sample_data<R: ChunkReader, W: ChunkWriter>(
        mut persistent_reader: R,
        mut persistent_writer: W,
    ) {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();

        let mut tags = Tags::new();
        tags.insert((String::from("A"), String::from("1")));
        tags.insert((String::from("B"), String::from("2")));

        let compression = Compression::LZ4(1);

        let buffer = Buffer::new(6000);
        let list = List::new(buffer.clone());

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

        let mut reader = list.reader().unwrap();
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

    #[test]
    fn test_kafka_bytes() {
        if env::var("KAFKA").is_ok() {
            let kafka_writer = KafkaWriter::new().unwrap();
            let kafka_reader = KafkaReader::new().unwrap();
            test_multiple(kafka_reader, kafka_writer);
        }
    }

    #[test]
    fn test_vec_simple() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut persistent_writer = VectorWriter::new(vec.clone());
        let mut persistent_reader = VectorReader::new(vec.clone());
        test_single(persistent_reader, persistent_writer);
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

    #[test]
    fn test_kafka_data() {
        if env::var("KAFKA").is_ok() {
            let mut persistent_writer = KafkaWriter::new().unwrap();
            let mut persistent_reader = KafkaReader::new().unwrap();
            test_sample_data(persistent_reader, persistent_writer);
        }
    }
}
