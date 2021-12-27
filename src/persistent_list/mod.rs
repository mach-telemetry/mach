mod inner;
mod kafka_backend;
mod vector_backend;

use rdkafka::error::KafkaError;

#[cfg(not(test))]
pub const FLUSH_THRESHOLD: usize = 1_000_000; // 1MB threshold

#[cfg(test)]
pub const FLUSH_THRESHOLD: usize = 100; // 1MB threshold

#[derive(Debug)]
pub enum Error {
    InconsistentRead,
    Kafka(KafkaError),
}

impl From<KafkaError> for Error {
    fn from(item: KafkaError) -> Self {
        Error::Kafka(item)
    }
}

pub use inner::{Buffer, List, ListReader};
pub use kafka_backend::{KafkaReader, KafkaWriter};
pub use vector_backend::{VectorReader, VectorWriter};

#[cfg(test)]
mod test {
    use super::*;
    use crate::persistent_list::{inner::*, vector_backend::*};
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_single() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut vec_writer = VectorWriter::new(vec.clone());
        let mut vec_reader = VectorReader::new(vec.clone());
        let buffer = Buffer::new();
        let list = List::new(buffer.clone());
        let data: Vec<Vec<u8>> = (0..5).map(|x| vec![x; 15]).collect();
        data.iter()
            .for_each(|x| list.push_bytes(x.as_slice(), &mut vec_writer));

        let mut reader = list.reader().unwrap();
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data[4], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data[3], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data[2], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data[1], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data[0], res);
    }

    #[test]
    fn test_many() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut vec_writer = VectorWriter::new(vec.clone());
        let mut vec_reader = VectorReader::new(vec.clone());
        let buffer = Buffer::new();
        let list1 = List::new(buffer.clone());
        let list2 = List::new(buffer.clone());

        let data1: Vec<Vec<u8>> = (0..5).map(|x| vec![x; 15]).collect();
        let data2: Vec<Vec<u8>> = (5..10).map(|x| vec![x; 15]).collect();

        list1.push_bytes(data1[0].as_slice(), &mut vec_writer);
        list1.push_bytes(data1[1].as_slice(), &mut vec_writer);
        list2.push_bytes(data2[0].as_slice(), &mut vec_writer);
        // Should have flushed in the last push
        list1.push_bytes(data1[2].as_slice(), &mut vec_writer);
        list2.push_bytes(data2[1].as_slice(), &mut vec_writer);
        list2.push_bytes(data2[2].as_slice(), &mut vec_writer);
        // Should have flushed in the last push
        list1.push_bytes(data1[3].as_slice(), &mut vec_writer);
        list1.push_bytes(data1[4].as_slice(), &mut vec_writer);
        list2.push_bytes(data2[3].as_slice(), &mut vec_writer);
        // Should have flushed in the last push
        list2.push_bytes(data2[4].as_slice(), &mut vec_writer);

        let mut reader = list1.reader().unwrap();
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data1[4], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data1[3], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data1[2], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data1[1], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data1[0], res);

        let mut reader = list2.reader().unwrap();
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data2[4], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data2[3], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data2[2], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data2[1], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data2[0], res);
    }
}
