use rdkafka::{
    config::ClientConfig,
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    Message,
};
pub use rdkafka::{
    consumer::{base_consumer::BaseConsumer, Consumer},
    util::Timeout,
};
use std::{
    time::Duration,
};
use crate::utils::wp_lock::{WpLock, WriteGuard};

#[derive(Debug)]
pub enum Error {
    Kafka(KafkaError),
    KafkaPollTimeout,
    Bincode(bincode::Error),
    ReadVersion,
    MultipleWriters,
    InvalidMagic,
    Tags(crate::tags::Error),
}

impl From<KafkaError> for Error {
    fn from(item: KafkaError) -> Self {
        Error::Kafka(item)
    }
}

impl From<bincode::Error> for Error {
    fn from(item: bincode::Error) -> Self {
        Error::Bincode(item)
    }
}


pub const KAFKA_TOPIC: &str = "MACHSTORAGE";
pub const KAFKA_BOOTSTRAP: &str = "localhost:29092";

struct KafkaWriter {
    producer: FutureProducer,
}

impl KafkaWriter {
    pub fn new() -> Result<Self, Error> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", KAFKA_BOOTSTRAP)
            .create()?;
        Ok(KafkaWriter { producer })
    }

    pub fn write(&mut self, bytes: &[u8]) -> Result<(i32, i64), Error> {
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(KAFKA_TOPIC).payload(bytes);
        let dur = Duration::from_secs(0);
        let stat = async_std::task::block_on(self.producer.send(to_send, dur));
        match stat {
            Ok(x) => Ok(x),
            Err((err, _)) => Err(err.into()),
        }
    }
}

struct InnerHead {
    kafka_partition: usize,
    kafka_offset: usize,
    bytes_offset: usize,
}

struct Inner {
    last_kafka_partition: usize,
    last_kafka_offset: usize,
    buf: Vec<u8>,
}

impl Inner {
    fn push(&mut self, data: &[u8], current_head: &WpLock<InnerHead>) {
        let len = self.buf.len();
        if len >= 1_000_000 {
            // write to kafka
        } else {
            self.buf.extend_from_slice(data);
            let mut guard = current_head.write();
            guard.kafka_partition = usize::MAX;
            guard.kafka_offset = usize::MAX;
            guard.bytes_offset = len;
        }
    }
}
