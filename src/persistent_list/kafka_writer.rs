use crate::persistent_list::{ChunkReader, ChunkWriter, Error};
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
use std::{convert::TryInto, time::Duration};

const KAFKA_TOPIC: &str = "MACHSTORAGE";
const KAFKA_BOOTSTRAP: &str = "localhost:29092";

pub struct KafkaWriter {
    producer: FutureProducer,
}

impl KafkaWriter {
    pub fn new() -> Result<Self, Error> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", KAFKA_BOOTSTRAP)
            .create()?;
        Ok(KafkaWriter { producer })
    }
}

impl ChunkWriter for KafkaWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<(i32, i64), Error> {
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(KAFKA_TOPIC).payload(bytes);
        let dur = Duration::from_secs(0);
        let stat = async_std::task::block_on(self.producer.send(to_send, dur));
        match stat {
            Ok(x) => Ok(x),
            Err((err, _)) => Err(err.into()),
        }
    }
}

pub struct KafkaReader {
    consumer: BaseConsumer,
    timeout: Timeout,
}

impl KafkaReader {
    pub fn new(consumer: BaseConsumer, timeout: Timeout) -> Self {
        KafkaReader { consumer, timeout }
    }
}

impl ChunkReader for KafkaReader {
    fn read(&self, partition: i32, offset: i64, buf: &mut [u8]) -> Result<(), Error> {
        let mut tp_list = TopicPartitionList::new();
        tp_list.add_partition_offset(KAFKA_TOPIC, partition, Offset::Offset(offset))?;
        self.consumer.assign(&tp_list).unwrap();
        let res = self.consumer.poll(self.timeout.clone());
        let payload: &[u8] = match &res {
            None => return Err(Error::KafkaPollTimeout),
            Some(Err(x)) => return Err(x.clone().into()),
            Some(Ok(msg)) => msg.payload().unwrap(), // we expect there to always be a payload
        };
        buf[..payload.len()].copy_from_slice(payload);
        Ok(())
    }
}
