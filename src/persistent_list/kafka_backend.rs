use crate::persistent_list::{inner::*, Error};
pub use rdkafka::consumer::{base_consumer::BaseConsumer, Consumer};
use rdkafka::{
    config::ClientConfig,
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    Message,
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
    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error> {
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(KAFKA_TOPIC).payload(bytes);
        let dur = Duration::from_secs(0);
        let stat = async_std::task::block_on(self.producer.send(to_send, dur));
        match stat {
            Ok(x) => Ok(PersistentHead {
                partition: x.0.try_into().unwrap(),
                offset: x.1.try_into().unwrap(),
            }),
            Err((err, _)) => Err(err.into()),
        }
    }
}

pub struct KafkaReader {
    consumer: BaseConsumer,
    timeout: Timeout,
    local_copy: Vec<u8>,
    current_head: Option<PersistentHead>,
}

impl KafkaReader {
    pub fn new(consumer: BaseConsumer) -> Self {
        KafkaReader {
            consumer,
            timeout: Timeout::After(Duration::from_secs(0)),
            local_copy: Vec::new(),
            current_head: None,
        }
    }
}

impl ChunkReader for KafkaReader {
    fn read(&mut self, persistent: PersistentHead, local: BufferHead) -> Result<&[u8], Error> {
        // If the current head is unset or is different from what we have loaded, request from
        // Kafka
        {
            let partition: i32 = persistent.partition.try_into().unwrap();
            let offset: i64 = persistent.offset.try_into().unwrap();
            if self.current_head.is_none() || *self.current_head.as_ref().unwrap() != persistent {
                let mut tp_list = TopicPartitionList::new();
                tp_list
                    .add_partition_offset(KAFKA_TOPIC, partition, Offset::Offset(offset))
                    .unwrap();
                self.consumer.assign(&tp_list)?;
                let msg = self.consumer.poll(self.timeout).unwrap()?;
                let payload = msg.payload().unwrap();
                self.local_copy.clear();
                self.local_copy.extend_from_slice(payload);
                self.current_head = Some(persistent);
            }
        }
        Ok(&self.local_copy[local.offset..local.offset + local.size])
    }
}
