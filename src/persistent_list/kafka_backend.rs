use crate::{
    constants::*,
    persistent_list::{inner::*, Error},
};
pub use rdkafka::consumer::{base_consumer::BaseConsumer, Consumer};
use rdkafka::{
    config::ClientConfig,
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    Message,
};
use std::{
    convert::TryInto,
    time::{Duration, Instant},
};
use uuid::Uuid;

fn random_id() -> String {
    Uuid::new_v4().to_hyphenated().to_string()
}

pub struct KafkaWriter {
    producer: FutureProducer,
}

impl KafkaWriter {
    pub fn new() -> Result<Self, Error> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", KAFKA_BOOTSTRAP)
            .set("message.max.bytes", "5000000")
            .set("linger.ms", "0")
            .set("batch.num.messages", "1")
            .set("queue.buffering.max.ms", "0")
            .set("queue.buffering.max.messages", "1")
            .set("message.copy.max.bytes", "0")
            //.set("queue.buffering.max.kbytes", "1000000")
            //.set("max.in.flight.requests.per.connection", "1")
            .set("acks", "1")
            .create()?;
        Ok(KafkaWriter { producer })
    }
}

impl ChunkWriter for KafkaWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error> {
        //println!("KAFKA FLUSHING");
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(KAFKA_TOPIC).payload(bytes);
        let sz = bytes.len();
        let dur = Duration::from_secs(0);
        let now = std::time::Instant::now();
        let stat = async_std::task::block_on(self.producer.send(to_send, dur));
        println!("Duration: {:?}", now.elapsed());
        //println!("KAFKA FLUSHED");
        match stat {
            Ok(x) => Ok(PersistentHead {
                partition: x.0.try_into().unwrap(),
                offset: x.1.try_into().unwrap(),
                sz,
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
    pub fn new() -> Result<Self, Error> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", KAFKA_BOOTSTRAP)
            .set("group.id", random_id())
            .create()?;
        Ok(KafkaReader {
            consumer,
            timeout: Timeout::After(Duration::from_secs(0)),
            local_copy: Vec::new(),
            current_head: None,
        })
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
                let start = Instant::now();
                let msg = loop {
                    match self.consumer.poll(self.timeout) {
                        Some(Ok(x)) => break x,
                        Some(Err(x)) => return Err(x.into()),
                        None => {}
                    };
                };
                println!("Kafka duration: {:?}", start.elapsed());
                let payload = msg.payload().unwrap();
                self.local_copy.clear();
                self.local_copy.extend_from_slice(payload);
                self.current_head = Some(persistent);
            }
        }
        Ok(&self.local_copy[local.offset..local.offset + local.size])
    }
}
