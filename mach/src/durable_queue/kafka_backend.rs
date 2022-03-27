use crate::{
    constants::*,
    id::SeriesId,
    runtime::RUNTIME,
    utils::random_id,
};
pub use rdkafka::consumer::{base_consumer::BaseConsumer, Consumer};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    Message,
    error::KafkaError as RdKafkaError,
    types::RDKafkaErrorCode
};
use std::{
    convert::TryInto,
    sync::Arc,
    time::{Duration, Instant},
};
use serde::*;

pub enum Error {
    Kafka(RdKafkaError),
    KafkaErrorCode((String, RDKafkaErrorCode)),
}

impl From<RdKafkaError> for Error {
    fn from(item: RdKafkaError) -> Self {
        Error::Kafka(item)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Kafka {
    bootstrap_servers: String,
    topic: String,
}

impl Kafka {
    pub fn new(bootstrap_servers: &str, topic: &str) -> Result<Self, Error> {
        //create_topic(bootstrap_servers, topic)?;
        let topic = topic.into();
        let bootstrap_servers = bootstrap_servers.into();
        Ok(Self {
            bootstrap_servers,
            topic,
        })
    }

    pub fn make_writer(&self) -> Result<KafkaWriter, Error> {
        KafkaWriter::new(self.bootstrap_servers.clone(), self.topic.clone())
    }

    pub fn make_reader(&self) -> Result<KafkaReader, Error> {
        KafkaReader::new(self.bootstrap_servers.clone(), self.topic.clone())
    }
}

pub struct KafkaWriter {
    producer: FutureProducer,
    topic: String,
    dur: Duration,
}

fn default_producer(bootstraps: String) -> Result<FutureProducer, Error> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstraps)
        .set("message.max.bytes", "1000000000")
        .set("linger.ms", "0")
        .set("message.copy.max.bytes", "5000000")
        .set("batch.num.messages", "1")
        .set("compression.type", "none")
        .set("acks", "all")
        .create()?;
    Ok(producer)
}

impl KafkaWriter {
    pub fn new(kafka_bootstrap: String, topic: String) -> Result<Self, Error> {
        let producer = default_producer(kafka_bootstrap)?;
        let dur = Duration::from_secs(0);
        Ok(Self {
            producer,
            topic,
            dur,
        })
    }

    pub fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(&self.topic).payload(bytes);
        let (partition, offset) = RUNTIME
            .block_on(self.producer.send(to_send, self.dur))
            .unwrap();
        assert_eq!(partition, 0);
        Ok(offset.try_into().unwrap())
    }
}

pub struct KafkaReader {
    consumer: BaseConsumer,
    timeout: Timeout,
    local_buffer: Vec<u8>,
    topic: String,
}

impl KafkaReader {
    pub fn new(bootstrap_servers: String, topic: String) -> Result<Self, Error> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", random_id())
            .create()?;
        let topic = topic.into();
        Ok(KafkaReader {
            consumer,
            topic,
            timeout: Timeout::After(Duration::from_secs(0)),
            local_buffer: Vec::new(),
        })
    }

    pub fn read(&mut self, at: u64) -> Result<&[u8], Error> {
        self.local_buffer.clear();
        let offset: i64 = at.try_into().unwrap();
        let mut tp_list = TopicPartitionList::new();
        let offset = Offset::Offset(offset);
        tp_list
            .add_partition_offset(&self.topic, 0, offset)
            .unwrap();
        self.consumer.assign(&tp_list)?;
        let msg = loop {
            match self.consumer.poll(self.timeout) {
                Some(Ok(x)) => break x,
                Some(Err(x)) => return Err(x.into()),
                None => {}
            };
        };
        self.local_buffer.extend_from_slice(msg.payload().unwrap());
        Ok(self.local_buffer.as_slice())
    }
}
