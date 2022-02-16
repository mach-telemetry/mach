use crate::{
    constants::*,
    id::{SeriesId, WriterId},
    tags::Tags,
};
use lazy_static::*;
use std::convert::From;
use std::time::Duration;
use serde::*;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    Message,
    error::KafkaError,
    types::RDKafkaErrorCode,
};
use serde::*;

lazy_static! {
    static ref PRODUCER: FutureProducer = {
        init_metadata_topic(KAFKA_BOOTSTRAP).unwrap();
        init_metadata_producer(KAFKA_BOOTSTRAP).unwrap()
    };
}

#[derive(Debug)]
pub enum Error {
    Kafka(KafkaError),
    KafkaErrorCode((String, RDKafkaErrorCode)),
}

impl From<KafkaError> for Error {
    fn from(item: KafkaError) -> Self {
        Error::Kafka(item)
    }
}

#[derive(Serialize, Deserialize)]
pub enum Metadata {
    Mach(String),
    WriterTopic(String, String)
}

impl Metadata {
    pub fn send(&self) -> Result<(), Error> {
        let bytes: Vec<u8> = bincode::serialize(&self).unwrap();
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(MACH_METADATA).payload(bytes.as_slice());
        let dur = Duration::from_secs(0);
        async_std::task::block_on(PRODUCER.send(to_send, dur)).unwrap();
        Ok(())
    }
}

fn init_metadata_topic(bootstrap: &str) -> Result<(), Error> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()?;
    let topic = [NewTopic {
        name: MACH_METADATA,
        num_partitions: 1,
        replication: TopicReplication::Fixed(3),
        config: Vec::new(),
    }];
    let opts = AdminOptions::new();
    let fut = admin.create_topics(&topic, &opts);
    let result = async_std::task::block_on(fut)?;
    if let Err((s, c)) = &result[0] {
        return Err(Error::KafkaErrorCode((s.into(), c.clone())));
    }
    Ok(())
}

fn init_metadata_producer(bootstraps: &str) -> Result<FutureProducer, Error> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstraps)
        .set("message.max.bytes", "2000000")
        .set("linger.ms", "0")
        .set("message.copy.max.bytes", "5000000")
        .set("batch.num.messages", "1")
        .set("compression.type", "none")
        .set("acks", "1")
        //.set("max.in.flight", "1")
        .create()?;
    Ok(producer)
}

//#[derive(Debug)]
//pub enum Error {
//    Redis(RedisError),
//}
//
//impl From<RedisError> for Error {
//    fn from(item: RedisError) -> Self {
//        Error::Redis(item)
//    }
//}
//
//pub struct Metadata {
//    addr: &'static str,
//}
//
//impl Metadata {
//    fn make_conn(&self) -> Result<Connection, Error> {
//        let client = Client::open(self.addr)?;
//        Ok(client.get_connection()?)
//    }
//
//    pub fn set<K: Serialize, V: Serialize>(&self, kvs: &[(K, V)]) -> Result<(), Error> {
//        let mut con = self.make_conn()?;
//        let mut p = redis::pipe();
//        for (k, v) in kvs.iter() {
//            p.cmd("SET").arg(bincode::serialize(k).unwrap()).arg(bincode::serialize(v).unwrap()).ignore();
//        }
//        p.query(&mut con)?;
//        Ok(())
//    }
//
//    pub fn get<K: Serialize, V: Deserialize>(&self, k: K) -> V {
//        let mut con = self.make_conn()?;
//    }
//}
