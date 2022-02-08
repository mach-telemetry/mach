use crate::{
    constants::KAFKA_TOPIC,
    persistent_list::{inner2, Error, PersistentListBackend},
    tags::Tags,
};
use async_std::channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use rand::prelude::*;
use rdkafka::{
    config::ClientConfig,
    consumer::{base_consumer::BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    Message,
};
use redis::aio::Connection;

use redis::{Commands, RedisError};
use std::{collections::HashMap, sync::Arc, time::Duration};

fn random_id() -> String {
    uuid::Uuid::new_v4().to_hyphenated().to_string()
}

async fn worker(
    mut redis_con: Connection,
    mut kafka_con: FutureProducer,
    transfer_map: Arc<DashMap<u64, Arc<[u8]>>>,
    queue: Receiver<u64>,
) {
    let dur = Duration::from_secs(0);
    while let Ok(key) = queue.recv().await {
        let data = transfer_map.get(&key).unwrap().clone();
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(KAFKA_TOPIC).payload(&data[..]);
        let (partition, offset): (i32, i64) = kafka_con.send(to_send, dur).await.unwrap();
        let _: () = redis::Cmd::new()
            .arg("SET")
            .arg(key)
            .arg((partition, offset))
            .query_async(&mut redis_con)
            .await
            .unwrap();
        transfer_map.remove(&key);
    }
}

pub struct RedisKafkaWriter {
    offset: u32,
    prefix: u32,
    transfer_map: Arc<DashMap<u64, Arc<[u8]>>>,
    sender: Sender<u64>,
}

impl RedisKafkaWriter {
    pub fn new(
        redis: Connection,
        kafka: FutureProducer,
        transfer_map: Arc<DashMap<u64, Arc<[u8]>>>,
    ) -> Self {
        let (sender, receiver) = unbounded();
        let map = transfer_map.clone();
        async_std::task::spawn(worker(redis, kafka, map, receiver));
        Self {
            offset: 0,
            prefix: thread_rng().gen(),
            transfer_map,
            sender,
        }
    }
}

impl inner2::ChunkWriter for RedisKafkaWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        let key = ((self.prefix as u64) << 32) | (self.offset as u64);
        self.offset += 1;
        self.transfer_map.insert(key, bytes.into());
        self.sender.try_send(key).unwrap();
        Ok(key)
    }
}

pub struct RedisKafkaReader {
    redis: Connection,
    kafka: BaseConsumer,
    transfer_map: Arc<DashMap<u64, Arc<[u8]>>>,
    local: Vec<u8>,
}

impl RedisKafkaReader {
    pub fn new(
        redis: Connection,
        kafka: BaseConsumer,
        transfer_map: Arc<DashMap<u64, Arc<[u8]>>>,
    ) -> Self {
        Self {
            redis,
            kafka,
            transfer_map,
            local: Vec::new(),
        }
    }
}

impl inner2::ChunkReader for RedisKafkaReader {
    fn read(&mut self, offset: u64) -> Result<&[u8], Error> {
        self.local.clear();
        match self.transfer_map.get(&offset) {
            Some(x) => {
                let x = x.clone();
                self.local.extend_from_slice(&x[..]);
            }
            None => {
                let mut cmd = redis::Cmd::new();
                cmd.arg("GET").arg(offset);
                let (partition, offset): (i32, i64) =
                    async_std::task::block_on(cmd.query_async(&mut self.redis)).unwrap();
                let offset = Offset::Offset(offset);
                let mut tp_list = TopicPartitionList::new();
                tp_list
                    .add_partition_offset(KAFKA_TOPIC, partition, offset)
                    .unwrap();
                self.kafka.assign(&tp_list)?;
                let msg = loop {
                    match self.kafka.poll(Duration::from_secs(0)) {
                        Some(Ok(x)) => break x,
                        Some(Err(x)) => return Err(x.into()),
                        None => {}
                    };
                };
                self.local.clear();
                self.local.extend_from_slice(msg.payload().unwrap());
            }
        }
        Ok(self.local.as_slice())
    }
}

pub struct RedisKafkaMeta {
    con: Connection,
}

impl RedisKafkaMeta {
    fn new(con: Connection) -> Self {
        Self { con }
    }
}

impl inner2::ChunkMeta for RedisKafkaMeta {
    fn update(
        &mut self,
        tags: &HashMap<Tags, inner2::ChunkMetadata>,
        chunk_id: u64,
    ) -> Result<(), Error> {
        let mut pipe = redis::pipe();
        for (k, v) in tags.iter() {
            let meta = (chunk_id, v.offset, v.size);
            pipe.cmd("SET").arg(("meta", k.id())).arg(meta).ignore();
        }
        let _: () = async_std::task::block_on(pipe.query_async(&mut self.con))?;
        Ok(())
    }
}

pub struct RedisKafkaBackend {
    transfer_map: Arc<DashMap<u64, Arc<[u8]>>>,
    redis_addr: &'static str,
    kafka_bootstrap: &'static str,
}

impl RedisKafkaBackend {
    pub fn new(redis_addr: &'static str, kafka_bootstrap: &'static str) -> Self {
        let transfer_map = Arc::new(DashMap::new());
        Self {
            transfer_map,
            redis_addr,
            kafka_bootstrap,
        }
    }

    pub fn default_producer(bootstraps: &'static str) -> Result<FutureProducer, Error> {
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
}

impl PersistentListBackend for RedisKafkaBackend {
    type Writer = RedisKafkaWriter;
    type Reader = RedisKafkaReader;
    type Meta = RedisKafkaMeta;
    fn writer(&self) -> Result<Self::Writer, Error> {
        let client = redis::Client::open(self.redis_addr)?;
        let mut redis = async_std::task::block_on(client.get_async_connection())?;
        let kafka = Self::default_producer(self.kafka_bootstrap)?;
        Ok(RedisKafkaWriter::new(
            redis,
            kafka,
            self.transfer_map.clone(),
        ))
    }

    fn reader(&self) -> Result<Self::Reader, Error> {
        let client = redis::Client::open(self.redis_addr)?;
        let mut redis = async_std::task::block_on(client.get_async_connection())?;
        let kafka: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.kafka_bootstrap)
            .set("group.id", random_id())
            .create()?;
        Ok(RedisKafkaReader::new(
            redis,
            kafka,
            self.transfer_map.clone(),
        ))
    }

    fn meta(&self) -> Result<Self::Meta, Error> {
        let client = redis::Client::open(self.redis_addr)?;
        let mut redis = async_std::task::block_on(client.get_async_connection())?;
        Ok(RedisKafkaMeta::new(redis))
    }
}
