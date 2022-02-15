use crate::{
    constants::*,
    id::{SeriesId, WriterId},
    tags::Tags,
};
use lazy_static::*;
use std::convert::From;
use serde::*;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    Message,
};

pub const METADATA: Metadata = Metadata { addr: REDIS_ADDR };

#[derive(Debug)]
pub enum Error {
    Redis(RedisError),
}

impl From<RedisError> for Error {
    fn from(item: RedisError) -> Self {
        Error::Redis(item)
    }
}

pub struct Metadata {
    addr: &'static str,
}

impl Metadata {
    fn make_conn(&self) -> Result<Connection, Error> {
        let client = Client::open(self.addr)?;
        Ok(client.get_connection()?)
    }

    pub fn set<K: Serialize, V: Serialize>(&self, kvs: &[(K, V)]) -> Result<(), Error> {
        let mut con = self.make_conn()?;
        let mut p = redis::pipe();
        for (k, v) in kvs.iter() {
            p.cmd("SET").arg(bincode::serialize(k).unwrap()).arg(bincode::serialize(v).unwrap()).ignore();
        }
        p.query(&mut con)?;
        Ok(())
    }

    pub fn get<K: Serialize, V: Deserialize>(&self, k: K) -> V {
        let mut con = self.make_conn()?;
    }
}
