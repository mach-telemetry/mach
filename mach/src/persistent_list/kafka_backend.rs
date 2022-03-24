use crate::{
    constants::*,
    id::SeriesId,
    persistent_list::{inner, Config, Error, PersistentListBackend},
    runtime::RUNTIME,
    utils::random_id,
};
use dashmap::DashMap;
use rand::prelude::*;
pub use rdkafka::consumer::{base_consumer::BaseConsumer, Consumer};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    Message,
};
use std::{
    convert::TryInto,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

//async fn worker(
//    producer: FutureProducer,
//    map: Arc<DashMap<i64, Arc<[u8]>>>,
//    mut queue: UnboundedReceiver<i64>,
//    topic: String,
//) {
//    let dur = Duration::from_secs(0);
//    while let Some(offset) = queue.recv().await {
//        let item = map.get(&offset).unwrap();
//        let data = item.clone();
//        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(&topic).payload(&data[..]);
//        let result = producer.send(to_send, dur).await;
//        match result {
//            Err(err) => {
//                println!("{:?}", err.0);
//                panic!("HERE");
//            }
//            Ok((rp, ro)) => {
//                assert_eq!(rp, 0);
//                assert_eq!(ro, offset);
//            }
//        }
//        map.remove(&offset);
//    }
//}

pub struct KafkaWriter {
    producer: FutureProducer,
    topic: String,
    dur: Duration,
}

impl KafkaWriter {
    pub fn default_producer(bootstraps: String) -> Result<FutureProducer, Error> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstraps)
            .set("message.max.bytes", "2000000")
            .set("linger.ms", "0")
            .set("message.copy.max.bytes", "5000000")
            .set("batch.num.messages", "1")
            .set("compression.type", "none")
            .set("acks", "all")
            //.set("max.in.flight", "1")
            .create()?;
        Ok(producer)
    }

    pub fn new(kafka_bootstrap: String, topic: String) -> Result<Self, Error> {
        let producer = Self::default_producer(kafka_bootstrap)?;
        let dur = Duration::from_secs(0);
        Ok(Self {
            producer,
            topic,
            dur,
        })
    }
}

impl inner::ChunkWriter for KafkaWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
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
}

impl inner::ChunkReader for KafkaReader {
    fn read(&mut self, at: u64) -> Result<&[u8], Error> {
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

//fn create_topic(bootstrap: &str, topic: &str) -> Result<(), Error> {
//    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
//        .set("bootstrap.servers", bootstrap)
//        .create()?;
//    let topic = [NewTopic {
//        name: topic,
//        num_partitions: 1,
//        replication: TopicReplication::Fixed(3),
//        config: Vec::new(),
//    }];
//    let opts = AdminOptions::new();
//    let fut = admin.create_topics(&topic, &opts);
//
//    // block on current thread instead of global runtime
//    let rt = tokio::runtime::Builder::new_current_thread()
//        .enable_all()
//        .build()
//        .unwrap();
//    let result = rt.block_on(fut)?;
//
//    if let Err((s, c)) = &result[0] {
//        return Err(Error::KafkaErrorCode((s.into(), c.clone())));
//    }
//    Ok(())
//}

pub struct KafkaBackend {
    bootstrap_servers: String,
    topic: String,
}

impl KafkaBackend {
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

impl PersistentListBackend for KafkaBackend {
    type Writer = KafkaWriter;
    type Reader = KafkaReader;
    fn id(&self) -> &str {
        self.topic.as_str()
    }
    fn default_backend() -> Result<Self, Error> {
        Self::new(KAFKA_BOOTSTRAP, random_id().as_str())
    }
    fn with_config(conf: Config) -> Result<Self, Error> {
        match conf.kafka_bootstrap() {
            Some(x) => Self::new(x, random_id().as_str()),
            None => Err(Error::InvalidConfig(conf))
        }
    }
    fn writer(&self) -> Result<Self::Writer, Error> {
        self.make_writer()
    }
    fn reader(&self) -> Result<Self::Reader, Error> {
        self.make_reader()
    }
}

//impl Backend for KafkaBackend {
//    type Writer = KafkaWriter;
//    type Reader = KafkaReader;
//    fn make_backend(&mut self) -> Result<(KafkaWriter, KafkaReader), Error> {
//        let writer = self.make_kafka_writer()?;
//        let reader = self.make_reader()?;
//        Ok((writer, reader))
//    }
//}
