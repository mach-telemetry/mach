use crate::utils::random_id;
use crate::runtime::RUNTIME;
pub use rdkafka::consumer::{base_consumer::BaseConsumer, stream_consumer::StreamConsumer, Consumer};
use rdkafka::{
    config::ClientConfig,
    error::KafkaError as RdKafkaError,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    types::RDKafkaErrorCode,
    util::Timeout,
    Message,
};
use std::sync::mpsc::{SyncSender, Receiver, sync_channel};
//use tokio::sync::mpsc::{Sender, Receiver, channel, error::TryRecvError};
use serde::*;
use std::{convert::TryInto, time::Duration};

#[derive(Debug)]
pub enum Error {
    Kafka(RdKafkaError),
    KafkaErrorCode((String, RDKafkaErrorCode)),
    ReadInvalidOffset,
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
    println!("making producer to bootstraps: {}", bootstraps);
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

    pub async fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(&self.topic).payload(bytes);
        let (partition, offset) = self.producer.send(to_send, self.dur).await.unwrap();
        assert_eq!(partition, 0);
        Ok(offset.try_into().unwrap())
    }
}

async fn stream_consumer(offset_request: Receiver<(u64, u64)>, sender: SyncSender<Vec<Vec<u8>>>, bootstrap_servers: String, topic: String) -> () {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", random_id())
        .create().unwrap();

    loop {
        let (first, last) = if let Ok(offset) = offset_request.recv() {
            offset
        } else {
            break;
        };
        let mut tp_list = TopicPartitionList::new();
        let offset = Offset::Offset(first as i64);
        tp_list
            .add_partition_offset(&topic, 0, offset)
            .unwrap();
        consumer.assign(&tp_list).unwrap();
        let mut v = Vec::new();
        let lim = last - first + 1;
        for _ in 0..lim {
            match consumer.recv().await {
                Ok(msg) => {
                    let data: Vec<u8> = msg.payload().unwrap().into();
                    v.push(data);
                },
                Err(x) => break,
            }
        }
        sender.send(v).unwrap();
    }
}

pub struct KafkaStreamReader {
    local_buffer: Vec<Vec<u8>>,
    kafka_stream: Receiver<Vec<Vec<u8>>>,
    kafka_request: SyncSender<(u64, u64)>,
    offsets: (u64, u64),
}

impl KafkaStreamReader {
    pub fn new(bootstrap_servers: String, topic: String) -> Result<Self, Error> {
        let (stream_sender, stream_receiver) = sync_channel(1);
        let (request_sender, request_receiver) = sync_channel(1);
        RUNTIME.spawn(stream_consumer(request_receiver, stream_sender, bootstrap_servers, topic));
        Ok(KafkaStreamReader {
            local_buffer: Vec::new(),
            kafka_stream: stream_receiver,
            kafka_request: request_sender,
            offsets: (u64::MAX, u64::MAX)
        })
    }

    pub fn read(&mut self, at: u64) -> Result<&[u8], Error> {
        //println!("READING: {}", at);
        if at < self.offsets.0 || at > self.offsets.1 {
            let first_offset = if at < 10 { 0 } else { at - 10 };
            //println!("Loading: {:?}", (first_offset, at));
            self.kafka_request.send((first_offset, at)).unwrap();
            self.local_buffer = self.kafka_stream.recv().unwrap();
            self.offsets = (first_offset, at);
        }
        //println!("offset: {:?}", self.offsets);
        let offset = at - self.offsets.0;
        Ok(self.local_buffer[offset as usize].as_slice())
    }
}

pub type KafkaReader = KafkaStreamReader;

//pub struct KafkaReader {
//    consumer: BaseConsumer,
//    timeout: Timeout,
//    local_buffer: Vec<u8>,
//    topic: String,
//    //last_offset: u64,
//}
//
//impl KafkaReader {
//    pub fn new(bootstrap_servers: String, topic: String) -> Result<Self, Error> {
//        let consumer: BaseConsumer = ClientConfig::new()
//            .set("bootstrap.servers", bootstrap_servers)
//            .set("group.id", random_id())
//            .create()?;
//        Ok(KafkaReader {
//            consumer,
//            topic,
//            timeout: Timeout::After(Duration::from_secs(0)),
//            local_buffer: Vec::new(),
//            //last_offset: u64::MAX,
//        })
//    }
//
//    pub fn read(&mut self, at: u64) -> Result<&[u8], Error> {
//        self.local_buffer.clear();
//        let offset: i64 = at.try_into().unwrap();
//        let mut tp_list = TopicPartitionList::new();
//        let offset = Offset::Offset(offset);
//        tp_list
//            .add_partition_offset(&self.topic, 0, offset)
//            .unwrap();
//        self.consumer.assign(&tp_list)?;
//        //self.consumer.seek(&self.topic, 0, offset.clone(), Duration::from_secs(0))?;
//        println!("CONSUMING {:?}", offset);
//        let msg = loop {
//            match self.consumer.poll(self.timeout) {
//                Some(Ok(x)) => break x,
//                Some(Err(x)) => return Err(x.into()),
//                None => {}
//            };
//        };
//        self.local_buffer.extend_from_slice(msg.payload().unwrap());
//        Ok(self.local_buffer.as_slice())
//    }
//}
