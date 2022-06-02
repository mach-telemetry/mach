use crate::utils::random_id;
pub use rdkafka::consumer::{
    base_consumer::BaseConsumer, stream_consumer::StreamConsumer, Consumer,
};
use rdkafka::{
    config::ClientConfig,
    error::KafkaError as RdKafkaError,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    types::RDKafkaErrorCode,
    util::Timeout,
    Message,
    admin::{NewTopic, AdminOptions, TopicReplication, AdminClient},
    client::DefaultClientContext,
};
//use std::sync::mpsc::{SyncSender, Receiver, sync_channel};
use serde::*;
use std::{convert::TryInto, time::Duration};
use zstd::stream::{decode_all, encode_all};
use crate::durable_queue::TOTAL_SZ;
//use tokio::sync::mpsc::{channel, error::TryRecvError, Receiver, Sender};

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

async fn create_topic(bootstraps: String, topic: String) -> Result<(), Error> {
    println!("creating topic: {}", topic);
    println!("at bootstraps: {}", bootstraps);
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstraps)
        .create()?;

    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
    let topics = &[NewTopic {
        name: topic.as_str(),
        num_partitions: 1,
        replication: TopicReplication::Fixed(3),
        config: Vec::new()
    }];
    client.create_topics(topics, &admin_opts).await?;
    Ok(())
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
        .set("message.timeout.ms", "10000")
        .create()?;
    Ok(producer)
}

impl KafkaWriter {
    pub fn new(kafka_bootstrap: String, topic: String) -> Result<Self, Error> {
        let f = create_topic(kafka_bootstrap.clone(), topic.clone());
        futures::executor::block_on(f)?;
        let producer = default_producer(kafka_bootstrap)?;
        let dur = Duration::from_secs(0);
        Ok(Self {
            producer,
            topic,
            dur,
        })
    }

    pub fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        // ZStd compression
        let encoded = encode_all(bytes, 0).unwrap();
        let bytes = encoded.as_slice();
        TOTAL_SZ.fetch_add(bytes.len(), std::sync::atomic::Ordering::SeqCst);

        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(&self.topic).payload(bytes);
        //let now = std::time::Instant::now();
        let (partition, offset) = futures::executor::block_on(self.producer.send(to_send, self.dur)).unwrap();
        //println!("Item successfully produced in {:?}", now.elapsed());
        //assert_eq!(partition, 0);
        Ok(offset.try_into().unwrap())
    }
}

//async fn stream_consumer(
//    mut offset_request: Receiver<(u64, u64)>,
//    sender: Sender<Vec<Vec<u8>>>,
//    bootstrap_servers: String,
//    topic: String,
//) {
//    let consumer: StreamConsumer = ClientConfig::new()
//        .set("bootstrap.servers", bootstrap_servers)
//        .set("group.id", random_id())
//        .create()
//        .unwrap();
//
//    loop {
//        let (first, last) = if let Some(offset) = offset_request.recv().await {
//            offset
//        } else {
//            break;
//        };
//        let mut tp_list = TopicPartitionList::new();
//        let offset = Offset::Offset(first as i64);
//        tp_list.add_partition_offset(&topic, 0, offset).unwrap();
//        consumer.assign(&tp_list).unwrap();
//        let mut v = Vec::new();
//        let lim = last - first + 1;
//        for _ in 0..lim {
//            match consumer.recv().await {
//                Ok(msg) => {
//                    let data: Vec<u8> = msg.payload().unwrap().into();
//                    v.push(data);
//                }
//                Err(_) => break,
//            }
//        }
//        sender.send(v).await.unwrap();
//    }
//}

//pub struct KafkaReader {
//    local_buffer: Vec<Vec<u8>>,
//    kafka_stream: Receiver<Vec<Vec<u8>>>,
//    kafka_request: Sender<(u64, u64)>,
//    offsets: (u64, u64),
//}
//
//impl KafkaReader {
//    pub fn new(bootstrap_servers: String, topic: String) -> Result<Self, Error> {
//        let (stream_sender, stream_receiver) = channel(1);
//        let (request_sender, request_receiver) = channel(1);
//        RUNTIME.spawn(stream_consumer(
//            request_receiver,
//            stream_sender,
//            bootstrap_servers,
//            topic,
//        ));
//        Ok(Self {
//            local_buffer: Vec::new(),
//            kafka_stream: stream_receiver,
//            kafka_request: request_sender,
//            offsets: (u64::MAX, u64::MAX),
//        })
//    }
//
//    pub fn read(&mut self, at: u64) -> Result<&[u8], Error> {
//        //println!("READING: {}", at);
//        if at < self.offsets.0 || at > self.offsets.1 {
//            let first_offset = if at < 10 { 0 } else { at - 10 };
//            //println!("Loading: {:?}", (first_offset, at));
//            self.kafka_request.try_send((first_offset, at)).unwrap();
//            loop {
//                match self.kafka_stream.try_recv() {
//                    Ok(x) => {
//                        self.local_buffer = x;
//                        break;
//                    }
//                    Err(TryRecvError::Empty) => {}
//                    Err(TryRecvError::Disconnected) => panic!("worker failed"),
//                }
//            }
//            self.offsets = (first_offset, at);
//        }
//        //println!("offset: {:?}", self.offsets);
//        let offset = at - self.offsets.0;
//        Ok(self.local_buffer[offset as usize].as_slice())
//    }
//}

pub struct KafkaReader {
    consumer: BaseConsumer,
    timeout: Timeout,
    local_buffer: Vec<Vec<u8>>,
    topic: String,
    offsets: (u64, u64),
}

impl KafkaReader {
    pub fn new(bootstrap_servers: String, topic: String) -> Result<Self, Error> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", random_id())
            .create()?;
        Ok(KafkaReader {
            consumer,
            topic,
            timeout: Timeout::After(Duration::from_secs(0)),
            local_buffer: Vec::new(),
            offsets: (u64::MAX, u64::MAX),
        })
    }

    pub fn read(&mut self, at: u64) -> Result<&[u8], Error> {
        if at < self.offsets.0 || at > self.offsets.1 {
            let start = if at < 10 { 0 } else { at - 10 };
            let mut tp_list = TopicPartitionList::new();
            let offset = Offset::Offset(start.try_into().unwrap());
            tp_list
                .add_partition_offset(&self.topic, 0, offset)
                .unwrap();
            self.consumer.assign(&tp_list)?;
            for i in 0..(1 + at - start) as usize {
                let msg = loop {
                    match self.consumer.poll(self.timeout) {
                        Some(Ok(x)) => break x,
                        Some(Err(x)) => return Err(x.into()),
                        None => {}
                    };
                };
                let decoded = decode_all(msg.payload().unwrap()).unwrap();
                //let decoded = msg.payload().unwrap().into();
                if self.local_buffer.len() < i + 1 {
                    self.local_buffer.push(decoded);
                } else {
                    self.local_buffer[i].clear();
                    self.local_buffer[i].extend_from_slice(&decoded);
                }
            }
            self.offsets = (start, at);
        }
        let idx = at - self.offsets.0;
        Ok(self.local_buffer[idx as usize].as_slice())
    }
}
