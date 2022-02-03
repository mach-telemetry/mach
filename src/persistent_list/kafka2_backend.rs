use crate::{
    constants::*,
    persistent_list::{inner::*, inner2, BackendOld, Error},
};
use rand::prelude::*;
use async_std::channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
pub use rdkafka::consumer::{base_consumer::BaseConsumer, Consumer};
use rdkafka::{
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
use uuid::Uuid;

fn random_id() -> String {
    Uuid::new_v4().to_hyphenated().to_string()
}

async fn kafka_partition_writer(
    partition: usize,
    producer: FutureProducer,
    transfer_map: Arc<DashMap<usize, Arc<[u8]>>>,
    queue: Receiver<usize>,
) {
    //println!("interval, length, duration");
    let dur = Duration::from_secs(0);
    //let mut now = Instant::now();
    while let Ok(offset) = queue.recv().await {
        //let interval = now.elapsed().as_secs_f64();
        println!("Flush Queue len: {}", queue.len());
        //now = Instant::now();
        let data = transfer_map.get(&offset).unwrap().clone();
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(KAFKA_TOPIC)
            .payload(&data[..])
            .partition(partition.try_into().unwrap());
        let result = producer.send(to_send, dur).await;
        match result {
            Err(err) => {
                println!("{:?}", err.0);
                panic!("HERE");
            }
            Ok((rp, ro)) => {
                let rp: usize = rp.try_into().unwrap();
                let ro: usize = ro.try_into().unwrap();
                assert_eq!(rp, partition);
                assert_eq!(ro, offset);
            }
        }
        transfer_map.remove(&offset);
        //let dur = now.elapsed().as_secs_f64();
        //println!("{}, {}, {}", interval, q_len, dur);
    }
}

pub struct KafkaWriter {
    key: [u8; 8],
    producer: FutureProducer,
    topic: &'static str,
}

impl KafkaWriter {
    fn with_producer(
        producer: FutureProducer,
        key: [u8; 8],
        topic: &'static str,
    ) -> Result<Self, Error> {
        Ok(KafkaWriter {
            key,
            producer,
            topic
        })
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

    pub fn new(kafka_bootstrap: &'static str) -> Result<Self, Error> {
        let producer = Self::default_producer(kafka_bootstrap)?;
        let key: usize = thread_rng().gen();
        Self::with_producer(producer, key.to_be_bytes(), KAFKA_TOPIC)
    }
}

// TODO: Figure out how to make flush to Kafka async. Currently, this is necessarily sync because
// we need the partition + offset unless we restrict to a single partition.
impl inner2::ChunkWriter for KafkaWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        let to_send: FutureRecord<[u8; 8], [u8]> = FutureRecord::to(KAFKA_TOPIC)
            .key(&self.key)
            .payload(&bytes[..]);
        let result = async_std::task::block_on(self.producer.send(to_send, Duration::from_secs(0)));
        match result {
            Err(err) => {
                println!("{:?}", err.0);
                panic!("HERE");
            }
            Ok((partition, offset)) => {
                let partition: u32 = partition.try_into().unwrap();
                let offset: u32 = offset.try_into().unwrap();
                Ok(((partition as u64) << 32) | (offset as u64))
            }
        }
    }
}

pub struct KafkaReader {
    consumer: BaseConsumer,
    timeout: Timeout,
    local_buffer: Vec<u8>,
}

impl KafkaReader {
    pub fn new(bootstrap_servers: &'static str) -> Result<Self, Error> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", random_id())
            .create()?;
        Ok(KafkaReader {
            consumer,
            timeout: Timeout::After(Duration::from_secs(0)),
            local_buffer: Vec::new(),
        })
    }
}

impl inner2::ChunkReader for KafkaReader {
    fn read(&mut self, at: u64) -> Result<&[u8], Error> {
        // unpack partition and offset from the u64
        let (partition, offset) = (at >> 32, at & (u32::MAX as u64));
        let partition: i32 = partition.try_into().unwrap();
        let offset = Offset::Offset(offset.try_into().unwrap());

        // Setup the consumer
        let mut tp_list = TopicPartitionList::new();
        tp_list.add_partition_offset(KAFKA_TOPIC, partition, offset).unwrap();
        self.consumer.assign(&tp_list)?;
        let msg = loop {
            match self.consumer.poll(self.timeout) {
                Some(Ok(x)) => break x,
                Some(Err(x)) => return Err(x.into()),
                None => {}
            };
        };
        self.local_buffer.clear();
        self.local_buffer.extend_from_slice(msg.payload().unwrap());
        Ok(self.local_buffer.as_slice())
    }
}

pub struct KafkaBackend {
    bootstrap_servers: &'static str,
    key: [u8; 8]
}

impl KafkaBackend {
    pub fn new(bootstrap_servers: &'static str) -> Self {
        let key: usize = thread_rng().gen();
        Self {
            bootstrap_servers,
            key: key.to_be_bytes(),
        }
    }

    pub fn make_writer(&self) -> Result<KafkaWriter, Error> {
        let producer = KafkaWriter::default_producer(self.bootstrap_servers)?;
        KafkaWriter::with_producer(producer, self.key, KAFKA_TOPIC)
    }

    pub fn make_reader(&mut self) -> Result<KafkaReader, Error> {
        KafkaReader::new(self.bootstrap_servers)
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
