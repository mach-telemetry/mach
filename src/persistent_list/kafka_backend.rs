use crate::{
    constants::*,
    persistent_list::{inner::*, Backend, Error},
};
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
    map: Arc<DashMap<usize, Arc<[u8]>>>,
    partition: usize,
    offset: usize,
    sender: Sender<usize>,
}

impl KafkaWriter {
    fn with_producer(
        producer: FutureProducer,
        partition: usize,
        topic: &'static str,
    ) -> Result<Self, Error> {
        let map = Arc::new(DashMap::new());
        println!("Warming up producer");
        for _ in 0..10 {
            let to_send: FutureRecord<str, [u8]> = FutureRecord::to(topic)
                .payload(&[0][..])
                .partition(partition.try_into().unwrap());
            async_std::task::block_on(producer.send(to_send, Duration::from_secs(0))).unwrap();
        }
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(topic)
            .payload(&[0][..])
            .partition(partition.try_into().unwrap());
        println!("Getting offset");
        let res = async_std::task::block_on(producer.send(to_send, Duration::from_secs(0)));
        let (rp, offset) = match res {
            Ok(x) => x,
            Err((e, _)) => return Err(e.into()),
        };
        let rp: usize = rp.try_into().unwrap();
        assert_eq!(rp, partition);
        let offset: usize = (offset + 1).try_into().unwrap();

        println!(
            "Kafka writer for partition {} starting at offset {}",
            partition, offset
        );
        let (sender, receiver) = unbounded();
        async_std::task::spawn(kafka_partition_writer(
            partition,
            producer,
            map.clone(),
            receiver,
        ));
        Ok(KafkaWriter {
            map,
            partition,
            offset,
            sender,
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

    pub fn new(partition: usize) -> Result<Self, Error> {
        let producer = Self::default_producer(KAFKA_BOOTSTRAP)?;
        Self::with_producer(producer, partition, KAFKA_TOPIC)
    }
}

impl ChunkWriter for KafkaWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error> {
        let offset = self.offset;
        self.offset += 1;
        let map = self.map.clone();
        let data: Arc<[u8]> = bytes.into();
        while self.sender.len() > 100 {
            std::thread::sleep(std::time::Duration::from_millis(1))
        }
        map.insert(offset, data.clone());
        self.sender.try_send(offset).unwrap();
        let sz = bytes.len();
        Ok(PersistentHead {
            partition: self.partition,
            offset,
            sz,
        })
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
                let payload = msg.payload().unwrap();
                self.local_copy.clear();
                self.local_copy.extend_from_slice(payload);
                self.current_head = Some(persistent);
            }
        }
        Ok(&self.local_copy[local.offset..local.offset + local.size])
    }
}

pub struct KafkaBackend {
    bootstrap_servers: &'static str,
    topic: &'static str,
    partitions: usize,
    count: usize,
}

impl KafkaBackend {
    pub fn new() -> Self {
        Self {
            bootstrap_servers: "",
            topic: "",
            partitions: 0,
            count: 0,
        }
    }

    pub fn bootstrap_servers(mut self, servers: &'static str) -> Self {
        self.bootstrap_servers = servers;
        self
    }

    pub fn topic(mut self, topic: &'static str) -> Self {
        self.topic = topic;
        self
    }

    pub fn partitions(mut self, parts: usize) -> Self {
        self.partitions = parts;
        self
    }

    pub fn make_kafka_writer(&mut self) -> Result<KafkaWriter, Error> {
        if self.count >= self.partitions {
            Err(Error::FactoryError)
        } else {
            let partition = self.count;
            self.count += 1;
            let producer = KafkaWriter::default_producer(self.bootstrap_servers)?;
            KafkaWriter::with_producer(producer, partition, self.topic)
        }
    }

    pub fn make_reader(&mut self) -> Result<KafkaReader, Error> {
        KafkaReader::new()
    }
}

impl Backend for KafkaBackend {
    type Writer = KafkaWriter;
    type Reader = KafkaReader;
    fn make_backend(&mut self) -> Result<(KafkaWriter, KafkaReader), Error> {
        let writer = self.make_kafka_writer()?;
        let reader = self.make_reader()?;
        Ok((writer, reader))
    }
}
