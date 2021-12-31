use crate::{
    constants::*,
    persistent_list::{inner::*, Error},
};
use async_std::channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
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
    let dur = Duration::from_secs(0);
    let mut now = Instant::now();
    while let Ok(offset) = queue.recv().await {
        println!("since last flush request {:?}, queue length {}", now.elapsed(), queue.len());
        now = Instant::now();
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
        println!("flush duration {:?}", now.elapsed());
    }
}

pub struct KafkaWriter {
    map: Arc<DashMap<usize, Arc<[u8]>>>,
    partition: usize,
    offset: usize,
    last_flush: Instant,
    sender: Sender<usize>,
}

impl KafkaWriter {
    pub fn new(partition: usize) -> Result<Self, Error> {
        let map = Arc::new(DashMap::new());
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", KAFKA_BOOTSTRAP)
            //.set("queue.buffering.max.messages", "1")
            //.set("queue.buffering.max.kbytes", "2000")
            //.set("queue.buffering.max.ms", "0")
            .set("message.max.bytes", "2000000")
            .set("linger.ms", "0")
            .set("message.copy.max.bytes", "5000000")
            .set("batch.num.messages", "1")
            .set("compression.type", "none")
            .set("acks", "1")
            .create()?;

        println!("Warming up producer");
        for _ in 0..10 {
            let to_send: FutureRecord<str, [u8]> = FutureRecord::to(KAFKA_TOPIC)
                .payload(&[0][..])
                .partition(partition.try_into().unwrap());
            let res = async_std::task::block_on(producer.send(to_send, Duration::from_secs(0)));
        }
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(KAFKA_TOPIC)
            .payload(&[0][..])
            .partition(partition.try_into().unwrap());
        println!("Getting offset");
        let res = async_std::task::block_on(producer.send(to_send, Duration::from_secs(0)));
        let (rp, offset) = match res {
            Ok(x) => x,
            Err((e, m)) => return Err(e.into())
        };
        let rp: usize = rp.try_into().unwrap();
        assert_eq!(rp, partition);
        let offset: usize = (offset + 1).try_into().unwrap();

        println!("Kafka writer for partition {} starting at offset {}", partition, offset);
        let (sender, receiver) = unbounded();
        async_std::task::spawn(kafka_partition_writer(partition, producer, map.clone(), receiver));
        Ok(KafkaWriter {
            map,
            partition,
            offset,
            sender,
            last_flush: Instant::now(),
        })
    }
}

impl ChunkWriter for KafkaWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error> {
        //let partition = self.partition as i32 % 10;
        //println!("Since last flush: {:?}", self.last_flush.elapsed());
        let now = std::time::Instant::now();
        let offset = self.offset;
        self.offset += 1;
        let map = self.map.clone();
        let data: Arc<[u8]> = bytes.into();
        map.insert(offset, data.clone());
        self.sender.try_send(offset).unwrap();
        //println!("Queue length: {}", self.sender.len());
        let sz = bytes.len();
        self.last_flush = Instant::now();
        //println!("Duration: {:?}", now.elapsed());
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
