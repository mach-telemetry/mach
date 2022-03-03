use crate::{
    persistent_list::ListBuffer,
    id::SeriesId,
    series::{self, Series},
    segment::SegmentSnapshot,
    persistent_list,
    constants::*,
    runtime::RUNTIME,
};
use dashmap::DashMap;
use std::{
    sync::Arc,
    time::{Instant, Duration},
};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    Message,
    types::RDKafkaErrorCode,
};
use tokio::{
    sync::{
        Mutex,
        mpsc::{UnboundedSender, UnboundedReceiver, unbounded_channel}
    },
    time::{timeout, sleep},
};

pub struct DurabilityHandle {
    chan: UnboundedSender<Series>
}

impl DurabilityHandle {
    pub fn new(writer_id: &str, list: ListBuffer) -> Self {
        init(writer_id, list)
    }

    pub fn register_series(&self, series: Series) {
        if let Err(_) = self.chan.send(series) {
            panic!("Can't send series to durability task");
        }
    }
}

fn init(writer_id: &str, list: ListBuffer) -> DurabilityHandle {
    let writer_id = writer_id.into();
    let (tx, rx) = unbounded_channel();
    let series = Arc::new(Mutex::new(Vec::new()));
    let series2 = series.clone();
    RUNTIME.spawn(durability_receiver(rx, series2));
    RUNTIME.spawn(durability_worker(writer_id, series, list));
    DurabilityHandle {
        chan: tx
    }
}

async fn durability_receiver(mut recv: UnboundedReceiver<Series>, series: Arc<Mutex<Vec<Series>>>) {
    while let Some(item) = recv.recv().await {
        series.lock().await.push(item);
    }
}

async fn durability_worker(writer_id: String, series: Arc<Mutex<Vec<Series>>>, list: ListBuffer) {
    let topic = format!("durability_{}", writer_id);
    create_topic(KAFKA_BOOTSTRAP, topic.as_str()).await;
    let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", KAFKA_BOOTSTRAP)
            .set("message.max.bytes", "100000000")
            .set("linger.ms", "0")
            .set("message.copy.max.bytes", "5000000")
            .set("batch.num.messages", "1")
            .set("compression.type", "none")
            .set("acks", "all")
            .create().unwrap();

    let mut encoded = Vec::new();
    loop {
        sleep(Duration::from_secs(1)).await;
        let guard = series.lock().await;
        let mut snapshots = Vec::new();
        for series in guard.iter() {
            match series.segment_snapshot() {
                Ok(x) => snapshots.push(x),
                _ => {}
            }
        }
        drop(guard);
        let buffer = match unsafe { list.copy_buffer() } {
            Ok(x) => x,
            _ => Vec::new().into_boxed_slice()
        };
        let data: (Vec<SegmentSnapshot>, Box<[u8]>) = (snapshots, buffer);
        bincode::serialize_into(&mut encoded, &data).unwrap();
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(&topic).payload(&encoded[..]);
        match producer.send(to_send, Duration::from_secs(0)).await {
            Ok((p, o)) => {},
            Err((e, m)) => println!("DURABILITY ERROR {:?}", e),
        }
        //let (partition, offset) = producer.send(to_send, Duration::from_secs(0)).await.unwrap();
        encoded.clear();
    }
}

async fn create_topic(bootstrap: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create().unwrap();
    let topic = [NewTopic {
        name: topic,
        num_partitions: 1,
        replication: TopicReplication::Fixed(2),
        config: Vec::new(),
    }];
    let opts = AdminOptions::new();
    let result = admin.create_topics(&topic, &opts).await.unwrap();
    match result[0].as_ref() {
        Ok(_) => {},
        Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}, // Ok if topic already exists
        Err(x) => println!("DURABILITY ERROR: Cant create topic {:?}",x),
    };
}


