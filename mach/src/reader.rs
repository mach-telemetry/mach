use crate::{
    constants::*, id::SeriesId, persistent_list::ListSnapshot, runtime::RUNTIME, sample::Type,
    segment::SegmentSnapshot, series::Series,
};
use bincode::{deserialize_from, serialize_into};
use dashmap::DashMap;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    types::RDKafkaErrorCode,
    util::Timeout,
    Message,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::From;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{
    sync::{self, mpsc, RwLock},
    time,
};

#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    segments: SegmentSnapshot,
    list: ListSnapshot,
}

impl Snapshot {
    pub fn new(segments: SegmentSnapshot, list: ListSnapshot) -> Self {
        Snapshot { segments, list }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        let mut v = Vec::new();
        serialize_into(&mut v, &self).unwrap();
        v
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        deserialize_from(bytes).unwrap()
    }
}

pub enum SnapshotterRequest {
    Read(sync::oneshot::Sender<(i32, i64)>),
    Close,
}

async fn snapshot_worker(
    duration: Duration,
    series_id: SeriesId,
    series: Series,
    snapshotters: Arc<RwLock<HashMap<SeriesId, Snapshotter>>>,
    mut receiver: mpsc::UnboundedReceiver<SnapshotterRequest>,
) {
    let topic = format!("read_ahead_log_{}", series_id.0);
    //create_topic(KAFKA_BOOTSTRAP, topic.as_str()).await;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAP)
        .set("message.max.bytes", "2000000")
        .set("linger.ms", "0")
        .set("message.copy.max.bytes", "5000000")
        .set("batch.num.messages", "1")
        .set("compression.type", "none")
        .set("acks", "all")
        .create()
        .unwrap();
    let mut snapshot: Arc<[u8]> = series.snapshot().unwrap().to_bytes().into();
    let mut durable = false;
    let mut partition = 0;
    let mut offset = 0;
    let mut last_snapshot = Instant::now();
    loop {
        match time::timeout(Duration::from_secs(30), receiver.recv()).await {
            Ok(Some(SnapshotterRequest::Read(sender))) => {
                // Make a new snapshot only when there is a request and the last snapshot is older
                // than the snapshot interval. If so, mark as not durable
                if last_snapshot.elapsed() >= duration {
                    snapshot = series.snapshot().unwrap().to_bytes().into();
                    durable = false;
                }

                // If the snapshot is not durable, send to kafka topic for durability
                if !durable {
                    let to_send: FutureRecord<str, [u8]> =
                        FutureRecord::to(&topic).payload(&snapshot[..]);
                    let r = producer
                        .send(to_send, Duration::from_secs(0))
                        .await
                        .unwrap();
                    partition = r.0;
                    offset = r.1;
                    durable = true;
                }

                sender.send((partition, offset)).unwrap();
            }

            // Got a close signal from the timeout match below in a prior loop
            Ok(Some(Close)) => break,

            // This should be unreachable, panic if we get a None, but this is only when the
            // snapshotter was removed from the snapshotters map
            Ok(None) => unreachable!(),

            // Reached timeout, lock the hashmap to prevent other items from requesting, send a
            // close signal over the channel, then loop back around, service all remaining
            // requests
            Err(_) => {
                let snapshotter = snapshotters.write().await.remove(&series_id).unwrap();
                snapshotter.close().await;
            }
        }
    }
}

//async fn create_topic(bootstrap: &str, topic: &str) {
//    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
//        .set("bootstrap.servers", bootstrap)
//        .create().unwrap();
//    let topic = [NewTopic {
//        name: topic,
//        num_partitions: 1,
//        replication: TopicReplication::Fixed(3),
//        config: Vec::new(),
//    }];
//    let opts = AdminOptions::new();
//    let result = admin.create_topics(&topic, &opts).await.unwrap();
//    match result[0].as_ref() {
//        Ok(_) => {},
//        Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}, // Ok if topic already exists
//        Err(_) => panic!("Can't create topic"),
//    };
//}

pub struct Snapshotter {
    worker: mpsc::UnboundedSender<SnapshotterRequest>,
}

impl Snapshotter {
    async fn request(&self) -> (i32, i64) {
        let (tx, rx) = sync::oneshot::channel();
        if let Err(_) = self.worker.send(SnapshotterRequest::Read(tx)) {
            panic!("Requesting to non-existent snapshot worker");
        }
        rx.await.unwrap()
    }
    async fn close(&self) {
        if let Err(_) = self.worker.send(SnapshotterRequest::Close) {
            panic!("Requesting to non-existent snapshot worker");
        }
    }
}

#[derive(Clone)]
pub struct ReadServer {
    series_table: Arc<DashMap<SeriesId, Series>>,

    // Use RwLock hashmap here because DashMap might deadlock if getting a &mut ref and holding
    // another reference.
    snapshotters: Arc<RwLock<HashMap<SeriesId, Snapshotter>>>,
}

impl ReadServer {
    pub fn new(series_table: Arc<DashMap<SeriesId, Series>>) -> Self {
        Self {
            series_table,
            snapshotters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn initialize_snapshotter(&self, snapshot_interval: Duration, series_id: SeriesId) {
        let mut write_guard = self.snapshotters.write().await;

        // If there already is one, must have been another request that called this method and we
        // don't insert. Otherwise, init a new snapshotter. This will NOT content with an existing
        // snapshotter in that is on the way to closing because initialize is only called when
        // there are no snapshotters in the map in the current read request
        write_guard.entry(series_id).or_insert({
            let (worker, rx) = mpsc::unbounded_channel();
            let series = self.series_table.get(&series_id).unwrap().clone();
            RUNTIME.spawn(snapshot_worker(
                snapshot_interval,
                series_id,
                series,
                self.snapshotters.clone(),
                rx,
            ));
            Snapshotter { worker }
        });
    }

    pub async fn read_request(&self, series_id: SeriesId) -> (i32, i64) {
        let read_guard = self.snapshotters.read().await;
        if let Some(snapshotter) = read_guard.get(&series_id) {
            snapshotter.request().await
        } else {
            drop(read_guard);
            self.initialize_snapshotter(Duration::from_secs(1), series_id)
                .await;
            self.snapshotters
                .read()
                .await
                .get(&series_id)
                .unwrap()
                .request()
                .await
        }
    }
}

#[cfg(test)]
mod test {
    use crate::compression::*;
    use crate::constants::*;
    use crate::persistent_list::*;
    use crate::series::*;
    use crate::tags::*;
    use crate::test_utils::*;
    use crate::tsdb::Mach;
    use rand::prelude::*;
    use std::{
        collections::HashMap,
        env,
        sync::{Arc, Mutex},
    };
    use tempfile::tempdir;

    #[test]
    fn read_test() {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let mut compression = Vec::new();
        for _ in 0..nvars {
            compression.push(CompressFn::XOR);
        }
        let compression = Compression::from(compression);
        let buffer = ListBuffer::new(BUFSZ);
        let tags = {
            let mut map = HashMap::new();
            map.insert(String::from("foo"), String::from("bar"));
            Tags::from(map)
        };
        let series_conf = SeriesConfig {
            tags: Tags::from(tags),
            compression,
            seg_count: 1,
            nvars,
            types: vec![Types::F64; nvars],
        };
        let series_id = series_conf.tags.id();

        // Setup Mach and writers
        let mut mach = Mach::<VectorBackend>::new();
        let mut writer = mach.new_writer().unwrap();
        let _writer_id = mach.add_series(series_conf).unwrap();
        let ref_id = writer.get_reference(series_id);

        let mut values = vec![[0u8; 8]; nvars];
        for sample in data.iter() {
            'inner: loop {
                for (i, v) in sample.values.iter().enumerate() {
                    values[i] = v.to_be_bytes();
                }
                let res = writer.push(ref_id, sample.ts, &values);
                match res {
                    Ok(_) => {
                        break 'inner;
                    }
                    Err(_) => {}
                }
            }
        }

        let reader = mach.reader(series_id).unwrap();
    }
}
