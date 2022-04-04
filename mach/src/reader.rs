use crate::{
    constants::*,
    durable_queue::{KafkaConfig, QueueConfig},
    id::SeriesId,
    runtime::*,
    series::Series,
    utils::*,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::From;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{
    sync::{self, mpsc, RwLock},
    time,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadResponse {
    response_queue: QueueConfig,
    data_queue: QueueConfig,
    offset: u64,
}

pub enum SnapshotterRequest {
    Read(sync::oneshot::Sender<ReadResponse>),
    Close,
}

async fn snapshot_worker(
    duration: Duration,
    series_id: SeriesId,
    series: Series,
    snapshotters: Arc<RwLock<HashMap<SeriesId, Snapshotter>>>,
    mut receiver: mpsc::UnboundedReceiver<SnapshotterRequest>,
) {
    let response_config = KafkaConfig {
        bootstrap: String::from(KAFKA_BOOTSTRAP),
        topic: random_id(),
    };

    let queue_config = response_config.config();
    let queue = queue_config.clone().make().unwrap();
    let mut queue_writer = queue.writer().unwrap();

    let mut response = ReadResponse {
        response_queue: queue_config,
        data_queue: series.queue().clone(),
        offset: 0,
    };

    let mut snapshot: Arc<[u8]> = series.snapshot().unwrap().to_bytes().into();
    let mut durable = false;
    let mut last_snapshot = Instant::now();
    loop {
        match time::timeout(Duration::from_secs(30), receiver.recv()).await {
            Ok(Some(SnapshotterRequest::Read(sender))) => {
                // Make a new snapshot only when there is a request and the last snapshot is older
                // than the snapshot interval. If so, mark as not durable
                if last_snapshot.elapsed() >= duration {
                    snapshot = series.snapshot().unwrap().to_bytes().into();
                    durable = false;
                    last_snapshot = Instant::now();
                }

                // If the snapshot is not durable, send to kafka topic for durability
                if !durable {
                    response.offset = queue_writer.write(&snapshot[..]).await.unwrap();
                    durable = true;
                }

                sender.send(response.clone()).unwrap();
            }

            // Got a close signal from the timeout match below in a prior loop
            Ok(Some(SnapshotterRequest::Close)) => break,

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

pub struct Snapshotter {
    worker: mpsc::UnboundedSender<SnapshotterRequest>,
}

impl Snapshotter {
    async fn request(&self) -> ReadResponse {
        let (tx, rx) = sync::oneshot::channel();
        if self.worker.send(SnapshotterRequest::Read(tx)).is_err() {
            panic!("Requesting to non-existent snapshot worker");
        }
        rx.await.unwrap()
    }
    async fn close(&self) {
        if self.worker.send(SnapshotterRequest::Close).is_err() {
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
        // don't insert. Otherwise, init a new snapshotter. This will NOT contend with an existing
        // snapshotter  that is on the way to closing because initialize is only called when
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

    pub async fn read_request(&self, series_id: SeriesId) -> ReadResponse {
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
    //use crate::compression::*;
    //use crate::constants::*;
    //use crate::persistent_list::*;
    //use crate::series::*;
    //use crate::tags::*;
    //use crate::test_utils::*;
    //use crate::tsdb::{Mach, Config};
    //use rand::prelude::*;
    //use std::{
    //    collections::HashMap,
    //    env,
    //    sync::{Arc, Mutex},
    //};
    //use tempfile::tempdir;

    //#[test]
    //fn read_test() {
    //    let data = &MULTIVARIATE_DATA[0].1;
    //    let nvars = data[0].values.len();
    //    let mut compression = Vec::new();
    //    for _ in 0..nvars {
    //        compression.push(CompressFn::XOR);
    //    }
    //    let compression = Compression::from(compression);
    //    let buffer = ListBuffer::new(BUFSZ);
    //    let tags = {
    //        let mut map = HashMap::new();
    //        map.insert(String::from("foo"), String::from("bar"));
    //        Tags::from(map)
    //    };
    //    let series_conf = SeriesConfig {
    //        tags: Tags::from(tags),
    //        compression,
    //        seg_count: 1,
    //        nvars,
    //        types: vec![Types::F64; nvars],
    //    };
    //    let series_id = series_conf.tags.id();

    //    // Setup Mach and writers
    //    let mut mach = Mach::<VectorBackend>::new(Config::default());
    //    let mut writer = mach.new_writer().unwrap();
    //    let _writer_id = mach.add_series(series_conf).unwrap();
    //    let ref_id = writer.get_reference(series_id);

    //    let mut values = vec![[0u8; 8]; nvars];
    //    for sample in data.iter() {
    //        'inner: loop {
    //            for (i, v) in sample.values.iter().enumerate() {
    //                values[i] = v.to_be_bytes();
    //            }
    //            let res = writer.push(ref_id, sample.ts, &values);
    //            match res {
    //                Ok(_) => {
    //                    break 'inner;
    //                }
    //                Err(_) => {}
    //            }
    //        }
    //    }

    //    //let reader = mach.read(series_id).unwrap();
    //}
}
