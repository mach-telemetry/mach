use crate::{
    constants::*,
    id::SeriesId,
    series::Series,
    snapshot::Snapshot,
    utils::kafka::{KafkaEntry, Producer},
};
use dashmap::DashMap;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotterId(usize);

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct SnapshotId {
    kafka: KafkaEntry,
}

impl SnapshotId {
    pub fn load(&self) -> Snapshot {
        let mut vec = Vec::new();
        self.kafka.load(&mut vec).unwrap();
        bincode::deserialize(&vec[..]).unwrap()
    }
}

type SnapshotTable = Arc<DashMap<SnapshotterId, Arc<Mutex<SnapshotWorkerData>>>>;

struct SnapshotWorkerData {
    snapshot_id: SnapshotId,
    series: Series,
    last_request: Instant,
    interval: Duration,
    time_out: Duration,
}

pub struct Snapshotter {
    series_table: Arc<DashMap<SeriesId, Series>>, // Same series table as in tsdb
    snapshot_table: SnapshotTable,
    id_map: Arc<DashMap<(SeriesId, Duration, Duration), SnapshotterId>>,
    worker_id: AtomicUsize,
}

impl Snapshotter {
    pub fn new(series_table: Arc<DashMap<SeriesId, Series>>) -> Self {
        Self {
            series_table,
            snapshot_table: Arc::new(DashMap::new()),
            id_map: Arc::new(DashMap::new()),
            worker_id: AtomicUsize::new(0),
        }
    }

    pub fn initialize_snapshotter(
        &self,
        series_id: SeriesId,
        interval: Duration,
        time_out: Duration,
    ) -> SnapshotterId {
        let triple = (series_id, interval, time_out);
        *self.id_map.entry(triple).or_insert_with(|| {
            let worker_id = SnapshotterId(self.worker_id.fetch_add(1, SeqCst));

            let series = self.series_table.get(&series_id).unwrap().clone();
            let last_request = Instant::now();

            let mut producer = Producer::new();
            let snapshot = series.snapshot();
            let bytes = bincode::serialize(&snapshot).unwrap();
            let kafka_entry = producer.send(bytes.as_slice(), PARTITIONS/2..PARTITIONS);

            let snapshot_worker_data = Arc::new(Mutex::new(SnapshotWorkerData {
                snapshot_id: SnapshotId { kafka: kafka_entry },
                series,
                last_request,
                interval,
                time_out,
            }));

            self.snapshot_table.insert(worker_id, snapshot_worker_data);
            let snapshot_table = self.snapshot_table.clone();

            thread::spawn(move || snapshot_worker(worker_id, snapshot_table));

            worker_id
        })
    }

    pub fn get(&self, id: SnapshotterId) -> Option<SnapshotId> {
        let data = self.snapshot_table.get(&id)?.value().clone();
        let mut guard = data.lock().unwrap();
        guard.last_request = Instant::now();
        Some(guard.snapshot_id.clone())
    }
}

fn snapshot_worker(worker_id: SnapshotterId, snapshot_table: SnapshotTable) {
    let data = snapshot_table.get(&worker_id).unwrap().clone();
    let guard = data.lock().unwrap();
    let time_out = guard.time_out;
    let series = guard.series.clone();
    let interval = guard.interval;
    drop(guard);

    let mut producer = Producer::new();

    loop {
        {
            let guard = data.lock().unwrap();
            let last = guard.last_request;
            if Instant::now() - last > time_out {
                snapshot_table.remove(&worker_id);
                break;
            }
        }
        {
            let snapshot = series.snapshot();
            let bytes = bincode::serialize(&snapshot).unwrap();
            let entry = producer.send(bytes.as_slice(), PARTITIONS/2..PARTITIONS);
            //let snapshot = Arc::new(snapshot);
            let id = SnapshotId { kafka: entry };
            let mut guard = data.lock().unwrap();
            guard.snapshot_id = id;
        }
        thread::sleep(interval);
    }
}
