use crate::{
    id::SeriesId,
    series::Series,
    snapshot::Snapshot,
    utils::kafka::{Producer, BOOTSTRAPS, TOPIC, random_partition, Client},
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

#[derive(Debug, serde::Serialize, serde::Deserialize, Copy, Clone)]
pub struct SnapshotId((i32, i64, usize));

impl SnapshotId {
    pub fn load(&self, client: &mut Client) -> Snapshot {
        let bytes = client.load(TOPIC, self.0.0, self.0.1, self.0.2);
        bincode::deserialize(&bytes[..]).unwrap()
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

            let mut producer = Producer::new(BOOTSTRAPS);
            let snapshot = series.snapshot();
            let bytes = bincode::serialize(&snapshot).unwrap();
            let l = bytes.len();
            let partition = random_partition();
            let (p, o) = producer.send(TOPIC, partition, bytes.as_slice());

            let snapshot_worker_data = Arc::new(Mutex::new(SnapshotWorkerData {
                snapshot_id: SnapshotId((p, o, l)),
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
        Some(guard.snapshot_id)
    }
}

fn snapshot_worker(worker_id: SnapshotterId, snapshot_table: SnapshotTable) {
    let data = snapshot_table.get(&worker_id).unwrap().clone();
    let guard = data.lock().unwrap();
    let time_out = guard.time_out;
    let series = guard.series.clone();
    let interval = guard.interval;
    drop(guard);

    let mut producer = Producer::new(BOOTSTRAPS);

    loop {
        {
            let mut guard = data.lock().unwrap();
            let last = guard.last_request;
            if Instant::now() - last > time_out {
                snapshot_table.remove(&worker_id);
                break;
            }
        }
        {
            let snapshot = series.snapshot();
            let bytes = bincode::serialize(&snapshot).unwrap();
            let l = bytes.len();
            let partition = random_partition();
            let (p, o) = producer.send(TOPIC, partition, bytes.as_slice());
            let snapshot = Arc::new(snapshot);
            let mut guard = data.lock().unwrap();
            guard.snapshot_id = SnapshotId((p, o, l));
        }
        thread::sleep(interval);
    }
}
