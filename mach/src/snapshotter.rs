use crate::{id::SeriesId, series::Series, snapshot::Snapshot};
use dashmap::DashMap;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

#[derive(PartialEq, Eq, Hash, Copy, Clone)]
pub struct SnapshotterId(usize);

type SnapshotTable = Arc<DashMap<SnapshotterId, Arc<Mutex<SnapshotWorkerData>>>>;

struct SnapshotWorkerData {
    snapshot: Arc<Snapshot>,
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
            let snapshot = Arc::new(series.snapshot());
            let last_request = Instant::now();

            let snapshot_worker_data = Arc::new(Mutex::new(SnapshotWorkerData {
                snapshot,
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

    pub fn get(&self, id: SnapshotterId) -> Option<Arc<Snapshot>> {
        let data = self.snapshot_table.get(&id)?.value().clone();
        let mut guard = data.lock().unwrap();
        guard.last_request = Instant::now();
        Some(guard.snapshot.clone())
    }
}

fn snapshot_worker(worker_id: SnapshotterId, snapshot_table: SnapshotTable) {
    let data = snapshot_table.get(&worker_id).unwrap().clone();
    let guard = data.lock().unwrap();
    let time_out = guard.time_out;
    let series = guard.series.clone();
    let interval = guard.interval;
    drop(guard);

    loop {
        {
            let mut guard = data.lock().unwrap();
            let last = guard.last_request;
            if Instant::now() - last > time_out {
                snapshot_table.remove(&worker_id);
                break;
            }
            let snapshot = Arc::new(series.snapshot());
            guard.snapshot = snapshot;
        }
        thread::sleep(interval);
    }
}
