use crate::{
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
    convert::TryInto,
};
use lzzzz::lz4::*;
use lazy_static::*;
use crossbeam::channel::{bounded, Sender, Receiver};

lazy_static! {
    static ref SNAPSHOT_TABLE: Arc<DashMap<SnapshotterId, Arc<Mutex<SnapshotWorkerData>>>> = {
        Arc::new(DashMap::new())
    };

    static ref SNAPSHOT_SENDER: Sender<SnapshotterId> = {
        let _ = SNAPSHOT_TABLE.clone();
        let (tx, rx) = bounded(1000);
        thread::spawn(move || snapshots_to_kafka(rx));
        tx
    };

    static ref PRODUCER: Arc<Mutex<Producer>> = Arc::new(Mutex::new(Producer::new()));
}

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
        decompress_snapshot(vec.as_slice())
    }
}


fn snapshots_to_kafka(rx: Receiver<SnapshotterId>) {
    let mut producer = Producer::new();
    while let Ok(id) = rx.recv() {
        let entry = SNAPSHOT_TABLE.get(&id).unwrap().value().clone();
        let mut guard = entry.lock().unwrap();
        let new_entry = guard.snapshot_id.flush(&mut producer);
        guard.snapshot_id = new_entry;
    }
}

struct SnapshotWorkerData {
    snapshot_id: SnapshotEntry,
    series: Series,
    //last_request: Instant,
    //interval: Duration,
    //time_out: Duration,
}

#[derive(Clone, Debug)]
enum SnapshotEntry {
    Id(SnapshotId),
    Bytes(Box<[u8]>)
}

impl SnapshotEntry {
    fn flush(&self, p: &mut Producer) -> Self {
        match self {
            Self::Id(_) => self.clone(),
            Self::Bytes(x) => {
                let now = Instant::now();
                let entry = p.send(0, &x[..]);
                println!("Snapshot flushed {} in {:?}", x.len(), now.elapsed());
                let id = SnapshotId { kafka: entry };
                Self::Id(id)
            }
        }
    }
}

pub struct Snapshotter {
    series_table: Arc<DashMap<SeriesId, Series>>, // Same series table as in tsdb
    id_map: Arc<DashMap<SeriesId, SnapshotterId>>,
    worker_id: AtomicUsize,
}

impl Snapshotter {
    pub fn new(series_table: Arc<DashMap<SeriesId, Series>>) -> Self {
        Self {
            series_table,
            id_map: Arc::new(DashMap::new()),
            worker_id: AtomicUsize::new(0),
        }
    }

    pub fn initialize_snapshotter(
        &self,
        series_id: SeriesId,
        _interval: Duration,
        _time_out: Duration,
    ) -> SnapshotterId {
        //let triple = (series_id, interval, time_out);
        *self.id_map.entry(series_id).or_insert_with(|| {
            let worker_id = SnapshotterId(self.worker_id.fetch_add(1, SeqCst));

            let series = self.series_table.get(&series_id).unwrap().clone();
            //let last_request = Instant::now();

            println!("Initial snapshotting");
            let now = Instant::now();
            let mut producer = Producer::new();
            let snapshot = series.snapshot();
            let d = now.elapsed();
            let now = Instant::now();
            let compressed_bytes = compress_snapshot(snapshot);
            let d2 = now.elapsed();
            println!("Snapshot creation: {:?}, compression: {:?}", d, d2);
            let snapshot_entry = SnapshotEntry::Bytes(compressed_bytes).flush(&mut producer);

            let snapshot_worker_data = Arc::new(Mutex::new(SnapshotWorkerData {
                snapshot_id: snapshot_entry,
                series,
                //last_request,
                //interval,
                //time_out,
            }));

            SNAPSHOT_TABLE.insert(worker_id, snapshot_worker_data);
            //thread::spawn(move || snapshot_worker(worker_id));

            worker_id
        })
    }

    pub fn get(&self, id: SnapshotterId) -> Option<SnapshotId> {
        let entry = SNAPSHOT_TABLE.get(&id)?.value().clone();
        let guard = entry.lock().unwrap();

        let now = Instant::now();
        let snapshot = guard.series.snapshot();
        let d = now.elapsed();
        let now = Instant::now();
        let compressed_bytes = compress_snapshot(snapshot);
        let d2 = now.elapsed();
        println!("Snapshot creation: {:?}, compression: {:?}", d, d2);
        let mut guard = PRODUCER.lock().unwrap();
        let snapshot_entry = SnapshotEntry::Bytes(compressed_bytes).flush(&mut *guard);
        match snapshot_entry {
            SnapshotEntry::Id(x) => Some(x),
            _ => unreachable!(),
        }


        //match &mut guard.snapshot_id {
        //    SnapshotEntry::Id(x) => {},
        //    SnapshotEntry::Bytes(x) => {
        //        guard.last_request = Instant::now();
        //        let new_entry = guard.snapshot_id.flush(&mut Producer::new());
        //        guard.snapshot_id = new_entry;
        //    },
        //}
        //guard.last_request = Instant::now();
        //match &guard.snapshot_id {
        //    SnapshotEntry::Id(x) => {
        //        println!("SENDING {:?}", x);
        //        Some(x.clone())
        //    },
        //    _ => unreachable!(),
        //}
    }
}

//fn snapshot_worker(worker_id: SnapshotterId) {
//    println!("INITING Snapshot*****************");
//    let snapshot_table = SNAPSHOT_TABLE.clone();
//    let data = snapshot_table.get(&worker_id).unwrap().clone();
//    let guard = data.lock().unwrap();
//    let time_out = guard.time_out;
//    let series = guard.series.clone();
//    let interval = guard.interval;
//    drop(guard);
//
//    //let mut producer = Producer::new();
//
//    loop {
//        {
//            let guard = data.lock().unwrap();
//            let last = guard.last_request;
//            if Instant::now() - last > time_out {
//                snapshot_table.remove(&worker_id);
//                break;
//            }
//        }
//        {
//            let now = Instant::now();
//            let snapshot = series.snapshot();
//            let snapshot_time = now.elapsed();
//
//            let now = Instant::now();
//            let compressed = compress_snapshot(snapshot);
//            let compress_time = now.elapsed();
//
//            let snapshot_entry = SnapshotEntry::Bytes(compressed);
//
//            println!("Snapshot creation: {:?}, compression: {:?}", snapshot_time, compress_time);
//            let mut guard = data.lock().unwrap();
//            guard.snapshot_id = snapshot_entry;
//        }
//        thread::sleep(interval);
//    }
//}

fn compress_snapshot(snapshot: Snapshot) -> Box<[u8]> {
    let bytes = bincode::serialize(&snapshot).unwrap();
    let og_sz = bytes.len();
    let mut compressed_bytes = Vec::new();
    compressed_bytes.extend_from_slice(&og_sz.to_be_bytes());
    compress_to_vec(bytes.as_slice(), &mut compressed_bytes, ACC_LEVEL_DEFAULT).unwrap();
    println!("Snapshot compression result: {} -> {}", og_sz, compressed_bytes.len());
    assert_eq!(og_sz, usize::from_be_bytes(compressed_bytes[..8].try_into().unwrap()));
    compressed_bytes.into_boxed_slice()
}

fn decompress_snapshot(bytes: &[u8]) -> Snapshot {
    let og_sz = usize::from_be_bytes((&bytes[0..8]).try_into().unwrap());
    let mut data = vec![0u8; og_sz];
    decompress(&bytes[8..], &mut data).unwrap();
    bincode::deserialize(&data).unwrap()
}

