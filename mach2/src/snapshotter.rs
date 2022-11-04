use std::sync::Arc;
use dashmap::DashMap;
use crate::{
    constants::{PARTITIONS, SNAPSHOTTER_INTERVAL_SECS},
    source::{SourceId, Source},
    kafka::{Producer, KafkaEntry},
    snapshot::Snapshot,
};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use serde::*;
use lzzzz::lz4;
use rand::{thread_rng, Rng};

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub struct SnapshotterId(usize);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotId(KafkaEntry);

impl SnapshotId {
    pub fn load(&self) -> Snapshot {
        let mut vec = Vec::new();
        self.0.load(&mut vec).unwrap();
        decompress_snapshot(vec.as_slice())
    }
}

struct SnapshotMetadata {
    id: SnapshotId,
    time: Instant,
}

pub struct Snapshotter {
    source_table: Arc<DashMap<SourceId, Source>>,
    snapshot_table: HashMap<SourceId, SnapshotMetadata>,
    producer: Producer,
    interval: Duration,
}

impl Snapshotter {
    pub fn new(source_table: Arc<DashMap<SourceId, Source>>) -> Self {
        Self {
            source_table,
            snapshot_table: HashMap::new(),
            producer: Producer::new(),
            interval: Duration::from_secs_f64(SNAPSHOTTER_INTERVAL_SECS),
        }
    }

    pub fn get_snapshot(&mut self, source_id: SourceId) -> SnapshotId {
        if let Some(metadata) = self.snapshot_table.get_mut(&source_id) {
                if metadata.time.elapsed() < self.interval {
                    return metadata.id.clone()
                }
        }
        let metadata = self.make_snapshot(source_id);
        let id = metadata.id.clone();
        self.snapshot_table.insert(source_id, metadata);
        id
    }

    fn make_snapshot(&mut self, source_id: SourceId) -> SnapshotMetadata {
        let source = self.source_table.get(&source_id).unwrap().clone();
        let snapshot = source.snapshot();
        let bytes = compress_snapshot(snapshot);
        let id = SnapshotId(self.producer.send(thread_rng().gen_range(0..PARTITIONS), &bytes[..]));
        let time = Instant::now();
        SnapshotMetadata { id, time }
    }
}

fn compress_snapshot(snapshot: Snapshot) -> Box<[u8]> {
    let bytes = bincode::serialize(&snapshot).unwrap();
    let og_sz = bytes.len();
    let mut compressed_bytes = Vec::new();
    compressed_bytes.extend_from_slice(&og_sz.to_be_bytes());
    lz4::compress_to_vec(bytes.as_slice(), &mut compressed_bytes, lz4::ACC_LEVEL_DEFAULT).unwrap();
    println!("Snapshot compression result: {} -> {}", og_sz, compressed_bytes.len());
    assert_eq!(og_sz, usize::from_be_bytes(compressed_bytes[..8].try_into().unwrap()));
    compressed_bytes.into_boxed_slice()
}

fn decompress_snapshot(bytes: &[u8]) -> Snapshot {
    let og_sz = usize::from_be_bytes((&bytes[0..8]).try_into().unwrap());
    let mut data = vec![0u8; og_sz];
    lz4::decompress(&bytes[8..], &mut data).unwrap();
    bincode::deserialize(&data).unwrap()
}
