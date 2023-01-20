// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use std::sync::{Arc, Mutex};
use dashmap::DashMap;
use crate::{
    constants::{PARTITIONS, SNAPSHOTTER_INTERVAL_SECS},
    source::{SourceId, Source},
    kafka::{Producer, KafkaEntry},
    snapshot::Snapshot,
};
use std::time::{Duration, Instant};
use serde::*;
use lzzzz::lz4;
use rand::{thread_rng, Rng};
use log::*;

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
    snapshot_table: Arc<DashMap<SourceId, SnapshotMetadata>>,
    producer: Arc<Mutex<Producer>>,
    interval: Duration,
}

impl Snapshotter {
    pub fn new(source_table: Arc<DashMap<SourceId, Source>>) -> Self {
        Self {
            source_table,
            snapshot_table: Arc::new(DashMap::new()),
            producer: Arc::new(Mutex::new(Producer::new())),
            interval: Duration::from_secs_f64(SNAPSHOTTER_INTERVAL_SECS),
        }
    }

    pub fn get_snapshot(&self, source_id: SourceId) -> SnapshotId {
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

    fn make_snapshot(&self, source_id: SourceId) -> SnapshotMetadata {
        let source = self.source_table.get(&source_id).unwrap().clone();
        let snapshot = source.snapshot();
        let bytes = compress_snapshot(snapshot);
        let id = SnapshotId(self.producer.lock().unwrap().send(thread_rng().gen_range(0..PARTITIONS), &bytes[..]));
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
    info!("Snapshot compression result: {} -> {}", og_sz, compressed_bytes.len());
    assert_eq!(og_sz, usize::from_be_bytes(compressed_bytes[..8].try_into().unwrap()));
    compressed_bytes.into_boxed_slice()
}

fn decompress_snapshot(bytes: &[u8]) -> Snapshot {
    let og_sz = usize::from_be_bytes((&bytes[0..8]).try_into().unwrap());
    let mut data = vec![0u8; og_sz];
    lz4::decompress(&bytes[8..], &mut data).unwrap();
    bincode::deserialize(&data).unwrap()
}
