use crate::{
    persistent_list::ListSnapshot,
    segment::SegmentSnapshot,
    sample::Type,
    series::Series,
};
use serde::{Serialize, Deserialize};
use std::convert::From;
use bincode::{serialize_into, deserialize_from};
use tokio::{
    time,
    sync::mpsc,
};
use std::time::{Instant, Duration};

#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    segments: SegmentSnapshot,
    list: ListSnapshot,
}

impl Snapshot {
    pub fn new(segments: SegmentSnapshot, list: ListSnapshot) -> Self {
        Snapshot {
            segments,
            list
        }
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
    Read,
    Close
}

async fn snapshot_worker(duration: Duration, series: Series, mut receiver: mpsc::Receiver<SnapshotterRequest>) {
    let mut snapshot = series.snapshot().unwrap().to_bytes();
    let mut last_snapshot = Instant::now();
    loop {
        match receiver.recv() {
            _ => {
                if last_snapshot.elapsed() >= duration {
                    snapshot = series.snapshot().unwrap().to_bytes();
                }
                // Do stuff with snapshot
            },
            Close => break,
        }
    }
}

pub struct Snapshotter {
    worker: mpsc::Sender<SnapshotterRequest>
}

#[cfg(test)]
mod test {
    use crate::compression::*;
    use crate::constants::*;
    use crate::test_utils::*;
    use crate::tags::*;
    use crate::series::*;
    use crate::tsdb::Mach;
    use crate::persistent_list::*;
    use rand::prelude::*;
    use std::{
        env,
        sync::{Arc, Mutex},
        collections::HashMap,
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
                    Ok(_) => { break 'inner; },
                    Err(_) => { }
                }
            }
        }

        let reader = mach.reader(series_id).unwrap();
    }
}



