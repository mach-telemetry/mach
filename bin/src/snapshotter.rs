use mach::{
    id::{SeriesId, SeriesRef},
    sample::SampleType,
    utils::kafka::{make_topic, Producer, BufferedConsumer, ConsumerOffset},
    tsdb::Mach,
    series::Series,
    writer::{Writer as MachWriter, WriterConfig},
    mem_list::{BOOTSTRAPS, TOPIC, UNFLUSHED_COUNT},
    snapshotter::{Snapshotter, SnapshotId, SnapshotterId},
    snapshot::Snapshot,
};
use crate::bytes_server::{BytesServer, BytesHandler, Status};
use std::time::Duration;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum SnapshotterRequest {
    Initialize {
        series_id: SeriesId,
        interval: Duration,
        timeout: Duration,
    },
    Get(SnapshotterId)
}

pub enum SnapshotterResponse {
    Id(SnapshotterId),
    Snapshot(Snapshot),
}

impl SnapshotterResponse {
    pub fn id_from_bytes(bytes: &[u8]) -> Self {
        SnapshotterResponse::Id(bincode::deserialize(bytes).unwrap())
    }

    pub fn snapshot_from_bytes(bytes: &[u8]) -> Self {
        SnapshotterResponse::Snapshot(bincode::deserialize(bytes).unwrap())
    }
}

pub struct SnapshotterHandler(Snapshotter);

impl BytesHandler for SnapshotterHandler {
    fn handle_bytes(&self, bytes: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, Status> {
        match bincode::deserialize(bytes.unwrap().as_slice()).unwrap() {
            SnapshotterRequest::Initialize {
                series_id,
                interval,
                timeout
            } => {
                let id = self.0.initialize_snapshotter(series_id, interval, timeout);
                Ok(Some(bincode::serialize(&id).unwrap()))
            },
            SnapshotterRequest::Get(id) => {
                match self.0.get(id) {
                    None => Ok(None),
                    Some(x) => {
                        let serialized = bincode::serialize(x.as_ref()).unwrap();
                        Ok(Some(serialized))
                    }
                }
            }
        }
    }
}

pub fn initialize_snapshot_server(mach: &Mach) {
    let server = BytesServer::new(SnapshotterHandler(mach.init_snapshotter()));
}

