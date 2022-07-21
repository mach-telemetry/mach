use mach::{
    id::{SeriesId, SeriesRef},
    sample::SampleType,
    utils::kafka::{make_topic, Producer, BufferedConsumer, ConsumerOffset, BOOTSTRAPS, TOPIC},
    tsdb::Mach,
    series::Series,
    writer::{Writer as MachWriter, WriterConfig},
    mem_list::{UNFLUSHED_COUNT},
    snapshotter::{Snapshotter, SnapshotId, SnapshotterId},
    snapshot::Snapshot,
};
use crate::bytes_server::{BytesServer, BytesHandler, Status, BytesClient};
use std::time::Duration;
use futures::executor::block_on;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum SnapshotRequest {
    Initialize {
        series_id: SeriesId,
        interval: Duration,
        timeout: Duration,
    },
    Get(SnapshotterId)
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum SnapshotResponse {
    SnapshotterId(SnapshotterId),
    SnapshotId(SnapshotId),
}

pub struct SnapshotterHandler(Snapshotter);

impl BytesHandler for SnapshotterHandler {
    fn handle_bytes(&self, bytes: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, Status> {
        let result: Option<SnapshotResponse> = 
            match bincode::deserialize(bytes.unwrap().as_slice()).unwrap() {
                SnapshotRequest::Initialize {
                    series_id,
                    interval,
                    timeout
                } => {
                    let id = self.0.initialize_snapshotter(series_id, interval, timeout);
                    Some(SnapshotResponse::SnapshotterId(id))
                },
                    SnapshotRequest::Get(id) => {
                        if let Some(id) = self.0.get(id) {
                            Some(SnapshotResponse::SnapshotId(id))
                        } else {
                            None
                        }
                    }
            };

        if let Some(x) = result {
            Ok(Some(bincode::serialize(&x).unwrap()))
        } else {
            Ok(None)
        }
    }
}

pub fn initialize_snapshot_server(mach: &Mach) {
    let server = BytesServer::new(SnapshotterHandler(mach.init_snapshotter()));
    std::thread::spawn(move || {
        server.serve()
    });
}


pub struct SnapshotClient(BytesClient);

impl SnapshotClient {
    pub fn new() -> Self {
        Self(block_on(BytesClient::new()))
    }

    pub fn request(&mut self, request: SnapshotRequest) -> Option<SnapshotResponse> {
        let bytes = bincode::serialize(&request).unwrap();
        match block_on(self.0.send(Some(bytes))) {
            None => None,
            Some(result) => Some(bincode::deserialize(&result).unwrap())
        }
    }
}
