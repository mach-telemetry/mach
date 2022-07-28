use crate::bytes_server::{BytesClient, BytesHandler, BytesServer, Status};
use mach::{
    id::{SeriesId},
    snapshotter::{SnapshotId, Snapshotter, SnapshotterId},
    tsdb::Mach,
};
use std::time::Duration;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum SnapshotRequest {
    Initialize {
        series_id: SeriesId,
        interval: Duration,
        timeout: Duration,
    },
    Get(SnapshotterId),
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
                    timeout,
                } => {
                    let id = self.0.initialize_snapshotter(series_id, interval, timeout);
                    Some(SnapshotResponse::SnapshotterId(id))
                }
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
    println!("INITTING SERVER");
    let server = BytesServer::new(SnapshotterHandler(mach.init_snapshotter()));
    std::thread::spawn(move || server.serve());
    println!("INITTED SERVER");
}

pub struct SnapshotClient(BytesClient);

impl SnapshotClient {
    pub async fn new() -> Self {
        Self(BytesClient::new().await)
    }

    pub async fn initialize(
        &mut self,
        series_id: SeriesId,
        interval: Duration,
        timeout: Duration,
    ) -> Option<SnapshotterId> {
        let request = SnapshotRequest::Initialize {
            series_id,
            interval,
            timeout,
        };

        match self.request(request).await? {
            SnapshotResponse::SnapshotterId(x) => Some(x),
            _ => panic!("Unexpected returned type"),
        }
    }

    pub async fn get(&mut self, snapshotter_id: SnapshotterId) -> Option<SnapshotId> {
        let request = SnapshotRequest::Get(snapshotter_id);
        match self.request(request).await? {
            SnapshotResponse::SnapshotId(x) => Some(x),
            _ => panic!("Unexpected returned type"),
        }
    }

    async fn request(&mut self, request: SnapshotRequest) -> Option<SnapshotResponse> {
        let bytes = bincode::serialize(&request).unwrap();
        match self.0.send(Some(bytes)).await {
            None => None,
            Some(result) => Some(bincode::deserialize(&result).unwrap()),
        }
    }
}
