use crate::bytes_server::{BytesClient, BytesHandler, BytesServer, Status};
use mach2::{
    id::SourceId,
    snapshotter::{SnapshotId, Snapshotter},
    tsdb::Mach,
};
use std::time::Duration;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum SnapshotRequest {
    Get(SourceId),
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum SnapshotResponse {
    SnapshotId(SnapshotId),
}

pub struct SnapshotterHandler(Snapshotter);

impl BytesHandler for SnapshotterHandler {
    fn handle_bytes(&self, bytes: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, Status> {
        let result: Option<SnapshotResponse> =
            match bincode::deserialize(bytes.unwrap().as_slice()).unwrap() {
                //SnapshotRequest::Initialize {
                //    series_id,
                //    interval,
                //    timeout,
                //} => {
                //    let id = self.0.initialize_snapshotter(series_id, interval, timeout);
                //    Some(SnapshotResponse::SnapshotterId(id))
                //}
                SnapshotRequest::Get(id) => {
                    let id = self.0.get_snapshot(id);
                    Some(SnapshotResponse::SnapshotId(id))
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
    println!("Initing snapshot server");
    let server = BytesServer::new(SnapshotterHandler(mach.snapshotter()));
    std::thread::spawn(move || server.serve());
    println!("Inited snapshot server");
}

pub struct SnapshotClient(BytesClient);

impl SnapshotClient {
    pub async fn new(addr: &'static str) -> Self {
        Self(BytesClient::new(addr).await)
    }

    //pub async fn initialize(
    //    &mut self,
    //    series_id: SeriesId,
    //    interval: Duration,
    //    timeout: Duration,
    //) -> Option<SnapshotterId> {
    //    let request = SnapshotRequest::Initialize {
    //        series_id,
    //        interval,
    //        timeout,
    //    };

    //    match self.request(request).await? {
    //        SnapshotResponse::SnapshotterId(x) => Some(x),
    //        _ => panic!("Unexpected returned type"),
    //    }
    //}

    pub async fn get(&mut self, source_id: SourceId) -> Option<SnapshotId> {
        let request = SnapshotRequest::Get(source_id);
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
