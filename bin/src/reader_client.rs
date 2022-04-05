pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use mach::durable_queue::{DurableQueueReader, FileConfig, KafkaConfig, QueueConfig};
use mach::snapshot::{Snapshot, SnapshotItem, SnapshotReader};
use mach_rpc::tsdb_service_client::TsdbServiceClient;
use mach_rpc::writer_service_client::WriterServiceClient;
use mach_rpc::{
    add_series_request::ValueType, queue_config, value::PbType, AddSeriesRequest,
    AddSeriesResponse, GetSeriesReferenceRequest, GetSeriesReferenceResponse, PushRequest,
    ReadSeriesRequest, Sample, Value,
};
use std::collections::HashMap;
use std::path::PathBuf;

impl queue_config::Configs {
    fn to_mach_config(&self) -> QueueConfig {
        match self {
            queue_config::Configs::Kafka(cfg) => QueueConfig::Kafka(KafkaConfig {
                bootstrap: cfg.bootstrap.clone(),
                topic: cfg.topic.clone(),
            }),
            queue_config::Configs::File(cfg) => QueueConfig::File(FileConfig {
                dir: PathBuf::from(cfg.dir.clone()),
                file: cfg.file.clone(),
            }),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = TsdbServiceClient::connect("http://127.0.0.1:50050")
        .await
        .unwrap();

    let req = AddSeriesRequest {
        types: vec![ValueType::F64.into(), ValueType::Bytes.into()],
        tags: [(String::from("hello"), String::from("world"))]
            .into_iter()
            .collect(),
    };
    let AddSeriesResponse {
        writer_address,
        series_id,
    } = client.add_series(req).await.unwrap().into_inner();

    let writer_address = format!("http://{}", writer_address);
    let mut writer_client = WriterServiceClient::connect(writer_address).await.unwrap();

    let series_ref_request = GetSeriesReferenceRequest { series_id };
    let GetSeriesReferenceResponse { series_reference } = writer_client
        .get_series_reference(series_ref_request)
        .await
        .unwrap()
        .into_inner();
    println!("Series reference: {:?}", series_reference);

    // Samples
    let mut samples = HashMap::new();
    let sample = Sample {
        timestamp: 12345,
        values: vec![
            Value {
                pb_type: Some(PbType::F64(123.4)),
            },
            Value {
                pb_type: Some(PbType::Str("hello world".into())),
            },
        ],
    };
    samples.insert(series_reference, sample);
    let request = PushRequest { samples };

    let results = writer_client
        .push(request)
        .await
        .unwrap()
        .into_inner()
        .results;
    println!("{:?}", results);

    // Read snapshot
    let r = client
        .read(ReadSeriesRequest { series_id })
        .await
        .unwrap()
        .into_inner();

    let resp_queue_cfg = r.response_queue.unwrap().configs.unwrap().to_mach_config();
    let data_queue_cfg = r.data_queue.unwrap().configs.unwrap().to_mach_config();

    let mut resp_reader = DurableQueueReader::from_config(resp_queue_cfg).unwrap();
    let snapshot = Snapshot::from_bytes(resp_reader.read(r.offset).unwrap());

    let mut snap_reader = SnapshotReader::new(
        &snapshot,
        DurableQueueReader::from_config(data_queue_cfg).unwrap(),
    )
    .unwrap();

    while let Ok(Some(item)) = snap_reader.next_item() {
        match item {
            SnapshotItem::Active(item) => println!("{:?}", item.timestamps()),
            SnapshotItem::Compressed(item) => println!("{:?}", item.timestamps()),
        }
    }
    println!("all items read");

    Ok(())
}
