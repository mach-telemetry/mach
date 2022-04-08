pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}
use tonic::transport::Channel;

use mach::durable_queue::{DurableQueueReader, FileConfig, KafkaConfig, QueueConfig};
use mach::snapshot::{Snapshot, SnapshotItem, SnapshotReader};
use mach_rpc::reader_service_client::ReaderServiceClient;
use mach_rpc::tsdb_service_client::TsdbServiceClient;
use mach_rpc::writer_service_client::WriterServiceClient;
use mach_rpc::{
    add_series_request::ValueType, queue_config, value::PbType, AddSeriesRequest,
    AddSeriesResponse, GetSeriesReferenceRequest, GetSeriesReferenceResponse, PushRequest,
    ReadSeriesRequest, Sample, Value,
};
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;

const NUM_SAMPLES: usize = 300;

type SeriesReference = u64;

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

struct TsdbClient {
    inner: TsdbServiceClient<Channel>,
}

impl TsdbClient {
    async fn new(address: &'static str) -> Result<Self, tonic::transport::Error> {
        let inner = TsdbServiceClient::connect(address).await?;
        Ok(Self { inner })
    }

    async fn add_series(
        self: &mut Self,
        req: AddSeriesRequest,
    ) -> Result<AddSeriesResponse, Box<dyn Error>> {
        let r = self.inner.add_series(req).await?;
        Ok(r.into_inner())
    }
}

struct WriterClient {
    inner: WriterServiceClient<Channel>,
}

impl WriterClient {
    async fn new(address: String) -> Result<Self, tonic::transport::Error> {
        Ok(Self {
            inner: WriterServiceClient::connect(address).await?,
        })
    }

    async fn get_series_ref(
        self: &mut Self,
        series_id: u64,
    ) -> Result<SeriesReference, Box<dyn Error>> {
        let GetSeriesReferenceResponse { series_reference } = self
            .inner
            .get_series_reference(GetSeriesReferenceRequest { series_id })
            .await?
            .into_inner();
        Ok(series_reference)
    }

    async fn push(
        self: &mut Self,
        samples: HashMap<SeriesReference, Sample>,
    ) -> Result<HashMap<SeriesReference, bool>, Box<dyn Error>> {
        let results = self
            .inner
            .push(PushRequest { samples })
            .await?
            .into_inner()
            .results;

        Ok(results)
    }
}

struct ReaderClient {
    inner: ReaderServiceClient<Channel>,
}

impl ReaderClient {
    async fn new(address: &'static str) -> Result<Self, tonic::transport::Error> {
        Ok(Self {
            inner: ReaderServiceClient::connect(address).await?,
        })
    }

    async fn get_snapshot(
        self: &mut Self,
        series_id: u64,
    ) -> Result<SnapshotReader, Box<dyn Error>> {
        let r = self
            .inner
            .read(ReadSeriesRequest { series_id })
            .await?
            .into_inner();

        let resp_queue_cfg = r.response_queue.unwrap().configs.unwrap().to_mach_config();
        let data_queue_cfg = r.data_queue.unwrap().configs.unwrap().to_mach_config();

        let mut resp_reader = DurableQueueReader::from_config(resp_queue_cfg).unwrap();
        let snapshot = Snapshot::from_bytes(resp_reader.read(r.offset).unwrap());

        Ok(SnapshotReader::new(
            &snapshot,
            DurableQueueReader::from_config(data_queue_cfg).unwrap(),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    use ValueType::{Bytes, F64};

    let mut tsdb_client = TsdbClient::new("http://127.0.0.1:50050")
        .await
        .expect("TSDB service not started");

    // register a time series
    let r = tsdb_client
        .add_series(AddSeriesRequest {
            types: vec![F64.into(), Bytes.into()],
            tags: HashMap::from([("hello".into(), "world".into())]),
        })
        .await?;
    let series_id = r.series_id;
    let writer_address = r.writer_address;

    // push sample
    let mut writer_client = WriterClient::new(format!("http://{}", writer_address))
        .await
        .expect("writer client could not be connected");

    let series_ref = writer_client.get_series_ref(series_id).await?;
    println!("Series reference: {:?}", series_ref);

    for _ in 0..NUM_SAMPLES {
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
        let samples = HashMap::from([(series_ref, sample)]);

        let push_resp = writer_client.push(samples).await?;
        assert!(push_resp.get(&series_ref).unwrap());
    }

    // read sample
    let mut reader_client = ReaderClient::new("http://127.0.0.1:51000")
        .await
        .expect("Reader Service not started");

    let mut snap_reader = reader_client.get_snapshot(series_id).await?;

    let mut c = 0;
    while let Ok(Some(item)) = snap_reader.next_item() {
        match item {
            SnapshotItem::Active(item) => {
                println!("{:?}", item.timestamps());
                c += item.timestamps().len();
            }
            SnapshotItem::Compressed(item) => {
                println!("{:?}", item.timestamps());
                c += item.timestamps().len();
            }
        }
    }
    println!("{} items read", c);
    assert_eq!(c, NUM_SAMPLES);

    Ok(())
}
