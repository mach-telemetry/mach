use crate::{
    active_block::ActiveBlock,
    constants::*,
    durable_queue::KafkaConfig,
    id::WriterId,
    runtime::RUNTIME,
    segment::SegmentSnapshot,
    series::Series,
    utils::{random_id, wp_lock::WpLock},
};
use lzzzz::lz4;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    time::sleep,
};

pub struct DurabilityWorker {
    chan: UnboundedSender<Series>,
}

impl DurabilityWorker {
    pub fn new(writer_id: WriterId, active_block: Arc<WpLock<ActiveBlock>>) -> Self {
        init(writer_id, active_block)
    }

    pub fn register_series(&self, series: Series) {
        if self.chan.send(series).is_err() {
            panic!("Can't send series to durability task");
        }
    }
}

fn init(writer_id: WriterId, active_block: Arc<WpLock<ActiveBlock>>) -> DurabilityWorker {
    let writer_id: String = writer_id.inner().into();
    let (tx, rx) = unbounded_channel();
    let series = Arc::new(Mutex::new(Vec::<Series>::new()));
    let series2 = series.clone();
    RUNTIME.spawn(durability_receiver(rx, series2));
    RUNTIME.spawn(durability_worker(writer_id, series, active_block));
    DurabilityWorker { chan: tx }
}

async fn durability_receiver(mut recv: UnboundedReceiver<Series>, series: Arc<Mutex<Vec<Series>>>) {
    while let Some(item) = recv.recv().await {
        series.lock().await.push(item);
    }
}

async fn durability_worker(
    _writer_id: String,
    series: Arc<Mutex<Vec<Series>>>,
    active_block: Arc<WpLock<ActiveBlock>>,
) {
    let k_config = KafkaConfig {
        bootstrap: String::from(KAFKA_BOOTSTRAP),
        topic: random_id(),
    };

    let queue_config = k_config.config();
    let queue = queue_config.clone().make().unwrap();
    let mut queue_writer = queue.writer().unwrap();

    let mut encoded = Vec::new();
    let mut compressed = Vec::new();
    loop {
        sleep(Duration::from_secs(1)).await;
        let guard = series.lock().await;
        let mut snapshots = Vec::new();
        for series in guard.iter() {
            if let Ok(x) = series.segment_snapshot() {
                snapshots.push(x);
            }
        }
        drop(guard);
        let guard = active_block.protected_read();
        let mut buffer = guard.copy_buffer();
        if guard.release().is_err() {
            buffer = Vec::new().into_boxed_slice();
        }
        let data: (Vec<SegmentSnapshot>, Box<[u8]>) = (snapshots, buffer);
        bincode::serialize_into(&mut encoded, &data).unwrap();
        lz4::compress_to_vec(&encoded, &mut compressed, lz4::ACC_LEVEL_DEFAULT).unwrap();
        match queue_writer.write(&compressed[..]).await {
            Ok(offset) => println!(
                "Durability at offset {}, data size: {}",
                offset,
                compressed.len()
            ),
            Err(x) => println!("Durablity error {:?}", x),
        }
        //let (partition, offset) = producer.send(to_send, Duration::from_secs(0)).await.unwrap();
        encoded.clear();
        compressed.clear();
    }
}
