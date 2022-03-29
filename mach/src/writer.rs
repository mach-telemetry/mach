use crate::{
    active_block::*,
    compression::Compression,
    constants::*,
    //wal::Wal,
    durable_queue::{DurableQueue, DurableQueueWriter, QueueConfig},
    id::{SeriesId, SeriesRef, WriterId},
    persistent_list::*,
    runtime::*,
    sample::{Sample, Type},
    segment::{self, FlushSegment, FullSegment, Segment, WriteSegment},
    series::*,
    utils::wp_lock::WpLock,
};
use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

#[derive(Debug)]
pub enum Error {
    Segment(segment::Error),
}

impl From<segment::Error> for Error {
    fn from(item: segment::Error) -> Self {
        Error::Segment(item)
    }
}

pub struct WriterConfig {
    pub queue_config: QueueConfig,
    pub active_block_flush_sz: usize,
}

pub struct WriterMetadata {
    pub id: WriterId,
    pub queue_config: QueueConfig,
    pub active_block: Arc<WpLock<ActiveBlock>>,
    pub durable_queue: Arc<DurableQueue>,
}

pub struct Writer {
    global_meta: Arc<DashMap<SeriesId, Series>>,
    local_meta: HashMap<SeriesId, Series>,
    references: HashMap<SeriesId, usize>,
    writers: Vec<WriteSegment>,
    active_block: Arc<WpLock<ActiveBlock>>,
    durable_queue: Arc<DurableQueue>,
    lists: Vec<List>,
    list_maker_id: Vec<usize>,
    list_maker: ListMaker,
    id: WriterId,
}

impl Writer {
    pub fn new(
        global_meta: Arc<DashMap<SeriesId, Series>>,
        writer_config: WriterConfig,
    ) -> (Self, WriterMetadata) {
        let block_flush_sz = writer_config.active_block_flush_sz;
        let queue_config = writer_config.queue_config;
        let id = WriterId::random();
        let active_block = Arc::new(WpLock::new(ActiveBlock::new(block_flush_sz)));
        let durable_queue = Arc::new(queue_config.clone().make().unwrap());
        let list_maker = ListMaker::new(durable_queue.writer().unwrap());

        let meta = WriterMetadata {
            id: id.clone(),
            queue_config: queue_config.clone(),
            active_block: active_block.clone(),
            durable_queue: durable_queue.clone(),
        };

        let writer = Self {
            global_meta,
            local_meta: HashMap::new(),
            references: HashMap::new(),
            list_maker_id: Vec::new(),
            writers: Vec::new(),
            lists: Vec::new(),
            list_maker,
            id,
            active_block,
            durable_queue,
        };

        (writer, meta)
    }

    pub fn id(&self) -> WriterId {
        self.id.clone()
    }

    pub fn get_reference(&mut self, id: SeriesId) -> SeriesRef {
        let meta = self.global_meta.get(&id).unwrap().clone();
        let writer = meta.segment().writer().unwrap();
        let list = meta.list().clone();

        let list_maker_id = self.list_maker.register(ListMakerMeta {
            segment: writer.flush(),
            list: meta.list().writer(),
            id,
            compression: meta.compression().clone(),
        });

        let len = self.writers.len();
        self.references.insert(id, len);
        self.local_meta.insert(id, meta);
        self.writers.push(writer);
        self.lists.push(list);
        self.list_maker_id.push(list_maker_id);
        SeriesRef(len)
    }

    //pub fn push(&mut self, reference: SeriesRef, ts: u64, data: &[[u8; 8]]) -> Result<(), Error> {
    //    let reference = *reference;
    //    match self.writers[reference].push(ts, data)? {
    //        segment::PushStatus::Done => {}
    //        segment::PushStatus::Flush(_) => self.list_maker.flush(self.list_maker_id[reference]),
    //    }
    //    Ok(())
    //}

    pub fn push(&mut self, reference: SeriesRef, ts: u64, data: &[Type]) -> Result<(), Error> {
        let reference = *reference;
        match self.writers[reference].push_type(ts, data)? {
            segment::PushStatus::Done => {}
            segment::PushStatus::Flush(_) => self.list_maker.flush(self.list_maker_id[reference]),
        }
        Ok(())
    }
}

struct ListMakerMeta {
    segment: FlushSegment,
    list: ListWriter,
    id: SeriesId,
    compression: Compression,
}

impl ListMakerMeta {
    async fn write(&mut self, w: &mut DurableQueueWriter) {
        println!("WRITING TO ACTIVE BLOCK");
        if self.list.push(self.id, self.segment.clone(), &self.compression, w).await.is_err() {
            println!("Error writing to list");
        }

        // Safety: self.list.push call above that contains a clone doesn't mark the segment as
        // flushed
        unsafe {
            self.segment.flushed();
        }
    }
}

enum ListMakerRequest {
    Register(ListMakerMeta),
    Flush(usize),
}

impl std::fmt::Debug for ListMakerRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("ListMakerRequest");
        match self {
            Self::Register(_) => {
                ds.field("Register", &"");
            }
            Self::Flush(id) => {
                ds.field("Flush", &id);
            }
        }
        ds.finish()
    }
}

struct ListMaker {
    sender: UnboundedSender<ListMakerRequest>,
    register_counter: usize,
}

impl ListMaker {
    fn new(durable_queue_writer: DurableQueueWriter) -> Self {
        let (sender, receiver) = unbounded_channel();
        RUNTIME.spawn(worker(durable_queue_writer, receiver));
        ListMaker {
            sender,
            register_counter: 0,
        }
    }

    fn register(&mut self, meta: ListMakerMeta) -> usize {
        let id = self.register_counter;
        self.register_counter += 1;
        let e = "failed to send to flush worker";
        self.sender.send(ListMakerRequest::Register(meta)).unwrap();
        id
    }

    fn flush(&self, id: usize) {
        self.sender
            .send(ListMakerRequest::Flush(id))
            .expect("failed to send to flush worker");
    }
}

async fn worker(mut w: DurableQueueWriter, mut queue: UnboundedReceiver<ListMakerRequest>) {
    let mut metadata: Vec<ListMakerMeta> = Vec::new();
    while let Some(item) = queue.recv().await {
        match item {
            ListMakerRequest::Register(meta) => metadata.push(meta),
            ListMakerRequest::Flush(id) => metadata[id].write(&mut w).await,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::compression::*;
    use crate::constants::*;
    use crate::id::*;
    use crate::tags::*;
    use crate::test_utils::*;
    use rand::prelude::*;
    use std::{
        env,
        sync::{Arc, Mutex},
    };
    use tempfile::tempdir;
    use crate::utils::*;

    //#[test]
    //fn test_vec_writer() {
    //    let vec = Arc::new(Mutex::new(Vec::new()));
    //    let mut persistent_writer = VectorWriter::new(vec.clone());
    //    let mut persistent_reader = VectorReader::new(vec.clone());
    //    sample_data(persistent_reader, persistent_writer);
    //}

    #[test]
    fn test_file_writer() {
        use crate::durable_queue::FileConfig;
        let dir = tempdir().unwrap().into_path();
        let file = String::from("test");
        let queue_config = FileConfig { dir, file };
        sample_data(queue_config.config());
    }

    #[test]
    fn test_kafka_writer() {
        use crate::durable_queue::KafkaConfig;
        let bootstrap = String::from(KAFKA_BOOTSTRAP);
        let topic = random_id();
        let queue_config = KafkaConfig { bootstrap, topic };
        sample_data(queue_config.config());
    }

    //#[cfg(feature = "kafka-backend")]
    //#[test]
    //fn test_kafka_writer() {
    //    let mut persistent_writer = KafkaWriter::new(KAFKA_BOOTSTRAP).unwrap();
    //    let mut persistent_reader = KafkaReader::new(KAFKA_BOOTSTRAP).unwrap();
    //    sample_data(persistent_reader, persistent_writer);
    //}

    //#[cfg(feature = "redis-backend")]
    //#[test]
    //fn test_redis_writer() {
    //    let client = redis::Client::open(REDIS_ADDR).unwrap();
    //    let map = Arc::new(DashMap::new());
    //    let mut con = client.get_connection().unwrap();
    //    let mut persistent_writer = RedisWriter::new(con, map.clone());

    //    let client = redis::Client::open(REDIS_ADDR).unwrap();
    //    let mut con = client.get_connection().unwrap();
    //    let mut persistent_reader = RedisReader::new(con, map.clone());
    //    sample_data(persistent_reader, persistent_writer);
    //}

    fn sample_data(queue_config: QueueConfig) {
        // Setup writer
        //let active_block = Arc::new(WpLock::new(ActiveBlock::new(6000)));
        let writer_config = WriterConfig {
            queue_config: queue_config.clone(),
            active_block_flush_sz: 6000
        };
        let dict = Arc::new(DashMap::new());
        let (mut write_thread, write_meta) = Writer::new(dict.clone(), writer_config);

        // Setup series
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let mut compression = Vec::new();
        for _ in 0..nvars {
            compression.push(CompressFn::XOR);
        }
        let compression = Compression::from(compression);
        let id = SeriesId(thread_rng().gen());
        let mut tags = HashMap::new();
        tags.insert(String::from("attrib"), String::from("a"));

        let series_conf = SeriesConfig {
            tags: Tags::from(tags),
            compression,
            seg_count: 1,
            nvars,
            types: vec![Types::F64; nvars],
            queue_config: queue_config.clone(),
        };
        let list = List::new(write_meta.active_block.clone());
        let series_meta = Series::new(series_conf, list);
        let serid = SeriesId(0);

        // Register series
        dict.insert(serid, series_meta.clone());
        let series_ref = write_thread.get_reference(serid);

        let mut to_values = |items: &[f64]| -> Vec<Type> {
            let mut values = Vec::with_capacity(nvars);
            for (i, v) in items.iter().enumerate() {
                values.push(Type::F64(*v));
            }
            values
        };

        let mut expected_timestamps = Vec::new();
        for item in data.iter() {
            let v = to_values(&item.values[..]);
            expected_timestamps.push(item.ts);
            loop {
                match write_thread.push(series_ref, item.ts, &v[..]) {
                    Ok(_) => break,
                    Err(_) => {}
                }
            }
        }
        expected_timestamps.reverse();

        let snapshot = dict.get(&serid).unwrap().snapshot().unwrap();
        let durable_queue = queue_config.make().unwrap();
        let durable_queue_reader = durable_queue.reader().unwrap();
        let mut reader = snapshot.reader(durable_queue_reader).unwrap();

        let mut count = 0;
        //let buf = reader.next_item().unwrap().unwrap();
        let mut timestamps: Vec<u64> = Vec::new();
        while let Ok(Some(item)) = reader.next_item() {
            item.get_timestamps().for_each(|x| timestamps.push(*x));
        }

        println!("expected: {:?}", expected_timestamps.len());
        println!("result: {:?}", timestamps.len());
        assert_eq!(expected_timestamps, timestamps);

        //println!("COUNT {}", count);

        //let mut exp_ts: Vec<u64> = Vec::new();
        //let mut exp_values: Vec<Vec<[u8; 8]>> = Vec::new();
        //for _ in 0..nvars {
        //    exp_values.push(Vec::new());
        //}

        //let ss = series_meta.segment().snapshot().unwrap();
        //for item in &data[768..782] {
        //    let v = to_values(&item.values[..]);
        //    exp_ts.push(item.ts);
        //    v.iter()
        //        .zip(exp_values.iter_mut())
        //        .for_each(|(v, e)| e.push(*v));
        //}
        //assert_eq!(ss[0].timestamps(), exp_ts.as_slice());
        //exp_values
        //    .iter()
        //    .enumerate()
        //    .for_each(|(i, v)| assert_eq!(ss[0].variable(i), (Types::F64, v.as_slice())));
        //exp_ts.clear();
        //exp_values.iter_mut().for_each(|e| e.clear());

        //let snapshot = series_meta.list().read().unwrap();
        //let mut reader = ListSnapshotReader::new(snapshot);
        //let res: &DecompressBuffer = reader
        //    .next_segment(&mut persistent_reader)
        //    .unwrap()
        //    .unwrap();
        //for item in &data[512..768] {
        //    let v = to_values(&item.values[..]);
        //    exp_ts.push(item.ts);
        //    v.iter()
        //        .zip(exp_values.iter_mut())
        //        .for_each(|(v, e)| e.push(*v));
        //}
        //assert_eq!(res.timestamps(), exp_ts.as_slice());
        //exp_values
        //    .iter()
        //    .enumerate()
        //    .for_each(|(i, v)| assert_eq!(res.variable(i), (Types::F64, v.as_slice())));
        //exp_ts.clear();
        //exp_values.iter_mut().for_each(|e| e.clear());

        //let res: &DecompressBuffer = reader
        //    .next_segment(&mut persistent_reader)
        //    .unwrap()
        //    .unwrap();
        //for item in &data[256..512] {
        //    let v = to_values(&item.values[..]);
        //    exp_ts.push(item.ts);
        //    v.iter()
        //        .zip(exp_values.iter_mut())
        //        .for_each(|(v, e)| e.push(*v));
        //}
        //assert_eq!(res.timestamps(), exp_ts.as_slice());
        //exp_values
        //    .iter()
        //    .enumerate()
        //    .for_each(|(i, v)| assert_eq!(res.variable(i), (Types::F64, v.as_slice())));
        //exp_ts.clear();
        //exp_values.iter_mut().for_each(|e| e.clear());

        //let res: &DecompressBuffer = reader
        //    .next_segment(&mut persistent_reader)
        //    .unwrap()
        //    .unwrap();
        //for item in &data[0..256] {
        //    let v = to_values(&item.values[..]);
        //    exp_ts.push(item.ts);
        //    v.iter()
        //        .zip(exp_values.iter_mut())
        //        .for_each(|(v, e)| e.push(*v));
        //}
        //assert_eq!(res.timestamps(), exp_ts.as_slice());
        //exp_values
        //    .iter()
        //    .enumerate()
        //    .for_each(|(i, v)| assert_eq!(res.variable(i), (Types::F64, v.as_slice())));
        //exp_ts.clear();
        //exp_values.iter_mut().for_each(|e| e.clear());
    }
}
