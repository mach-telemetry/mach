use crate::{
    active_block::*,
    compression::Compression,
    //wal::Wal,
    //durable_queue::{DurableQueue, DurableQueueWriter, QueueConfig},
    id::{SeriesId, SeriesRef, WriterId},
    //persistent_list::*,
    mem_list::{List, BlockList},
    //runtime::*,
    sample::Type,
    segment::{self, FlushSegment, WriteSegment},
    series::*,
    utils::wp_lock::WpLock,
};
use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use std::sync::mpsc::{channel, Sender, Receiver};

#[derive(Debug)]
pub enum Error {
    Segment(segment::Error),
}

impl From<segment::Error> for Error {
    fn from(item: segment::Error) -> Self {
        Error::Segment(item)
    }
}

#[derive(Clone)]
pub struct WriterConfig {
    //pub queue_config: QueueConfig,
    pub active_block_flush_sz: usize,
}

pub struct WriterMetadata {
    pub id: WriterId,
    //pub queue_config: QueueConfig,
    pub block_list: Arc<BlockList>,
    //pub active_block: Arc<WpLock<ActiveBlock>>,
    //pub durable_queue: Arc<DurableQueue>,
}

pub struct Writer {
    global_meta: Arc<DashMap<SeriesId, Series>>,
    local_meta: HashMap<SeriesId, Series>,
    references: HashMap<SeriesId, usize>,
    series: Vec<Series>,
    writers: Vec<WriteSegment>,
    block_list: Arc<BlockList>,
    id: WriterId,
    block_worker: Sender<FlushItem>,
}

impl Writer {
    pub fn new(
        global_meta: Arc<DashMap<SeriesId, Series>>,
        writer_config: WriterConfig,
    ) -> (Self, WriterMetadata) {
        let block_flush_sz = writer_config.active_block_flush_sz;
        //let queue_config = writer_config.queue_config;
        let id = WriterId::new();
        let block_list = Arc::new(BlockList::new());
        let block_list_clone = block_list.clone();
        let (block_worker, rx) = channel();
        std::thread::spawn(move || {
            block_list_worker(block_list_clone, rx);
        });
        //let active_block = Arc::new(WpLock::new(ActiveBlock::new(block_flush_sz)));
        //let durable_queue = Arc::new(queue_config.clone().make().unwrap());
        //let list_maker = ListMaker::new(durable_queue.writer().unwrap());

        let meta = WriterMetadata {
            id: id.clone(),
            block_list: block_list.clone(),
            //queue_config,
            //active_block,
            //durable_queue,
        };

        let writer = Self {
            global_meta,
            local_meta: HashMap::new(),
            references: HashMap::new(),
            writers: Vec::new(),
            series: Vec::new(),
            block_list,
            block_worker,
            //list_maker,
            id,
            //list_maker_id: Vec::new(),
            //active_block,
            //durable_queue,
        };

        (writer, meta)
    }

    pub fn id(&self) -> WriterId {
        self.id.clone()
    }

    pub fn get_reference(&mut self, id: SeriesId) -> SeriesRef {
        let series_ref = self.writers.len();
        let series = self.global_meta.get(&id).unwrap().clone();
        let writer = series.segment.writer().unwrap();
        self.writers.push(writer);
        self.series.push(series.clone());
        SeriesRef(series_ref)


        //let meta = self.global_meta.get(&id).unwrap().clone();
        //self.references.insert(id, len);
        //self.local_meta.insert(id, meta);
        //self.lists.push(list);
        //self.list_maker_id.push(list_maker_id);
    }

    pub fn push(&mut self, reference: SeriesRef, ts: u64, data: &[Type]) -> Result<(), Error> {
        let reference = *reference;
        match self.writers[reference].push_type(ts, data)? {
            segment::PushStatus::Done => {},
            segment::PushStatus::Flush(_) => {
                let series = &self.series[reference];
                let id = series.config.id;
                let compression = series.config.compression.clone();
                let segment = self.writers[reference].flush();
                self.block_worker.send( FlushItem {
                    id,
                    segment,
                    compression,
                }).unwrap();
            }
            //segment::PushStatus::Flush(_) => self.list_maker.flush(self.list_maker_id[reference]),
        }
        Ok(())
    }
}

struct FlushItem {
    id: SeriesId,
    segment: FlushSegment,
    compression: Compression,
}

fn block_list_worker(block_list: Arc<BlockList>, chan: Receiver<FlushItem>) {
    while let Ok(x) = chan.recv() {
        //println!("GOT SOMETHING");
        block_list.push(x.id, &x.segment, &x.compression);
        // Safety: self.list.push call above that contains a clone doesn't mark the segment as
        // flushed
        unsafe {
            x.segment.flushed();
        }
    }
}

//struct ListMakerMeta {
//    segment: FlushSegment,
//    list: ListWriter,
//    id: SeriesId,
//    compression: Compression,
//}
//
//impl ListMakerMeta {
//    fn write(&mut self, w: &mut DurableQueueWriter) {
//        //println!("WRITING TO ACTIVE BLOCK");
//        if self
//            .list
//            .push(self.id, self.segment.clone(), &self.compression, w)
//            .is_err()
//        {
//            println!("Error writing to list");
//        }
//
//        // Safety: self.list.push call above that contains a clone doesn't mark the segment as
//        // flushed
//        unsafe {
//            self.segment.flushed();
//        }
//    }
//}
//
//enum ListMakerRequest {
//    Register(ListMakerMeta),
//    Flush(usize),
//}
//
//impl std::fmt::Debug for ListMakerRequest {
//    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//        let mut ds = f.debug_struct("ListMakerRequest");
//        match self {
//            Self::Register(_) => {
//                ds.field("Register", &"");
//            }
//            Self::Flush(id) => {
//                ds.field("Flush", &id);
//            }
//        }
//        ds.finish()
//    }
//}
//
//struct ListMaker {
//    sender: crossbeam_channel::Sender<ListMakerRequest>,
//    register_counter: usize,
//}
//
//impl ListMaker {
//    fn new(durable_queue_writer: DurableQueueWriter) -> Self {
//        let (sender, receiver) = crossbeam_channel::unbounded();
//        std::thread::spawn(move || {
//            worker(durable_queue_writer, receiver)
//        });
//        ListMaker {
//            sender,
//            register_counter: 0,
//        }
//    }
//
//    fn register(&mut self, meta: ListMakerMeta) -> usize {
//        let id = self.register_counter;
//        self.register_counter += 1;
//        //let e = "failed to send to flush worker";
//        self.sender.send(ListMakerRequest::Register(meta)).unwrap();
//        id
//    }
//
//    fn flush(&self, id: usize) {
//        self.sender
//            .send(ListMakerRequest::Flush(id))
//            .expect("failed to send to flush worker");
//        //QUEUE_LEN.fetch_add(1, SeqCst);
//    }
//}
//
//use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
//lazy_static::lazy_static! {
//    static ref QUEUE_LEN: AtomicUsize = AtomicUsize::new(0);
//}
//
//fn worker(mut w: DurableQueueWriter, mut queue: crossbeam_channel::Receiver<ListMakerRequest>) {
//    let mut metadata: Vec<ListMakerMeta> = Vec::new();
//    while let Ok(item) = queue.recv() {
//        //QUEUE_LEN.fetch_sub(1, SeqCst);
//        //println!("Queue len {}", QUEUE_LEN.load(SeqCst));
//        match item {
//            ListMakerRequest::Register(meta) => metadata.push(meta),
//            ListMakerRequest::Flush(id) => metadata[id].write(&mut w),
//        }
//    }
//}

#[cfg(test)]
mod test {
    use super::*;
    use crate::compression::*;
    use crate::constants::*;
    use crate::id::*;
    use crate::tags::*;
    use crate::test_utils::*;
    use crate::utils::*;
    use rand::*;
    use std::sync::Arc;
    use tempfile::tempdir;

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
        let bootstrap = String::from("localhost:9093,localhost:9094,localhost:9095");
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
            active_block_flush_sz: 6000,
        };
        let dict = Arc::new(DashMap::new());
        let (mut write_thread, write_meta) = Writer::new(dict.clone(), writer_config);

        // Setup series
        let mut data = (*MULTIVARIATE_DATA[0].1).clone();
        let mut rng = thread_rng();
        for item in data.iter_mut() {
            for val in item.values.iter_mut() {
                *val = rng.gen::<f64>() * 100.0f64;
            }
        }
        let nvars = data[0].values.len();
        let mut compression = Vec::new();
        for _ in 0..nvars {
            compression.push(CompressFn::Decimal(3));
        }
        let compression = Compression::from(compression);
        //let _id = SeriesId(thread_rng().gen());
        let mut tags = HashMap::new();
        tags.insert(String::from("attrib"), String::from("a"));

        let series_conf = SeriesConfig {
            id: Tags::from(tags).id(),
            compression,
            seg_count: 1,
            nvars,
            types: vec![Types::F64; nvars],
        };
        let list = List::new(write_meta.active_block.clone());
        let series_meta = Series::new(series_conf, queue_config.clone(), list);
        let serid = SeriesId(0);

        // Register series
        dict.insert(serid, series_meta.clone());
        let series_ref = write_thread.get_reference(serid);

        let to_values = |items: &[f64]| -> Vec<Type> {
            let mut values = Vec::with_capacity(nvars);
            for v in items.iter() {
                values.push(Type::F64(*v));
            }
            values
        };

        let mut expected_timestamps = Vec::new();
        let mut expected_field0 = Vec::new();
        for item in data.iter() {
            let v = to_values(&item.values[..]);
            expected_timestamps.push(item.ts);
            expected_field0.push(item.values[0]);
            loop {
                match write_thread.push(series_ref, item.ts, &v[..]) {
                    Ok(_) => break,
                    Err(_) => {}
                }
            }
        }
        expected_timestamps.reverse();
        expected_field0.reverse();

        let snapshot = dict.get(&serid).unwrap().snapshot().unwrap();
        let durable_queue = queue_config.make().unwrap();
        let durable_queue_reader = durable_queue.reader().unwrap();
        let mut reader = snapshot.reader(durable_queue_reader);

        //let count = 0;
        //let buf = reader.next_item().unwrap().unwrap();
        let mut timestamps: Vec<u64> = Vec::new();
        let mut field0: Vec<f64> = Vec::new();
        //let r = reader.next_item();
        loop {
            match reader.next_item() {
                Ok(Some(item)) => {
                    item.get_timestamps().for_each(|x| timestamps.push(*x));
                    item.get_field(0)
                        .1
                        .for_each(|x| field0.push(f64::from_be_bytes(*x)));
                }
                Ok(None) => println!("OK NONE PROBLEM"),
                Err(x) => {
                    println!("{:?}", x);
                    break;
                }
            }
        }
        assert_eq!(expected_timestamps, timestamps);
        for (e, r) in expected_field0.iter().zip(field0.iter()) {
            assert!((*e - *r).abs() < 0.001);
        }
    }
}
