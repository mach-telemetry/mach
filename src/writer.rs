use crate::{
    compression::Compression,
    id::{SeriesId, SeriesRef, WriterId},
    persistent_list::*,
    runtime::RUNTIME,
    sample::Sample,
    segment::{self, FlushSegment, FullSegment, Segment, WriteSegment},
    series::*,
};
use dashmap::DashMap;
use std::{collections::HashMap, ops::Deref, sync::Arc};
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

pub struct Writer {
    global_meta: Arc<DashMap<SeriesId, Series>>,
    local_meta: HashMap<SeriesId, Series>,
    references: HashMap<SeriesId, usize>,
    writers: Vec<WriteSegment>,
    lists: Vec<List>,
    flush_id: Vec<usize>,
    flush_worker: FlushWorker,
    id: WriterId,
}

impl Writer {
    pub fn new<W: ChunkWriter + 'static>(
        id: WriterId,
        global_meta: Arc<DashMap<SeriesId, Series>>,
        w: W,
    ) -> Self {
        let flush_worker = FlushWorker::new(w);
        Self {
            global_meta,
            local_meta: HashMap::new(),
            references: HashMap::new(),
            flush_id: Vec::new(),
            writers: Vec::new(),
            lists: Vec::new(),
            flush_worker,
            id,
        }
    }

    pub fn id(&self) -> WriterId {
        self.id.clone()
    }

    pub fn get_reference(&mut self, id: SeriesId) -> SeriesRef {
        let meta = self.global_meta.get(&id).unwrap().clone();
        let writer = meta.segment().writer().unwrap();
        let list = meta.list().clone();

        let flush_id = self.flush_worker.register(FlushMeta {
            segment: writer.flush(),
            list: meta.list().clone(),
            id,
            compression: meta.compression().clone(),
        });

        let len = self.writers.len();
        self.references.insert(id, len);
        self.local_meta.insert(id, meta);
        self.writers.push(writer);
        self.lists.push(list);
        self.flush_id.push(flush_id);
        SeriesRef(len)
    }

    pub fn push(&mut self, reference: SeriesRef, ts: u64, data: &[[u8; 8]]) -> Result<(), Error> {
        let reference = *reference;
        match self.writers[reference].push(ts, data)? {
            segment::PushStatus::Done => {}
            segment::PushStatus::Flush(_) => self.flush_worker.flush(self.flush_id[reference]),
        }
        Ok(())
    }

    pub fn push_sample<const V: usize>(
        &mut self,
        reference: SeriesRef,
        sample: Sample<V>,
    ) -> Result<(), Error> {
        let reference = *reference;
        match self.writers[reference].push_item(sample.timestamp, &sample.values[..])? {
            segment::PushStatus::Done => {}
            segment::PushStatus::Flush(_) => self.flush_worker.flush(self.flush_id[reference]),
        }
        Ok(())
    }

    //pub fn push_univariate(
    //    &mut self,
    //    reference: SeriesRef,
    //    ts: u64,
    //    data: [u8; 8],
    //) -> Result<(), Error> {
    //    let reference = *reference;
    //    match self.writers[reference].push_univariate(ts, data)? {
    //        segment::PushStatus::Done => {}
    //        segment::PushStatus::Flush(_) => self.flush_worker.flush(self.flush_id[reference]),
    //    }
    //    Ok(())
    //}
}

struct FlushMeta {
    segment: FlushSegment,
    list: List,
    id: SeriesId,
    compression: Compression,
}

impl FlushMeta {
    fn flush<W: ChunkWriter>(&self, w: &mut W) {
        let seg: FullSegment = self.segment.to_flush().unwrap();
        self.list
            .writer()
            .push_segment(self.id, &seg, &self.compression, w);
        self.segment.flushed();
    }
}

enum FlushRequest {
    Register(FlushMeta),
    Flush(usize),
}

impl std::fmt::Debug for FlushRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("FlushRequest");
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

struct FlushWorker {
    sender: UnboundedSender<FlushRequest>,
    register_counter: usize,
}

impl FlushWorker {
    fn new<W: ChunkWriter + 'static>(w: W) -> Self {
        let (sender, receiver) = unbounded_channel();
        RUNTIME.spawn(worker(w, receiver));
        FlushWorker {
            sender,
            register_counter: 0,
        }
    }

    fn register(&mut self, meta: FlushMeta) -> usize {
        let id = self.register_counter;
        self.register_counter += 1;
        let e = "failed to send to flush worker";
        self.sender.send(FlushRequest::Register(meta)).unwrap();
        id
    }

    fn flush(&self, id: usize) {
        self.sender
            .send(FlushRequest::Flush(id))
            .expect("failed to send to flush worker");
    }
}

async fn worker<W: ChunkWriter + 'static>(mut w: W, mut queue: UnboundedReceiver<FlushRequest>) {
    let mut metadata: Vec<FlushMeta> = Vec::new();
    while let Some(item) = queue.recv().await {
        match item {
            FlushRequest::Register(meta) => metadata.push(meta),
            FlushRequest::Flush(id) => metadata[id].flush(&mut w),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::compression::*;
    use crate::constants::*;
    use crate::tags::*;
    use crate::test_utils::*;
    use rand::prelude::*;
    use std::{
        env,
        sync::{Arc, Mutex},
    };
    use tempfile::tempdir;

    #[test]
    fn test_vec_writer() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut persistent_writer = VectorWriter::new(vec.clone());
        let mut persistent_reader = VectorReader::new(vec.clone());
        sample_data(persistent_reader, persistent_writer);
    }

    //#[test]
    //fn test_file_writer() {
    //    let dir = tempdir().unwrap();
    //    let file_path = dir.path().join("test_path");
    //    let mut persistent_writer = FileWriter::new(&file_path).unwrap();
    //    let mut persistent_reader = FileReader::new(&file_path).unwrap();
    //    sample_data(persistent_reader, persistent_writer);
    //}

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

    fn sample_data<R: ChunkReader, W: ChunkWriter + 'static>(
        mut persistent_reader: R,
        mut persistent_writer: W,
    ) {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let mut compression = Vec::new();
        for _ in 0..nvars {
            compression.push(CompressFn::XOR);
        }
        let compression = Compression::from(compression);
        let buffer = ListBuffer::new(6000);
        let id = SeriesId(thread_rng().gen());
        let mut tags = HashMap::new();
        tags.insert(String::from("attrib"), String::from("a"));

        let series_conf = SeriesConfig {
            tags: Tags::from(tags),
            compression,
            seg_count: 1,
            nvars,
            types: vec![Types::F64; nvars],
        };

        let series_meta = Series::new(series_conf, buffer.clone());
        let serid = SeriesId(0);
        let dict = Arc::new(DashMap::new());
        dict.insert(serid, series_meta.clone());
        let mut write_thread = Writer::new(dict.clone(), persistent_writer);
        let series_ref = write_thread.get_reference(serid);

        let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
            let mut values = vec![[0u8; 8]; nvars];
            for (i, v) in items.iter().enumerate() {
                values[i] = v.to_be_bytes();
            }
            values
        };

        // Enough for three flushes to list
        for item in &data[..782] {
            let v = to_values(&item.values[..]);
            loop {
                match write_thread.push(series_ref, item.ts, &v[..]) {
                    Ok(_) => break,
                    Err(_) => {}
                }
            }
        }

        let mut exp_ts: Vec<u64> = Vec::new();
        let mut exp_values: Vec<Vec<[u8; 8]>> = Vec::new();
        for _ in 0..nvars {
            exp_values.push(Vec::new());
        }

        let ss = series_meta.segment().snapshot().unwrap();
        for item in &data[768..782] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            v.iter()
                .zip(exp_values.iter_mut())
                .for_each(|(v, e)| e.push(*v));
        }
        assert_eq!(ss[0].timestamps(), exp_ts.as_slice());
        exp_values
            .iter()
            .enumerate()
            .for_each(|(i, v)| assert_eq!(ss[0].variable(i), v));
        exp_ts.clear();
        exp_values.iter_mut().for_each(|e| e.clear());

        let mut reader = series_meta.list().read().unwrap();
        let res: &DecompressBuffer = reader
            .next_segment(&mut persistent_reader)
            .unwrap()
            .unwrap();
        for item in &data[512..768] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            v.iter()
                .zip(exp_values.iter_mut())
                .for_each(|(v, e)| e.push(*v));
        }
        assert_eq!(res.timestamps(), exp_ts.as_slice());
        exp_values
            .iter()
            .enumerate()
            .for_each(|(i, v)| assert_eq!(res.variable(i), v));
        exp_ts.clear();
        exp_values.iter_mut().for_each(|e| e.clear());

        let res: &DecompressBuffer = reader
            .next_segment(&mut persistent_reader)
            .unwrap()
            .unwrap();
        for item in &data[256..512] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            v.iter()
                .zip(exp_values.iter_mut())
                .for_each(|(v, e)| e.push(*v));
        }
        assert_eq!(res.timestamps(), exp_ts.as_slice());
        exp_values
            .iter()
            .enumerate()
            .for_each(|(i, v)| assert_eq!(res.variable(i), v));
        exp_ts.clear();
        exp_values.iter_mut().for_each(|e| e.clear());

        let res: &DecompressBuffer = reader
            .next_segment(&mut persistent_reader)
            .unwrap()
            .unwrap();
        for item in &data[0..256] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            v.iter()
                .zip(exp_values.iter_mut())
                .for_each(|(v, e)| e.push(*v));
        }
        assert_eq!(res.timestamps(), exp_ts.as_slice());
        exp_values
            .iter()
            .enumerate()
            .for_each(|(i, v)| assert_eq!(res.variable(i), v));
        exp_ts.clear();
        exp_values.iter_mut().for_each(|e| e.clear());
    }
}
