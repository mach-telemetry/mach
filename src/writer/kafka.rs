use crate::{
    backend::kafka,
    chunk, segment,
    tags::Tags,
    writer::{Error, PushStatus},
    SeriesMetadata,
    KafkaBackend,
};
use async_std::channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc, Barrier,
    },
    time::Duration,
};

struct SeriesWriter {
    tags: Tags,
    segment: segment::WriteSegment,
    chunk: chunk::KafkaChunk,
    list: kafka::KafkaList,
    flush_worker: Arc<FlushWorker>,
}

impl SeriesWriter {
    fn new(
        tags: Tags,
        segment: segment::Segment,
        chunk: chunk::KafkaChunk,
        list: kafka::KafkaList,
        flush_worker: Arc<FlushWorker>,
    ) -> Self {
        Self {
            tags,
            segment: segment.writer().unwrap(),
            chunk,
            list,
            flush_worker,
        }
    }

    fn push(&mut self, ts: u64, val: &[[u8; 8]]) -> Result<PushStatus, Error> {
        let status = self.segment.push(ts, val)?;
        match status {
            segment::PushStatus::Done => {}
            segment::PushStatus::Flush(flusher) => {
                // queue is unbounded but segment is bounded. should always succeed unless flush
                // worker panics.
                //println!("SENDING FLUSH");
                let msg = FlushMsg::ToFlush(ToFlush {
                    segment: flusher,
                    chunk: self.chunk.clone(),
                    list: self.list.clone(),
                    full_flush: false,
                });
                self.flush_worker.try_send(msg).unwrap();
            }
        }
        Ok(PushStatus::Done)
    }

    fn flush(&self) {
        let flusher = self.segment.flush();
        let msg = FlushMsg::ToFlush(ToFlush {
            segment: flusher,
            chunk: self.chunk.clone(),
            list: self.list.clone(),
            full_flush: true,
        });
        self.flush_worker.try_send(msg).unwrap();
    }
}


enum FlushMsg {
    ToFlush(ToFlush),
    ToWait(Arc<Barrier>),
}

struct ToFlush {
    segment: segment::FlushSegment,
    chunk: chunk::KafkaChunk,
    list: kafka::KafkaList,
    full_flush: bool,
}

impl ToFlush {
    fn flush(self, kafka: &mut kafka::KafkaWriter) {
        if let Some(x) = self.segment.to_flush() {
            let full_segment = self.segment.to_flush().unwrap();
            let mut write_chunk = self.chunk.writer().unwrap();
            // Try to push segment to chunk
            let res = write_chunk.push(&full_segment);
            self.segment.flushed();
            match res {
                Some(buffer) => {
                    self.list.writer().unwrap().push(kafka, buffer).unwrap();
                    write_chunk.reset();
                }
                None => {
                    if self.full_flush {
                        if let Some(buff) = write_chunk.flush_buffer() {
                            self.list.writer().unwrap().push(kafka, buff).unwrap();
                            write_chunk.reset();
                        }
                    }
                }
            }
        } else if self.full_flush {
            let mut write_chunk = self.chunk.writer().unwrap();
            if let Some(buff) = write_chunk.flush_buffer() {
                self.list.writer().unwrap().push(kafka, buff).unwrap();
                write_chunk.reset();
            }
        }
    }
}

struct FlushWorker {
    sender: Sender<FlushMsg>,
}

impl std::ops::Deref for FlushWorker {
    type Target = Sender<FlushMsg>;
    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl FlushWorker {
    fn new() -> Self {
        let (sender, receiver) = unbounded();
        async_std::task::spawn(kafka_flush_worker(receiver));
        FlushWorker { sender }
    }
}

async fn kafka_flush_worker(queue: Receiver<FlushMsg>) {
    let mut kafka = kafka::KafkaWriter::new().unwrap();
    //let mut file = fs::FileWriter::new(file_allocator).unwrap();
    while let Ok(item) = queue.recv().await {
        match item {
            FlushMsg::ToFlush(item) => {
                item.flush(&mut kafka);
            }
            FlushMsg::ToWait(barrier) => {
                barrier.wait();
            }
        };
    }
}

struct Metadata {
    writer: SeriesWriter,
    meta: Arc<SeriesMetadata>,
}

pub struct KafkaWriter {
    reference: Arc<DashMap<Tags, Arc<SeriesMetadata>>>, // communicate with Global
    id_map: HashMap<Tags, u64>,
    thread_id: u64,
    flush_worker: Arc<FlushWorker>,
    writers: Vec<Metadata>,
}

impl KafkaWriter {
    fn new(
        thread_id: u64,
        reference: Arc<DashMap<Tags, Arc<SeriesMetadata>>>,
    ) -> Self {
        let flush_worker = Arc::new(FlushWorker::new());
        KafkaWriter {
            reference,
            flush_worker,
            id_map: HashMap::new(),
            thread_id,
            writers: Vec::new(),
        }
    }

    fn init_series(&mut self, tags: &Tags) -> Result<u64, Error> {
        let entry = self.id_map.entry(tags.clone());
        match entry {
            Entry::Occupied(_) => Err(Error::SeriesReinitialized),
            Entry::Vacant(x) => match self.reference.get(&tags) {
                Some(item) => {
                    if item.thread_id.load(SeqCst) == self.thread_id {
                        let id = self.writers.len();
                        let segment = item.segment.clone();

                        // TODO: This is messy...
                        let KafkaBackend { chunk, list } = item.backend.kafka_backend();
                        let flush_worker = self.flush_worker.clone();
                        let writer =
                            SeriesWriter::new(tags.clone(), segment, chunk.clone(), list.clone(), flush_worker);
                        let meta = Metadata {
                            writer,
                            meta: item.clone(),
                        };
                        self.writers.push(meta);
                        Ok(id as u64)
                    } else {
                        Err(Error::SeriesNotFound)
                    }
                }
                None => Err(Error::SeriesNotFound),
            },
        }
    }

    fn get_reference_id(&mut self, tags: &Tags) -> Option<u64> {
        Some(*(self.id_map.get(&tags)?))
    }

    fn push(&mut self, id: u64, ts: u64, values: &[[u8; 8]]) -> Result<(), Error> {
        self.writers[id as usize].writer.push(ts, values)?;
        Ok(())
    }

    fn flush(&mut self, id: u64) {
        self.writers[id as usize].writer.flush()
    }

    fn close(self) {
        for i in 0..self.writers.len() {
            self.writers[i].writer.flush();
        }
        let barrier = Arc::new(Barrier::new(2));
        let msg = FlushMsg::ToWait(barrier.clone());
        self.flush_worker.try_send(msg).unwrap();
        barrier.wait();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{chunk::*, compression::*, test_utils::*};

    #[test]
    fn test_pipeline() {
        // Setup data
        let mut tags = Tags::new();
        tags.insert(("A".to_string(), "B".to_string()));
        tags.insert(("C".to_string(), "D".to_string()));
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();

        let meta = Arc::new(SeriesMetadata::with_kafka_backend(
            5,
            nvars,
            3,
            &tags,
            Compression::LZ4(1),
        ));

        // Setup writer
        let reference = Arc::new(DashMap::new());
        reference.insert(tags.clone(), meta.clone());
        let mut writer = KafkaWriter::new(5, reference.clone());
        let ref_id = writer.init_series(&tags).unwrap();

        // Push data into the writer
        let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
            let mut values = vec![[0u8; 8]; nvars];
            for (i, v) in items.iter().enumerate() {
                values[i] = v.to_be_bytes();
            }
            values
        };

        let mut exp_ts = Vec::new();
        let mut exp_values = Vec::new();
        for _ in 0..nvars {
            exp_values.push(Vec::new());
        }

        // 3 for the segments
        // 16 for the first chunk flushed
        // 2 for another set of segments not flushed
        //let tot = data.len() - data.len() % 256;
        for item in &data[..] {
            let v = to_values(&item.values[..]);
            exp_ts.push(item.ts);
            for i in 0..nvars {
                exp_values[i].push(v[i]);
            }
            loop {
                if writer.push(ref_id, item.ts, &v[..]).is_ok() {
                    break;
                }
            }
        }
        writer.close();

        let consumer = kafka::default_consumer().unwrap();
        let timeout = kafka::Timeout::After(Duration::from_secs(1));
        let mut file_list_iterator = meta.backend.kafka_backend().list.reader(consumer, timeout).unwrap();
        let mut count = 0;
        let rev_exp_ts = exp_ts.iter().rev().copied().collect::<Vec<u64>>();
        let mut timestamps = Vec::new();
        while let Some(byte_entry) = file_list_iterator.next_item().unwrap() {
            count += 1;
            let chunk = SerializedChunk::new(byte_entry.bytes).unwrap();
            let counter = chunk.n_segments();
            for i in 0..counter {
                let mut decompressed = DecompressBuffer::new();
                let bytes = chunk.get_segment_bytes(i);
                let bytes_read = Compression::decompress(bytes, &mut decompressed).unwrap();
                for i in 0..decompressed.len() {
                    timestamps.push(decompressed.timestamp_at(i));
                }
            }
        }
        assert_eq!(timestamps, rev_exp_ts);
    }
}

