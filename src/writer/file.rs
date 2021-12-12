use crate::{SeriesMetadata, backend::fs, chunk, segment, tags::Tags, writer::{PushStatus, Error}};
use async_std::channel::{unbounded, Receiver, Sender};
//use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc,
    }
};

pub struct ToFlush {
    segment: segment::FlushSegment,
    chunk: chunk::FileChunk,
    list: fs::FileList,
}

impl ToFlush {
    fn flush(self, file: &mut fs::FileWriter) {
        let full_segment = self.segment.to_flush().unwrap();
        let mut write_chunk = self.chunk.writer().unwrap();
        // Try to push segment to chunk
        match write_chunk.push(&full_segment) {
            Some(buffer) => {
                self.list.writer().unwrap().push(file, buffer).unwrap();
                write_chunk.reset();
            }
            None => {}
        }
        self.segment.flushed();
    }
}

pub struct SeriesWriter {
    tags: Tags,
    segment: segment::WriteSegment,
    chunk: chunk::FileChunk,
    list: fs::FileList,
    flush_worker: Arc<FlushWorker>,
}

impl SeriesWriter {
    pub fn new(
        tags: Tags,
        segment: segment::Segment,
        chunk: chunk::FileChunk,
        list: fs::FileList,
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
    pub fn push(&mut self, ts: u64, val: &[[u8; 8]]) -> Result<PushStatus, Error> {
        let status = self.segment.push(ts, val)?;
        match status {
            segment::PushStatus::Done => {}
            segment::PushStatus::Flush(flusher) => {
                // queue is unbounded but segment is bounded. should always succeed unless flush
                // worker panics.
                self.flush_worker
                    .try_send(ToFlush {
                        segment: flusher,
                        chunk: self.chunk.clone(),
                        list: self.list.clone(),
                    })
                    .unwrap();
            }
        }
        Ok(PushStatus::Done)
    }
}

pub struct FlushWorker {
    sender: Sender<ToFlush>,
}

impl std::ops::Deref for FlushWorker {
    type Target = Sender<ToFlush>;
    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl FlushWorker {
    pub fn new(file_allocator: Arc<AtomicU64>) -> Self {
        let (sender, receiver) = unbounded();
        async_std::task::spawn(async { file_flush_worker(file_allocator, receiver) });
        FlushWorker { sender }
    }
}

async fn file_flush_worker(file_allocator: Arc<AtomicU64>, queue: Receiver<ToFlush>) {
    let mut file = fs::FileWriter::new(file_allocator).unwrap();
    while let Ok(item) = queue.recv().await {
        item.flush(&mut file);
    }
}

struct Metadata {
    writer: SeriesWriter,
    meta: Arc<SeriesMetadata>,
}

pub struct FileWriter {
    reference: Arc<DashMap<Tags, Arc<SeriesMetadata>>>, // communicate with Global
    id_map: HashMap<Tags, u64>,
    thread_id: u64,
    flush_worker: Arc<FlushWorker>,
    writers: Vec<Metadata>,
}

impl FileWriter {
    fn new(
        thread_id: Option<u64>,
        reference: Arc<DashMap<Tags, Arc<SeriesMetadata>>>,
        shared_file: Arc<AtomicU64>,
    ) -> Self {
        let flush_worker = Arc::new(FlushWorker::new(shared_file));
        let thread_id = match thread_id {
            Some(x) => x,
            None => std::thread::current().id().as_u64().get(),
        };
        FileWriter {
            reference,
            flush_worker,
            id_map: HashMap::new(),
            thread_id,
            writers: Vec::new(),
        }
    }

    fn init_series(&mut self, tags: Tags) -> Result<u64, Error> {
        let entry = self.id_map.entry(tags.clone());
        match entry {
            Entry::Occupied(_) => Err(Error::SeriesReinitialized),
            Entry::Vacant(x) => match self.reference.get(&tags) {
                Some(item) => {
                    if item.thread_id.load(SeqCst) == self.thread_id {
                        let id = self.writers.len();
                        let segment = item.segment.clone();
                        let chunk = item.chunk.clone();
                        let list = item.list.clone();
                        let flush_worker = self.flush_worker.clone();
                        let writer = SeriesWriter::new(
                            tags.clone(),
                            segment,
                            chunk,
                            list,
                            flush_worker,
                        );
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

    fn push(&mut self, id: u64, ts: u64, values: &[[u8; 8]]) {
        self.writers[id as usize].writer.push(ts, values).unwrap();
    }
}

