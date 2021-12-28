use crate::{
    compression::Compression,
    persistent_list::*,
    segment::{self, FlushSegment, FullSegment, Segment, WriteSegment},
    tags::Tags,
};
use async_std::channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

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
struct SeriesMetadata {
    segment: Segment,
    tags: Tags,
    list: List,
    compression: Compression,
}

struct WriteThread {
    local_meta: HashMap<u64, SeriesMetadata>,
    references: HashMap<u64, usize>,
    writers: Vec<WriteSegment>,
    lists: Vec<List>,
    flush_id: Vec<usize>,
    flush_worker: FlushWorker,
}

impl WriteThread {
    fn new<W: ChunkWriter>(w: W) -> Self {
        let flush_worker = FlushWorker::new(w);
        Self {
            local_meta: HashMap::new(),
            references: HashMap::new(),
            flush_id: Vec::new(),
            writers: Vec::new(),
            lists: Vec::new(),
            flush_worker,
        }
    }

    fn add_series(&mut self, id: u64, meta: SeriesMetadata) -> usize {
        let writer = meta.segment.writer().unwrap();
        let list = meta.list.clone();

        let flush_id = self.flush_worker.register(FlushMeta {
            segment: writer.flush(),
            list: meta.list.clone(),
            tags: meta.tags.clone(),
            compression: meta.compression,
        });

        let len = self.writers.len();
        self.references.insert(id, len);
        self.local_meta.insert(id, meta);
        self.writers.push(writer);
        self.lists.push(list);
        self.flush_id.push(flush_id);
        len
    }

    fn push(&mut self, reference: usize, ts: u64, data: &[[u8; 8]]) -> Result<(), Error> {
        match self.writers[reference].push(ts, data)? {
            segment::PushStatus::Done => {},
            segment::PushStatus::Flush(_) => {
                self.flush_worker.flush(self.flush_id[reference])
            }
        }
        Ok(())
    }
}

struct FlushMeta {
    segment: FlushSegment,
    list: List,
    tags: Tags,
    compression: Compression,
}

impl FlushMeta {
    fn flush<W: ChunkWriter>(&self, w: &mut W) {
        let seg: FullSegment = self.segment.to_flush().unwrap();
        self.list
            .push_segment(&seg, &self.tags, &self.compression, w);
        self.segment.flushed();
    }
}

enum FlushRequest {
    Register(FlushMeta),
    Flush(usize),
}

struct FlushWorker {
    sender: Sender<FlushRequest>,
    register_counter: usize,
}

impl FlushWorker {
    fn new<W: ChunkWriter>(w: W) -> Self {
        let (sender, receiver) = unbounded();
        //async_std::task::spawn(worker(w, receiver));
        FlushWorker {
            sender,
            register_counter: 0,
        }
    }

    fn register(&mut self, meta: FlushMeta) -> usize {
        self.register_counter += 1;
        self.sender.try_send(FlushRequest::Register(meta)).unwrap();
        self.register_counter
    }

    fn flush(&self, id: usize) {
        self.sender.try_send(FlushRequest::Flush(id)).unwrap();
    }
}

async fn worker<W: ChunkWriter>(mut w: W, queue: Receiver<FlushRequest>) {
    let mut metadata: Vec<FlushMeta> = Vec::new();
    while let Ok(item) = queue.recv().await {
        match item {
            FlushRequest::Register(meta) => metadata.push(meta),
            FlushRequest::Flush(id) => metadata[id].flush(&mut w),
        }
    }
}
