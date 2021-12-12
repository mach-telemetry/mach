use crate::{backend::fs, chunk, segment, tags::Tags};
use async_std::channel::{unbounded, Receiver, Sender};
//use crossbeam::queue::SegQueue;
use std::sync::{
    atomic::{AtomicU64, Ordering::SeqCst},
    Arc,
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

pub enum PushStatus {
    Done,
}

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
