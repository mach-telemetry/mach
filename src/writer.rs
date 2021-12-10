use crate::{ segment, tags::Tags, };
use crossbeam::queue::SegQueue;
use std::sync::Arc;

#[derive(Debug)]
pub enum Error {
    Segment(segment::Error)
}

impl From<segment::Error> for Error {
    fn from(item: segment::Error) -> Self {
        Error::Segment(item)
    }
}

pub enum PushStatus {
    Done
}

pub struct SeriesWriter {
    tags: Tags,
    segment: segment::WriteSegment,
    flush_worker: Arc<SegQueue<segment::FlushSegment>>,
}

impl SeriesWriter {
    pub fn push(&mut self, ts: u64, val: &[[u8; 8]]) -> Result<PushStatus, Error> {
        let status = self.segment.push(ts, val)?;
        match status {
            segment::PushStatus::Done => {},
            segment::PushStatus::Flush(flusher) => {
                self.flush_worker.push(flusher);
            }
        }
        Ok(PushStatus::Done)
    }
}

pub struct FlushWorker {
}

