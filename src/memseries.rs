use crate::{
    segment::{self, Flushable, PushStatus, FlushSegment, WriteSegment, Segment},
    chunk::{self, WriteChunk, Chunk},
    compression::Compression,
};
use async_std::{task, channel};

struct MemSeries {
    chunk: Chunk,
    segment: Segment,
}

impl MemSeries {
    fn new(tsid: u64, compression: Compression, b: usize, v: usize) -> Self {
        MemSeries {
            chunk: Chunk::new(tsid, compression),
            segment: Segment::new(b, v),
        }
    }
}

struct MemSeriesWriter {
    segment: WriteSegment,
    chunk: channel::Sender<FlushSegment>,
}

impl MemSeriesWriter {
    fn push(&mut self, ts: u64, values: &[[u8; 8]]) -> Result<(), &str> {
        match self.segment.push(ts, values) {
            Ok(PushStatus::Done) => Ok(()),
            Ok(PushStatus::Flush) => {
                self.chunk.try_send(self.segment.flush_segment()).unwrap();
                Ok(())
            },
            Err(segment::Error::PushIntoFull) => {
                self.chunk.try_send(self.segment.flush_segment()).unwrap();
                Err("Pushing into full")
            },
            Err(_) => Err("Unkown error"),
        }
    }
}

//fn chunk_setup(chunk: Chunk) -> channel::Sender<FlushSegment> {
//    let (sender, receiver) = channel::bounded(1);
//    task::spawn(
//    sender
//}

//async fn chunk_worker(chunk: WriteChunk, receiver: channel::Receiver<FlushSegment>) {
//
//    let flushing_function = move |x: &[u64], y: &[&[[u8; 8]]]| -> Result<(), segment::Error> {
//        chunk.push(x, y)
//    }
//
//    while let Ok(segment) = receiver.recv().await {
//        segment.flush(flushing_function);
//    }
//}

