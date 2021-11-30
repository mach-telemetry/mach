use crate::{
    segment::{self, FullSegment, PushStatus, FlushSegment, WriteSegment, Segment},
    chunk::{self, WriteChunk, FlushChunk},
    compression::Compression,
};
use async_std::{task, channel};

struct SequentialWriter {
    write_segment: WriteSegment,
    flush_segment: FlushSegment,
    write_chunk: WriteChunk,
    flush_chunk: FlushChunk,
}

impl SequentialWriter {
    fn push(&mut self, ts: u64, values: &[[u8; 8]]) -> Result<(), &str> {
        match self.write_segment.push(ts, values) {
            Ok(PushStatus::Done) => Ok(()),
            Ok(PushStatus::Flush) => {
                let full_segment = self.flush_segment.to_flush();
                //self.write_chunk.push(
                Ok(())
            }
            Err(_) => {
                Ok(())
            }
        }

    }



    //    match self.segment.push(ts, values) {
    //        Ok(PushStatus::Done) => Ok(()),
    //        Ok(PushStatus::Flush) => {
    //            self.chunk.try_send(self.segment.flush_segment()).unwrap();
    //            Ok(())
    //        },
    //        Err(segment::Error::PushIntoFull) => {
    //            self.chunk.try_send(self.segment.flush_segment()).unwrap();
    //            Err("Pushing into full")
    //        },
    //        Err(_) => Err("Unkown error"),
    //    }
    //}
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

