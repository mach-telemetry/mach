use crate::{
    segment::{self, FullSegment, FlushSegment, WriteSegment, Segment},
    chunk::{self, SerializedChunk, WriteChunk, FlushChunk},
    compression::Compression,
};
use async_std::{task, channel};

struct SequentialWriter {
    write_segment: WriteSegment,
    flush_segment: FlushSegment,
    write_chunk: WriteChunk,
    flush_chunk: FlushChunk,
    data: Vec<SerializedChunk>,
}

impl SequentialWriter {
    fn push(&mut self, ts: u64, values: &[[u8; 8]]) -> Result<(), &str> {

        // Try to push into the segment
        match self.write_segment.push(ts, values) {
            Ok(segment::PushStatus::Done) => Ok(()),

            // Push succeeded but we can move segment to chunk
            Ok(segment::PushStatus::Flush) => {

                let full_segment = self.flush_segment.to_flush().unwrap();

                // Try to push segment to chunk
                match self.write_chunk.push(&full_segment) {
                    Ok(status) => {

                        // Data was written to the chunk so segment can be marked as flushed
                        self.flush_segment.flushed();

                        match status {
                            chunk::PushStatus::Done => {},
                            chunk::PushStatus::Flush => {
                                let chunk = self.flush_chunk.serialize().unwrap();
                                self.data.push(chunk);
                                self.flush_chunk.clear();
                            },
                        };
                        Ok(())
                    }
                    Err(_) => unimplemented!(),
                }
            }
            Err(_) => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;
}
