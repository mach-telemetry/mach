
#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        segment::{self, FullSegment, FlushSegment, WriteSegment, Segment},
        chunk::{self, SerializedChunk, WriteChunk, FlushChunk, Chunk},
        compression::Compression,
        test_utils::*,
    };

    fn test_pipeline() {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let segment = Segment::new(3, nvars);
        let chunk = Chunk::new(1, Compression::LZ4(1));

        let mut writer = Writer {
            write_segment: segment.writer().unwrap(),
            flush_segment: segment.flusher().unwrap(),
            write_chunk: chunk.writer().unwrap(),
            flush_chunk: chunk.flusher().unwrap(),
            data: Vec::new(),
        };

        let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
            let mut values = vec![[0u8; 8]; nvars];
            for (i, v) in items.iter().enumerate() {
                values[i] = v.to_be_bytes();
            }
            values
        };

        for item in &data[..255] {
            let v = to_values(&item.values[..]);
            //assert_eq!(writer.push(item.ts, &v[..]), Ok(segment::PushStatus::Done));
        }
    }

    struct Writer {
        write_segment: WriteSegment,
        flush_segment: FlushSegment,
        write_chunk: WriteChunk,
        flush_chunk: FlushChunk,
        data: Vec<SerializedChunk>,
    }

    impl Writer {
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
                        Err(x) => {
                            println!("{:?}", x);
                            unimplemented!();
                        },
                    }
                }
                Err(_) => unimplemented!(),
            }
        }
    }
}
