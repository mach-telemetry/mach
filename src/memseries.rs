#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        backend::fs::{FileList, FileListWriter, FileWriter, SHARED_ID},
        chunk::{self, FileChunk, SerializedChunk, WriteFileChunk},
        compression::Compression,
        segment::{self, FlushSegment, FullSegment, Segment, WriteSegment},
        tags::Tags,
        test_utils::*,
    };
    use std::sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc,
    };

    #[test]
    fn test_pipeline() {
        let shared_id = SHARED_ID.clone();
        let mut file = FileWriter::new(shared_id.clone()).unwrap();
        let mut tags = Tags::new();
        tags.insert(("A".to_string(), "B".to_string()));
        tags.insert(("C".to_string(), "D".to_string()));
        let mut file_list = FileList::new();
        let mut writer = file_list.writer().unwrap();

        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();
        let segment = Segment::new(3, nvars);
        let chunk = FileChunk::new(&tags, Compression::LZ4(1));

        let mut writer = Writer {
            write_segment: segment.writer().unwrap(),
            write_chunk: chunk.writer().unwrap(),
            persistent: writer,
            file,
        };

        let mut to_values = |items: &[f64]| -> Vec<[u8; 8]> {
            let mut values = vec![[0u8; 8]; nvars];
            for (i, v) in items.iter().enumerate() {
                values[i] = v.to_be_bytes();
            }
            values
        };

        // 3 for the segments
        // 16 for the first chunk flushed
        // 2 for another set of segments not flushed
        for item in &data[..256 * (3 + 16 + 2)] {
            let v = to_values(&item.values[..]);
            assert_eq!(writer.push(item.ts, &v[..]), Ok(PushStatus::Done));
        }

        let mut file_list_iterator = file_list.reader().unwrap();
        let chunk = file_list_iterator.next_item().unwrap().unwrap();

        //assert_eq!(chunk.tags, tags);

        //assert_eq!(writer.data.len(), 1);
    }

    #[derive(PartialEq, Eq, Debug)]
    enum PushStatus {
        Done,
    }

    struct Writer {
        write_segment: WriteSegment,
        write_chunk: WriteFileChunk,
        persistent: FileListWriter,
        file: FileWriter,
    }

    impl Writer {
        fn push(&mut self, ts: u64, values: &[[u8; 8]]) -> Result<PushStatus, &str> {
            // Try to push into the segment
            match self.write_segment.push(ts, values) {
                Ok(segment::PushStatus::Done) => Ok(PushStatus::Done),

                // Push succeeded but we can move segment to chunk
                Ok(segment::PushStatus::Flush) => {
                    let flusher = self.write_segment.flush();
                    let full_segment = flusher.to_flush().unwrap();

                    // Try to push segment to chunk
                    match self.write_chunk.push(&full_segment) {
                        Some(buffer) => {
                            self.persistent.push(&mut self.file, buffer).unwrap();
                            self.write_chunk.reset();
                        },
                        None => {},
                    }
                    flusher.flushed();
                    Ok(PushStatus::Done)
                }
                Err(_) => {
                    println!("HERE1");
                    unimplemented!();
                }
            }
        }
    }
}
