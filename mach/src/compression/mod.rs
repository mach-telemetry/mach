// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

pub mod compression_scheme;
pub mod delta_of_delta;
pub mod heap;
pub mod lz4;
pub mod timestamps;
pub use compression_scheme::CompressionScheme;

use crate::{
    byte_buffer::ByteBuffer,
    constants::SEG_SZ,
    field_type::FieldType,
    segment::{Segment, SegmentArray, SegmentRef},
};
use serde::*;

const MAGIC: &[u8] = "COMPRESSION".as_bytes();

pub trait CompressDecompress: Clone {
    fn compress(&self, data_len: usize, data: &SegmentArray, buffer: &mut ByteBuffer);
    fn decompress(&self, data: &[u8], data_len: &mut usize, buffer: &mut SegmentArray);
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Compression {
    schemes: Vec<CompressionScheme>,
}

impl Compression {
    pub fn new(schemes: Vec<CompressionScheme>) -> Self {
        Self { schemes }
    }

    pub fn compress_segment(&self, segment: &SegmentRef, buffer: &mut ByteBuffer) {
        self.compress(
            segment.len,
            segment.heap_len,
            segment.timestamps,
            segment.heap,
            segment.columns(),
            segment.types,
            buffer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn compress(
        &self,
        len: usize,
        heap_len: usize,
        timestamps: &[u64; SEG_SZ],
        heap: &[u8],
        data: &[SegmentArray],
        types: &[FieldType],
        buffer: &mut ByteBuffer,
    ) {
        let nvars = types.len();
        assert_eq!(self.schemes.len(), nvars);

        // Write stats
        buffer.extend_from_slice(MAGIC);
        buffer.extend_from_slice(&len.to_be_bytes());
        buffer.extend_from_slice(&heap_len.to_be_bytes());
        buffer.extend_from_slice(&nvars.to_be_bytes());
        for t in types {
            buffer.push(*t as u8);
        }

        {
            // Write schemes
            let schemes_offset = buffer.len();
            buffer.extend_from_slice(&0usize.to_be_bytes());
            let schemes_start = buffer.len();
            bincode::serialize_into(&mut *buffer, &self.schemes).unwrap();
            let schemes_end = buffer.len();
            let sz = (schemes_end - schemes_start).to_be_bytes();
            buffer.as_mut_slice()[schemes_offset..schemes_start].copy_from_slice(&sz);
        }

        {
            // Write timestamps
            let offset = buffer.len();
            buffer.extend_from_slice(&0usize.to_be_bytes());
            let start = buffer.len();
            timestamps::compress(len, timestamps, buffer);
            let sz = (buffer.len() - start).to_be_bytes();
            buffer.as_mut_slice()[offset..start].copy_from_slice(&sz);
        }

        // Compress each column
        for (scheme, column) in self.schemes.iter().zip(data.iter()) {
            //let column: &SegmentArray = (*column).try_into().unwrap();
            let offset = buffer.len();
            buffer.extend_from_slice(&0usize.to_be_bytes());
            let start = buffer.len();
            scheme.compress(len, column, buffer);
            let sz = (buffer.len() - start).to_be_bytes();
            buffer.as_mut_slice()[offset..start].copy_from_slice(&sz);
        }

        // Compress the heap
        if heap_len > 0 {
            let offset = buffer.len();
            buffer.extend_from_slice(&0usize.to_be_bytes());
            let start = buffer.len();
            heap::compress(&heap[..heap_len], buffer);
            let sz = (buffer.len() - start).to_be_bytes();
            buffer.as_mut_slice()[offset..start].copy_from_slice(&sz);
        }
    }

    pub fn decompress_segment(bytes: &[u8], segment: &mut Segment) {
        assert_eq!(&bytes[..MAGIC.len()], MAGIC);
        let mut offset = MAGIC.len();

        let len = {
            let end = offset + 8;
            let len = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;
            segment.len = len;
            len
        };

        let heap_len = {
            let end = offset + 8;
            let heap_len = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;
            segment.heap_len = heap_len;
            heap_len
        };

        let nvars = {
            let end = offset + 8;
            let nvars = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;
            nvars
        };

        segment.clear_fields();
        for _ in 0..nvars {
            segment.add_field(FieldType::from(bytes[offset]));
            offset += 1;
        }

        let schemes: Vec<CompressionScheme> = {
            let end = offset + 8;
            let schema_sz = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;

            let end = offset + schema_sz;
            let schemes = bincode::deserialize(&bytes[offset..end]).unwrap();
            offset = end;
            schemes
        };
        assert_eq!(schemes.len(), nvars);

        {
            let end = offset + 8;
            let sz = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;
            let timestamps = (&mut segment.timestamps[..]).try_into().unwrap();

            let end = offset + sz;
            let mut ts_len = 0;
            timestamps::decompress(&bytes[offset..end], &mut ts_len, timestamps);
            offset = end;
        }

        let data: &mut [SegmentArray] = segment.columns_mut();
        for (i, scheme) in schemes.iter().enumerate() {
            let end = offset + 8;
            let sz = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;

            let end = offset + sz;
            let mut col_len = 0;
            scheme.decompress(&bytes[offset..end], &mut col_len, &mut data[i]);
            offset = end;
            assert_eq!(len, col_len);
        }

        if heap_len > 0 {
            let end = offset + 8;
            let sz = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;

            let end = offset + sz;
            let mut l = 0;
            heap::decompress(&bytes[offset..end], &mut l, &mut segment.heap[..]);
            assert_eq!(l, heap_len);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::active_segment::ActiveSegment;
    use crate::field_type::FieldType;
    use crate::test_utils::*;

    #[test]
    fn test() {
        let types = &[FieldType::Bytes, FieldType::F64];
        let samples = random_samples(types, SEG_SZ, 16..1024);
        let (_active_segment, mut writer) = ActiveSegment::new(types);

        assert_eq!(fill_active_segment(&samples, &mut writer), SEG_SZ);

        let segment_reference = writer.as_segment_ref();

        let compression = Compression {
            schemes: vec![
                CompressionScheme::delta_of_delta(),
                CompressionScheme::lz4(),
            ],
        };

        let mut buffer = vec![0u8; 1_000_000];
        let mut byte_buffer = ByteBuffer::new(0, buffer.as_mut_slice());
        compression.compress_segment(&segment_reference, &mut byte_buffer);

        let mut segment = Segment::new_empty();
        Compression::decompress_segment(byte_buffer.as_slice(), &mut segment);
        assert_eq!(segment.len, segment_reference.len);
        assert_eq!(segment.heap_len, segment_reference.heap_len);
        assert_eq!(&segment.timestamps[..], &segment_reference.timestamps[..]);
        assert_eq!(
            &segment.heap[..segment.heap_len],
            &segment_reference.heap[..segment_reference.heap_len]
        );
        for (a, b) in segment
            .columns()
            .iter()
            .zip(segment_reference.columns().iter())
        {
            assert_eq!(a, b);
        }
        assert_eq!(segment.types, segment_reference.types);
    }
}
