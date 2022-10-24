pub mod compression_scheme;
pub mod lz4;
pub mod timestamps;
pub mod delta_of_delta;
pub mod heap;

use compression_scheme::CompressionScheme;

use crate::{
    constants::{HEAP_SZ, SEG_SZ},
    segment::{zero_segment_array, SegmentArray},
    field_type::FieldType,
    byte_buffer::ByteBuffer,
};

const MAGIC: &[u8] = "COMPRESSION".as_bytes();

pub trait CompressDecompress {
    fn compress(&self, data_len: usize, data: &SegmentArray, buffer: &mut ByteBuffer);
    fn decompress(&self, data: &[u8], data_len: &mut usize, buffer: &mut SegmentArray);
}

pub struct Compression {
    schemes: Vec<CompressionScheme>
}

impl Compression {
    pub fn compress(&self,
        len: usize,
        heap_len: usize,
        timestamps: &[u64; SEG_SZ],
        heap: &[u8; HEAP_SZ],
        data: &[&SegmentArray],
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
            let offset = buffer.len();
            buffer.extend_from_slice(&0usize.to_be_bytes());
            let start = buffer.len();
            scheme.compress(len, column, buffer);
            let sz = (buffer.len() - start).to_be_bytes();
            buffer.as_mut_slice()[offset..start].copy_from_slice(&sz);
        }

        {
            // Compress the heap
            let offset = buffer.len();
            buffer.extend_from_slice(&0usize.to_be_bytes());
            let start = buffer.len();
            heap::compress(&heap[..heap_len], buffer);
            let sz = (buffer.len() - start).to_be_bytes();
            buffer.as_mut_slice()[offset..start].copy_from_slice(&sz);
        }
    }

    pub fn decompress(
        bytes: &[u8],
        out_len: &mut usize,
        out_heap_len: &mut usize,
        out_timestamps: &mut [u64; SEG_SZ],
        out_heap: &mut [u8; HEAP_SZ],
        out_data: &mut Vec<SegmentArray>,
        out_types: &mut Vec<FieldType>,
    ) {

        let mut offset = 0;
        assert_eq!(&bytes[..MAGIC.len()], MAGIC);
        offset = MAGIC.len();

        let len = {
            let end = offset + 8;
            let len = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;
            *out_len = len;
            len
        };

        let heap_len = {
            let end = offset + 8;
            let heap_len = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;
            *out_heap_len = heap_len;
            heap_len
        };

        let nvars = {
            let end = offset + 8;
            let nvars = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;
            nvars
        };

        for _ in 0..nvars {
            out_types.push(FieldType::from(bytes[offset]));
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

            let end = offset + sz;
            let mut ts_len = 0;
            let schemes = timestamps::decompress(&bytes[offset..end], &mut ts_len, out_timestamps);
            offset = end;
            schemes
        }

        out_data.clear();
        for scheme in schemes.iter() {
            let end = offset + 8;
            let sz = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;

            let end = offset + sz;
            let mut col_len = 0;
            let mut buffer = zero_segment_array();
            scheme.decompress(&bytes[offset..end], &mut col_len, &mut buffer);
            offset = end;
            assert_eq!(len, col_len);
            out_data.push(buffer);
        }

        {
            let end = offset + 8;
            let sz = usize::from_be_bytes(bytes[offset..end].try_into().unwrap());
            offset = end;

            let end = offset + sz;
            let mut l = 0;
            heap::decompress(&bytes[offset..end], &mut l, out_heap.as_mut_slice());
            assert_eq!(l, heap_len);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::active_segment::{ActiveSegment, PushStatus};
    use crate::field_type::FieldType;
    use crate::sample::SampleType;
    use rand::{Rng, thread_rng, distributions::{Alphanumeric, DistString}};

    #[test]
    fn test() {
        let mut rng = thread_rng();
        let expected_floats: Vec<SampleType> =
            (0..SEG_SZ).map(|_| SampleType::F64(rng.gen())).collect();
        let expected_strings: Vec<SampleType> =
            (0..SEG_SZ).map(|_| {
                let string = Alphanumeric.sample_string(&mut rng, 16);
                SampleType::Bytes(string.into_bytes())
            }).collect();


        let types = &[FieldType::Bytes, FieldType::F64];
        let active_segment = ActiveSegment::new(types);
        let mut writer = active_segment.writer();

        let mut values = Vec::new();
        for i in 0..SEG_SZ-1 {
            let a = expected_strings[i].clone();
            let b = expected_floats[i].clone();
            values.push(a);
            values.push(b);
            assert_eq!(writer.push(i as u64, values.as_slice()), PushStatus::Ok);
            values.clear();
        }
        let a = expected_strings[SEG_SZ-1].clone();
        let b = expected_floats[SEG_SZ-1].clone();
        values.push(a);
        values.push(b);
        assert_eq!(writer.push(SEG_SZ as u64, values.as_slice()), PushStatus::IsFull);
        assert_eq!(writer.push(SEG_SZ as u64 + 1, values.as_slice()), PushStatus::ErrorFull);

        let segment_reference = writer.as_active_segment_ref();

        let compression = Compression {
            schemes: vec![
                CompressionScheme::DeltaOfDelta(delta_of_delta::DeltaOfDelta {}),
                CompressionScheme::LZ4(lz4::LZ4 {}),
            ],
        };

        let mut buffer = vec![0u8; 1_000_000];
        let mut byte_buffer = ByteBuffer::new(0, buffer.as_mut_slice());
        compression.compress(
            segment_reference.len,
            segment_reference.heap_len,
            segment_reference.ts,
            segment_reference.heap,
            segment_reference.data.as_slice(),
            segment_reference.types,
            &mut byte_buffer,
        );

        let mut len = 0;
        let mut heap_len = 0;
        let mut ts = [0u64; SEG_SZ];
        let mut heap = vec![0u8; HEAP_SZ];
        let mut data = Vec::new();
        let mut types = Vec::new();
        Compression::decompress(
            byte_buffer.as_slice(),
            &mut len,
            &mut heap_len,
            &mut ts,
            heap.as_mut_slice().try_into().unwrap(),
            &mut data,
            &mut types
        );
        assert_eq!(len, segment_reference.len);
        assert_eq!(heap_len, segment_reference.heap_len);
        assert_eq!(&ts, segment_reference.ts);
        assert_eq!(&heap[..heap_len], &segment_reference.heap[..heap_len]);
        for (a, b) in data.iter().zip(segment_reference.data.iter()) {
            assert_eq!(&a, b);
        }
        assert_eq!(types, segment_reference.types);

    //pub fn compress(&self,
    //    len: usize,
    //    heap_len: usize,
    //    timestamps: &[u64; SEG_SZ],
    //    heap: &[u8; HEAP_SZ],
    //    data: &[&SegmentArray],
    //    types: &[FieldType],
    //    buffer: &mut ByteBuffer,
    //) {
    }
}

