pub mod compression_scheme;
pub mod lz4;
pub mod timestamps;
pub mod delta_of_delta;
pub mod heap;

use compression_scheme::CompressionScheme;

use crate::{
    constants::SEG_SZ,
    segment::SegmentArray,
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
        timestamps: &[u64; SEG_SZ],
        heap: &[u8],
        data: &[&SegmentArray],
        types: &[FieldType],
        buffer: &mut ByteBuffer,
    ) {

        let heap_len = heap.len();
        let nvars = types.len();
        assert_eq!(self.schemes.len(), nvars);

        buffer.extend_from_slice(MAGIC);
        buffer.extend_from_slice(&len.to_be_bytes());
        buffer.extend_from_slice(&heap_len.to_be_bytes());
        buffer.extend_from_slice(&nvars.to_be_bytes());
        for t in types {
            buffer.push(*t as u8);
        }

        let ts_offset = buffer.len();
        buffer.extend_from_slice(&0usize.to_be_bytes());
        let ts_start = buffer.len();
        timestamps::compress(len, timestamps, buffer);
        let ts_end = buffer.len();
        let sz = ts_end - ts_start;
        buffer.as_mut_slice()[ts_offset..ts_start].copy_from_slice(&sz.to_be_bytes());

        for (scheme, column) in self.schemes.iter().zip(data.iter()) {
            let offset = buffer.len();
            buffer.extend_from_slice(&0usize.to_be_bytes());
            let start = buffer.len();
            scheme.compress(len, column, buffer);
            let end = buffer.len();
            let sz = end - start;
            buffer.as_mut_slice()[offset..start].copy_from_slice(&sz.to_be_bytes());
        }

        let heap_offset = buffer.len();
        buffer.extend_from_slice(&0usize.to_be_bytes());
        let heap_start = buffer.len();
        heap::compress_heap(heap, buffer);
        let heap_end = buffer.len();
        let sz = heap_end - heap_start;
        buffer.as_mut_slice()[heap_offset..heap_start].copy_from_slice(&sz.to_be_bytes());
    }
}

