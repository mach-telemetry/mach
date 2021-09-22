mod rows;
mod simple;
mod utils;

use crate::{
    compression::rows::{rows_compression, rows_decompression},
    compression::simple::{simple_compression, simple_decompression},
    segment::{Segment, SegmentLike},
    tsdb::Dt,
};
use num::NumCast;
use std::convert::TryFrom;
use serde::*;

const MAGIC: &str = "202107280428";

#[derive(Clone, Serialize, Deserialize)]
pub enum Compression {
    Simple { precision: Vec<u8> },
    Rows { precision: Vec<u8> },
}

pub struct Header {
    _size: usize,
    _mint: Dt,
    _maxt: Dt,
    len: usize,
    section_code: u8,
}

impl Compression {
    fn write_header<S: SegmentLike>(&self, data: &S, buf: &mut [u8]) -> usize {
        // Compressed section format:
        // n bytes MAGIC number
        // 1 byte (u8) section code
        // 2 bytes (u16) total size of the section in bytes
        // 8 bytes (u64) min timestamp
        // 8 bytes (u64) max timestamp
        // 2 bytes (u16) length (number of samples in this section, almost always SECTSZ)
        let mut off = 0;
        let len = data.len();
        assert!(len > 0);
        //let mut scratch: [u64; 256] = [0; 256];

        off += 2; // Reserve the first two bytes as the total size of the section

        let magic_len = MAGIC.len();
        buf[off..off + magic_len].copy_from_slice(MAGIC.as_bytes());
        off += magic_len;

        buf[off] = self.section_code();
        off += 1;

        // Write the min and max timestamps
        let timestamps = data.timestamps();
        let min_ts = timestamps[0];
        let sz: [u8; 8] = min_ts.to_le_bytes();
        buf[off..off + 8].copy_from_slice(&sz[..]);
        off += 8;

        let max_ts = timestamps[len - 1];
        let sz: [u8; 8] = max_ts.to_le_bytes();
        buf[off..off + 8].copy_from_slice(&sz[..]);
        off += 8;

        // Write the length of the section in the first bytes; 256 requires 2 bytes...
        let sz: [u8; 2] = <u16 as NumCast>::from(len).unwrap().to_le_bytes();
        buf[off..off + 2].copy_from_slice(&sz[..]);
        off += 2;
        off
    }

    fn section_code(&self) -> u8 {
        match self {
            Self::Simple { .. } => 1,
            Self::Rows { .. } => 2,
        }
    }

    pub fn compress<S: SegmentLike>(&self, data: &S, buf: &mut [u8]) -> usize {
        let mut offset = 0;
        offset += self.write_header(data, buf);
        offset += match self {
            Self::Simple { precision } => {
                simple_compression(data, &mut buf[offset..], precision.as_slice())
            }
            Self::Rows { precision } => {
                rows_compression(data, &mut buf[offset..], precision.as_slice())
            }
        };

        // write length in the first 2 bytes of the header
        let sz: [u8; 2] = <u16 as NumCast>::from(offset).unwrap().to_le_bytes();
        buf[..2].copy_from_slice(&sz[..]);

        assert!(offset > 0);
        offset
    }

    pub fn read_header(data: &[u8]) -> (Header, usize) {
        let mut oft = 0;

        // First two bytes is the length of the section slice in bytes
        let size = u16::from_le_bytes(<[u8; 2]>::try_from(&data[oft..oft + 2]).unwrap()) as usize;
        oft += 2;

        // Then magic and code
        let magic_len = MAGIC.len();
        assert_eq!(
            &data[oft..oft + magic_len],
            MAGIC.as_bytes(),
            "Section magic does not match"
        );
        oft += magic_len;

        let section_code = data[oft];
        oft += 1;

        // Next 8 bytes is number of items in section
        let min_timestamp = u64::from_le_bytes(<[u8; 8]>::try_from(&data[oft..oft + 8]).unwrap());
        oft += 8;

        // Next 8 bytes is number of items in section
        let max_timestamp = u64::from_le_bytes(<[u8; 8]>::try_from(&data[oft..oft + 8]).unwrap());
        oft += 8;

        // Next two bytes is number of items in section
        let len = u16::from_le_bytes(<[u8; 2]>::try_from(&data[oft..oft + 2]).unwrap()) as usize;
        assert!(len > 0);
        oft += 2;

        let h = Header {
            _size: size,
            _mint: min_timestamp,
            _maxt: max_timestamp,
            len,
            section_code,
        };

        (h, oft)
    }

    pub fn decompress(data: &[u8], buf: &mut Segment) -> Header {
        let mut offset = 0;
        let (header, header_sz) = Self::read_header(data);
        offset += header_sz;

        match header.section_code {
            1 => simple_decompression(&header, buf, &data[offset..]),
            2 => rows_decompression(&header, buf, &data[offset..]),
            _ => panic!("Invalid compression code!"),
        };
        buf.len = header.len;

        header
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{active_segment::ActiveSegment, compression::utils::round, tsdb::Sample};
    use rand::prelude::*;

    #[test]
    fn test_simple_compress_decompress() {
        let mut rng = thread_rng();
        let active_segment = ActiveSegment::new(2);
        let mut writer = active_segment.writer();

        let mut samples = Vec::new();
        let mut var0 = Vec::new();
        let mut var1 = Vec::new();
        for dt in 0..256 {
            let v0 = rng.gen();
            let v1 = rng.gen();
            var0.push(round(3, v0));
            var1.push(round(3, v1));
            let s = Sample {
                ts: dt,
                values: Box::new([v0, v1]),
            };
            samples.push(s);
        }

        for s in samples {
            writer.push(s);
        }

        let segment_buf = writer.yield_replace();
        let compress = Compression::Simple {
            precision: vec![3, 3],
        };
        let mut compressed_buf = [0u8; 8192];
        let sz = compress.compress(&segment_buf, &mut compressed_buf[..]);

        let mut segment = Segment::new();
        Compression::decompress(&compressed_buf[..sz], &mut segment);
        assert_eq!(segment.len(), 256);
        assert_eq!(segment.nvars(), 2);
        assert_eq!(segment.values.len(), 512);
        assert_eq!(segment.variable(0), var0);
        assert_eq!(segment.variable(1), var1);
    }

    #[test]
    fn test_rows_compress_decompress() {
        let mut rng = thread_rng();
        let active_segment = ActiveSegment::new(2);
        let mut writer = active_segment.writer();

        let mut samples = Vec::new();
        let mut var0 = Vec::new();
        let mut var1 = Vec::new();

        // This should be less than 256 / nvars
        for dt in 0..120 {
            let v0 = rng.gen();
            let v1 = rng.gen();
            var0.push(round(3, v0));
            var1.push(round(3, v1));
            let s = Sample {
                ts: dt,
                values: Box::new([v0, v1]),
            };
            samples.push(s);
        }

        for s in samples {
            writer.push(s);
        }

        let segment_buf = writer.yield_replace();
        let compress = Compression::Rows {
            precision: vec![3, 3],
        };
        let mut compressed_buf = [0u8; 8192];
        let sz = compress.compress(&segment_buf, &mut compressed_buf[..]);

        let mut segment = Segment::new();
        Compression::decompress(&compressed_buf[..sz], &mut segment);
        assert_eq!(segment.len(), 120);
        assert_eq!(segment.nvars(), 2);
        assert_eq!(segment.values.len(), 240);
        assert_eq!(segment.variable(0), var0);
        assert_eq!(segment.variable(1), var1);
    }

    #[test]
    #[should_panic]
    fn test_rows_too_big_compress_decompress() {
        let mut rng = thread_rng();
        let active_segment = ActiveSegment::new(2);
        let mut writer = active_segment.writer();

        let mut samples = Vec::new();
        let mut var0 = Vec::new();
        let mut var1 = Vec::new();

        // This should cause the rows compressor to panic because total number of values being
        // compressed is > 256
        for dt in 0..250 {
            let v0 = rng.gen();
            let v1 = rng.gen();
            var0.push(round(3, v0));
            var1.push(round(3, v1));
            let s = Sample {
                ts: dt,
                values: Box::new([v0, v1]),
            };
            samples.push(s);
        }

        for s in samples {
            writer.push(s);
        }

        let segment_buf = writer.yield_replace();
        let compress = Compression::Rows {
            precision: vec![3, 3],
        };
        let mut compressed_buf = [0u8; 8192];
        compress.compress(&segment_buf, &mut compressed_buf[..]);

        //let mut segment = Segment::new();
        //Compression::decompress(&compressed_buf[..sz], &mut segment);
        //assert_eq!(segment.len(), 120);
        //assert_eq!(segment.nvars(), 2);
        //assert_eq!(segment.values.len(), 240);
        //assert_eq!(segment.variable(0), var0);
        //assert_eq!(segment.variable(1), var1);
    }
}
