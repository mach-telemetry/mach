mod simple;
mod utils;

use crate::{
    compression::simple::simple_compression,
    segment::SegmentLike,
    tsdb::{Dt, Fl},
};
use num::NumCast;
//use std::convert::TryFrom;

const MAGIC: &str = "202107280428";

//pub type CompressFn<S> = fn(S, &SeriesOptions, &mut [u8]) -> usize;
//pub type DecompressFn = fn(&mut DecompressBuf, &[u8]) -> usize;

#[derive(Debug)]
pub struct DecompressBuf<'a> {
    pub ts: &'a mut [Dt],
    pub values: &'a mut [&'a mut [Fl]],
    pub len: usize,
}

#[derive(Clone)]
pub enum Compression {
    Simple { precision: Vec<u8> },
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
            //Self::Rows => 2,
        }
    }

    pub fn compress<S: SegmentLike>(&self, data: &S, buf: &mut [u8]) -> usize {
        let mut offset = 0;
        offset += self.write_header(data, buf);
        offset += match self {
            Self::Simple { precision } => {
                simple_compression(data, &mut buf[offset..], precision.as_slice())
            } //Self::Rows => rows_compression(data, &mut buf[offset..], precision),
        };

        // write length in the first 2 bytes of the header
        let sz: [u8; 2] = <u16 as NumCast>::from(offset).unwrap().to_le_bytes();
        buf[..2].copy_from_slice(&sz[..]);

        assert!(offset > 0);
        offset
    }

    //pub fn read_header(data: &[u8]) -> (Header, usize) {
    //    let mut oft = 0;

    //    // First two bytes is the length of the section slice in bytes
    //    let size = u16::from_le_bytes(<[u8; 2]>::try_from(&data[oft..oft + 2]).unwrap()) as usize;
    //    oft += 2;

    //    // Then magic and code
    //    let magic_len = MAGIC.len();
    //    assert_eq!(
    //        &data[oft..oft + magic_len],
    //        MAGIC.as_bytes(),
    //        "Section magic does not match"
    //    );
    //    oft += magic_len;

    //    let section_code = data[oft];
    //    oft += 1;

    //    // Next 8 bytes is number of items in section
    //    let min_timestamp = u64::from_le_bytes(<[u8; 8]>::try_from(&data[oft..oft + 8]).unwrap());
    //    oft += 8;

    //    // Next 8 bytes is number of items in section
    //    let max_timestamp = u64::from_le_bytes(<[u8; 8]>::try_from(&data[oft..oft + 8]).unwrap());
    //    oft += 8;

    //    // Next two bytes is number of items in section
    //    let len = u16::from_le_bytes(<[u8; 2]>::try_from(&data[oft..oft + 2]).unwrap()) as usize;
    //    assert!(len > 0);
    //    oft += 2;

    //    let h = Header {
    //        size,
    //        rng: Interval::new(min_timestamp, max_timestamp),
    //        len,
    //        section_code,
    //    };

    //    (h, oft)
    //}

    //pub fn decompress(data: &[u8], buf: &mut DecompressBuf) -> Header {
    //    let mut offset = 0;
    //    let (header, header_sz) = Self::read_header(data);
    //    offset += header_sz;

    //    match header.section_code {
    //        1 => simple_decompression(&header, buf, &data[offset..]),
    //        2 => rows_decompression(&header, buf, &data[offset..]),
    //        _ => panic!("Invalid compression code!"),
    //    };
    //    buf.len = header.len;

    //    header
    //}
}
