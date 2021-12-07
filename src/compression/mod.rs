mod timestamps;

mod fixed;
mod xor;
mod utils;

use crate::segment::FullSegment;
use lzzzz::lz4;
use std::convert::TryInto;

#[derive(Debug)]
pub enum Error {
    UnrecognizedMagic,
    UnrecognizedCompressionType,
    InconsistentBuffer,
}

const MAGIC: &[u8; 12] = b"202107280428";

pub struct DecompressBuffer {
    ts: Vec<u64>,
    values: Vec<Vec<[u8; 8]>>,
    len: usize,
    nvars: usize,
}

impl DecompressBuffer {
    pub fn clear(&mut self) {
        self.ts.clear();
        self.values.iter_mut().for_each(|x| x.clear());
        self.len = 0;
    }

    pub fn set_nvars(&mut self, nvars: usize) {
        if self.nvars < nvars {
            for _ in 0..nvars - self.nvars {
                self.values.push(Vec::new());
            }
        } else if self.nvars > nvars {
            for _ in 0..self.nvars - nvars {
                self.values.pop();
            }
        }
        self.nvars = nvars;
    }

    pub fn new() -> Self {
        Self {
            ts: Vec::new(),
            values: Vec::new(),
            len: 0,
            nvars: 0,
        }
    }

    pub fn timestamps(&self) -> &[u64] {
        &self.ts[..self.len]
    }

    pub fn variable(&self, var: usize) -> &[[u8; 8]] {
        println!("{} {}", var, self.len);
        &self.values[var][..self.len]
    }
}

#[derive(Copy, Clone)]
struct Header {
    code: usize,
    nvars: usize,
    len: usize,
}

pub enum Compression {
    LZ4(i32)
}

impl Compression {

    // Header format:
    // Magic: [0..12]
    // Compression type: [12..20]
    // N Variables: [20..28]
    // N Samples: [28..36]
    fn set_header(&self, segment: &FullSegment, buf: &mut Vec<u8>) -> Header {
        let compression_id: u64 = match self {
            Compression::LZ4(_) => 1,
        };
        buf.extend_from_slice(&MAGIC[..]);
        buf.extend_from_slice(&compression_id.to_be_bytes()[..]);
        buf.extend_from_slice(&segment.nvars.to_be_bytes()[..]);
        buf.extend_from_slice(&segment.len.to_be_bytes()[..]);

        Header {
            code: compression_id as usize,
            nvars: segment.nvars,
            len: segment.len,
        }
    }

    fn get_header(data: &[u8]) -> Result<(Header, usize), Error> {
        let mut off = 0;
        if &data[off..MAGIC.len()] != &MAGIC[..] {
            return Err(Error::UnrecognizedMagic)
        }

        off += MAGIC.len();

        let code = u64::from_be_bytes(data[off..off+8].try_into().unwrap()) as usize;
        off += 8;

        let nvars = u64::from_be_bytes(data[off..off+8].try_into().unwrap()) as usize;
        off += 8;

        let len = u64::from_be_bytes(data[off..off+8].try_into().unwrap()) as usize;
        off += 8;

        Ok((Header { code, nvars, len, }, off))
    }

    pub fn compress(&self, segment: &FullSegment, buf: &mut Vec<u8>) {
        self.set_header(segment, buf);
        match self {
            Compression::LZ4(acc) => lz4_compress(segment, buf, *acc),
        }
    }

    pub fn decompress(data: &[u8], buf: &mut DecompressBuffer) -> Result<usize, Error> {
        let (header, mut off) = Self::get_header(data)?;

        buf.len += header.len as usize;
        println!("buf.nvars {}", buf.nvars);
        if buf.nvars == 0 {
            println!("HERE");
            buf.set_nvars(header.nvars);
        }

        if header.nvars != buf.nvars {
            return Err(Error::InconsistentBuffer)
        }

        off += match header.code {
            1 => lz4_decompress(header, &data[off..], buf)?,
            _ => return Err(Error::UnrecognizedCompressionType),
        };


        Ok(off)
    }
}

fn lz4_compress(segment: &FullSegment, buf: &mut Vec<u8>, acc: i32) {
    let mut len: u64 = segment.len as u64;
    let mut nvars: u64 = segment.nvars as u64;

    let mut bytes = Vec::new();
    for ts in segment.timestamps().iter() {
        bytes.extend_from_slice(&ts.to_be_bytes()[..]);
    }

    for var in 0..nvars {
        for v in segment.variable(var as usize).iter() {
            bytes.extend_from_slice(&v[..]);
        }
    }
    let raw_sz = bytes.len() as u64;

    // Uncompressed size
    buf.extend_from_slice(&raw_sz.to_be_bytes()[..]);

    // write a place holder of compressed size
    let csz_off = buf.len();
    buf.extend_from_slice(&0u64.to_be_bytes()[..]);         // compressed sz placeholder

    // Compress the raw data and record the compressed size
    let csz = lz4::compress_to_vec(&bytes[..], buf, acc).unwrap() as u64;

    // Write the compressed size
    buf[csz_off..csz_off + 8].copy_from_slice(&csz.to_be_bytes()[..]);
}

fn lz4_decompress(header: Header, data: &[u8], buf: &mut DecompressBuffer) -> Result<usize, Error> {
    let mut off = 0;

    let raw_sz = u64::from_be_bytes(data[off..off+8].try_into().unwrap());
    off += 8;

    let cmp_sz = u64::from_be_bytes(data[off..off+8].try_into().unwrap());
    off += 8;

    let mut bytes = vec![0u8; raw_sz as usize].into_boxed_slice();
    lz4::decompress(&data[off..off+cmp_sz as usize], &mut bytes[..]).unwrap();

    let mut off = 0;
    for i in 0..header.len {
        buf.ts.push(u64::from_be_bytes(bytes[off..off + 8].try_into().unwrap()));
        off += 8;
    }

    for var in 0..header.nvars {
        for i in 0..header.len {
            buf.values[var].push(bytes[off..off + 8].try_into().unwrap());
            off += 8;
        }
    }

    Ok(off)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;

    #[test]
    fn test_lz4() {

        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();

        let mut timestamps = [0; 256];
        let mut v = Vec::new();
        for _ in 0..nvars {
            v.push([[0u8; 8]; 256]);
        }

        for (idx, sample) in data[0..256].iter().enumerate() {
            timestamps[idx] = sample.ts;
            for (var, val) in sample.values.iter().enumerate() {
                v[var][idx] = val.to_be_bytes();
            }
        }

        let segment = FullSegment {
            len: 256,
            nvars,
            ts: &timestamps,
            data: v.as_slice(),
        };

        let mut compressed = Vec::new();
        let mut buf = DecompressBuffer::new();
        let header = Header {
            code: 1,
            nvars,
            len: 256,
        };

        // Need to set this manually because the header is generated separately
        buf.set_nvars(nvars);
        buf.len = 256;

        lz4_compress(&segment, &mut compressed, 1);
        lz4_decompress(header, &compressed[..], &mut buf).unwrap();

        assert_eq!(&buf.ts[..], &timestamps[..]);
        for i in 0..nvars {
            assert_eq!(buf.variable(i), segment.variable(i));
        }

        for (idx, sample) in data[256..512].iter().enumerate() {
            timestamps[idx] = sample.ts;
            for (var, val) in sample.values.iter().enumerate() {
                v[var][idx] = val.to_be_bytes();
            }
        }

        let segment = FullSegment {
            len: 256,
            nvars,
            ts: &timestamps,
            data: v.as_slice(),
        };

        // Need to set this manually because the header is generated separately
        buf.len += 256;

        let mut compressed = Vec::new();
        lz4_compress(&segment, &mut compressed, 1);
        lz4_decompress(header, &compressed[..], &mut buf).unwrap();

        assert_eq!(buf.len, 512);
        assert_eq!(&buf.timestamps()[256..512], &timestamps[..]);
        for i in 0..nvars {
            let exp: &[[u8; 8]] = &buf.variable(i)[256..];
            let cmp: &[[u8; 8]] = segment.variable(i);
            assert_eq!(exp, cmp);
        }
    }
}
