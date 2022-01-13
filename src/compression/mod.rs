mod fixed;
mod timestamps;
mod utils;
mod xor;
mod decimal;
mod bytes_lz4;

use crate::segment::FullSegment;
use crate::utils::byte_buffer::ByteBuffer;
use lzzzz::lz4;
use std::convert::TryInto;

//pub use utils::ByteBuffer;

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
    fn clear(&mut self) {
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

    pub fn timestamp_at(&self, i: usize) -> u64 {
        self.ts[self.len - i - 1]
    }

    pub fn value_at(&self, var: usize, i: usize) -> [u8; 8] {
        self.values[var][self.len - i - 1]
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn timestamps(&self) -> &[u64] {
        &self.ts[..self.len]
    }

    pub fn variable(&self, var: usize) -> &[[u8; 8]] {
        //println!("{} {}", var, self.len);
        &self.values[var][..self.len]
    }
}

#[derive(Copy, Clone)]
struct Header {
    code: usize,
    nvars: usize,
    len: usize,
}

#[derive(Copy, Clone)]
pub enum Compression {
    LZ4(i32),
    Fixed(usize),
    XOR,
    Decimal(u8),
}

impl Compression {
    // Header format:
    // Magic: [0..12]
    // Compression type: [12..20]
    // N Variables: [20..28]
    // N Samples: [28..36]
    fn set_header(&self, segment: &FullSegment, buf: &mut ByteBuffer) -> Header {
        let compression_id: u64 = match self {
            Compression::LZ4(_) => 1,
            Compression::Fixed(_) => 2,
            Compression::XOR => 3,
            Compression::Decimal(_) => 4,
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
            return Err(Error::UnrecognizedMagic);
        }

        off += MAGIC.len();

        let code = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;

        let nvars = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;

        let len = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;

        Ok((Header { code, nvars, len }, off))
    }

    pub fn compress(&self, segment: &FullSegment, buf: &mut ByteBuffer) {
        self.set_header(segment, buf);
        match self {
            Compression::LZ4(acc) => lz4_compress(segment, buf, *acc),
            Compression::Fixed(bits) => fixed_compress(segment, buf, *bits),
            Compression::XOR => xor_compress(segment, buf),
            Compression::Decimal(precision) => decimal_compress(segment, buf, *precision),
        }
    }

    pub fn decompress(data: &[u8], buf: &mut DecompressBuffer) -> Result<usize, Error> {
        buf.clear();
        let (header, mut off) = Self::get_header(data)?;

        buf.len += header.len as usize;
        //println!("buf.nvars {}", buf.nvars);
        if buf.nvars == 0 {
            //println!("HERE");
            buf.set_nvars(header.nvars);
        }

        if header.nvars != buf.nvars {
            return Err(Error::InconsistentBuffer);
        }

        off += match header.code {
            1 => lz4_decompress(header, &data[off..], buf)?,
            2 => fixed_decompress(header, &data[off..], buf)?,
            3 => xor_decompress(header, &data[off..], buf)?,
            4 => decimal_decompress(header, &data[off..], buf)?,
            _ => return Err(Error::UnrecognizedCompressionType),
        };

        Ok(off)
    }
}

fn lz4_compress(segment: &FullSegment, buf: &mut ByteBuffer, acc: i32) {
    //let mut len: u64 = segment.len as u64;
    let nvars: u64 = segment.nvars as u64;

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
    buf.extend_from_slice(&0u64.to_be_bytes()[..]); // compressed sz placeholder

    // Compress the raw data and record the compressed size
    let csz = lz4::compress(&bytes[..], buf.unused(), acc).unwrap();
    buf.add_len(csz);

    //println!("LZ4 Compress: len {} nvars {} csz {} buflen {}", len, nvars, csz, buf.len());

    // Write the compressed size
    buf.as_mut_slice()[csz_off..csz_off + 8].copy_from_slice(&csz.to_be_bytes()[..]);
}

fn lz4_decompress(header: Header, data: &[u8], buf: &mut DecompressBuffer) -> Result<usize, Error> {
    let mut off = 0;

    let raw_sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap());
    off += 8;

    let cmp_sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap());
    off += 8;

    let mut bytes = vec![0u8; raw_sz as usize].into_boxed_slice();
    lz4::decompress(&data[off..off + cmp_sz as usize], &mut bytes[..]).unwrap();

    let mut off = 0;
    for _ in 0..header.len {
        buf.ts
            .push(u64::from_be_bytes(bytes[off..off + 8].try_into().unwrap()));
        off += 8;
    }

    for var in 0..header.nvars {
        for _ in 0..header.len {
            buf.values[var].push(bytes[off..off + 8].try_into().unwrap());
            off += 8;
        }
    }

    Ok(off)
}

fn fixed_compress(segment: &FullSegment, buf: &mut ByteBuffer, frac: usize) {
    // write the frac
    buf.extend_from_slice(&(frac as u64).to_be_bytes()[..]);

    // compress the timesetamps
    let len_offset = buf.len();
    buf.extend_from_slice(&0u64.to_be_bytes()[..]); // compressed sz placeholder
    let start_len = buf.len();
    timestamps::compress(segment.timestamps(), buf);
    let end_len = buf.len();
    let len = (end_len - start_len) as u64;
    buf.as_mut_slice()[len_offset..len_offset + 8].copy_from_slice(&len.to_be_bytes()[..]);

    // compress the values
    let nvars = segment.nvars;
    for i in 0..nvars {
        //let p = buf.len();
        let len_offset = buf.len();
        buf.extend_from_slice(&0u64.to_be_bytes()[..]); // compressed sz placeholder
        let start_len = len_offset + 8;
        fixed::compress(segment.variable(i), buf, frac);
        let end_len = buf.len();
        let len = (end_len - start_len) as u64;
        buf.as_mut_slice()[len_offset..len_offset + 8].copy_from_slice(&len.to_be_bytes()[..]);
    }
}

fn fixed_decompress(
    header: Header,
    data: &[u8],
    buf: &mut DecompressBuffer,
) -> Result<usize, Error> {
    let mut off = 0;

    // read the frac
    let frac = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
    off += 8;

    // decompress timestamps
    let sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
    off += 8;

    timestamps::decompress(&data[off..off + sz], &mut buf.ts);
    off += sz;

    // decompress values
    for i in 0..header.nvars {
        let sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;
        fixed::decompress(&data[off..off + sz], &mut buf.values[i], frac);
        off += sz;
    }
    Ok(off)
}

fn xor_compress(segment: &FullSegment, buf: &mut ByteBuffer) {
    // compress the timesetamps
    let len_offset = buf.len();
    buf.extend_from_slice(&0u64.to_be_bytes()[..]); // compressed sz placeholder
    let start_len = buf.len();
    timestamps::compress(segment.timestamps(), buf);
    let end_len = buf.len();
    let len = (end_len - start_len) as u64;
    buf.as_mut_slice()[len_offset..len_offset + 8].copy_from_slice(&len.to_be_bytes()[..]);

    // compress the values
    let nvars = segment.nvars;
    for i in 0..nvars {
        //let p = buf.len();
        let len_offset = buf.len();
        buf.extend_from_slice(&0u64.to_be_bytes()[..]); // compressed sz placeholder
        let start_len = len_offset + 8;
        xor::compress(segment.variable(i), buf);
        let end_len = buf.len();
        let len = (end_len - start_len) as u64;
        buf.as_mut_slice()[len_offset..len_offset + 8].copy_from_slice(&len.to_be_bytes()[..]);
    }
}

fn xor_decompress(header: Header, data: &[u8], buf: &mut DecompressBuffer) -> Result<usize, Error> {
    let mut off = 0;

    // decompress timestamps
    let sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
    off += 8;

    timestamps::decompress(&data[off..off + sz], &mut buf.ts);
    off += sz;

    // decompress values
    for i in 0..header.nvars {
        let sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;
        xor::decompress(&data[off..off + sz], &mut buf.values[i]);
        off += sz;
    }
    Ok(off)
}

fn decimal_compress(segment: &FullSegment, buf: &mut ByteBuffer, precision: u8) {
    // compress the timesetamps
    let len_offset = buf.len();
    buf.extend_from_slice(&0u64.to_be_bytes()[..]); // compressed sz placeholder
    let start_len = buf.len();
    timestamps::compress(segment.timestamps(), buf);
    let end_len = buf.len();
    let len = (end_len - start_len) as u64;
    buf.as_mut_slice()[len_offset..len_offset + 8].copy_from_slice(&len.to_be_bytes()[..]);

    // compress the values
    let nvars = segment.nvars;
    for i in 0..nvars {
        //let p = buf.len();
        let len_offset = buf.len();
        buf.extend_from_slice(&0u64.to_be_bytes()[..]); // compressed sz placeholder
        let start_len = len_offset + 8;
        decimal::compress(segment.variable(i), buf, precision);
        let end_len = buf.len();
        let len = (end_len - start_len) as u64;
        buf.as_mut_slice()[len_offset..len_offset + 8].copy_from_slice(&len.to_be_bytes()[..]);
    }
}

fn decimal_decompress(
    header: Header,
    data: &[u8],
    buf: &mut DecompressBuffer,
) -> Result<usize, Error> {
    let mut off = 0;
    // decompress timestamps
    let sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
    off += 8;

    timestamps::decompress(&data[off..off + sz], &mut buf.ts);
    off += sz;

    // decompress values
    for i in 0..header.nvars {
        let sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;
        decimal::decompress(&data[off..off + sz], &mut buf.values[i]);
        off += sz;
    }
    Ok(off)
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;

    #[test]
    fn test_xor() {
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

        let mut compressed = vec![0u8; 4096];
        let mut byte_buf = ByteBuffer::new(&mut compressed[..]);
        let mut buf = DecompressBuffer::new();
        let header = Header {
            code: 1,
            nvars,
            len: 256,
        };

        // Need to set this manually because the header is generated separately
        buf.set_nvars(nvars);
        buf.len = 256;

        xor_compress(&segment, &mut byte_buf);
        xor_decompress(header, &compressed[..], &mut buf).unwrap();

        assert_eq!(&buf.ts[..], &timestamps[..]);
        for i in 0..nvars {
            let exp = segment.variable(i);
            let res = buf.variable(i);
            let diff = exp
                .iter()
                .zip(res.iter())
                .map(|(x, y)| (f64::from_be_bytes(*x) - f64::from_be_bytes(*y)).abs())
                .fold(f64::NAN, f64::max);

            assert!(diff < 0.001);
        }
    }

    #[test]
    fn test_fixed() {
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

        let mut compressed = vec![0u8; 4096];
        let mut byte_buf = ByteBuffer::new(&mut compressed[..]);
        let mut buf = DecompressBuffer::new();
        let header = Header {
            code: 1,
            nvars,
            len: 256,
        };

        // Need to set this manually because the header is generated separately
        buf.set_nvars(nvars);
        buf.len = 256;

        fixed_compress(&segment, &mut byte_buf, 10);
        fixed_decompress(header, &compressed[..], &mut buf).unwrap();

        assert_eq!(&buf.ts[..], &timestamps[..]);
        for i in 0..nvars {
            let exp = segment.variable(i);
            let res = buf.variable(i);
            let diff = exp
                .iter()
                .zip(res.iter())
                .map(|(x, y)| (f64::from_be_bytes(*x) - f64::from_be_bytes(*y)).abs())
                .fold(f64::NAN, f64::max);

            assert!(diff < 0.001);
        }
    }

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

        let mut compressed = vec![0u8; 8094];
        let mut byte_buf = ByteBuffer::new(&mut compressed[..]);
        let mut buf = DecompressBuffer::new();
        let header = Header {
            code: 1,
            nvars,
            len: 256,
        };

        // Need to set this manually because the header is generated separately
        buf.set_nvars(nvars);
        buf.len = 256;

        lz4_compress(&segment, &mut byte_buf, 1);
        let sz = byte_buf.len();
        lz4_decompress(header, &compressed[..sz], &mut buf).unwrap();

        assert_eq!(&buf.ts[..], &timestamps[..]);
        for i in 0..nvars {
            assert_eq!(buf.variable(i), segment.variable(i));
        }
    }

    #[test]
    fn test_decimal() {
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

        let mut compressed = vec![0u8; 4096];
        let mut byte_buf = ByteBuffer::new(&mut compressed[..]);
        let mut buf = DecompressBuffer::new();
        let header = Header {
            code: 1,
            nvars,
            len: 256,
        };

        // Need to set this manually because the header is generated separately
        buf.set_nvars(nvars);
        buf.len = 256;

        decimal_compress(&segment, &mut byte_buf, 3);
        decimal_decompress(header, &compressed[..], &mut buf).unwrap();

        assert_eq!(&buf.ts[..], &timestamps[..]);
        for i in 0..nvars {
            let exp = segment.variable(i);
            let res = buf.variable(i);
            let diff = exp
                .iter()
                .zip(res.iter())
                .map(|(x, y)| (f64::from_be_bytes(*x) - f64::from_be_bytes(*y)).abs())
                .fold(f64::NAN, f64::max);

            assert!(diff < 0.001);
        }
    }

}
