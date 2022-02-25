mod bytes_lz4;
mod bytes_lz42;
mod decimal;
//mod fixed;
mod timestamps;
mod utils;
mod xor;

use crate::segment::*;
use crate::utils::byte_buffer::ByteBuffer;
use lzzzz::lz4;
use std::convert::TryInto;
use std::sync::Arc;

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
        &self.values[var][..self.len]
    }
}

#[derive(Copy, Clone)]
pub enum CompressFn {
    Decimal(u8),
    BytesLZ4,
    XOR,
}

impl CompressFn {
    pub fn compress(&self, segment: &[[u8; 8]], buf: &mut ByteBuffer) {
        match self {
            CompressFn::Decimal(precision) => decimal::compress(segment, buf, *precision),
            //CompressFn::BytesLZ4 => bytes_lz4::compress(segment, buf),
            CompressFn::XOR => xor::compress(segment, buf),
            _ => unimplemented!(),
        };
    }

    pub fn compress_heap(&self, len: usize, heap: &[u8], buf: &mut ByteBuffer) {
        match self {
            CompressFn::BytesLZ4 => bytes_lz42::compress(len, heap, buf),
            _ => unimplemented!(),
        };
    }

    pub fn decompress(id: u64, data: &[u8], buf: &mut Vec<[u8; 8]>) {
        match id {
            2 => xor::decompress(data, buf),
            3 => decimal::decompress(data, buf),
            4 => bytes_lz4::decompress(data, buf),
            _ => panic!("Error"),
        }
    }

    fn id(&self) -> u64 {
        match self {
            CompressFn::XOR => 2,
            CompressFn::Decimal(_) => 3,
            CompressFn::BytesLZ4 => 4,
        }
    }
}

struct Header {
    codes: Vec<u64>,
    len: usize,
}

#[derive(Clone)]
pub struct Compression(Arc<Vec<CompressFn>>);

impl std::ops::Deref for Compression {
    type Target = [CompressFn];
    fn deref(&self) -> &Self::Target {
        &self.0.as_slice()
    }
}

impl std::convert::From<Vec<CompressFn>> for Compression {
    fn from(v: Vec<CompressFn>) -> Self {
        Compression(Arc::new(v))
    }
}

impl Compression {
    //pub fn new() -> Self {
    //    Compression(Vec::new())
    //}

    fn set_header(&self, segment: &FullSegment, buf: &mut ByteBuffer) {
        buf.extend_from_slice(&MAGIC[..]);
        buf.extend_from_slice(&self.0.len().to_be_bytes()[..]);
        buf.extend_from_slice(&segment.len().to_be_bytes()[..]);
        for c in self.0.iter() {
            buf.extend_from_slice(&c.id().to_be_bytes()[..]);
        }
    }

    pub fn compress(&self, segment: &FullSegment, buf: &mut ByteBuffer) {
        self.set_header(segment, buf);

        // compress the timestamps

        // placeholder for size post compression
        let len_offset = buf.len();
        buf.extend_from_slice(&0u64.to_be_bytes()[..]);
        //println!("TIMESTAMPS AT: {}", buf.len());

        // compress
        let start_len = buf.len();
        timestamps::compress(segment.timestamps(), buf);
        let end_len = buf.len();

        // write size
        let len = (end_len - start_len) as u64;
        buf.as_mut_slice()[len_offset..len_offset + 8].copy_from_slice(&len.to_be_bytes()[..]);

        // compress each variable
        let seg_len = segment.len();
        for i in 0..segment.nvars() {
            let compression = &self.0[i];
            let variable = segment.get_variable(i);

            // placeholder for size post compression
            let len_offset = buf.len();
            buf.extend_from_slice(&0u64.to_be_bytes()[..]); // compressed sz placeholder
                                                            //println!("VAR {} AT: {}", i, buf.len());

            // compress
            let start_len = buf.len();
            match variable {
                Variable::Var(x) => compression.compress(x, buf),
                Variable::Heap(x) => compression.compress_heap(seg_len, x, buf),
            }

            // write size
            let len = (buf.len() - start_len) as u64;
            buf.as_mut_slice()[len_offset..len_offset + 8].copy_from_slice(&len.to_be_bytes()[..]);
        }
    }

    fn get_header(data: &[u8]) -> Result<(Header, usize), Error> {
        let mut off = 0;
        if &data[off..MAGIC.len()] != &MAGIC[..] {
            return Err(Error::UnrecognizedMagic);
        }
        off += MAGIC.len();

        let nvars = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;

        let len = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;

        let mut codes = Vec::new();
        for _ in 0..nvars {
            codes.push(u64::from_be_bytes(data[off..off + 8].try_into().unwrap()));
            off += 8;
        }

        Ok((Header { codes, len }, off))
    }

    pub fn decompress(data: &[u8], buf: &mut DecompressBuffer) -> Result<usize, Error> {
        buf.clear();
        let (header, mut off) = Self::get_header(data)?;

        buf.len += header.len as usize;
        if buf.nvars == 0 {
            buf.set_nvars(header.codes.len());
        }

        // decompress timestamps
        let sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;

        //println!("DECOMP TIMESTAMPS AT: {}", off);
        timestamps::decompress(&data[off..off + sz], &mut buf.ts);
        off += sz;

        // decompress values
        for (i, code) in header.codes.iter().enumerate() {
            let sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
            off += 8;
            //println!("DECOMP VAR {} AT: {}", i, off);
            CompressFn::decompress(*code, &data[off..off + sz], &mut buf.values[i]);
            off += sz;
        }
        Ok(off)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;
    use crate::segment::Buffer;

    #[test]
    fn test_decimal() {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();

        let heaps = vec![false; nvars];
        let mut buf = Buffer::new(heaps.as_slice());

        let mut item = vec![[0u8; 8]; nvars];
        let mut timestamps = [0; 256];
        for (idx, sample) in data[0..256].iter().enumerate() {
            for (i, val) in sample.values.iter().enumerate() {
                item[i] = val.to_be_bytes();
            }
            timestamps[idx] = sample.ts;
            buf.push_item(sample.ts, item.as_slice()).unwrap();
        }
        let segment = buf.to_flush().unwrap();

        let mut compression = Vec::new();
        for _ in 0..nvars {
            compression.push(CompressFn::Decimal(3));
        }
        let compression = Compression::from(compression);

        let mut compressed = vec![0u8; 4096];
        let mut byte_buf = ByteBuffer::new(&mut compressed[..]);
        let mut buf = DecompressBuffer::new();

        //println!("COMPRESSING DECIMAL NVARS {}", nvars);
        compression.compress(&segment, &mut byte_buf);
        //println!("DONE COMPRESSING DECIMAL");
        let len = byte_buf.len();
        drop(byte_buf);

        Compression::decompress(&compressed[..len], &mut buf).unwrap();

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
    fn test_bytes_lz4() {
        use crate::sample::Bytes;
        let data = &*LOG_DATA;

        let heaps = vec![true; 1];
        let mut buf = Buffer::new(heaps.as_slice());

        let mut item = vec![[0u8; 8]; 1];
        let mut timestamps = [0; 256];
        for (idx, sample) in data[0..256].iter().enumerate() {
            timestamps[idx] = idx as u64;
            let ptr = Bytes::from_slice(sample.as_bytes()).into_raw();
            buf.push_item(idx as u64, &[(ptr as u64).to_be_bytes()]).unwrap();
        }
        let segment = buf.to_flush().unwrap();

        let mut compression = Vec::new();
        compression.push(CompressFn::BytesLZ4);
        let compression = Compression::from(compression);

        let mut compressed = vec![0u8; 8192];
        let mut byte_buf = ByteBuffer::new(&mut compressed[..]);
        let mut buf = DecompressBuffer::new();

        compression.compress(&segment, &mut byte_buf);
        let len = byte_buf.len();
        drop(byte_buf);

        Compression::decompress(&compressed[..len], &mut buf).unwrap();

        assert_eq!(&buf.ts[..], &timestamps[..]);
        let exp = &data[0..256];
        let res = buf.variable(0);
        for (r, e) in res.iter().zip(exp.iter()) {
            let ptr = usize::from_be_bytes(*r) as *const u8;
            let bytes = unsafe { Bytes::from_raw(ptr) };
            let s = std::str::from_utf8(bytes.bytes()).unwrap();
            assert_eq!(s, e);
        }
    }
}
