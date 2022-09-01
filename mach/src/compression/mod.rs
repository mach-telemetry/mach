//mod bytes_lz4;
mod bytes_lz42;
mod decimal;
//mod noop;
//mod fixed;
mod bitpack;
mod delta_of_delta;
mod lz4;
mod timestamps;
mod utils;
mod xor;

use crate::segment::*;
use crate::series::FieldType;
use crate::snapshot::Segment;
use crate::timer::*;
use crate::utils::byte_buffer::ByteBuffer;
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
//pub struct ReadBuffer {
//    len: usize,
//    ts: Vec<u64>,
//    data: Vec<Vec<[u8; 8]>>,
//    heap: Vec<Option<Vec<u8>>>,
//    heap_flags: Vec<Types>,
//}

#[derive(Copy, Clone)]
pub enum CompressFn {
    Decimal(u8),
    BytesLZ4,
    NOOP,
    XOR,
    IntBitpack,
    DeltaDelta,
    LZ4,
}

impl CompressFn {
    pub fn compress(&self, segment: &[[u8; 8]], buf: &mut ByteBuffer) {
        match self {
            CompressFn::Decimal(precision) => decimal::compress(segment, buf, *precision),
            CompressFn::XOR => xor::compress(segment, buf),
            CompressFn::IntBitpack => bitpack::compress(segment, buf),
            CompressFn::DeltaDelta => delta_of_delta::compress(segment, buf),
            CompressFn::LZ4 => lz4::compress(segment, buf),
            _ => unimplemented!(),
        };
    }

    pub fn compress_heap(
        &self,
        len: usize,
        indexes: &[[u8; 8]],
        heap: &[u8],
        buf: &mut ByteBuffer,
    ) {
        match self {
            CompressFn::BytesLZ4 => bytes_lz42::compress(len, indexes, heap, buf),
            CompressFn::NOOP => unimplemented!(),
            _ => unimplemented!(),
        };
    }

    pub fn decompress(&self, data: &[u8], col: &mut Vec<[u8; 8]>, heap: &mut Option<Vec<u8>>) {
        match self {
            CompressFn::XOR => xor::decompress(data, col),
            CompressFn::Decimal(_) => decimal::decompress(data, col),
            CompressFn::IntBitpack => bitpack::decompress(data, col),
            CompressFn::DeltaDelta => delta_of_delta::decompress(data, col),
            CompressFn::LZ4 => lz4::decompress(data, col),
            CompressFn::BytesLZ4 => bytes_lz42::decompress(data, col, heap.as_mut().unwrap()),
            CompressFn::NOOP => unimplemented!(),
        }
    }

    fn id(&self) -> u64 {
        match self {
            CompressFn::XOR => 2,
            CompressFn::Decimal(_) => 3,
            CompressFn::BytesLZ4 => 4,
            CompressFn::NOOP => 5,
            CompressFn::IntBitpack => 6,
            CompressFn::DeltaDelta => 7,
            CompressFn::LZ4 => 8,
        }
    }

    fn from_id(id: u64) -> Self {
        match id {
            2 => CompressFn::XOR,
            3 => CompressFn::Decimal(u8::MAX),
            4 => CompressFn::BytesLZ4,
            5 => CompressFn::NOOP,
            6 => CompressFn::IntBitpack,
            7 => CompressFn::DeltaDelta,
            8 => CompressFn::LZ4,
            _ => unimplemented!(),
        }
    }
}

struct Header {
    id: usize,
    types: Vec<FieldType>,
    codes: Vec<u64>,
    len: usize,
}

//impl Header {
//    fn new() -> Self {
//        Self {
//            types: Vec::new(),
//            codes: Vec::new(),
//            len: 0,
//        }
//    }
//}

#[derive(Clone)]
pub struct Compression(Arc<Vec<CompressFn>>);

impl std::ops::Deref for Compression {
    type Target = [CompressFn];
    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
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
        buf.extend_from_slice(&segment.id().to_be_bytes()[..]); // n variables
        buf.extend_from_slice(&segment.nvars().to_be_bytes()[..]); // n variables
        buf.extend_from_slice(&segment.len().to_be_bytes()[..]); // number of samples
        for t in segment.types() {
            buf.push(t.to_u8());
        }

        for c in self.0.iter() {
            buf.extend_from_slice(&c.id().to_be_bytes()[..]);
        }
    }

    pub fn compress(&self, segment: &FullSegment, buf: &mut ByteBuffer) {
        //println!("Compressing Segment Id: {}", segment.id());
        self.set_header(segment, buf);
        //println!("after header {:?}", &buf.as_mut_slice()[0..12]);
        //println!("len after header {}", buf.len());

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
        //println!("timestamps compressed len {} {:?}", len, len.to_be_bytes());
        buf.as_mut_slice()[len_offset..len_offset + 8].copy_from_slice(&len.to_be_bytes()[..]);
        //println!("after timetsamps {:?}", &buf.as_mut_slice()[0..12]);
        //buf.add_len(8);

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
            //println!("before variable {:?}", &buf.as_mut_slice()[0..12]);
            match variable {
                Variable::Var(x) => compression.compress(x, buf),
                Variable::Heap { indexes, bytes } => {
                    compression.compress_heap(seg_len, indexes, bytes, buf)
                }
            }
            //println!("after variable {:?}", &buf.as_mut_slice()[0..12]);

            // write size
            let len = (buf.len() - start_len) as u64;
            buf.as_mut_slice()[len_offset..len_offset + 8].copy_from_slice(&len.to_be_bytes()[..]);
            //buf.add_len(8);
        }
        //println!("after everything {:?}", &buf.as_mut_slice()[0..12]);
    }

    fn get_header(data: &[u8]) -> Result<(Header, usize), Error> {
        let mut off = 0;
        if data[off..MAGIC.len()] != MAGIC[..] {
            //println!("{:?} {:?}", &data[off..MAGIC.len()], MAGIC);
            //println!("{:?} {:?}", u64::from_be_bytes(data[0..8].try_into().unwrap()), u64::from_be_bytes(data[8..16].try_into().unwrap()));
            return Err(Error::UnrecognizedMagic);
        }
        off += MAGIC.len();

        let id = usize::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;

        let nvars = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;

        let len = u64::from_be_bytes(data[off..off + 8].try_into().unwrap()) as usize;
        off += 8;

        let mut types = Vec::new();
        for _ in 0..nvars {
            types.push(FieldType::from_u8(data[off]));
            off += 1;
        }

        let mut codes = Vec::new();
        for _ in 0..nvars {
            codes.push(u64::from_be_bytes(data[off..off + 8].try_into().unwrap()));
            off += 8;
        }

        Ok((
            Header {
                id,
                types,
                codes,
                len,
            },
            off,
        ))
    }

    pub fn decompress(data: &[u8], buf: &mut Segment) -> Result<usize, Error> {
        let _timer_1 = ThreadLocalTimer::new("Compression::decompress");
        buf.clear();
        let (header, mut off) = Self::get_header(data)?;

        buf.reset_types(header.types.as_slice());

        buf.segment_id = header.id;
        buf.len = header.len as usize;

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
            let d = &data[off..off + sz];
            let h = &mut buf.heap[i];
            let v = &mut buf.data[i];
            let code = CompressFn::from_id(*code);
            code.decompress(d, v, h);
            off += sz;
        }
        Ok(off)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::sample::SampleType;
    use crate::segment::Buffer;
    use crate::series::FieldType;
    use crate::snapshot::Segment;
    use crate::test_utils::*;

    type DecompressBuffer = Segment;

    #[test]
    fn test_decimal() {
        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();

        let heaps = vec![FieldType::F64; nvars];
        let mut buf = Buffer::new(heaps.as_slice());

        let _item = vec![[0u8; 8]; nvars];
        let mut timestamps = [0; 256];
        for (idx, sample) in data[0..256].iter().enumerate() {
            let mut item = Vec::new();
            for (_i, val) in sample.values.iter().enumerate() {
                item.push(SampleType::F64(*val));
                //item[i] = val.to_be_bytes();
            }
            timestamps[idx] = sample.ts;
            buf.push_type(sample.ts, item.as_slice()).unwrap();
        }
        let segment = buf.to_flush().unwrap();

        let mut compression = Vec::new();
        for _ in 0..nvars {
            compression.push(CompressFn::Decimal(3));
        }
        let compression = Compression::from(compression);

        let mut compressed = vec![0u8; 4096];
        let mut byte_buf = ByteBuffer::new(&mut compressed[..]);
        let mut buf = DecompressBuffer::new_empty();

        //println!("COMPRESSING DECIMAL NVARS {}", nvars);
        compression.compress(&segment, &mut byte_buf);
        //println!("DONE COMPRESSING DECIMAL");
        let len = byte_buf.len();
        drop(byte_buf);

        Compression::decompress(&compressed[..len], &mut buf).unwrap();

        assert_eq!(&buf.ts[..], &timestamps[..]);
        assert_eq!(buf.types.as_slice(), heaps.as_slice());
        for i in 0..nvars {
            let exp = segment.variable(i);
            let res = buf.field(i);
            let diff = exp
                .iter()
                .zip(res.iter())
                .map(|(x, y)| (f64::from_be_bytes(*x) - f64::from_be_bytes(*y)).abs())
                .fold(f64::NAN, f64::max);

            assert!(diff < 0.001);
        }
    }
}
