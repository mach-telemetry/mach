pub use crate::series::FieldType;
pub use crate::utils::bytes::*;
pub use serde::*;
use std::convert::TryInto;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum SampleType {
    I64(i64),
    U64(u64),
    Timestamp(u64),
    F64(f64),
    Bytes(Vec<u8>),
}

impl SampleType {
    pub fn as_i64(&self) -> i64 {
        match self {
            SampleType::I64(x) => *x,
            _ => unimplemented!(),
        }
    }

    pub fn as_u64(&self) -> u64 {
        match self {
            SampleType::U64(x) => *x,
            _ => unimplemented!(),
        }
    }

    pub fn as_f64(&self) -> f64 {
        match self {
            SampleType::F64(x) => *x,
            _ => unimplemented!(),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            SampleType::Bytes(x) => std::str::from_utf8(&x[..]).unwrap(),
            _ => unimplemented!(),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            SampleType::Bytes(x) => x.as_slice(),
            _ => unimplemented!(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            SampleType::Bytes(x) => x.as_slice().len(),
            SampleType::F64(_) => 8,
            SampleType::I64(_) => 8,
            SampleType::U64(_) => 8,
            SampleType::Timestamp(_) => 8,
        }
    }

    pub fn from_field_item(field_type: FieldType, bytes: [u8; 8], heap: Option<&[u8]>) -> Self {
        match field_type {
            FieldType::I64 => SampleType::I64(i64::from_be_bytes(bytes)),
            FieldType::U64 => SampleType::U64(u64::from_be_bytes(bytes)),
            FieldType::F64 => SampleType::F64(f64::from_be_bytes(bytes)),
            FieldType::Timestamp => SampleType::Timestamp(u64::from_be_bytes(bytes)),
            FieldType::Bytes => {
                let heap = heap.as_ref().unwrap();
                let idx = usize::from_be_bytes(bytes);
                let sz = usize::from_be_bytes(heap[idx..idx + 8].try_into().unwrap());
                let bytes: Vec<u8> = heap[idx + 8..idx + 8 + sz].into();
                SampleType::Bytes(bytes)
            }
        }
    }
}

//#[derive(PartialEq, Eq, Copy, Clone, Serialize, Deserialize, Debug)]
//pub enum FieldType {
//    I64 = 0,
//    U64 = 1,
//    F64 = 2,
//    Bytes = 3,
//    Timestamp = 4,
//    U32 = 5,
//}

//pub struct HeapAlloc(*const u8);
//
//impl HeapAlloc {
//    pub fn len(&self) -> usize {
//        let slice: &[u8] = unsafe { std::slice::from_raw_parts(self.0, 8) };
//        usize::from_be_bytes(slice.try_into().unwrap())
//    }
//
//    pub fn as_bytes(&self) -> &[u8] {
//        let sz = self.len();
//        unsafe {
//            let ptr = self.0.offset(8);
//            std::slice::from_raw_parts(ptr, sz)
//        }
//    }
//
//    pub fn as_str(&self) -> &str {
//        std::str::from_utf8(self.as_bytes()).unwrap()
//    }
//
//    pub fn from_slice(data: &[u8]) -> Self {
//        let usz = size_of::<usize>();
//        let len = data.len();
//        let total_len = usz + len;
//        let layout = Layout::from_size_align(total_len, align_of::<u8>()).unwrap();
//        let ptr = unsafe { alloc(layout) };
//        let sl = unsafe { std::slice::from_raw_parts_mut(ptr, total_len) };
//        sl[..usz].copy_from_slice(&len.to_be_bytes());
//        sl[usz..].copy_from_slice(data);
//        Self(ptr)
//    }
//}

//#[derive(Copy, Clone)]
//pub struct Sample<const V: usize> {
//    pub timestamp: u64,
//    pub values: [[u8; 8]; V],
//}
//
//impl<const V: usize> Sample<V> {
//    pub fn new(timestamp: u64, values: [[u8; 8]; V]) -> Self {
//        Sample { timestamp, values }
//    }
//
//    pub fn from_f64(timestamp: u64, data: [f64; V]) -> Self {
//        let mut values = [[0; 8]; V];
//        for i in 0..V {
//            values[i] = data[i].to_be_bytes();
//        }
//        Sample { timestamp, values }
//    }
//
//    pub fn from_u64(timestamp: u64, data: [u64; V]) -> Self {
//        let mut values = [[0; 8]; V];
//        for i in 0..V {
//            values[i] = data[i].to_be_bytes();
//        }
//        Sample { timestamp, values }
//    }
//
//    pub fn from_bytes(timestamp: u64, data: [Bytes; V]) -> Self {
//        let mut values = [[0; 8]; V];
//        let mut counter = 0;
//        #[allow(clippy::explicit_counter_loop)]
//        for item in data {
//            values[counter] = item.into_sample_entry();
//            counter += 1;
//        }
//        Sample { timestamp, values }
//    }
//}
