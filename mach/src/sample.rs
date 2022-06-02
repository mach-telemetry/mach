pub use crate::utils::bytes::*;
pub use serde::*;

#[derive(Clone, Serialize, Deserialize)]
pub enum Type {
    I64(i64),
    U64(u64),
    Timestamp(u64),
    F64(f64),
    Bytes(Vec<u8>),
    BorrowedBytes(Vec<u8>),
    U32(u32),
}

impl Type {
    pub fn byte_vec_mut(&mut self) -> &mut Vec<u8> {
        match self {
            Type::Bytes(x) => x,
            _ => unimplemented!(),
        }
    }
}

unsafe impl Sync for Type {}
unsafe impl Send for Type {}

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
