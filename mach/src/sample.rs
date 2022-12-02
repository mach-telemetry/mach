pub use crate::field_type::FieldType;
//pub use crate::utils::bytes::*;
use serde::{Deserialize, Serialize};
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
