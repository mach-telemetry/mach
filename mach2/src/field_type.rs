use serde::{Serialize, Deserialize};
use std::convert::From;

#[derive(PartialEq, Eq, Copy, Clone, Serialize, Deserialize, Debug)]
pub enum FieldType {
    I64 = 0,
    U64 = 1,
    F64 = 2,
    Bytes = 3,
    Timestamp = 4,
}

#[derive(Debug)]
struct Number {
    value: i32,
}

impl From<u8> for FieldType {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::I64,
            1 => Self::U64,
            2 => Self::F64,
            3 => Self::Bytes,
            4 => Self::Timestamp,
            _ => unreachable!(),
        }
    }
}
