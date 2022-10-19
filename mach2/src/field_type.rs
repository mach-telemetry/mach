use serde::{Serialize, Deserialize};

#[derive(PartialEq, Eq, Copy, Clone, Serialize, Deserialize, Debug)]
pub enum FieldType {
    I64 = 0,
    U64 = 1,
    F64 = 2,
    Bytes = 3,
    Timestamp = 4,
}

impl FieldType {
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::I64,
            1 => Self::U64,
            2 => Self::F64,
            3 => Self::Bytes,
            4 => Self::Timestamp,
            //5 => Self::U32,
            _ => unimplemented!(),
        }
    }
}
