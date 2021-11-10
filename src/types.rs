use std::{
    collections::HashMap,
    convert::{TryFrom},
    str
};
use serde::*;

pub trait MachType {
    fn bytes(self) -> [u8; 8];
}

impl MachType for f64 {
    fn bytes(self) -> [u8; 8] {
        self.to_be_bytes()
    }
}

impl MachType for u64 {
    fn bytes(self) -> [u8; 8] {
        self.to_be_bytes()
    }
}

impl MachType for String {
    fn bytes(self) -> [u8; 8] {
        self.into_bytes().into_boxed_slice().bytes()
    }
}

impl MachType for Box<[u8]> {
    fn bytes(self) -> [u8; 8] {
        let ptr = Box::into_raw(self) as * const () as usize;
        ptr.to_be_bytes()
    }
}

pub struct Sample {
    pub data: Vec<[u8; 8]>,
}

#[derive(Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Type {
    F64,
    U64,
    STR,
}

use Type::*;

pub struct Schema {
    inner: Vec<Type>
}

#[macro_export]
macro_rules! sample {
    ( $( $x:expr ),* ) => {
        {
            let mut tmp: Vec<[u8; 8]> = Vec::new();
            $(
                tmp.push(x.bytes());
            )*
            Sample {
                data: tmp
            }
        }
    };
}

#[macro_export]
macro_rules! schema {
    ( $( $x:expr ),* ) => {
        {
            let mut tmp: Vec<Type> = Vec::new();
            $(
                tmp.push($x);
            )*
        }
    }
}
