use std::collections::HashSet;
use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub enum Error {
    Serialize,
    Deserialize,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Tags(HashSet<(String, String)>);

impl Tags {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn serialize_into(&self, data: &mut Vec<u8>) {
        bincode::serialize_into(data, self).unwrap()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, Error> {
        match bincode::deserialize(data) {
            Ok(x) => Ok(x),
            Err(_) => Err(Error::Deserialize)
        }
    }
}

impl std::ops::Deref for Tags {
    type Target = HashSet<(String, String)>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Tags {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Tags {
    pub fn new() -> Self {
        Tags(HashSet::new())
    }
}


