use crate::id::SeriesId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::From;
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub enum Error {
    Serialize,
    Deserialize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct Tags {
    data: Vec<(String, String)>,
    hash: u64,
}

//impl Tags {
//    pub fn serialize(&self) -> Vec<u8> {
//        bincode::serialize(self).unwrap()
//    }
//
//    pub fn serialize_into(&self, data: &mut ByteBuffer) {
//        bincode::serialize_into(data, self).unwrap();
//    }
//
//    pub fn from_bytes(data: &[u8]) -> Result<Self, Error> {
//        match bincode::deserialize(data) {
//            Ok(x) => Ok(x),
//            Err(_) => Err(Error::Deserialize),
//        }
//    }
//}

//impl std::ops::Deref for Tags {
//    type Target = HashSet<(String, String)>;
//    fn deref(&self) -> &Self::Target {
//        &self.0
//    }
//}
//
//impl std::ops::DerefMut for Tags {
//    fn deref_mut(&mut self) -> &mut Self::Target {
//        &mut self.0
//    }
//}

impl From<HashMap<String, String>> for Tags {
    fn from(map: HashMap<String, String>) -> Self {
        Self::from_map(map)
    }
}

impl From<Tags> for HashMap<String,String> {
    fn from(mut tags: Tags) -> Self {
        tags.data.drain(..).collect()
    }
}

impl Tags {
    pub fn data(&self) -> &[(String, String)] {
        self.data.as_slice()
    }

    pub fn id(&self) -> SeriesId {
        SeriesId(self.hash)
    }

    fn from_map(mut data: HashMap<String, String>) -> Self {
        let mut data: Vec<(String, String)> = data.drain().collect();
        data.sort();
        let mut hasher = rustc_hash::FxHasher::default();
        data.hash(&mut hasher);
        let hash = hasher.finish();
        Tags { data, hash }
    }
    //pub fn new() -> Self {
    //    Tags(HashSet::new())
    //}
}
