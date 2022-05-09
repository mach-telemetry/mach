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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Tags {
    data: HashMap<String, String>,
    hash_id: u64,
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
        tags.data
        //tags.data.drain().collect()
    }
}

impl Tags {
    pub fn data(&self) -> Vec<(String, String)> {
        self.data.iter().map(|x| (x.0.clone(), x.1.clone())).collect()
    }

    pub fn id(&self) -> SeriesId {
        SeriesId(self.hash_id)
    }

    fn from_map(mut data: HashMap<String, String>) -> Self {
        //let mut hash = 0;
        //for item in data.iter() {
        //    let mut hasher = rustc_hash::FxHasher::default();
        //    item.hash(&mut hasher);
        //    hash ^= hasher.finish();
        //}
        let mut hasher = rustc_hash::FxHasher::default();
        let mut v: Vec<(String, String)> = data.drain().collect();
        v.sort();
        v.hash(&mut hasher);
        let hash_id = hasher.finish();
        Tags { data, hash_id }
    }
    //pub fn new() -> Self {
    //    Tags(HashSet::new())
    //}
}

impl Hash for Tags {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.hash_id.hash(hasher)
    }

}
