use std::collections::HashSet;
use serde::{Serialize, Deserialize};
#[derive(Serialize, Deserialize)]
pub struct Tags(HashSet<(String, String)>);

impl Tags {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn serialize_into(&self, data: &mut Vec<u8>) {
        bincode::serialize_into(data, self).unwrap()
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


