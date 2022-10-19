
pub struct ReadOnlyBlock {
    buf: Vec<u8>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum InnerReadOnlyBlock {
    Bytes(Box<[u8]>),
    Offset(usize, usize),
}

impl std::convert::From<InnerBlockListEntry> for ReadOnlyBlock {
    fn from(item: InnerBlockListEntry) -> Self {
        match item {
            InnerBlockListEntry::Bytes(x) => ReadOnlyBlock::Bytes(x[..].into()),
            InnerBlockListEntry::Offset(x, y) => ReadOnlyBlock::Offset(x, y),
        }
    }
}

impl ReadOnlyBlock {
    pub fn into_bytes(self) -> ReadOnlyBlock {
    }
}


