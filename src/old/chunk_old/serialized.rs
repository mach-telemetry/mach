use crate::chunk::{inner::MAGIC, Error};

struct InnerSerializedEntry {
    start: usize,
    end: usize,
    mint: u64,
    maxt: u64,
}

pub struct SerializedChunk<'buffer> {
    bytes: &'buffer[u8],
    entries: Vec<InnerSerializedEntry>
}

impl<'buffer> SerializedChunk<'buffer> {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
    // > magic
    // magic: [0..10]
    //
    // len: [10..18]
    // tsid: [18..26]
    // mint: [26..34]
    // maxt: [34..42]
    // segment count: [42..50]
    //
    // > Segment metadata information
    // segment offset: [40..48]
    // segment mint: [48..56]
    // segment maxt: [56..64]
    //
    // > Segment data information
    // segments: [header bytes + segment count * 8 * 3 ..]

        let mut off = 0;

        // check magic
        let end = off + MAGIC.len();
        if &bytes[off..end] != MAGIC {
            return Err(Error::InvalidMagic);
        }
        off = end;

        // get length
        let len = u64::from_be_bytes(&bytes[off..off+8].try_into().unwrap());
        off += 8;

        // tsid
        let len = u64::from_be_bytes(&bytes[off..off+8].try_into().unwrap());
        off += 8;

    }
}


