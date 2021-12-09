pub mod fs;

pub struct ByteEntry<'a> {
    pub mint: u64,
    pub maxt: u64,
    pub bytes: &'a [u8],
}

impl<'a> ByteEntry<'a> {
    pub fn new(mint: u64, maxt: u64, bytes: &'a [u8]) -> Self {
        Self { mint, maxt, bytes }
    }
}




//pub enum PushMetadata {
//    FS({
//        offset: u64,
//        file_id: u64,
//        ts_id: u64,
//    }),
//}
