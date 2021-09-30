#![deny(unused_must_use)]
#![feature(get_mut_unchecked)]
#![feature(is_sorted)]

mod active_block;
mod active_segment;
mod block;
mod compression;
mod read_set;
mod segment;
mod tsdb;
mod utils;

pub use block::file::{FileBlockLoader, FileStore, ThreadFileWriter};
pub use read_set::SeriesReadSet;
pub use segment::SegmentLike;
pub use tsdb::{
    Db, RefId, Sample, SeriesId, SeriesMetadata, SeriesOptions, Writer, WriterMetadata,
};

#[cfg(test)]
mod test_utils;
