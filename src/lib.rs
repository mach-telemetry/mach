#![deny(unused_must_use)]
#![feature(get_mut_unchecked)]
#![feature(is_sorted)]
#![feature(maybe_uninit_uninit_array)]
#![feature(cell_update)]
#![feature(box_syntax)]

#![allow(warnings)]

//mod active_block;
//mod active_segment;
mod types;
//mod block;
//mod compression;
//mod read_set;
//mod segment;
//mod tsdb;
//mod utils;
//mod tsdb2;
//mod ids;
mod managed;
mod buffer;
mod active_buffer;

//pub use block::file::{FileBlockLoader, FileStore, ThreadFileWriter};
//pub use read_set::SeriesReadSet;
//pub use segment::SegmentLike;
//pub use tsdb::{
//    Db, RefId, Sample, SeriesId, SeriesMetadata, SeriesOptions, Writer, WriterMetadata,
//};

#[cfg(test)]
mod test_utils;
