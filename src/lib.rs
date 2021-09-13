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

pub use read_set::SeriesReadSet;
pub use tsdb::{Db, SeriesOptions, Writer, WriterMetadata};
