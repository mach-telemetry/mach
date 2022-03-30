#![deny(unused_must_use)]
#![feature(get_mut_unchecked)]
#![feature(is_sorted)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
#![feature(cell_update)]
#![feature(box_syntax)]
#![feature(thread_id_value)]
#![feature(trait_alias)]
#![allow(clippy::new_without_default)]
#![allow(clippy::len_without_is_empty)]
#![allow(warnings)]

//mod backend;
//mod chunk;
//mod flush_buffer;
//mod series_metadata;
//mod writer;
//pub mod compression;
//pub mod metadata;
//pub mod replication;
//pub mod into;

pub mod compression;
pub mod constants;
pub mod id;
//pub mod persistent_list;
pub mod reader;
pub mod runtime;
pub mod sample;
pub mod segment;
pub mod series;
pub mod tags;
pub mod tsdb;
pub mod utils;
//pub mod writer;
//pub mod wal;
pub mod durability;

// new shit
pub mod active_block;
pub mod durable_queue;
pub mod persistent_list;
pub mod writer;
pub mod snapshot;
//pub mod segment2;

#[cfg(test)]
mod test_utils;
