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

pub mod tsdb;
//pub mod compression;
pub mod compression2;
pub mod constants;
pub mod id;
pub mod metadata;
pub mod persistent_list;
pub mod sample;
pub mod segment;
pub mod series;
pub mod tags;
pub mod utils;
pub mod writer;
//pub mod into;

#[cfg(test)]
mod test_utils;
