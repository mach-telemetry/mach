#![deny(unused_must_use)]
#![feature(get_mut_unchecked)]
#![feature(is_sorted)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
#![feature(cell_update)]
#![feature(box_syntax)]
#![feature(thread_id_value)]
#![allow(clippy::new_without_default)]
#![allow(clippy::len_without_is_empty)]

//mod backend;
//mod chunk;
//mod flush_buffer;
//mod series_metadata;
//mod writer;

pub mod compression;
pub mod constants;
pub mod persistent_list;
pub mod segment;
pub mod tags;
pub mod utils;
pub mod write_thread;

#[cfg(test)]
mod test_utils;
