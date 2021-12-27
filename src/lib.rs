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
#![allow(warnings)]

//mod backend;
//mod chunk;
//mod flush_buffer;
//mod series_metadata;
//mod writer;

mod compression;
mod persistent_list;
mod segment;
mod tags;
mod utils;

#[cfg(test)]
mod test_utils;
