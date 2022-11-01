#![feature(box_syntax)]
#![feature(slice_as_chunks)]
#![feature(maybe_uninit_slice)]
#![feature(maybe_uninit_uninit_array)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::new_without_default)]

pub mod active_block;
pub mod active_segment;
pub mod byte_buffer;
pub mod compression;
pub mod constants;
pub mod field_type;
pub mod id;
pub mod kafka;
pub mod mem_list;
pub mod sample;
pub mod segment;
pub mod source;
#[cfg(test)]
pub mod test_utils;
pub mod utils;
pub mod writer;
pub mod tsdb;
