#![feature(box_syntax)]
#![feature(slice_as_chunks)]


pub mod sample;
pub mod field_type;
pub mod segment;
pub mod id;
pub mod constants;
pub mod source;
pub mod active_segment;
pub mod compression;
pub mod mem_list;
mod byte_buffer;
mod kafka;
//pub mod utils;