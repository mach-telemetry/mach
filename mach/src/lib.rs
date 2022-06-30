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

#![deny(unused_imports)]
#![deny(unreachable_patterns)]
#![deny(unused_variables)]
#![deny(unused_mut)]
#![deny(dead_code)]
#![deny(non_upper_case_globals)]

pub mod compression;
pub mod constants;
pub mod id;
pub mod sample;
pub mod segment;
pub mod series;
pub mod tsdb;
pub mod utils;
pub mod mem_list;
pub mod snapshot;
pub mod writer;

#[cfg(test)]
mod test_utils;
