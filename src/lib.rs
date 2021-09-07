#![feature(get_mut_unchecked)]
#![feature(is_sorted)]

mod active_block;
mod active_segment;
mod block;
mod read_set;
mod tsdb;
mod utils;
//pub mod series;

//pub use series::series::{Series, SeriesWriter, SeriesReader};
pub use read_set::SeriesReadSet;
pub use tsdb::{Db, SeriesOptions, WriterMetadata, Writer};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
