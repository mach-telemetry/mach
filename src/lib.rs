#![feature(get_mut_unchecked)]
#![feature(is_sorted)]

mod block;
mod utils;
mod tsdb;
mod active_segment;
mod active_block;
//pub mod series;

//pub use series::series::{Series, SeriesWriter, SeriesReader};
pub use tsdb::{SeriesOptions, Db, Writer};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
