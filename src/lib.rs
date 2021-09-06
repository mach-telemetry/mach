#![feature(get_mut_unchecked)]
#![feature(is_sorted)]

mod block;
mod utils;
mod tsdb;

pub use tsdb::Db;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
