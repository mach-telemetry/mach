//pub mod byte_buffer;
//pub mod bytes;
//pub mod counter;
//pub mod kafka;
//pub mod timer;
pub mod wp_lock;


//pub use crate::utils::q_allocator::{Qrc, QueueAllocator};

//pub mod q;
//
//pub fn overlaps<T>(mint1: T, maxt1: T, mint2: T, maxt2: T) -> bool
//where
//    T: PartialOrd + PartialEq,
//{
//    mint1 <= maxt2 && mint2 <= maxt1
//}
//
//#[cfg(test)]
//mod test {
//    use super::*;
//
//    #[test]
//    fn test_overlaps() {
//        assert!(overlaps(0, 3, 1, 4));
//        assert!(!overlaps(0, 3, 4, 5));
//        assert!(overlaps(0, 1, 1, 5));
//        assert!(overlaps(1, 1, 1, 5));
//        assert!(overlaps(1, 1, 1, 2));
//    }
//}
