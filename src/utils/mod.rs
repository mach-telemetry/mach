mod q_allocator;
mod list;

pub use crate::utils::q_allocator::{QueueAllocator, Qrc};

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
