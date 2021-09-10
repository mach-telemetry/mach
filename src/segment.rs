use crate::tsdb::{Dt, Fl};

pub trait SegmentLike {
    fn timestamps(&self) -> &[Dt];
    fn variable(&self, id: usize) -> &[Fl];
    fn value(&self, varid: usize, idx: usize) -> Fl;
    fn row(&self, idx: usize) -> &[Fl];
    fn nvars(&self) -> usize;
    fn len(&self) -> usize {
        self.timestamps().len()
    }
}

pub trait SegmentIterator<S: SegmentLike> {
    fn next_segment(&mut self, mint: Dt, maxt: Dt) -> Option<S>;
    fn reset(&mut self);
}
