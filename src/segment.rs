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

pub trait SegmentIterator {
    fn set_range(&mut self, mint: Dt, maxt: Dt);
    fn next_segment(&mut self) -> Option<Segment>;
}

pub struct Segment {
    pub timestamps: Vec<Dt>,
    pub values: Vec<Fl>,
    pub len: usize,
}

impl Segment {
    pub fn new() -> Self {
        Self {
            timestamps: Vec::new(),
            values: Vec::new(),
            len: 0,
        }
    }
}

impl SegmentLike for Segment {
    fn timestamps(&self) -> &[Dt] {
        self.timestamps.as_slice()
    }

    fn variable(&self, id: usize) -> &[Fl] {
        let start = self.len() * id;
        let end = start + self.len();
        &self.values[start..end]
    }

    fn value(&self, varid: usize, idx: usize) -> Fl {
        self.variable(varid)[idx]
    }

    fn row(&self, idx: usize) -> &[Fl] {
        unimplemented!()
    }

    fn nvars(&self) -> usize {
        self.values.len() / self.len()
    }

    fn len(&self) -> usize {
        self.timestamps().len()
    }
}
