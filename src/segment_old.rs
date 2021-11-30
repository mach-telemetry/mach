
pub trait SegmentLike {
    fn timestamps(&self) -> &[u64];
    fn variable(&self, id: usize) -> &[f64];
    fn value(&self, varid: usize, idx: usize) -> f64;
    fn row(&self, idx: usize) -> &[f64];
    fn nvars(&self) -> usize;
    fn len(&self) -> usize {
        self.timestamps().len()
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait SegmentIterator {
    fn set_range(&mut self, mint: u64, maxt: u64);
    fn next_segment(&mut self) -> Option<Segment>;
}

pub struct Segment {
    pub timestamps: Vec<u64>,
    pub values: Vec<f64>,
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

impl Default for Segment {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentLike for Segment {
    fn timestamps(&self) -> &[u64] {
        self.timestamps.as_slice()
    }

    fn variable(&self, id: usize) -> &[f64] {
        let start = self.len() * id;
        let end = start + self.len();
        &self.values[start..end]
    }

    fn value(&self, varid: usize, idx: usize) -> f64 {
        self.variable(varid)[idx]
    }

    fn row(&self, _idx: usize) -> &[f64] {
        unimplemented!()
    }

    fn nvars(&self) -> usize {
        self.values.len() / self.len()
    }

    fn len(&self) -> usize {
        self.timestamps().len()
    }
}
