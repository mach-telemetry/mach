use std::ops::Deref;

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct WriterId(pub usize);

impl WriterId {
    pub fn inner(&self) -> usize {
        self.0
    }
}

impl Deref for WriterId {
    type Target = usize;
    fn deref(&self) -> &usize {
        &self.0
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct SeriesId(pub usize);

impl Deref for SeriesId {
    type Target = usize;
    fn deref(&self) -> &usize {
        &self.0
    }
}

impl SeriesId {
    pub fn inner(&self) -> usize {
        self.0
    }
}


