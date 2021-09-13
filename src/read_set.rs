use crate::{
    active_block::ActiveBlockReader,
    active_segment::ActiveSegmentReader,
    block::BlockReader,
    segment::{Segment, SegmentIterator},
    tsdb::{Dt, Fl, SeriesOptions},
};

pub struct ReadSegment {
    timestamps: Vec<Dt>,
    values: Vec<Fl>,
}

pub struct SeriesReadSet<R: BlockReader> {
    opts: SeriesOptions,
    active_block: ActiveBlockReader,
    active_segment: ActiveSegmentReader,
    blocks: R,
}

impl<R: BlockReader> SeriesReadSet<R> {
    pub fn new(
        opts: SeriesOptions,
        active_segment: ActiveSegmentReader,
        active_block: ActiveBlockReader,
        blocks: R,
    ) -> Self {
        SeriesReadSet {
            opts: opts,
            active_block: active_block,
            active_segment,
            blocks: blocks,
        }
    }

    pub fn set_range(&mut self, mint: Dt, maxt: Dt) {
        self.active_segment.set_range(mint, maxt);
        self.active_block.set_range(mint, maxt);
        self.blocks.set_range(mint, maxt);
    }

    pub fn next_segment(&mut self) -> Option<Segment> {
        None
    }
}
