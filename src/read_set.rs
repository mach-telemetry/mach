use crate::{
    active_block::ActiveBlockReader, active_segment::ActiveSegmentReader, block::BlockReader,
    tsdb::SeriesOptions,
};

pub struct SeriesReadSet<R: BlockReader> {
    _opts: SeriesOptions,
    _active_block: ActiveBlockReader,
    _active_segment: ActiveSegmentReader,
    _blocks: R,
}

impl<R: BlockReader> SeriesReadSet<R> {
    pub fn new(
        opts: SeriesOptions,
        active_segment: ActiveSegmentReader,
        active_block: ActiveBlockReader,
        blocks: R,
    ) -> Self {
        SeriesReadSet {
            _opts: opts,
            _active_block: active_block,
            _active_segment: active_segment,
            _blocks: blocks,
        }
    }
}
