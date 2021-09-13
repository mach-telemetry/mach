use crate::{
    active_block::{ActiveBlockReader, MemBlock},
    active_segment::ActiveSegmentReader,
    block::BlockReader,
    segment::{Segment, SegmentIterator},
    tsdb::Dt,
};

pub struct SeriesReadSet<R: BlockReader> {
    active_block: ActiveBlockReader,
    active_segment: ActiveSegmentReader,
    blocks: R,
    block_buf: Option<MemBlock>,
    stage: usize,
}

impl<R: BlockReader> SeriesReadSet<R> {
    pub fn new(
        active_segment: ActiveSegmentReader,
        active_block: ActiveBlockReader,
        blocks: R,
    ) -> Self {
        SeriesReadSet {
            active_block: active_block,
            active_segment,
            blocks: blocks,
            block_buf: None,
            stage: 0,
        }
    }

    pub fn set_range(&mut self, mint: Dt, maxt: Dt) {
        self.active_segment.set_range(mint, maxt);
        self.active_block.set_range(mint, maxt);
        self.blocks.set_range(mint, maxt);
    }

    pub fn next_segment(&mut self) -> Option<Segment> {
        if self.stage == 0 {
            let res = self.segment_from_blocks();
            if res.is_none() {
                self.stage = 1;
            } else {
                return res;
            }
        }

        if self.stage == 1 {
            let res = self.active_block.next_segment();
            if res.is_none() {
                self.stage = 2;
            } else {
                return res;
            }
        }

        self.active_segment.next_segment()
    }

    fn segment_from_blocks(&mut self) -> Option<Segment> {
        if self.block_buf.is_none() {
            let mut memblock = MemBlock::new();
            self.blocks.next_block(&mut *memblock)?;
            memblock.load_index();
            self.block_buf = Some(memblock);
        }

        let block_buf = self.block_buf.as_mut().unwrap();
        match block_buf.next_segment() {
            None => {
                self.blocks.next_block(&mut *block_buf)?;
                self.segment_from_blocks()
            }
            Some(segment) => Some(segment),
        }
    }

    //pub fn next_segment(&mut self) -> Option<Segment> {
    //    match self.phase {
    //        0 => {
    //        }
    //    }
    //}
}
