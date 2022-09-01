use crate::{
    id::SeriesId,
    mem_list::{ReadOnlyBlock, ReadOnlyBlockBytes, SourceBlocks},
    sample::SampleType,
    series::FieldType,
    utils::kafka,
    timer::*,
};
use std::convert::TryInto;

pub type Heap = Vec<Option<Vec<u8>>>;

pub struct Field<'a> {
    t: FieldType,
    v: &'a [[u8; 8]],
    h: Option<&'a [u8]>,
}

impl<'a> std::ops::Deref for Field<'a> {
    type Target = [[u8; 8]];
    fn deref(&self) -> &Self::Target {
        self.v
    }
}

impl<'a> Field<'a> {
    pub fn field_type(&self) -> FieldType {
        self.t
    }

    pub fn iterator(self) -> FieldIterator<'a> {
        FieldIterator::new(self)
    }
}

pub struct FieldIterator<'a> {
    field: Field<'a>,
    idx: usize,
}

impl<'a> FieldIterator<'a> {
    pub fn new(field: Field<'a>) -> Self {
        let idx = field.v.len();
        Self { field, idx }
    }

    pub fn next_item(&mut self) -> Option<SampleType> {
        if self.idx > 0 {
            self.idx -= 1;
            Some(SampleType::from_field_item(
                self.field.t,
                self.field[self.idx],
                self.field.h,
            ))
        } else {
            None
        }
    }
}

impl<'a> Iterator for FieldIterator<'a> {
    type Item = SampleType;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_item()
    }
}

pub struct Timestamps<'a> {
    v: &'a [u64],
}

impl<'a> Timestamps<'a> {
    pub fn iterator(self) -> TimestampsIterator<'a> {
        TimestampsIterator::new(self)
    }
}

impl<'a> std::ops::Deref for Timestamps<'a> {
    type Target = [u64];
    fn deref(&self) -> &Self::Target {
        self.v
    }
}

pub struct TimestampsIterator<'a> {
    timestamps: Timestamps<'a>,
    idx: usize,
}

impl<'a> TimestampsIterator<'a> {
    pub fn new(timestamps: Timestamps<'a>) -> Self {
        let idx = timestamps.v.len();
        //println!("new timestamps iterator with length: {}", idx);
        Self { timestamps, idx }
    }

    pub fn next_timestamp(&mut self) -> Option<u64> {
        if self.idx > 0 {
            self.idx -= 1;
            Some(self.timestamps[self.idx])
        } else {
            None
        }
    }
}

impl<'a> Iterator for TimestampsIterator<'a> {
    type Item = u64;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_timestamp()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct Segment {
    pub segment_id: usize,
    pub len: usize,
    pub ts: Vec<u64>,
    pub data: Vec<Vec<[u8; 8]>>,
    pub heap: Heap,
    pub types: Vec<FieldType>,
    pub nvars: usize,
}

impl Segment {
    pub fn clear(&mut self) {
        self.ts.clear();
        for col in self.data.iter_mut() {
            col.clear();
        }
        for h in self.heap.iter_mut() {
            match h {
                Some(x) => x.clear(),
                None => {}
            }
        }
        self.types.clear();
        self.len = 0;
        self.nvars = 0;
    }

    pub fn new_empty() -> Self {
        let heap = Vec::new();
        let data = Vec::new();
        Self {
            segment_id: usize::MAX,
            len: 0,
            ts: Vec::with_capacity(256),
            data,
            heap,
            types: Vec::new(),
            nvars: 0,
        }
    }

    //pub fn new_with_types(types: &[FieldType]) -> Self {
    //    let mut heap = Vec::new();
    //    let mut data = Vec::new();
    //    for t in types {
    //        data.push(Vec::with_capacity(256));
    //        match t {
    //            FieldType::Bytes => heap.push(Some(Vec::with_capacity(1024))),
    //            FieldType::I64 => heap.push(None),
    //            FieldType::U64 => heap.push(None),
    //            FieldType::F64 => heap.push(None),
    //            FieldType::Timestamp => heap.push(None),
    //        }
    //    }
    //    Self {
    //        len: 0,
    //        ts: Vec::with_capacity(256),
    //        data,
    //        heap,
    //        types: types.into(),
    //        nvars: 0,
    //    }
    //}

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn field(&self, i: usize) -> Field {
        let h = match &self.heap[i] {
            Some(x) => Some(x.as_slice()),
            None => None,
        };
        Field {
            t: self.types[i],
            v: &self.data[i][..self.len],
            h,
        }
    }

    pub fn timestamps(&self) -> Timestamps {
        Timestamps {
            v: &self.ts[..self.len],
        }
    }

    pub fn reset_types(&mut self, types: &[FieldType]) {
        if types == self.types.as_slice() {
            return;
        }

        self.types.clear();
        self.types.extend_from_slice(types);

        let nvars = types.len();

        #[allow(clippy::comparison_chain)]
        if self.nvars < nvars {
            for _ in 0..nvars - self.nvars {
                self.data.push(Vec::with_capacity(256));
            }
        } else if self.nvars > nvars {
            for _ in 0..self.nvars - nvars {
                self.data.pop();
            }
        }

        self.heap.clear();
        for t in types {
            match t {
                FieldType::Bytes => self.heap.push(Some(Vec::with_capacity(1024))),
                FieldType::I64 => self.heap.push(None),
                FieldType::U64 => self.heap.push(None),
                FieldType::F64 => self.heap.push(None),
                FieldType::Timestamp => self.heap.push(None),
            }
        }
        self.nvars = nvars;
        self.len = 0;
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub active_segment: Option<Vec<Segment>>,
    pub active_block: Option<ReadOnlyBlock>,
    pub source_blocks: SourceBlocks,
    pub id: SeriesId,
    //pub historical_blocks: Option<Vec<kafka::KafkaEntry>>,
}

impl Snapshot {
    pub fn into_iterator(self) -> SnapshotIterator {
        let id = self.id;
        SnapshotIterator::new(self, id)
    }
}

#[derive(Eq, PartialEq, Copy, Clone)]
enum State {
    ActiveSegment,
    Blocks,
}

struct ActiveSegmentReader {
    active_segment: Vec<Segment>,
    current_idx: i64,
}

impl ActiveSegmentReader {
    fn new(active_segment: Vec<Segment>) -> Self {
        Self {
            active_segment,
            current_idx: -1,
        }
    }

    fn next_segment(&mut self) -> Option<()> {
        let idx = self.current_idx + 1;
        if (idx as usize) < self.active_segment.len() {
            self.current_idx = idx;
            Some(())
        } else {
            None
        }
    }

    fn get_segment(&self) -> &Segment {
        &self.active_segment[self.current_idx as usize]
    }
}

struct ReadOnlyBlockReader {
    block: ReadOnlyBlockBytes,
    _offsets: Box<[(u64, usize)]>,
    offsets: Vec<usize>,
    id: SeriesId,
    read_buffer: Segment,
    current_idx: i64,
}

impl ReadOnlyBlockReader {
    fn new(block: ReadOnlyBlockBytes, id: SeriesId) -> Self {
        let _offsets = block.offsets();
        let mut offsets = Vec::new();
        for item in _offsets.iter() {
            if SeriesId(item.0) == id {
                offsets.push(item.1);
            }
        }
        let current_idx = offsets.len().try_into().unwrap();
        //println!("Current index: {}", current_idx);
        Self {
            _offsets,
            offsets,
            block,
            id,
            current_idx,
            read_buffer: Segment::new_empty(),
        }
    }

    fn next_segment(&mut self) -> Option<()> {
        let _timer_1 = ThreadLocalTimer::new("ReadOnlyBlockReader::next_segment");
        if self.current_idx > 0 {
            self.current_idx -= 1;
            //println!("Getting offset:{}", self.offsets[self.current_idx as usize]);
            self.block.segment_at_offset(
                self.offsets[self.current_idx as usize],
                &mut self.read_buffer,
            );
            Some(())
        } else {
            //println!("NONE HERE");
            None
        }
    }

    fn get_segment(&self) -> &Segment {
        //println!("Here's the current segment");
        &self.read_buffer
    }
}

pub struct SnapshotIterator {
    active_segment: Option<ActiveSegmentReader>,
    block_reader: ReadOnlyBlockReader,
    source_blocks: SourceBlocks,
    //consumer: &'a mut kafka::Client,
    state: State,
    blocks_read: usize,
    segments_read: usize,
}

impl SnapshotIterator {
    //pub fn cached_messages_read(&self) -> usize {
    //    self.source_blocks.cached_messages_read
    //}

    //pub fn messages_read(&self) -> usize {
    //    self.source_blocks.messages_read
    //}

    pub fn segments_read(&self) -> usize {
        self.segments_read
    }

    pub fn blocks_read(&self) -> usize {
        self.blocks_read
    }

    pub fn new(snapshot: Snapshot, id: SeriesId) -> Self {
        //{
        //    let _timer_1 = ThreadLocalTimer::new("SnapshotIterator::new prefetching historical blocks");
        //    match snapshot.historical_blocks {
        //        Some(x) => kafka::prefetch(x.as_slice()),
        //        None => {
        //            println!("NOTHING TO PREFETCH");
        //        }
        //    }
        //}
        let active_segment = match snapshot.active_segment {
            Some(x) => Some(ActiveSegmentReader::new(x)),
            None => None,
        };
        let mut source_blocks = snapshot.source_blocks;
        let block_reader = match snapshot.active_block {
            Some(x) => ReadOnlyBlockReader::new(x.as_bytes(), id),
            None => {
                let block = source_blocks.next_block().unwrap();
                let bytes = block.as_bytes();
                ReadOnlyBlockReader::new(bytes, id)
            }
        };
        let state = if active_segment.is_some() {
            State::ActiveSegment
        } else {
            State::Blocks
        };

        Self {
            active_segment,
            source_blocks,
            block_reader,
            //consumer,
            state,
            segments_read: 0,
            blocks_read: 0,
        }
    }

    pub fn next_segment(&mut self) -> Option<()> {
        let _timer_1 = ThreadLocalTimer::new("SnapshotIterator::next_segment");
        //println!("getting next segment");
        match self.state {
            State::ActiveSegment => {
                let _timer_2 = ThreadLocalTimer::new("SnapshotIterator::next_segment active segment");
                //println!("currently in active segment, getting next");
                match self.active_segment.as_mut().unwrap().next_segment() {
                    None => {
                        //println!("moving into blocks");
                        self.state = State::Blocks;
                        self.next_segment()
                    }
                    Some(_) => Some(()),
                }
            }
            State::Blocks => {
                let _timer_2 = ThreadLocalTimer::new("SnapshotIterator::next_segment blocks");
                //println!("currently in blocks");
                match self.block_reader.next_segment() {
                    None => {
                        //println!("No segment in current block, fetching next block");
                        if let Some(block) = self.source_blocks.next_block() {
                            self.blocks_read += 1;
                            //println!("Found next block");
                            let bytes = block.as_bytes();
                            let id = self.block_reader.id;
                            let block_reader = ReadOnlyBlockReader::new(bytes, id);
                            self.block_reader = block_reader;
                            self.next_segment()
                        } else {
                            //println!("No next block");
                            None
                        }
                    }
                    Some(_) => {
                        //println!("Found segment in current block");
                        self.segments_read += 1;
                        Some(())
                    }
                }
            }
        }
    }

    pub fn get_segment(&self) -> &Segment {
        match self.state {
            State::ActiveSegment => self.active_segment.as_ref().unwrap().get_segment(),
            State::Blocks => self.block_reader.get_segment(),
        }
    }
}
