use crate::{
    id::SeriesId,
    mem_list::{ReadOnlyBlock, ReadOnlyBlockBytes, SourceBlocks},
    sample::SampleType,
    series::FieldType,
    utils::kafka,
};
use std::convert::TryInto;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum Heap {
    ActiveHeap(Vec<Option<Vec<u8>>>),
    DecompressHeap(Vec<u8>),
}

pub struct Field<'a> {
    t: FieldType,
    v: &'a [[u8; 8]],
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
            ))
        } else {
            None
        }
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

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct Segment {
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
        match &mut self.heap {
            Heap::ActiveHeap(x) => x.clear(),
            Heap::DecompressHeap(x) => x.clear(),
        }
        self.types.clear();
        self.len = 0;
        self.nvars = 0;
    }

    pub fn new_decompress_segment() -> Self {
        Self {
            len: 0,
            ts: Vec::with_capacity(256),
            data: Vec::new(),
            heap: Heap::DecompressHeap(Vec::with_capacity(1024)),
            types: Vec::new(),
            nvars: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn field(&self, i: usize) -> Field {
        Field {
            t: self.types[i],
            v: &self.data[i][..self.len],
        }
    }

    pub fn timestamps(&self) -> Timestamps {
        Timestamps {
            v: &self.ts[..self.len],
        }
    }

    pub fn set_types(&mut self, types: &[FieldType]) {
        if types != self.types.as_slice() {
            self.types.clear();
            self.types.extend_from_slice(types);
        }

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
        self.nvars = nvars;
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub active_segment: Vec<Segment>,
    pub active_block: ReadOnlyBlock,
    pub source_blocks: SourceBlocks,
    pub id: SeriesId,
}

impl Snapshot {
    pub fn into_iterator(self, consumer: kafka::BufferedConsumer) -> SnapshotIterator {
        let id = self.id;
        SnapshotIterator::new(self, id, consumer)
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
        Self {
            _offsets,
            offsets,
            block,
            id,
            current_idx,
            read_buffer: Segment::new_decompress_segment(),
        }
    }

    fn next_segment(&mut self) -> Option<()> {
        if self.current_idx > 0 {
            self.current_idx -= 1;
            self.block.segment_at_offset(
                self.offsets[self.current_idx as usize],
                &mut self.read_buffer,
            );
            Some(())
        } else {
            None
        }
    }

    fn get_segment(&self) -> &Segment {
        &self.read_buffer
    }
}

pub struct SnapshotIterator {
    active_segment: ActiveSegmentReader,
    block_reader: ReadOnlyBlockReader,
    source_blocks: SourceBlocks,
    consumer: kafka::BufferedConsumer,
    state: State,
}

impl SnapshotIterator {
    pub fn new(snapshot: Snapshot, id: SeriesId, mut consumer: kafka::BufferedConsumer) -> Self {
        let active_segment = ActiveSegmentReader::new(snapshot.active_segment);
        let source_blocks = snapshot.source_blocks;
        let block_reader =
            ReadOnlyBlockReader::new(snapshot.active_block.as_bytes(&mut consumer), id);
        Self {
            active_segment,
            source_blocks,
            block_reader,
            consumer,
            state: State::ActiveSegment,
        }
    }

    pub fn next_segment(&mut self) -> Option<()> {
        match self.state {
            State::ActiveSegment => match self.active_segment.next_segment() {
                None => {
                    self.state = State::Blocks;
                    self.next_segment()
                }
                Some(_) => Some(()),
            },
            State::Blocks => match self.block_reader.next_segment() {
                None => {
                    if let Some(block) = self.source_blocks.next_block(&mut self.consumer) {
                        let bytes = block.as_bytes(&mut self.consumer);
                        let id = self.block_reader.id;
                        let block_reader = ReadOnlyBlockReader::new(bytes, id);
                        self.block_reader = block_reader;
                        self.next_segment()
                    } else {
                        None
                    }
                }
                Some(_) => Some(()),
            },
        }
    }

    pub fn get_segment(&self) -> &Segment {
        match self.state {
            State::ActiveSegment => self.active_segment.get_segment(),
            State::Blocks => self.block_reader.get_segment(),
        }
    }
}
