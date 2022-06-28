use crate::{
    mem_list::{ReadOnlyBlock, ReadOnlyBlockBytes, SourceBlocks},
    utils::kafka,
    series::Types,
    id::SeriesId,
};
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum Heap {
    ActiveHeap(Vec<Option<Vec<u8>>>),
    DecompressHeap(Vec<u8>),
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct Segment {
    pub len: usize,
    pub ts: Vec<u64>,
    pub data: Vec<Vec<[u8; 8]>>,
    //pub active_heap: Vec<Option<Vec<u8>>>,
    //pub decompress_heap: Vec<u8>,
    pub heap: Heap,
    pub types: Vec<Types>,
    pub nvars: usize,
}

impl Segment {

    pub fn clear(&mut self) {
        self.ts.clear();
        for mut col in self.data.iter_mut() {
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

    pub fn variable(&self, i: usize) -> (Types, &[[u8; 8]]) {
        (self.types[i], &self.data[i][..self.len])
    }

    pub fn get_timestamp_at(&self, i: usize) -> u64 {
        let i = self.len - i - 1;
        self.ts[i]
    }

    pub fn get_value_at(&self, var: usize, i: usize) -> (Types, [u8; 8]) {
        let i = self.len - i - 1;
        let (t, v) = self.variable(var);
        (t, v[i])
    }

    pub fn timestamps(&self) -> &[u64] {
        &self.ts[..self.len]
    }

    pub fn set_types(&mut self, types: &[Types]) {
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
        Self {
            _offsets,
            offsets,
            block,
            id,
            current_idx: 0,
            read_buffer: Segment::new_decompress_segment(),
        }
    }

    fn next_segment(&mut self) -> Option<()> {
        let idx = self.current_idx + 1;
        if idx as usize == self.offsets.len() {
            None
        } else {
            self.current_idx = idx;
            self.block.segment_at_offset(self.offsets[idx as usize], &mut self.read_buffer);
            Some(())
        }
    }

    fn get_segment(&self) -> &Segment {
        &self.read_buffer
    }

    fn reset(&mut self, block: Option<ReadOnlyBlockBytes>, id: Option<SeriesId>) {
        self.current_idx = -1;
        match (block, id) {
            (Some(block), Some(i)) => {
                let _offsets = block.offsets();
                self.offsets.clear();
                for item in _offsets.iter() {
                    if SeriesId(item.0) == i {
                        self.offsets.push(item.1);
                    }
                }
                self.block = block;
                self._offsets = _offsets;
            },
            (Some(block), None) => {
                let _offsets = block.offsets();
                self.offsets.clear();
                for item in _offsets.iter() {
                    if SeriesId(item.0) != self.id {
                        self.offsets.push(item.1);
                    }
                }
                self.block = block;
                self._offsets = _offsets;
            }
            (None, Some(id)) => {
                if id != self.id {
                    self.id = id;
                    self.offsets.clear();
                    for item in self._offsets.iter() {
                        if SeriesId(item.0) != self.id {
                            self.offsets.push(item.1);
                        }
                    }
                }
            }
            (None, None) => {},
        }
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
    pub fn new(snapshot: Snapshot, id: SeriesId, consumer: kafka::BufferedConsumer) -> Self {
        let active_segment = ActiveSegmentReader::new(snapshot.active_segment);
        let source_blocks = snapshot.source_blocks;
        let block_reader = ReadOnlyBlockReader::new(
            snapshot.active_block.as_bytes(&consumer),
            id,
        );
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
            State::ActiveSegment => {
                match self.active_segment.next_segment() {
                    None => {
                        self.state = State::Blocks;
                        self.next_segment()
                    },
                    Some(_) => Some(())
                }
            },
            State::Blocks => {
                match self.block_reader.next_segment() {
                    None => {
                        if let Some(block) = self.source_blocks.next_block(&self.consumer) {
                            let bytes = block.as_bytes(&self.consumer);
                            self.block_reader.reset(Some(bytes), None);
                            self.next_segment()
                        } else {
                            None
                        }
                    },
                    Some(_) => Some(())
                }
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
