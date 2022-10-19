use crate::{
    id::SeriesId,
    mem_list::{ReadOnlyBlock, SourceBlocks3, ChunkBytesOrKafka},
    sample::SampleType,
    series::FieldType,
    utils::{counter::*, timer::*},
};
use std::collections::BinaryHeap;
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

    pub fn field_idx(&self, field: usize, idx: usize) -> SampleType {
        let field_type = self.types[field];
        let heap_vec = self.heap[field].as_ref();
        let heap_slice = heap_vec.map(|x| x.as_slice());
        let value = self.data[field][idx];
        SampleType::from_field_item(field_type, value, heap_slice)
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

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn field(&self, i: usize) -> Field {
        let h = self.heap[i].as_deref();
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
    pub source_blocks: SourceBlocks3,
    pub id: SeriesId,
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
    block: ChunkBytesOrKafka,
    read_buffer: Segment,
    current_idx: i64,
}

impl ReadOnlyBlockReader {
    fn new(block: ChunkBytesOrKafka) -> Self {
        let current_idx = block.len().try_into().unwrap();
        Self {
            block,
            read_buffer: Segment::new_empty(),
            current_idx
        }
    }

    fn next_segment(&mut self) -> Option<()> {
        let _timer_1 = ThreadLocalTimer::new("ReadOnlyBlockReader::next_segment");
        if self.current_idx > 0 {
            self.current_idx -= 1;
            //println!("Getting offset:{}", self.offsets[self.current_idx as usize]);
            self.block.segment_at_offset(
                self.current_idx as usize,
                &mut self.read_buffer,
            );
            Some(())
        } else {
            //println!("NONE HERE");
            None
        }
    }


    fn next_segment_at_timestamp(&mut self, ts: u64) -> Option<()> {
        let _timer_1 = ThreadLocalTimer::new("ReadOnlyBlockReader::next_segment");
        if self.current_idx > 0 {
            self.current_idx -= 1;
            //println!("Getting offset:{}", self.offsets[self.current_idx as usize]);
            self.block.segment_at_offset(
                self.current_idx as usize,
                &mut self.read_buffer,
            );
            let min_ts = self.read_buffer.timestamps()[0];
            if min_ts > ts {
                //println!("Skipping segment");
                ThreadLocalCounter::new("skipping segment").increment(1);
                self.next_segment_at_timestamp(ts)
            } else {
                ThreadLocalCounter::new("loading segment").increment(1);
                Some(())
            }
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
    source_blocks: SourceBlocks3,
    //consumer: &'a mut kafka::Client,
    state: State,
    id: SeriesId,
}

impl SnapshotIterator {
    //pub fn cached_messages_read(&self) -> usize {
    //    self.source_blocks.cached_messages_read
    //}

    //pub fn messages_read(&self) -> usize {
    //    self.source_blocks.messages_read
    //}

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
        let active_segment = snapshot.active_segment.map(ActiveSegmentReader::new);
        let mut source_blocks = snapshot.source_blocks;
        let block_reader = match snapshot.active_block {
            Some(x) => ReadOnlyBlockReader::new(x.chunk_bytes_or_kafka(id.0).to_bytes(id.0)),
            None => {
                let block = source_blocks.next_block().unwrap();
                ReadOnlyBlockReader::new(block.block.to_bytes(id.0))
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
            id,
        }
    }

    pub fn next_segment(&mut self) -> Option<()> {
        let _timer_1 = ThreadLocalTimer::new("SnapshotIterator::next_segment");
        match self.state {
            State::ActiveSegment => {
                let _timer_2 =
                    ThreadLocalTimer::new("SnapshotIterator::next_segment active segment");
                match self.active_segment.as_mut().unwrap().next_segment() {
                    None => {
                        //println!("moving into blocks");
                        self.state = State::Blocks;
                        self.next_segment()
                    }
                    Some(_) => {
                        Some(())
                    }
                }
            }
            State::Blocks => {
                let _timer_2 = ThreadLocalTimer::new("SnapshotIterator::next_segment blocks");
                match self.block_reader.next_segment() {
                    None => 
                        match self.source_blocks.next_block() {
                            Some(block) => {
                                ThreadLocalCounter::new("loading block").increment(1);
                                let bytes = block.clone();
                                let block_reader = ReadOnlyBlockReader::new(bytes.block.to_bytes(self.id.0));
                                self.block_reader = block_reader;
                                self.next_segment()
                            }
                            None => None,
                        }
                    Some(_) => Some(()),
                }
            }
        }
    }


    pub fn next_segment_at_timestamp(&mut self, ts: u64) -> Option<()> {
        let _timer_1 = ThreadLocalTimer::new("SnapshotIterator::next_segment");
        match self.state {
            State::ActiveSegment => {
                let _timer_2 =
                    ThreadLocalTimer::new("SnapshotIterator::next_segment active segment");
                match self.active_segment.as_mut().unwrap().next_segment() {
                    None => {
                        //println!("moving into blocks");
                        self.state = State::Blocks;
                        self.next_segment_at_timestamp(ts)
                    }
                    Some(_) => {
                        let seg = self.active_segment.as_ref().unwrap().get_segment();
                        let timestamps = seg.timestamps();
                        let min_ts = timestamps[0];
                        if min_ts > ts {
                            self.next_segment_at_timestamp(ts)
                        } else {
                            Some(())
                        }
                    }
                }
            }
            State::Blocks => {
                let _timer_2 = ThreadLocalTimer::new("SnapshotIterator::next_segment blocks");
                match self.block_reader.next_segment_at_timestamp(ts) {
                    None => loop {
                        match self.source_blocks.next_block() {
                            Some(block) => {
                                if block.min_ts > ts {
                                    ThreadLocalCounter::new("skipping block").increment(1);
                                    continue;
                                } else {
                                    ThreadLocalCounter::new("loading block").increment(1);
                                    let bytes = block.clone();
                                    let block_reader = ReadOnlyBlockReader::new(bytes.block.to_bytes(self.id.0));
                                    self.block_reader = block_reader;
                                    return self.next_segment_at_timestamp(ts);
                                }
                            }
                            None => return None,
                        }
                    },
                    Some(_) => Some(()),
                }
            }
        }
    }

    pub fn get_segment(&self) -> &Segment {
        match self.state {
            State::ActiveSegment => {
                self.active_segment.as_ref().unwrap().get_segment()
            },
            State::Blocks => self.block_reader.get_segment(),
        }
    }
}

use std::cmp::*;

struct HeapEntry {
    timestamp: u64,
    data: Vec<SampleType>,
    idx: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

impl Eq for HeapEntry {}

struct IteratorMetadata {
    iterator: SnapshotIterator,
    idx: usize,
    fields: Vec<usize>,
    segment: *const Segment,
    timestamp: u64,
    last_timestamp: u64,
}

impl IteratorMetadata {
    fn new(snapshot: Snapshot, fields: Vec<usize>, timestamp: u64) -> Option<Self> {
        let mut iterator = snapshot.into_iterator();
        iterator.next_segment_at_timestamp(timestamp)?;
        let segment = iterator.get_segment();
        let idx = 0;
        let segment: *const Segment = segment;
        Some(IteratorMetadata {
            iterator,
            segment,
            fields,
            idx,
            timestamp,
            last_timestamp: u64::MAX,
        })
    }

    fn segment(&self) -> &Segment {
        unsafe { self.segment.as_ref().unwrap() }
    }

    fn next_item(&mut self) -> Option<(u64, Vec<SampleType>)> {
        let segment_done = { self.idx == self.segment().len() };
        if segment_done {
            self.iterator.next_segment_at_timestamp(self.timestamp)?;
            let segment = self.iterator.get_segment();

            // Segments could be duplicated between in-mem and kafka, skip if it is
            if *segment.ts.last().unwrap() > self.last_timestamp {
                self.next_item()
            } else {
                self.idx = 0;
                self.segment = segment;
                self.next_item()
            }
        } else {
            let idx = self.idx;
            self.idx += 1;

            // this scoping ensures borrowing isnt an issue
            let (timestamp, data) = {
                let segment = self.segment(); // borrowing occurs here
                let idx = segment.len() - idx - 1;
                let timestamp = segment.ts[idx];
                let data: Vec<SampleType> = self
                    .fields
                    .iter()
                    .map(|x| segment.field_idx(*x, idx))
                    .collect();
                (timestamp, data)
            };
            self.last_timestamp = timestamp;
            Some((timestamp, data))
        }
    }
}

//'segment: while let Some(_) = snapshot.next_segment_at_timestamp(now) {
//    let seg = snapshot.get_segment();
//    if last_segment == usize::MAX {
//        last_segment = seg.segment_id;
//    } else {
//        assert_eq!(last_segment, seg.segment_id + 1);
//        last_segment = seg.segment_id;
//    }
//    seg_count += 1;
//    let mut timestamps = seg.timestamps().iterator();
//    let mut field0 = seg.field(0).iterator();
//    while let Some(x) = timestamps.next_timestamp() {
//        if x > last_timestamp {
//            continue 'segment;
//        }
//        result_timestamps.push(x);
//        last_timestamp = x;
//    }
//    while let Some(x) = field0.next_item() {
//        result_field0.push(x.as_bytes().into());
//    }
//    println!("result_timestamps.len() {}", result_timestamps.len());
//}
//println!("seg count: {}", seg_count);

pub struct SnapshotZipper {
    iterators: Vec<IteratorMetadata>,
    queue: BinaryHeap<HeapEntry>,
}

impl SnapshotZipper {
    pub fn new(snapshot_map: Vec<(Snapshot, Vec<usize>)>, start: u64) -> Self {
        let mut iterators: Vec<IteratorMetadata> = snapshot_map
            .into_iter()
            .filter_map(|(k, v)| IteratorMetadata::new(k, v, start))
            .collect();
        let mut queue = BinaryHeap::new();
        for (idx, iterator) in iterators.iter_mut().enumerate() {
            if let Some((timestamp, data)) = iterator.next_item() {
                    queue.push(HeapEntry {
                        timestamp,
                        data,
                        idx,
                    });
            }
        }
        Self { iterators, queue }
    }

    pub fn next_item(&mut self) -> Option<(u64, Vec<SampleType>)> {
        let x = self.queue.pop()?;

        let HeapEntry {
            timestamp,
            data,
            idx,
        } = x;

        if let Some((timestamp, data)) = self.iterators[idx].next_item() {
            self.queue.push(HeapEntry {
                timestamp,
                data,
                idx,
            });
        }
        Some((timestamp, data))
    }
}

