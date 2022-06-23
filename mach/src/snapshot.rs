use crate::{
    mem_list::{ReadOnlyBlock, SourceBlocks},
    utils::kafka,
    series::Types,
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

    //pub fn decompress_heap_mut(&mut self) -> &mut Vec<u8> {
    //    match self.heap {
    //        Heap::DecompressHeap(x) => &mut x,
    //        _ => unimplemented!(),
    //    }
    //}

    //pub fn active_heap_mut(&mut self) -> &mut Vec<Option<Vec<u8>>> {
    //    match self.heap {
    //        Heap::ActiveHeap(x) => &mut x,
    //        _ => unimplemented!(),
    //    }
    //}

    //pub fn set_heap(&mut self, heap: Heap) {
    //    self.heap = heap
    //}

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
}

impl Snapshot {
    pub fn into_iterator(self, bootstraps: &str, topic: &str) -> SnapshotIterator {
        SnapshotIterator::new(self, bootstraps, topic)
    }
}

enum State {
    ActiveSegment,
    ActiveBlock,
    SourceBlocks,
}

pub struct SnapshotIterator {
    consumer: kafka::BufferedConsumer,
    buffer: Vec<u8>,
    snapshot: Snapshot,
    state: State,
    index: usize,
}

impl SnapshotIterator {
    fn new(snapshot: Snapshot, bootstraps: &str, topic: &str) -> Self {
        Self {
            snapshot,
            consumer: kafka::BufferedConsumer::new(bootstraps, topic),
            buffer: Vec::new(),
            state: State::ActiveSegment,
            index: 0,
        }
    }

    pub fn next_segment(&mut self) -> Option<&Segment> {
        match self.state {
            State::ActiveSegment => {
                if self.index < self.snapshot.active_segment.len() {
                    let idx = 0;
                    self.index += 1;
                    Some(&self.snapshot.active_segment[idx])
                } else {
                    self.index = 0;
                    self.state = State::ActiveBlock;
                    self.next_segment()
                }
            }
            _ => unimplemented!()
        }
    }
}
