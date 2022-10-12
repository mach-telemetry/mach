use crate::{
    snapshot::Segment,
    utils::kafka::*,
    series::Series,
    mem_list::{ChunkBytes, ChunkBytesOrKafka, ReadOnlyBlock},
};
use serde::*;

#[derive(Serialize, Deserialize)]
pub enum NextItem {
    Snapshot(KafkaEntry),
    SourceBlock(KafkaEntry),
}


#[derive(Serialize, Deserialize)]
pub struct Snapshot2 {
    pub segment: Option<Segment>,
    pub chunks: Vec<ChunkBytesOrKafka>,
    pub last_id: usize,
    pub next: NextItem,
}

impl Snapshot2 {
    pub fn new(series: &Series, last_compressed_block_id: usize, prev_snapshot: Option<KafkaEntry>) { //-> Self {
        let now = std::time::Instant::now();
        let serid = series.config.id;

        let mut chunks: Vec<ChunkBytesOrKafka> = Vec::new();
        let mut done = false;
        let mut last_id = usize::MAX;

        // Get the active segment
        let segment: Option<Segment> = match series.segment.snapshot() {
            Ok(mut x) => {
                assert_eq!(x.inner.len(), 1);
                Some(x.inner.swap_remove(0))
            },
            Err(_) => None,
        };

        // Get the active block
        let mut active_chunks: Option<Vec<ChunkBytes>> = series.block_list.snapshot().ok().map(|x| {
            x.as_bytes().chunks_for_id(serid.0)
        });

        match active_chunks.as_mut() {
            Some(x) => {
                x.sort_by(|x, y| y.id.cmp(&x.id)); // sort active chunks
                let mut v = Vec::new();
                for chunk in x.drain(..) {
                    if chunk.id > last_compressed_block_id {
                        if last_id == usize::MAX {
                            last_id = chunk.id;
                        }
                        v.push(chunk);
                    } else {
                        done = true;
                        break;
                    }
                }
                chunks.push(ChunkBytesOrKafka::Bytes(v));
            },
            None => {},
        }
    }
}

/*
        let active_segment = match self.segment.snapshot() {
            Ok(x) => Some(x.inner),
            Err(_) => None,
        };
        let active_block = match self.block_list.snapshot() {
            Ok(x) => Some(x),
            Err(_) => None,
        };
        let mut source_blocks = None;
        //let _start = std::time::Instant::now();
        while source_blocks.is_none() {
            if let Ok(blocks) = self.source_block_list.snapshot() {
                source_blocks = Some(blocks);
            }
            //if std::time::Instant::now() - start >= std::time::Duration::from_secs(1) {
            //    source_blocks = Some(self.source_block_list.periodic_snapshot());
            //}
        }
        //let historical_blocks = HISTORICAL_BLOCKS.snapshot(self.config.id);

        //let list = self.list.snapshot()?;
        //Ok(Snapshot::new(segment, list))
        Snapshot {
            active_segment,
            active_block,
            source_blocks: source_blocks.unwrap(),
            id: self.config.id,
            //historical_blocks,
        }


pub struct Snapshot {
    pub active_segment: Option<Vec<Segment>>,
    pub active_block: Option<ReadOnlyBlock>,
    pub source_blocks: SourceBlocks2,
    pub id: SeriesId,
}
*/

