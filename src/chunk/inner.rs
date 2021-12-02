use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, AtomicBool, Ordering::SeqCst}
    },
    mem::{self, MaybeUninit},
    ops::{Deref, DerefMut},
};
use crate::compression::Compression;
use crate::segment::FullSegment;
use crate::utils::{QueueAllocator, Qrc};
use crate::chunk::{SerializedChunk, PushStatus, Error, CHUNK_THRESHOLD_SIZE, CHUNK_THRESHOLD_COUNT};

#[derive(Clone)]
pub struct ChunkEntry {
    data: Qrc<Vec<u8>>,
    mint: u64,
    maxt: u64,
}

impl ChunkEntry {
    pub fn bytes(&self) -> &[u8] {
        &self.data[..]
    }
}

struct Entry {
    data: MaybeUninit<ChunkEntry>,
    version: AtomicUsize,
}

impl Entry {
    fn update(&mut self, data: ChunkEntry) {
        let mut data = MaybeUninit::new(data);
        mem::swap(&mut data, &mut self.data);
        if self.version.fetch_add(1, SeqCst) > 0 {
            // Safety: each update increments version so data must be inited if version > 1.
            // drop the data (including the Qrc)
            unsafe {
                data.assume_init();
            }
        }
    }

    // Unsafe because update needs to be called first
    unsafe fn load(&self) -> Result<ChunkEntry, Error> {
        let v = self.version.load(SeqCst);

        // This is a race with update function. However...
        let r = self.data.assume_init_ref().clone();

        // ... if the version is the same, then we know for sure that the data hasn't changed
        // between the two loads
        if v == self.version.load(SeqCst) {
            Ok(r)
        } else {
            Err(Error::ChunkEntryLoad)
        }
    }
}

pub struct InnerChunk {
    block: [Entry; CHUNK_THRESHOLD_COUNT],
    counter: AtomicUsize,
    compression: Compression,
    allocator: QueueAllocator<Vec<u8>>,
    tsid: u64,
    mint: u64,
    maxt: u64,
}

impl InnerChunk {
    pub fn new(tsid: u64, compression: Compression) -> Self {
        let block = {
            let mut block: [MaybeUninit<Entry>; CHUNK_THRESHOLD_COUNT] = MaybeUninit::uninit_array();
            for i in 0..CHUNK_THRESHOLD_COUNT {
                block[i].write(Entry {
                    data: MaybeUninit::uninit(),
                    version: AtomicUsize::new(0),
                });
            }
            // Safety: We write to every position in the uninit_array so the whole array was inited
            unsafe { MaybeUninit::array_assume_init(block) }
        };

        InnerChunk {
            block,
            counter: AtomicUsize::new(0),
            allocator: QueueAllocator::new(Vec::new),
            compression,
            tsid,
            mint: 0,
            maxt: 0,
        }
    }

    fn counter_pack(&self, count: usize, sz: usize) {
        self.counter.store(count << 32 | sz, SeqCst);
    }

    fn counter_unpack(&self) -> (usize, usize) {
        let x = self.counter.load(SeqCst);
        let sz = x & 0xffffffff;
        let ct = x >> 32;
        (ct, sz)
    }

    pub fn push(&mut self, segment: &FullSegment) -> Result<PushStatus, Error> {


        let full = |count: usize, size: usize| -> bool {
            count == CHUNK_THRESHOLD_COUNT || size == CHUNK_THRESHOLD_SIZE
        };


        let (mut count, mut size) = self.counter_unpack();

        if full(count, size) {
            Err(Error::PushIntoFull)
        } else {
            let ts = segment.timestamps();
            assert!(ts.len() > 0);
            if count == 0 {
                self.mint = ts[0];
            }
            self.maxt = *ts.last().unwrap();
            let mut data: Qrc<Vec<u8>> = self.allocator.allocate();
            self.compression.compress(segment, data.as_mut());
            let sz = data.len();
            let entry = ChunkEntry {
                data,
                mint: ts[0],
                maxt: self.maxt,
            };
            self.block[count].update(entry);

            count += 1;
            if count == 1 {
                size = sz;
            } else {
                size += sz
            }
            self.counter_pack(count, size);

            if full(count, size) {
                Ok(PushStatus::Flush)
            } else {
                Ok(PushStatus::Done)
            }
        }
    }

    // Chunk Format
    //
    // > header
    // len: [0..8]
    // tsid: [8..16]
    // mint: [16..24]
    // maxt: [24..32]
    // segment count: [32..40]
    //
    // > Segment metadata information
    // segment offset: [40..48]
    // segment mint: [48..56]
    // segment maxt: [56..64]
    // ... repeated for the segment count
    //
    // > Segment data information
    // segments: [header bytes + segment count * 8 * 3 ..]
    pub fn serialize(&self) -> Result<SerializedChunk, Error> {
        let mut v = Vec::new();

        let (counter, _)  = self.counter_unpack();

        // Placeholder for length
        v.extend_from_slice(&[0u8; 8]);

        // write the TSID
        v.extend_from_slice(&self.tsid.to_le_bytes()[..]);

        // write the Min and Max timestamps
        v.extend_from_slice(&self.mint.to_le_bytes()[..]);
        v.extend_from_slice(&self.maxt.to_le_bytes()[..]);

        // write the number of segments
        v.extend_from_slice(&counter.to_le_bytes()[..]);

        // Upto here, we've written 40 bytes
        let header = 40;

        // Reserve room for segment metadata information
        v.resize((header + counter * 8 * 3) as usize, 0);

        // segment area

        // Get the version of the first block. If as we are generating the block, the version
        // changes, return an error
        let version = self.block[0].version.load(SeqCst);

        // write each segment and the metadata for each segment
        for i in 0..counter {

            let mut off = (header + i * 8 * 3) as usize;

            // Safety: Will only be called if counter > 0 which means push happened
            let entry = unsafe { self.block[i as usize].load()? };

            // Return error if version changes
            if self.block[i as usize].version.load(SeqCst) != version {
                return Err(Error::InconsistentChunkGeneration);
            }

            // we know entry has the same version

            // write the segment offset
            let l = v.len() as u64;
            v[off..off+8].copy_from_slice(&l.to_le_bytes()[..]);
            off += 8;

            // write the segment mint
            v[off..off+8].copy_from_slice(&entry.mint.to_le_bytes()[..]);
            off += 8;

            // write the segment maxt
            v[off..off+8].copy_from_slice(&entry.maxt.to_le_bytes()[..]);

            // write the segment to the end of the vector
            v.extend_from_slice(&entry.data[..]);
        }

        // Write the total length
        let len = v.len() as u64;
        v[..8].copy_from_slice(&len.to_le_bytes()[..]);

        Ok(SerializedChunk {
            bytes: v.into_boxed_slice(),
            tsid: self.tsid,
            mint: self.mint,
            maxt: self.maxt,
        })
    }

    pub fn clear(&self) {
        self.counter.store(0, SeqCst);
    }

    pub fn read(&self) -> Result<Vec<ChunkEntry>, Error> {
        let c = self.counter.load(SeqCst);
        let mut res = Vec::new();

        for b in self.block[0..c].iter() {
            // Safety: This is safe because the load will only be called if C > 0 and it will only
            // be > 0 if data were pushed to block[0]
            unsafe { res.push(b.load()?); }
        }
        Ok(res)
    }
}


