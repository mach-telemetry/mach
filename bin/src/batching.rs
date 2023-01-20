// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use lzzzz::lz4;
use mach::sample::SampleType;
use std::collections::HashSet;
use std::sync::Arc;

static MAGIC: &str = "blahblah";

pub struct WriteBatch {
    buf: Vec<u8>,
    //scratch: Box<[u8]>,
    ids: HashSet<u64>,
    range: (u64, u64),
    offset: usize,
    count: usize,
}

impl WriteBatch {
    pub fn new(size: usize) -> Self {
        WriteBatch {
            buf: vec![0u8; size],
            ids: HashSet::new(),
            range: (u64::MAX, 0),
            offset: 0, // first 8 bytes show where the tail metadata of the batch is
            count: 0,
        }
    }

    pub fn insert(&mut self, id: u64, ts: u64, samples: &[SampleType]) -> Result<usize, usize> {
        let serialized = bincode::serialize(samples).unwrap();
        let serialized_size = serialized.len();
        let has_id = self.ids.contains(&id);

        let total_size = {
            // added size is serialized size + id + ts + 8 bytes if the id isnt in the id set
            let added_size = serialized_size + 16 + (has_id as usize) * 8usize;
            self.total_size() + added_size
        };
        if total_size > self.buf.len() {
            Err(total_size)
        } else {
            let mut offset = self.offset;

            // write id
            self.buf[offset..offset + 8].copy_from_slice(&id.to_be_bytes());
            offset += 8;

            // write timestamps
            self.buf[offset..offset + 8].copy_from_slice(&ts.to_be_bytes());
            offset += 8;

            // write samples size
            self.buf[offset..offset + 8].copy_from_slice(&serialized_size.to_be_bytes());
            offset += 8;

            // write samples
            self.buf[offset..offset + serialized_size].copy_from_slice(&serialized);
            //bincode::serialize_into(&mut self.buf[offset..offset + serialized_size], samples)
            //    .unwrap();
            offset += serialized_size;

            // update ids, range, and offset
            self.offset = offset;
            if self.range.0 == u64::MAX {
                self.range.0 = ts;
            }
            self.range.1 = ts;
            self.ids.insert(id);
            self.count += 1;

            Ok(self.total_size())
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn data_size(&self) -> usize {
        self.offset
    }

    pub fn total_size(&self) -> usize {
        // total size calculated this way because all the metadata required could still exceed the
        // logically assigned batch size - even after compression. Loglically, batch size should
        // include the metadata
        self.offset + 8 + self.ids.len() * 8 + 24 // bytes in buf + number of ids + ids + range of batch
    }

    pub fn close_no_compress(self) -> Box<[u8]> {
        let mut data = self.buf;
        data.resize(self.offset, 0u8);
        let data_offset = data.len();

        data.extend_from_slice(&bincode::serialize(&self.ids).unwrap());
        let ids_offset = data.len();

        data.extend_from_slice(&self.range.0.to_be_bytes());
        data.extend_from_slice(&self.range.1.to_be_bytes());
        data.extend_from_slice(&ids_offset.to_be_bytes());
        data.extend_from_slice(&data_offset.to_be_bytes());
        data.into_boxed_slice()
    }

    pub fn close(self) -> Box<[u8]> {
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC.as_bytes());
        data.extend_from_slice(&[0u8; 24]);

        let before_compress = data.len();
        let size =
            lz4::compress_to_vec(&self.buf[..self.offset], &mut data, lz4::ACC_LEVEL_DEFAULT)
                .unwrap();

        let mut offset = MAGIC.as_bytes().len();
        let tail = data.len();
        assert_eq!(size, tail - before_compress);
        // write offset of tail in first 8 bytes
        //let tail = 24 + size + MAGIC.as_bytes().len();
        data[offset..offset + 8].copy_from_slice(&tail.to_be_bytes());
        offset += 8;

        // write raw data size in second 8 bytes
        data[offset..offset + 8].copy_from_slice(&self.offset.to_be_bytes());
        offset += 8;

        // write raw data size in second 8 bytes
        data[offset..offset + 8].copy_from_slice(&self.count.to_be_bytes());
        //offset += 8;

        // write the number of ids
        data.extend_from_slice(&self.ids.len().to_be_bytes());

        // write each ID
        self.ids.iter().for_each(|x| {
            data.extend_from_slice(&x.to_be_bytes());
        });

        // write the range
        data.extend_from_slice(&self.range.0.to_be_bytes());
        data.extend_from_slice(&self.range.1.to_be_bytes());

        data.into_boxed_slice()
    }
}

#[derive(Clone)]
pub struct BytesBatch {
    bytes: Arc<[u8]>,
    raw_size: usize,
    tail: usize,
    count: usize,
}

impl BytesBatch {
    pub fn new(bytes: Arc<[u8]>) -> Self {
        assert_eq!(MAGIC.as_bytes(), &bytes[..MAGIC.as_bytes().len()]);
        let og = bytes;
        let bytes = &og[MAGIC.as_bytes().len()..];
        let tail = usize::from_be_bytes(bytes[..8].try_into().unwrap());
        let raw_size = usize::from_be_bytes(bytes[8..16].try_into().unwrap());
        let count = usize::from_be_bytes(bytes[16..24].try_into().unwrap());
        BytesBatch {
            bytes: og,
            raw_size,
            tail,
            count,
        }
    }

    pub fn metadata(&self) -> (HashSet<u64>, (u64, u64)) {
        //let bytes = &self.bytes[MAGIC.as_bytes().len()..];
        let tail = &self.bytes[self.tail..];
        let mut set = HashSet::new();

        let mut offset = 0;
        let n_ids = usize::from_be_bytes(tail[offset..offset + 8].try_into().unwrap());
        offset += 8;

        (0..n_ids).for_each(|_| {
            set.insert(u64::from_be_bytes(
                tail[offset..offset + 8].try_into().unwrap(),
            ));
            offset += 8;
        });

        assert_eq!(set.len(), n_ids);

        let low = u64::from_be_bytes(tail[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let high = u64::from_be_bytes(tail[offset..offset + 8].try_into().unwrap());

        (set, (low, high))
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn total_size(&self) -> usize {
        self.bytes.len() + 16
    }

    pub fn entries(&self) -> Vec<(u64, u64, Vec<SampleType>)> {
        let bytes = &self.bytes[MAGIC.as_bytes().len() + 24..self.tail];
        //println!("TAIL: {}", self.tail);
        let mut data = vec![0u8; 5_000_000];
        let decompress_sz = match lz4::decompress(&bytes, &mut data) {
            Ok(x) => x,
            Err(x) => {
                println!("tail {}, raw_sz: {}", self.tail, self.raw_size);
                panic!("Can't decompress: {:?}", x);
            }
        };

        let mut result = Vec::new();

        let mut offset = 0;

        while offset < decompress_sz {
            let id = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let ts = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let samples_size = usize::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let end_samples = offset + samples_size;
            let samples: Vec<SampleType> =
                bincode::deserialize(&data[offset..end_samples]).unwrap();
            offset = end_samples;

            result.push((id, ts, samples));
        }

        result
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::data_generator;
    use rand::prelude::*;

    #[test]
    fn test_batching() {
        let samples = data_generator::SAMPLES.clone();
        let expected: Vec<(u64, u64, &'static [SampleType])> = samples
            .iter()
            .map(|x| (x.0 .0, thread_rng().gen(), x.1))
            .collect();

        // Insert into batch
        let mut batch = WriteBatch::new(1_000_000);
        let mut counter = 0;
        for item in expected.iter() {
            if batch.insert(item.0, item.1, item.2).is_err() {
                break;
            } else {
                counter += 1;
            }
        }
        assert!(batch.total_size() < 1_000_000);
        let bytes = batch.close();

        let expected_samples = &expected[..counter];
        let mut expected_ids = HashSet::new();
        let mut expected_range = (u64::MAX, 0);
        expected_samples.iter().for_each(|x| {
            expected_ids.insert(x.0);
            if expected_range.0 == u64::MAX {
                expected_range.0 = x.1;
            }
            expected_range.1 = x.1;
        });

        // Read Batch
        let batch = BytesBatch::new(bytes.into());
        let (ids, range) = batch.metadata();
        let entries = batch.entries();

        assert_eq!(batch.count, counter);
        assert_eq!(ids, expected_ids);
        assert_eq!(range, expected_range);
        for (entry, expected) in entries.iter().zip(expected_samples.iter()) {
            assert_eq!(entry.0, expected.0);
            assert_eq!(entry.1, expected.1);
            assert_eq!(entry.2.as_slice(), expected.2);
        }
    }
}
