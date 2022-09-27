use mach::sample::SampleType;
use std::collections::HashSet;
use lzzzz::lz4;

pub struct WriteBatch {
    buf: Box<[u8]>,
    ids: HashSet<u64>,
    range: (u64, u64),
    offset: usize,
    count: usize,
}

impl WriteBatch {
    pub fn new(size: usize) -> Self {
        WriteBatch {
            buf: vec![0u8; size].into_boxed_slice(),
            ids: HashSet::new(),
            range: (u64::MAX, 0),
            offset: 0, // first 8 bytes show where the tail metadata of the batch is
            count: 0,
        }
    }

    pub fn insert(&mut self, id: u64, ts: u64, samples: &[SampleType]) -> Result<usize, usize> {
        let serialized_size = bincode::serialized_size(samples).unwrap() as usize;
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
            bincode::serialize_into(&mut self.buf[offset..offset + serialized_size], samples).unwrap();
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

    pub fn total_size(&self) -> usize {
        // total size calculated this way because all the metadata required could still exceed the
        // logically assigned batch size - even after compression. Loglically, batch size should
        // include the metadata
        self.offset + 8 + self.ids.len() * 8 + 16 // bytes in buf + number of ids + ids + range of batch
    }

    pub fn close(self) -> Box<[u8]> {

        let mut data = vec![0u8; 16];

        let size =
            lz4::compress_to_vec(&self.buf[..self.offset], &mut data, lz4::ACC_LEVEL_DEFAULT)
                .unwrap();

        // write offset of tail in first 8 bytes
        data[..8].copy_from_slice(&(16 + size).to_be_bytes());

        // write raw data size in second 8 bytes
        data[8..16].copy_from_slice(&self.offset.to_be_bytes());

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

pub struct BytesBatch {
    bytes: Box<[u8]>,
    raw_size: usize,
    tail: usize,
}

impl BytesBatch {
    pub fn new(bytes: Box<[u8]>) -> Self {
        let tail = usize::from_be_bytes(bytes[..8].try_into().unwrap());
        let raw_size = usize::from_be_bytes(bytes[8..16].try_into().unwrap());
        BytesBatch {
            bytes,
            raw_size,
            tail,
        }
    }

    pub fn metadata(&self) -> (HashSet<u64>, (u64, u64)) {
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

        let high = u64::from_be_bytes(tail[offset..offset+8].try_into().unwrap());

        (set, (low, high))
    }

    pub fn entries(&self) -> Vec<(u64, u64, Vec<SampleType>)> {
        let mut data = vec![0u8; self.raw_size];
        let _ = lz4::decompress(&self.bytes[16..self.tail], &mut data).unwrap();

        let mut result = Vec::new();

        let mut offset = 0;

        while offset < data.len() {
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
        let batch = BytesBatch::new(bytes);
        let (ids, range) = batch.metadata();
        let entries = batch.entries();

        assert_eq!(ids, expected_ids);
        assert_eq!(range, expected_range);
        for (entry, expected) in entries.iter().zip(expected_samples.iter()) {
            assert_eq!(entry.0, expected.0);
            assert_eq!(entry.1, expected.1);
            assert_eq!(entry.2.as_slice(), expected.2);
        }
    }
}
