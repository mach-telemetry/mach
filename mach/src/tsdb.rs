use crate::{
    //durability::*,
    id::*,
    //persistent_list::{self, List},
    //reader::ReadServer,
    series::{self, *},
    //durable_queue::QueueConfig,
    snapshotter::Snapshotter,
    writer::{Writer, WriterConfig, WriterMetadata},
};
use dashmap::DashMap;
use rand::seq::SliceRandom;
use std::sync::Arc;

#[derive(Debug)]
pub enum Error {
    //PersistentList(persistent_list::Error),
    //Metadata(metadata::Error),
    Series(series::Error),
    Uknown,
}

//impl From<persistent_list::Error> for Error {
//    fn from(item: persistent_list::Error) -> Self {
//        Error::PersistentList(item)
//    }
//}

impl From<series::Error> for Error {
    fn from(item: series::Error) -> Self {
        Error::Series(item)
    }
}

pub struct Mach {
    writers: Vec<WriterId>,
    writer_table: Arc<DashMap<WriterId, WriterMetadata>>,
    series_table: Arc<DashMap<SeriesId, Series>>,
}

impl Mach {
    pub fn new() -> Self {
        Mach {
            writers: Vec::new(),
            writer_table: Arc::new(DashMap::new()),
            series_table: Arc::new(DashMap::new()),
        }
    }

    //pub fn new_read_server(&self, config: QueueConfig) -> ReadServer {
    //    ReadServer::new(self.series_table.clone(), config)
    //}

    pub fn add_writer(&mut self, writer_config: WriterConfig) -> Result<Writer, Error> {
        let global_meta = self.series_table.clone();
        //let mut q = writer_config.queue_config.clone();
        //match &mut q {
        //    QueueConfig::Kafka(x) => x.topic.push_str("_durability"),
        //    QueueConfig::File(x) => x.file.push_str("_durability"),
        //    QueueConfig::Noop => {} ,
        //}
        let (writer, meta) = Writer::new(global_meta, writer_config);
        //let durability = DurabilityWorker::new(meta.id.clone(), meta.block_list.clone(), q);
        self.writers.push(meta.id);
        self.writer_table.insert(meta.id, meta);
        //.insert(meta.id.clone(), (meta, durability));
        Ok(writer)
    }

    pub fn add_series(&mut self, config: SeriesConfig) -> Result<(WriterId, SeriesId), Error> {
        // For now, randomly choose a writer
        let writer = *self.writers.choose(&mut rand::thread_rng()).unwrap();
        //let (writer_meta, durability) = self.writer_table.get(&writer).unwrap();
        let writer_meta = self.writer_table.get(&writer).unwrap();

        let series_id = config.id;
        let block_list = writer_meta.block_list.clone();
        let source_block_list = block_list.add_source(series_id);
        let series = Series::new(config, block_list, source_block_list);
        //durability.register_series(series.clone());
        self.series_table.insert(series_id, series);

        Ok((writer, series_id))
    }

    pub fn series_table(&self) -> Arc<DashMap<SeriesId, Series>> {
        self.series_table.clone()
    }

    pub fn init_snapshotter(&self) -> Snapshotter {
        Snapshotter::new(self.series_table.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        compression::*,
        //mem_list::{BOOTSTRAPS, TOPIC},
        //test_utils::*,
        sample::SampleType,
        snapshot::Snapshot,
        writer::WriterConfig,
    };
    use rand::{thread_rng, Rng};

    #[test]
    fn end_to_end() {
        let mut mach = Mach::new();
        let writer_config = WriterConfig {
            active_block_flush_sz: 1_000_000,
        };
        let mut writer = mach.add_writer(writer_config).unwrap();

        // Setup series
        const NVARS: usize = 1;
        //let mut rng = thread_rng();
        //for item in data.iter_mut() {
        //    for val in item.values.iter_mut() {
        //        *val = rng.gen::<f64>() * 100.0f64;
        //    }
        //}
        let compression = Compression::from(vec![CompressFn::BytesLZ4]);
        let series_conf = SeriesConfig {
            id: SeriesId(0),
            types: vec![FieldType::Bytes],
            compression,
            seg_count: 1,
            nvars: NVARS,
        };
        let (_writer_id, series_id) = mach.add_series(series_conf.clone()).unwrap();
        let series_ref = writer.get_reference(series_id);

        let mut expected_timestamps = Vec::new();
        let mut expected_values = Vec::new();
        let epoch = std::time::UNIX_EPOCH;
        let mut rng = thread_rng();
        println!("PUSHING");
        for _ in 0..10_000_000 {
            let mut v = vec![0; 400];
            rand::thread_rng().fill(&mut v[..]);
            let values: [SampleType; NVARS] = [SampleType::Bytes(v.clone())];
            let time = epoch.elapsed().unwrap().as_micros() as u64;
            loop {
                match writer.push(series_ref, time, &values[..]) {
                    Ok(_) => break,
                    Err(_) => {}
                }
            }
            expected_timestamps.push(time);
            expected_values.push(v);
        }
        expected_timestamps.reverse();
        expected_values.reverse();

        //std::thread::sleep(std::time::Duration::from_secs(1));

        //let start = std::time::SystemTime::now() - std::time::Duration::from_secs(120);
        //let dur = start
        //    .duration_since(std::time::UNIX_EPOCH)
        //    .unwrap()
        //    .as_millis()
        //    .try_into()
        //    .unwrap();
        //let mut consumer = BufferedConsumer::new(BOOTSTRAPS, TOPIC);

        let snapshot = mach
            .series_table
            .get(&series_conf.id)
            .unwrap()
            .value()
            .snapshot();
        let bytes = bincode::serialize(&snapshot).unwrap();
        let snapshot: Snapshot = bincode::deserialize(bytes.as_slice()).unwrap();

        let mut snapshot = snapshot.into_iterator();

        let mut result_timestamps = Vec::new();
        let mut result_field0: Vec<Vec<u8>> = Vec::new();
        let mut last_timestamp = u64::MAX;
        let mut seg_count = 0;
        let mut last_segment = usize::MAX;
        let now = epoch.elapsed().unwrap().as_micros() as u64;
        'segment: while let Some(_) = snapshot.next_segment_at_timestamp(now) {
            let seg = snapshot.get_segment();
            if last_segment == usize::MAX {
                last_segment = seg.segment_id;
            } else {
                assert_eq!(last_segment, seg.segment_id + 1);
                last_segment = seg.segment_id;
            }
            seg_count += 1;
            let mut timestamps = seg.timestamps().iterator();
            let mut field0 = seg.field(0).iterator();
            while let Some(x) = timestamps.next_timestamp() {
                if x > last_timestamp {
                    continue 'segment;
                }
                result_timestamps.push(x);
                last_timestamp = x;
            }
            while let Some(x) = field0.next_item() {
                result_field0.push(x.as_bytes().into());
            }
        }
        println!("seg count: {}", seg_count);

        assert_eq!(result_timestamps.len(), expected_timestamps.len());
        assert_eq!(&result_timestamps, &expected_timestamps);
        for (a, b) in result_field0.iter().zip(expected_values.iter()) {
            assert_eq!(&a[..], &b[..]);
            //if (a - b).abs() > 0.001 {
            //    panic!("{} - {} = {}", a, b, (a - b).abs());
            //}
        }
    }
}
