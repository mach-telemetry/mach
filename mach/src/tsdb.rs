use crate::{
    //durability::*,
    id::*,
    //persistent_list::{self, List},
    //reader::ReadServer,
    series::{self, *},
    writer::{Writer, WriterConfig, WriterMetadata},
    //durable_queue::QueueConfig,
};
use dashmap::DashMap;
use rand::seq::SliceRandom;
use std::{collections::HashMap, sync::Arc};

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
    //writer_table: HashMap<WriterId, (WriterMetadata, DurabilityWorker)>,
    writer_table: HashMap<WriterId, WriterMetadata>,
    series_table: Arc<DashMap<SeriesId, Series>>,
}

impl Mach {
    pub fn new() -> Self {
        Mach {
            writers: Vec::new(),
            writer_table: HashMap::new(),
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
        self.writers.push(meta.id.clone());
        self.writer_table
            .insert(meta.id.clone(), meta);
            //.insert(meta.id.clone(), (meta, durability));
        Ok(writer)
    }

    pub fn add_series(&mut self, config: SeriesConfig) -> Result<(WriterId, SeriesId), Error> {
        // For now, randomly choose a writer
        let writer = self
            .writers
            .choose(&mut rand::thread_rng())
            .unwrap()
            .clone();
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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        writer::{Writer, WriterConfig},
        compression::*,
        test_utils::*,
        sample::Type,
        snapshot::Snapshot,
        mem_list::{BOOTSTRAPS, TOPIC},
        utils::kafka::BufferedConsumer,
    };
    use rand::{Rng, thread_rng};
    use std::time::Instant;


    #[test]
    fn end_to_end() {
        let mut mach = Mach::new();
        let writer_config = WriterConfig {
            active_block_flush_sz: 1_000
        };
        let mut writer = mach.add_writer(writer_config).unwrap();

        // Setup series
        const nvars: usize = 2;
        //let mut rng = thread_rng();
        //for item in data.iter_mut() {
        //    for val in item.values.iter_mut() {
        //        *val = rng.gen::<f64>() * 100.0f64;
        //    }
        //}
        let mut compression = Vec::new();
        for _ in 0..nvars {
            compression.push(CompressFn::Decimal(3));
        }
        let compression = Compression::from(compression);

        let series_conf = SeriesConfig {
            id: SeriesId(0),
            types: vec![Types::F64; nvars],
            compression,
            seg_count: 1,
            nvars,
        };
        let (writer_id, series_id) = mach.add_series(series_conf.clone()).unwrap();
        let series_ref = writer.get_reference(series_id);

        let mut expected_timestamps = Vec::new();
        let mut expected_values = Vec::new();
        let epoch = std::time::UNIX_EPOCH;
        let mut rng = thread_rng();
        for _ in 0..1_000_000 {
            let values: [Type; nvars] = [Type::F64(rng.gen()), Type::F64(rng.gen())];
            let time = epoch.elapsed().unwrap().as_micros() as u64;
            loop {
                match writer.push(series_ref, time, &values[..]) {
                    Ok(_) => break,
                    Err(_) => {}
                }
            }
            expected_timestamps.push(time);
            expected_values.push(values);
        }
        expected_timestamps.reverse();
        expected_values.reverse();

        let snapshot = mach.series_table.get(&series_conf.id).unwrap().value().snapshot();
        let bytes = bincode::serialize(&snapshot).unwrap();
        let snapshot: Snapshot = bincode::deserialize(bytes.as_slice()).unwrap();

        let consumer = BufferedConsumer::new(BOOTSTRAPS, TOPIC);

        let mut snapshot = snapshot.into_iterator(consumer);
        assert!(snapshot.next_segment().is_some());
        let seg = snapshot.get_segment();
    }
}
