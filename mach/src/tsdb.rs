use crate::{
    durability::*,
    id::*,
    persistent_list::{self, List},
    reader::ReadServer,
    series::{self, *},
    writer::{Writer, WriterConfig, WriterMetadata},
    durable_queue::QueueConfig,
};
use dashmap::DashMap;
use rand::seq::SliceRandom;
use std::{collections::HashMap, sync::Arc};

#[derive(Debug)]
pub enum Error {
    PersistentList(persistent_list::Error),
    //Metadata(metadata::Error),
    Series(series::Error),
    Uknown,
}

impl From<persistent_list::Error> for Error {
    fn from(item: persistent_list::Error) -> Self {
        Error::PersistentList(item)
    }
}

impl From<series::Error> for Error {
    fn from(item: series::Error) -> Self {
        Error::Series(item)
    }
}

pub struct Mach {
    writers: Vec<WriterId>,
    writer_table: HashMap<WriterId, (WriterMetadata, DurabilityWorker)>,
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

    pub fn new_read_server(&self, config: QueueConfig) -> ReadServer {
        ReadServer::new(self.series_table.clone(), config)
    }

    pub fn add_writer(&mut self, writer_config: WriterConfig) -> Result<Writer, Error> {
        let global_meta = self.series_table.clone();
        let mut q = writer_config.queue_config.clone();
        match &mut q {
            QueueConfig::Kafka(x) => x.topic.push_str("_durability"),
            QueueConfig::File(x) => x.file.push_str("_durability"),
        }
        let (writer, meta) = Writer::new(global_meta, writer_config);
        let durability = DurabilityWorker::new(meta.id.clone(), meta.active_block.clone(), q);
        self.writers.push(meta.id.clone());
        self.writer_table
            .insert(meta.id.clone(), (meta, durability));
        Ok(writer)
    }

    pub fn add_series(&mut self, config: SeriesConfig) -> Result<(WriterId, SeriesId), Error> {
        // For now, randomly choose a writer
        let writer = self
            .writers
            .choose(&mut rand::thread_rng())
            .unwrap()
            .clone();
        let (writer_meta, durability) = self.writer_table.get(&writer).unwrap();

        let queue_config = writer_meta.queue_config.clone();
        let list = List::new(writer_meta.active_block.clone());
        let series_id = config.id;
        let series = Series::new(config, queue_config, list);
        durability.register_series(series.clone());
        self.series_table.insert(series_id, series);

        Ok((writer, series_id))
    }
}
