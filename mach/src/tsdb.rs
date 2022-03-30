use crate::{
    durability::*,
    id::*,
    persistent_list::{self, List},
    reader::{ReadResponse, ReadServer},
    series::{self, *},
    writer::{Writer, WriterConfig, WriterMetadata},
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
    read_server: ReadServer,
}

impl Mach {
    pub fn new() -> Self {
        let series_table = Arc::new(DashMap::new());
        let read_server = ReadServer::new(series_table.clone());

        Mach {
            writers: Vec::new(),
            writer_table: HashMap::new(),
            series_table,
            read_server,
        }
    }

    pub fn add_writer(&mut self, writer_config: WriterConfig) -> Result<Writer, Error> {
        let global_meta = self.series_table.clone();
        let (writer, meta) = Writer::new(global_meta, writer_config);
        let durability = DurabilityWorker::new(meta.id.clone(), meta.active_block.clone());
        self.writer_table
            .insert(meta.id.clone(), (meta, durability));
        Ok(writer)
    }

    pub async fn read(&self, id: SeriesId) -> ReadResponse {
        self.read_server.read_request(id).await
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
        let series_id = config.tags.id();
        let series = Series::new(config, queue_config.clone(), list);
        durability.register_series(series.clone());
        self.series_table.insert(series_id, series);

        Ok((writer, series_id))
    }
}
