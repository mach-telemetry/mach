use crate::{
    compression2::*,
    persistent_list::{self, ListBuffer, ListBackend},
    id::*,
    writer::Writer,
    series::*,
    constants::BUFSZ,
};
use std::{
    marker::PhantomData,
    collections::HashMap,
    sync::Arc,
};
use dashmap::DashMap;
use rand::seq::SliceRandom;

pub enum Error {
    PersistentList(persistent_list::Error),
    Uknown,
}

impl From<persistent_list::Error> for Error {
    fn from(item: persistent_list::Error) -> Self {
        Error::PersistentList(item)
    }
}

pub struct Mach<B: ListBackend> {
    writers: Vec<WriterId>,
    writer_table: HashMap<WriterId, (ListBuffer, B)>,
    series_table: Arc<DashMap<SeriesId, SeriesMetadata>>,
}

impl<B: ListBackend> Mach<B> {
    pub fn new() -> Self {
        Mach {
            writers: Vec::new(),
            writer_table: HashMap::new(),
            series_table: Arc::new(DashMap::new()),
        }
    }

    fn new_writer(&mut self) -> Result<Writer, Error> {
        let backend: B = B::default_backend()?;
        let buffer = ListBuffer::new(BUFSZ);
        let writer_id = WriterId::random();
        let writer = Writer::new(self.series_table.clone(), backend.writer()?);
        self.writer_table.insert(writer_id.clone(), (buffer, backend));
        self.writers.push(writer_id);
        // TODO Register metadata here
        Ok(writer)
    }

    fn add_series(&mut self, config: SeriesConfig) -> Result<WriterId, Error> {
        // For now, randomly choose a writer
        let writer = self.writers.choose(&mut rand::thread_rng()).unwrap().clone();
        let writer_meta = self.writer_table.get(&writer).unwrap();
        let buffer = writer_meta.0.clone();
        let id = config.tags.id();
        let series = SeriesMetadata::new(config, buffer);
        self.series_table.insert(id, series);
        Ok(writer)
    }
}
