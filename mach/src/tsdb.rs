use crate::{
    compression::*,
    constants::BUFSZ,
    id::*,
    persistent_list::{self, ListBackend, ListBuffer},
    reader::Snapshot,
    //metadata::{self, Metadata},
    series::{self, *},
    writer::Writer,
    durability::*,
};
use dashmap::DashMap;
use rand::seq::SliceRandom;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

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

pub struct Mach<B: ListBackend> {
    writers: Vec<WriterId>,
    writer_table: HashMap<WriterId, (ListBuffer, B, DurabilityHandle)>,
    series_table: Arc<DashMap<SeriesId, Series>>,
}

impl<B: ListBackend> Mach<B> {
    pub fn new() -> Self {
        Mach {
            writers: Vec::new(),
            writer_table: HashMap::new(),
            series_table: Arc::new(DashMap::new()),
        }
    }

    pub fn reader(&self, id: SeriesId) -> Result<Snapshot, Error> {
        Ok(self.series_table.get(&id).unwrap().snapshot()?)
    }

    pub fn new_writer(&mut self) -> Result<Writer, Error> {
        let writer_id = WriterId::random();

        // Setup persistent list backend for this writer
        let backend: B = B::default_backend()?;
        let backend_writer = backend.writer()?;
        let backend_id: String = backend.id().into();

        // Send metadata to metadata store
        //Metadata::WriterTopic(writer_id.inner().into(), backend_id).send()?;

        //  Setup ListBuffer for this writer
        let buffer = ListBuffer::new(BUFSZ);
        let writer = Writer::new(writer_id.clone(), self.series_table.clone(), backend_writer);
        let durability_handle = DurabilityHandle::new(writer_id.as_str(), buffer.clone());

        // Store writer information
        self.writer_table
            .insert(writer_id.clone(), (buffer, backend, durability_handle));
        self.writers.push(writer_id);
        Ok(writer)
    }

    pub fn add_series(&mut self, config: SeriesConfig) -> Result<(WriterId, SeriesId), Error> {
        // For now, randomly choose a writer
        let writer = self
            .writers
            .choose(&mut rand::thread_rng())
            .unwrap()
            .clone();

        // Get the ListBuffer for this series from the writer this series will be assigned to
        let writer_meta = self.writer_table.get(&writer).unwrap();
        let buffer = writer_meta.0.clone();

        // Initialize the series using the listbuffer for the assigned writer
        let series_id = config.tags.id();
        let series = Series::new(config, buffer);
        writer_meta.2.register_series(series.clone());
        self.series_table.insert(series_id, series);
        Ok((writer, series_id))
    }

