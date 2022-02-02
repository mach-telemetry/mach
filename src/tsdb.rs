use crate::{
    compression::Compression,
    constants::*,
    persistent_list::{self, Backend, Buffer},
    tags::Tags,
    writer::{SeriesMetadata, Writer, WriterId},
};
use dashmap::DashMap;
use std::{
    collections::HashMap,
    ops::Deref,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
};

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct SeriesId(pub usize);

impl Deref for SeriesId {
    type Target = usize;
    fn deref(&self) -> &usize {
        &self.0
    }
}

impl SeriesId {
    pub fn inner(&self) -> usize {
        self.0
    }
}

#[derive(Clone)]
pub struct SeriesConfig {
    pub compression: Compression,
    pub seg_count: usize,
    pub nvars: usize,
}

#[derive(Debug)]
pub enum Error {
    List(persistent_list::Error),
    /// This API can only be called with a writer_id returned from `add_writer`.
    WriterInit,
}

impl From<persistent_list::Error> for Error {
    fn from(item: persistent_list::Error) -> Self {
        Error::List(item)
    }
}

pub struct Mach<T: Backend> {
    backend: T,
    writer_table: HashMap<WriterId, T::Writer>,
    buffer_table: HashMap<WriterId, Buffer>,
    reader_table: HashMap<WriterId, T::Reader>,
    series_table: Arc<DashMap<SeriesId, SeriesMetadata>>,
    next_writer_id: AtomicUsize,
    next_series_id: AtomicUsize,
}

impl<T: Backend> Mach<T> {
    pub fn new(mut backend: T) -> Result<Self, Error> {
        let mut writer_table = HashMap::new();
        let mut reader_table = HashMap::new();
        let mut buffer_table = HashMap::new();

        Ok(Mach {
            backend,
            writer_table,
            reader_table,
            buffer_table,
            series_table: Arc::new(DashMap::new()),
            next_writer_id: AtomicUsize::new(0),
            next_series_id: AtomicUsize::new(0),
        })
    }

    /// Register a writer to Mach.
    pub fn add_writer(&mut self) -> Result<WriterId, Error> {
        let id = WriterId(self.next_writer_id.fetch_add(1, Ordering::SeqCst));
        let (w, r) = self.backend.make_backend()?;
        self.writer_table.insert(id, w);
        self.reader_table.insert(id, r);
        self.buffer_table.insert(id, Buffer::new(BUFSZ));
        Ok(id)
    }

    pub fn init_writer(&mut self, id: WriterId) -> Result<Writer, Error> {
        match self.writer_table.remove(&id) {
            Some(backend_Writer) => Ok(Writer::new(id, self.series_table.clone(), backend_Writer)),
            None => Err(Error::WriterInit),
        }
    }

    /// Register a new time series.
    pub fn register(
        &mut self,
        writer_id: WriterId,
        tags: Tags,
        config: SeriesConfig,
    ) -> Result<SeriesId, Error> {
        match self.buffer_table.get(&writer_id) {
            None => Err(Error::WriterInit),
            Some(buffer) => {
                let series = SeriesMetadata::new(
                    tags,
                    config.seg_count,
                    config.nvars,
                    config.compression,
                    buffer.clone(),
                );

                let id = self.next_series_id.fetch_add(1, Ordering::SeqCst);
                let id = SeriesId(id);

                self.series_table.insert(id, series);

                Ok(id)
            }
        }
    }
}
