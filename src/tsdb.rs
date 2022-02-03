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

#[derive(Copy, Debug, Clone, Eq, PartialEq, Hash)]
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
    pub tags: Tags,
}

#[derive(Debug)]
pub enum Error {
    List(persistent_list::Error),
    /// Err if Mach expects some writer to be initialized, but none could be found.
    NoWriter,
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

    pub fn add_writer(&mut self) -> Result<Writer, Error> {
        let id = WriterId(self.next_writer_id.fetch_add(1, Ordering::SeqCst));
        let (w, r) = self.backend.make_backend()?;

        let writer = Writer::new(id, self.series_table.clone(), w);
        self.reader_table.insert(id, r);
        self.buffer_table.insert(id, Buffer::new(BUFSZ));

        Ok(writer)
    }

    /// Register a new time series.
    pub fn register(&mut self, config: SeriesConfig) -> Result<(SeriesId, WriterId), Error> {
        let next_writer_id = self.next_writer_id.load(Ordering::SeqCst);
        if next_writer_id == 0 {
            return Err(Error::NoWriter);
        }

        let series_id = SeriesId(self.next_series_id.fetch_add(1, Ordering::SeqCst));
        let writer_id = WriterId(*series_id % (next_writer_id - 1));

        let buffer = self
            .buffer_table
            .get(&writer_id)
            .expect("buffer should be defined for an initialized writer");

        let series = SeriesMetadata::new(
            config.tags,
            config.seg_count,
            config.nvars,
            config.compression,
            buffer.clone(),
        );

        self.series_table.insert(series_id, series);

        Ok((series_id, writer_id))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use persistent_list::VectorBackend;

    #[test]
    fn test_cannot_add_series_if_no_writer() {
        let mut db = Mach::new(VectorBackend::new()).unwrap();

        match db.register(SeriesConfig {
            tags: Tags::new(),
            seg_count: 8,
            nvars: 8,
            compression: Compression::Fixed(10),
        }) {
            Ok(..) => panic!("should not be able to register time series"),
            Err(Error::NoWriter) => {}
            Err(..) => panic!("wrong error type"),
        }
    }
}
