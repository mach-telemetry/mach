use crate::{
    compression2::{CompressFn, Compression},
    constants::*,
    id::{SeriesId, WriterId},
    persistent_list::{self, Buffer, PersistentListBackend},
    writer::{SeriesMetadata, Writer},
};
use dashmap::DashMap;
use std::{collections::HashMap, ops::Deref, sync::Arc};

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Clone)]
pub struct SeriesConfig {
    pub compression: Compression,
    pub seg_count: usize,
    pub nvars: usize,
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

pub struct Mach<T: PersistentListBackend> {
    backend: T,
    writer_table: HashMap<WriterId, T::Writer>,
    buffer_table: HashMap<WriterId, Buffer>,
    reader_table: HashMap<WriterId, T::Reader>,
    series_table: Arc<DashMap<SeriesId, SeriesMetadata>>,
    next_writer_id: AtomicUsize,
    next_series_id: AtomicU64,
}

impl<T: PersistentListBackend> Mach<T> {
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
            next_series_id: AtomicU64::new(0),
        })
    }

    pub fn add_writer(&mut self) -> Result<Writer, Error> {
        let id = WriterId(self.next_writer_id.fetch_add(1, Ordering::SeqCst));

        let writer = Writer::new(id, self.series_table.clone(), self.backend.writer()?);
        self.reader_table.insert(id, self.backend.reader()?);
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
        let writer_id = WriterId((*series_id) as usize % next_writer_id);

        let buffer = self
            .buffer_table
            .get(&writer_id)
            .expect("buffer should be defined for an initialized writer");

        let series = SeriesMetadata::new(
            series_id,
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

        let nvars = 8;
        let compression = {
            let mut v = Vec::new();
            for _ in 0..nvars {
                v.push(CompressFn::Decimal(3));
            }
            Compression::from(v)
        };

        match db.register(SeriesConfig {
            seg_count: 8,
            nvars,
            compression,
        }) {
            Ok(..) => panic!("should not be able to register time series"),
            Err(Error::NoWriter) => {}
            Err(..) => panic!("wrong error type"),
        }
    }
}
