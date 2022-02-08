use crate::{
    compression::Compression,
    constants::*,
    id::{SeriesId, WriterId},
    persistent_list::{self, BackendOld, Buffer},
    tags::Tags,
    writer::{SeriesMetadata, Writer},
};
use dashmap::DashMap;
use std::{collections::HashMap, ops::Deref, sync::Arc};

pub enum Error {
    List(persistent_list::Error),
    WriterInit,
}

impl From<persistent_list::Error> for Error {
    fn from(item: persistent_list::Error) -> Self {
        Error::List(item)
    }
}

pub struct Mach<T: BackendOld> {
    backend: T,
    writers: usize,
    writer_table: HashMap<WriterId, T::Writer>,
    buffer_table: HashMap<WriterId, Buffer>,
    reader_table: HashMap<WriterId, T::Reader>,
    series_table: Arc<DashMap<SeriesId, SeriesMetadata>>,
    next_id: usize,
}

impl<T: BackendOld> Mach<T> {
    pub fn new(writers: usize, mut backend: T) -> Result<Self, Error> {
        let mut writer_table = HashMap::new();
        let mut reader_table = HashMap::new();
        let mut buffer_table = HashMap::new();
        for i in 0..writers {
            let (w, r) = backend.make_backend()?;
            writer_table.insert(WriterId(i), w);
            reader_table.insert(WriterId(i), r);
            buffer_table.insert(WriterId(i), Buffer::new(BUFSZ));
        }
        Ok(Mach {
            backend,
            writers,
            writer_table,
            reader_table,
            buffer_table,
            series_table: Arc::new(DashMap::new()),
            next_id: 0,
        })
    }

    pub fn add_writer(&mut self) -> Result<WriterId, Error> {
        let id = WriterId(self.writers);
        self.writers += 1;
        let (w, r) = self.backend.make_backend()?;
        self.writer_table.insert(id, w);
        self.reader_table.insert(id, r);
        Ok(id)
    }

    pub fn init_writer(&mut self, id: WriterId) -> Result<Writer, Error> {
        match self.writer_table.remove(&id) {
            Some(x) => Ok(Writer::new(self.series_table.clone(), x)),
            None => Err(Error::WriterInit),
        }
    }

    pub fn register(
        &mut self,
        tags: Tags,
        compression: Compression,
        seg_count: usize,
        nvars: usize,
    ) -> SeriesId {
        let id = self.next_id;
        self.next_id += 1;
        let writer = WriterId(id % self.writers);
        let series = SeriesMetadata::new(
            tags,
            seg_count,
            nvars,
            compression,
            self.buffer_table.get(&writer).unwrap().clone(),
        );
        let id = SeriesId(id);
        self.series_table.insert(id, series);
        id
    }
}
