mod file;

use crate::{tags::Tags, SeriesMetadata};
use dashmap::DashMap;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc,
    },
};

pub enum Error {
    SeriesNotFound,
    SeriesReinitialized,
}

struct Metadata {
    writer: file::SeriesWriter,
    meta: Arc<SeriesMetadata>,
}

pub struct FileWriter {
    reference: Arc<DashMap<Tags, Arc<SeriesMetadata>>>, // communicate with Global
    id_map: HashMap<Tags, u64>,
    thread_id: u64,
    flush_worker: Arc<file::FlushWorker>,
    writers: Vec<Metadata>,
}

impl FileWriter {
    fn new(
        thread_id: Option<u64>,
        reference: Arc<DashMap<Tags, Arc<SeriesMetadata>>>,
        shared_file: Arc<AtomicU64>,
    ) -> Self {
        let flush_worker = Arc::new(file::FlushWorker::new(shared_file));
        let thread_id = match thread_id {
            Some(x) => x,
            None => std::thread::current().id().as_u64().get(),
        };
        FileWriter {
            reference,
            flush_worker,
            id_map: HashMap::new(),
            thread_id,
            writers: Vec::new(),
        }
    }

    fn init_series(&mut self, tags: Tags) -> Result<u64, Error> {
        let entry = self.id_map.entry(tags.clone());
        match entry {
            Entry::Occupied(_) => Err(Error::SeriesReinitialized),
            Entry::Vacant(x) => match self.reference.get(&tags) {
                Some(item) => {
                    if item.thread_id.load(SeqCst) == self.thread_id {
                        let id = self.writers.len();
                        let segment = item.segment.clone();
                        let chunk = item.chunk.clone();
                        let list = item.list.clone();
                        let flush_worker = self.flush_worker.clone();
                        let writer = file::SeriesWriter::new(
                            tags.clone(),
                            segment,
                            chunk,
                            list,
                            flush_worker,
                        );
                        let meta = Metadata {
                            writer,
                            meta: item.clone(),
                        };
                        self.writers.push(meta);
                        Ok(id as u64)
                    } else {
                        Err(Error::SeriesNotFound)
                    }
                }
                None => Err(Error::SeriesNotFound),
            },
        }
    }

    fn get_reference_id(&mut self, tags: Tags) -> Result<u64, Error> {
        //id_map.entry(tags).or_insert(self.reference.get(tags
        Err(Error::SeriesNotFound)
    }
}
