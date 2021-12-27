use crate::{
    persistent_list::{Error, FLUSH_THRESHOLD},
    utils::wp_lock::WpLock,
};
use std::{
    cell::UnsafeCell,
    convert::TryInto,
    marker::PhantomData,
    mem,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
struct PersistentHead {
    partition: usize,
    offset: usize,
}

impl PersistentHead {
    fn new() -> Self {
        PersistentHead {
            partition: usize::MAX,
            offset: usize::MAX,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { mem::transmute::<&Self, &[u8; mem::size_of::<Self>()]>(self) }
    }

    fn from_bytes(bytes: [u8; Self::size()]) -> Self {
        unsafe { mem::transmute::<[u8; Self::size()], Self>(bytes) }
    }

    const fn size() -> usize {
        mem::size_of::<Self>()
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct BufferHead {
    offset: usize,
    size: usize,
}

impl BufferHead {
    fn new() -> Self {
        BufferHead {
            offset: usize::MAX,
            size: usize::MAX,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { mem::transmute::<&Self, &[u8; mem::size_of::<Self>()]>(self) }
    }

    fn from_bytes(bytes: [u8; Self::size()]) -> Self {
        unsafe { mem::transmute::<[u8; Self::size()], Self>(bytes) }
    }

    const fn size() -> usize {
        mem::size_of::<Self>()
    }
}

#[derive(Clone)]
struct Head {
    persistent: Arc<WpLock<PersistentHead>>,
    buffer: BufferHead,
}

impl Head {
    fn new() -> Self {
        Self {
            persistent: Arc::new(WpLock::new(PersistentHead::new())),
            buffer: BufferHead::new(),
        }
    }

    fn bytes(&self) -> [u8; Self::size()] {
        let persistent = *self.persistent.write();
        let mut bytes = [0u8; Self::size()];
        bytes[..PersistentHead::size()].copy_from_slice(persistent.as_bytes());
        bytes[BufferHead::size()..].copy_from_slice(self.buffer.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8; Self::size()]) -> Self {
        let ps = PersistentHead::size();
        let bs = BufferHead::size();
        let persistent = PersistentHead::from_bytes(bytes[..ps].try_into().unwrap());
        let buffer = BufferHead::from_bytes(bytes[ps..ps + bs].try_into().unwrap());
        Self {
            persistent: Arc::new(WpLock::new(persistent)),
            buffer,
        }
    }

    const fn size() -> usize {
        PersistentHead::size() + BufferHead::size()
    }
}

struct InnerBuffer {
    persistent_head: Arc<WpLock<PersistentHead>>,
    len: AtomicUsize,
    buffer: Box<[u8]>,
}

impl InnerBuffer {
    fn new() -> Self {
        Self {
            persistent_head: Arc::new(WpLock::new(PersistentHead::new())),
            len: AtomicUsize::new(0),
            buffer: vec![0u8; FLUSH_THRESHOLD * 2].into_boxed_slice(),
        }
    }

    fn push_bytes(&mut self, bytes: &[u8], last_head: &Head) -> Head {
        //println!("\nLast Head written to this push");
        //println!("{:?}", *unsafe {last_head.persistent.get_ref()});
        //println!("{:?}\n", last_head.buffer);
        let len = self.len.load(SeqCst);
        let mut offset = len;

        let end = offset + Head::size();
        self.buffer[offset..end].copy_from_slice(&last_head.bytes());
        offset = end;

        let end = offset + bytes.len();
        self.buffer[offset..end].copy_from_slice(bytes);
        offset = end;

        self.len.store(offset, SeqCst);

        Head {
            persistent: self.persistent_head.clone(),
            buffer: BufferHead {
                offset: len,
                size: offset - len,
            },
        }
    }

    fn flush<W: ChunkWriter>(&mut self, flusher: &mut W) {
        let head = flusher
            .write(&self.buffer[..self.len.load(SeqCst)])
            .unwrap();
        let mut guard = self.persistent_head.write();
        *guard = head;
        drop(guard);
        self.persistent_head = Arc::new(WpLock::new(PersistentHead::new()));
    }

    fn reset(&self) {
        self.len.store(0, SeqCst);
    }

    fn read(&self, offset: usize, bytes: usize) -> Box<[u8]> {
        self.buffer[offset..offset + bytes].into()
    }
}

#[derive(Clone)]
struct Buffer {
    inner: Arc<WpLock<InnerBuffer>>,
}

impl Buffer {
    fn new() -> Self {
        Buffer {
            inner: Arc::new(WpLock::new(InnerBuffer::new())),
        }
    }

    fn read(&self, offset: usize, bytes: usize) -> Result<Box<[u8]>, Error> {
        let guard = unsafe { self.inner.read() };
        let res = guard.read(offset, bytes);
        match guard.release() {
            Ok(()) => Ok(res),
            Err(_) => Err(Error::InconsistentRead),
        }
    }

    fn push_bytes<W: ChunkWriter>(&mut self, bytes: &[u8], last_head: &Head, w: &mut W) -> Head {
        // Safety: The push_bytes function does not race with readers
        let head = unsafe { self.inner.get_mut_ref().push_bytes(bytes, last_head) };
        if head.buffer.offset + head.buffer.size > FLUSH_THRESHOLD {
            //println!("FLUSHING");
            // Need to guard here because reset will conflict with concurrent readers
            let mut guard = self.inner.write();
            guard.flush(w);
            guard.reset();
        }
        head
    }
}

struct InnerList {
    head: Head,
    buffer: Buffer,
}

impl InnerList {
    fn new(buffer: Buffer) -> Self {
        let head = Head::new();
        Self { head, buffer }
    }

    fn push_bytes<W: ChunkWriter>(&mut self, bytes: &[u8], w: &mut W) {
        self.head = self.buffer.push_bytes(bytes, &self.head, w);
    }
}

struct List {
    inner: Arc<WpLock<InnerList>>,
}

impl List {
    fn new(buffer: Buffer) -> Self {
        List {
            inner: Arc::new(WpLock::new(InnerList::new(buffer))),
        }
    }

    fn push_bytes<W: ChunkWriter>(&self, bytes: &[u8], w: &mut W) {
        self.inner.write().push_bytes(bytes, w);
    }

    fn reader(&self) -> Result<ListReader, Error> {
        // Safety: safe because none of the write operations dealloc memory. Protect the list from
        // concurrent writers
        let guard = unsafe { self.inner.read() };
        let head = guard.head.clone();
        let buff = guard.buffer.clone();
        match guard.release() {
            Ok(()) => Ok(ListReader {
                head,
                buff,
                local_buf: Vec::new(),
                last_persistent: None,
            }),
            Err(_) => Err(Error::InconsistentRead),
        }
    }
}

struct ListReader {
    head: Head,
    buff: Buffer,
    last_persistent: Option<PersistentHead>,
    local_buf: Vec<u8>,
}

impl ListReader {
    fn next<R: ChunkReader>(&mut self, reader: &mut R) -> Result<Option<&[u8]>, Error> {
        if self.head.buffer.offset == usize::MAX {
            return Ok(None);
        }
        self.local_buf.clear();

        // Try to copy the persistent head
        let persistent = unsafe {
            let guard = self.head.persistent.read();
            let persistent = *guard;
            match guard.release() {
                Ok(()) => Ok(persistent),
                Err(_) => Err(Error::InconsistentRead),
            }
        }?;

        //println!("\nReading the head:");
        //println!("{:?}", persistent);
        //println!("{:?}\n", self.head.buffer);

        // last persistent is unset, try to copy from the local buffer
        if self.last_persistent.is_none() && persistent.offset == usize::MAX {
            let offset = self.head.buffer.offset;
            let bytes = self.head.buffer.size;
            self.local_buf
                .extend_from_slice(&*self.buff.read(offset, bytes)?);
        }
        // last persistent is set and the persistent data is unset, then must be in the same
        // partition as the last persistent
        else if self.last_persistent.is_some() && persistent.offset == usize::MAX {
            let persistent = *self.last_persistent.as_ref().unwrap();
            self.local_buf
                .extend_from_slice(reader.read(persistent, self.head.buffer))
        }
        // persistent offset has a value, means need to get the next persistent
        else if persistent.offset != usize::MAX {
            self.last_persistent = Some(persistent);
            self.local_buf
                .extend_from_slice(reader.read(persistent, self.head.buffer))
        }

        // get the next head
        self.head = Head::from_bytes(self.local_buf[..Head::size()].try_into().unwrap());

        //println!("\nNext head to read:");
        //println!("{:?}", *unsafe { self.head.persistent.get_ref() } );
        //println!("{:?}\n", self.head.buffer);

        // return item
        Ok(Some(&self.local_buf[Head::size()..]))
    }
}

trait ChunkWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error>;
}

trait ChunkReader {
    fn read(&mut self, persistent: PersistentHead, local: BufferHead) -> &[u8];
}

struct VectorReader {
    inner: Arc<Mutex<Vec<Box<[u8]>>>>,
    local_copy: Vec<u8>,
    current_head: Option<PersistentHead>,
}

impl VectorReader {
    fn new(inner: Arc<Mutex<Vec<Box<[u8]>>>>) -> Self {
        Self {
            inner,
            local_copy: Vec::new(),
            current_head: None,
        }
    }
}

impl ChunkReader for VectorReader {
    fn read(&mut self, persistent: PersistentHead, local: BufferHead) -> &[u8] {
        //println!("Reading in VectorReader");
        //println!("{:?}", self.current_head);
        if self.current_head.is_none() || *self.current_head.as_ref().unwrap() != persistent {
            self.current_head = Some(persistent);
            self.local_copy.clear();
            let mut guard = self.inner.lock().unwrap();
            //println!("Reading data of len {}", guard[persistent.offset].len());
            self.local_copy
                .extend_from_slice(&*guard[persistent.offset]);
        }
        //println!("DONE");
        &self.local_copy[local.offset..local.offset + local.size]
    }
}

#[derive(Clone)]
struct VectorWriter {
    inner: Arc<Mutex<Vec<Box<[u8]>>>>,
}

impl VectorWriter {
    fn new(inner: Arc<Mutex<Vec<Box<[u8]>>>>) -> Self {
        VectorWriter { inner }
    }
}

impl ChunkWriter for VectorWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<PersistentHead, Error> {
        let mut guard = self.inner.lock().unwrap();
        let offset = guard.len();
        //println!("writing bytes of length {}", bytes.len());
        guard.push(bytes.into());
        let head = PersistentHead {
            partition: usize::MAX,
            offset,
        };
        Ok(head)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_single() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut vec_writer = VectorWriter::new(vec.clone());
        let mut vec_reader = VectorReader::new(vec.clone());
        let buffer = Buffer::new();
        let list = List::new(buffer.clone());
        let data: Vec<Vec<u8>> = (0..5).map(|x| vec![x; 15]).collect();
        data.iter()
            .for_each(|x| list.push_bytes(x.as_slice(), &mut vec_writer));

        let mut reader = list.reader().unwrap();
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data[4], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data[3], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data[2], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data[1], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data[0], res);
    }

    #[test]
    fn test_many() {
        let vec = Arc::new(Mutex::new(Vec::new()));
        let mut vec_writer = VectorWriter::new(vec.clone());
        let mut vec_reader = VectorReader::new(vec.clone());
        let buffer = Buffer::new();
        let list1 = List::new(buffer.clone());
        let list2 = List::new(buffer.clone());

        let data1: Vec<Vec<u8>> = (0..5).map(|x| vec![x; 15]).collect();
        let data2: Vec<Vec<u8>> = (5..10).map(|x| vec![x; 15]).collect();

        list1.push_bytes(data1[0].as_slice(), &mut vec_writer);
        list1.push_bytes(data1[1].as_slice(), &mut vec_writer);
        list2.push_bytes(data2[0].as_slice(), &mut vec_writer);
        // Should have flushed in the last push
        list1.push_bytes(data1[2].as_slice(), &mut vec_writer);
        list2.push_bytes(data2[1].as_slice(), &mut vec_writer);
        list2.push_bytes(data2[2].as_slice(), &mut vec_writer);
        // Should have flushed in the last push
        list1.push_bytes(data1[3].as_slice(), &mut vec_writer);
        list1.push_bytes(data1[4].as_slice(), &mut vec_writer);
        list2.push_bytes(data2[3].as_slice(), &mut vec_writer);
        // Should have flushed in the last push
        list2.push_bytes(data2[4].as_slice(), &mut vec_writer);

        let mut reader = list1.reader().unwrap();
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data1[4], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data1[3], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data1[2], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data1[1], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data1[0], res);

        let mut reader = list2.reader().unwrap();
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data2[4], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data2[3], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data2[2], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data2[1], res);
        let res = reader.next(&mut vec_reader).unwrap().unwrap();
        assert_eq!(data2[0], res);
    }
}
