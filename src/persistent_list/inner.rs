use crate::{
    compression::{ByteBuffer, Compression, DecompressBuffer},
    persistent_list::{ChunkReader, ChunkWriter, Error, FLUSH_THRESHOLD},
    segment::FullSegment,
    utils::wp_lock::*,
};
use std::{
    cell::UnsafeCell,
    convert::TryInto,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
    time::Duration,
};

const METASZ: usize = 32;

#[derive(Debug, Copy, Clone)]
struct SharedMeta {
    partition: usize,
    offset: usize,
}

impl SharedMeta {
    fn new() -> Self {
        SharedMeta {
            partition: usize::MAX,
            offset: usize::MAX,
        }
    }
}

#[derive(Debug, Copy, Clone)]
struct LocalMeta {
    offset: usize,
    bytes: usize,
}

impl LocalMeta {
    fn new() -> Self {
        LocalMeta {
            offset: usize::MAX,
            bytes: usize::MAX,
        }
    }
}

enum NextMeta {
    Static(Meta),
    Active(ActiveMeta),
}

#[derive(Debug, Copy, Clone)]
struct Meta {
    shared: SharedMeta,
    local: LocalMeta,
}

impl Meta {
    fn from_bytes(data: [u8; METASZ]) -> Self {
        Meta {
            shared: SharedMeta {
                partition: usize::from_be_bytes(data[0..8].try_into().unwrap()),
                offset: usize::from_be_bytes(data[8..16].try_into().unwrap()),
            },
            local: LocalMeta {
                offset: usize::from_be_bytes(data[16..24].try_into().unwrap()),
                bytes: usize::from_be_bytes(data[24..32].try_into().unwrap()),
            },
        }
    }

    fn read<R: ChunkReader>(
        &self,
        buf: &mut [u8],
        reader: &R,
    ) -> Result<Option<InnerReadNode>, Error> {
        let partition = self.shared.partition;
        let offset = self.shared.offset;
        let chunk_offset = self.local.offset;
        let bytes = self.local.bytes;
        unimplemented!();
        //if partition != usize::MAX || offset != usize::MAX {
        //    reader.read(partition, offset, chunk_offset, bytes, buf).unwrap();
        //}
    }
}

struct ActiveMeta {
    shared: Arc<WpLock<SharedMeta>>,
    buffer: Arc<UnsafeCell<InnerBuffer>>,
    local: LocalMeta,
}

impl ActiveMeta {
    // Safety: Unsafe because this is meant to be called in the InnerBuffer push method which
    // itself is supposed to be called without concurrent pushes or flushes
    unsafe fn to_bytes(&self) -> [u8; METASZ] {
        let shared = *(*self.shared).as_ref();
        let mut ret = [0u8; METASZ];
        ret[0..8].copy_from_slice(&shared.partition.to_be_bytes()[..]);
        ret[8..16].copy_from_slice(&shared.offset.to_be_bytes()[..]);
        ret[16..24].copy_from_slice(&self.local.offset.to_be_bytes()[..]);
        ret[24..32].copy_from_slice(&self.local.bytes.to_be_bytes()[..]);
        ret
    }

    // Safety: Unsafe because this is meant to be called in the during a read when we know that the
    // meta has already been flushed and updated
    unsafe fn to_meta(&self) -> Meta {
        let shared = *(*self.shared).as_ref();
        Meta {
            local: self.local,
            shared,
        }
    }

    fn read<R: ChunkReader>(&self, reader: &R) -> Result<Option<InnerReadNode>, Error> {
        println!("ActiveMeta reading at local: {:?}", self.local);
        // This local offset is the termination
        if self.local.offset == usize::MAX {
            return Ok(None);
        }

        // This will be blocked if the buffer referenced by this shared meta is in the process of
        // flushing
        let read_guard = unsafe { self.shared.read() };

        if read_guard.partition != usize::MAX || read_guard.offset != usize::MAX {
            // The buffer was flushed prior or concurrent to obtaining the Read Guard. Releasing
            // the read guard guarantees that the shared data has been completely updated during
            // the return
            let _ = read_guard.release();

            // Safety: releasing the guard means that this meta was updated during the flush (that
            // completed)
            let meta = unsafe { self.to_meta() };
            meta.read(reader)
        } else {
            println!("HERE");
            // Grab the buffer and copy the data
            let start = self.local.offset;
            let end = start + self.local.bytes;
            let buf = &unsafe { &self.buffer.get().as_ref().unwrap().buffer[start..end] };
            println!("start: {:?} end: {:?}", start, end);
            println!("buf: {:?}", buf);
            //println!("inner buffer: {:?}", &unsafe { &self.buffer.get().as_ref().unwrap().buffer[188..235] });
            let next_meta = Meta::from_bytes(buf[..METASZ].try_into().unwrap());
            let bytes: Box<[u8]> = buf[METASZ..].into();

            // Setup the next meta
            let next = if next_meta.shared.partition == usize::MAX
                && next_meta.shared.offset == usize::MAX
            {
                NextMeta::Active(ActiveMeta {
                    shared: self.shared.clone(),
                    buffer: self.buffer.clone(),
                    local: next_meta.local,
                })
            } else {
                NextMeta::Static(next_meta)
            };

            // Release the read guard. If the lock was taken concurrently, then these data are
            // invalid and return None
            match read_guard.release() {
                Ok(_) => Ok(Some(InnerReadNode { next, bytes })),
                Err(_) => Err(Error::InconsistentRead),
            }
        }
    }
}

pub struct InnerReadNode {
    next: NextMeta,
    bytes: Box<[u8]>,
}

impl InnerReadNode {
    pub fn next<R: ChunkReader>(&mut self, reader: &R) -> Result<Option<InnerReadNode>, Error> {
        match self.next {
            NextMeta::Active(meta) => meta.read(reader),
            NextMeta::Static(meta) => meta.read(reader),
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes[..]
    }
}

struct InnerBuffer {
    len: usize,
    buffer: Box<[u8]>,
}

impl InnerBuffer {
    fn new() -> Self {
        Self {
            len: 0,
            buffer: vec![0u8; FLUSH_THRESHOLD * 2].into_boxed_slice(),
        }
    }

    fn push_bytes(&mut self, bytes: &[u8], last_head: [u8; METASZ]) -> LocalMeta {
        let start = self.len;
        let mut offset = self.len;
        println!(
            "writing to offset {} n bytes {}",
            offset,
            bytes.len() + METASZ
        );
        // Serialize the head
        self.buffer[offset..offset + METASZ].copy_from_slice(&last_head[..]);
        offset += METASZ;
        self.buffer[offset..offset + bytes.len()].copy_from_slice(bytes);
        //println!("after writing: {:?}", self.buffer);
        offset += bytes.len();
        let bytes = offset - self.len;
        self.len = offset;

        println!("After writing Local meta offset: {} {}", offset, bytes);

        LocalMeta {
            offset: start,
            bytes: self.len - start,
        }
    }

    fn push_segment(
        &mut self,
        segment: &FullSegment,
        compress: Compression,
        last_head: [u8; METASZ],
    ) -> LocalMeta {
        // Remember where this section started
        let start = self.len;

        // Serialize the head
        self.buffer[self.len..self.len + METASZ].copy_from_slice(&last_head[..]);
        self.len += METASZ;

        // Compress the data into the buffer
        self.len += {
            let mut byte_buffer = ByteBuffer::new(&mut self.buffer[self.len..]);
            compress.compress(segment, &mut byte_buffer);
            byte_buffer.len()
        };

        LocalMeta {
            offset: start,
            bytes: self.len - start,
        }
    }
}

struct Buffer {
    meta: Arc<WpLock<SharedMeta>>,
    inner: Arc<UnsafeCell<InnerBuffer>>,
    atomic_len: AtomicUsize,
}

impl Buffer {
    fn new() -> Self {
        Self {
            meta: Arc::new(WpLock::new(SharedMeta::new())),
            inner: Arc::new(UnsafeCell::new(InnerBuffer::new())),
            atomic_len: AtomicUsize::new(0),
        }
    }

    fn push_bytes(&mut self, bytes: &[u8], last_head: &ActiveMeta) -> ActiveMeta {
        let inner = unsafe { self.inner.get().as_mut().unwrap() };
        let local = inner.push_bytes(bytes, unsafe { last_head.to_bytes() });
        println!("Local meta result: {:?}", local);
        self.atomic_len.store(inner.len, SeqCst);
        ActiveMeta {
            shared: self.meta.clone(),
            buffer: self.inner.clone(),
            local,
        }
    }

    // Safety: It is unsafe to have multiple concurrent pushers / flushers but it is safe to have concurrent
    // readers and a single pusher / flusher
    fn push_segment(
        &mut self,
        segment: &FullSegment,
        compress: Compression,
        last_head: &ActiveMeta,
    ) -> ActiveMeta {
        let inner = unsafe { self.inner.get().as_mut().unwrap() };
        let local = inner.push_segment(segment, compress, unsafe { last_head.to_bytes() });
        self.atomic_len.store(inner.len, SeqCst);
        ActiveMeta {
            shared: self.meta.clone(),
            buffer: self.inner.clone(),
            local,
        }
    }

    // Safety: It is unsafe to have multiple concurrent pushers / flushers but it is safe to have
    // concurrent readers and a single flusher / pusher
    fn flush<W: ChunkWriter>(&mut self, writer: &mut W) {
        println!("FLUSHING");
        let inner = unsafe { self.inner.get().as_mut().unwrap() };

        // Flush to storage
        let (partition, offset) = writer.write(&inner.buffer[..inner.len]).unwrap();
        println!("PARTITION: {}, OFFSET: {}", partition, offset);

        // Lock the meta from any other reader
        let meta = self.meta.clone();
        let mut write_guard = meta.write();

        // Update meta
        write_guard.partition = partition.try_into().unwrap();
        write_guard.offset = offset.try_into().unwrap();

        // Update the offsets in the buffer
        inner.len = 0;
        self.atomic_len.store(inner.len, SeqCst);

        self.meta = Arc::new(WpLock::new(SharedMeta::new()));

        // release the meta to concurrent readers of meta
    }
}

pub struct InnerHead {
    active_meta: ActiveMeta,
    buffer: Arc<Buffer>,
}

impl InnerHead {
    pub fn new() -> Self {
        let buffer = Arc::new(Buffer::new());
        let active_meta = ActiveMeta {
            shared: buffer.meta.clone(),
            buffer: buffer.inner.clone(),
            local: LocalMeta::new(),
        };

        InnerHead {
            active_meta,
            buffer,
        }
    }

    pub fn push_segment<W: ChunkWriter>(
        &mut self,
        segment: &FullSegment,
        compress: Compression,
        writer: &mut W,
    ) {
        // Safe because &mut here enforces that this is the only pusher/flusher, and Arc<Buffer>
        // will only be used in a single thread
        let buffer = unsafe { Arc::get_mut_unchecked(&mut self.buffer) };
        self.active_meta = buffer.push_segment(segment, compress, &self.active_meta);
        if self.active_meta.local.offset + self.active_meta.local.bytes > FLUSH_THRESHOLD {
            buffer.flush(writer);
        }
    }

    // generally only used in testing
    pub fn push_bytes<W: ChunkWriter>(&mut self, bytes: &[u8], writer: &mut W) {
        // Safe because &mut here enforces that this is the only pusher/flusher, and Arc<Buffer>
        // will only be used in a single thread
        let buffer = unsafe { Arc::get_mut_unchecked(&mut self.buffer) };
        self.active_meta = buffer.push_bytes(bytes, &self.active_meta);
        println!("Active meta results {:?}", self.active_meta.local);
        if self.active_meta.local.offset + self.active_meta.local.bytes > FLUSH_THRESHOLD {
            buffer.flush(writer);
            println!("Shared post flush: {:?}", &*self.active_meta.shared.write());
        }
    }

    pub fn read_segment<R: ChunkReader>(&self, reader: &R) -> Result<Option<InnerReadNode>, Error> {
        self.active_meta.read(reader)
    }
}
