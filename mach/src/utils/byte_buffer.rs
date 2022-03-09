use std::io;

pub struct ByteBuffer<'a> {
    buf: &'a mut [u8],
    len: usize,
}

impl<'a> ByteBuffer<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, len: 0 }
    }

    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.buf[self.len..self.len + slice.len()].copy_from_slice(slice);
        self.len += slice.len();
    }

    pub fn push(&mut self, v: u8) {
        self.buf[self.len] = v;
        self.len += 1;
    }

    pub fn unused(&mut self) -> &mut [u8] {
        &mut self.buf[self.len..]
    }

    pub fn add_len(&mut self, sz: usize) {
        self.len += sz;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buf
    }
}

impl<'a> io::Write for ByteBuffer<'a> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        if buf.len() > self.buf.len() - self.len {
            Err(io::Error::from(io::ErrorKind::UnexpectedEof))
        } else {
            self.extend_from_slice(buf);
            Ok(buf.len())
        }
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}
