#[derive(Debug)]
pub enum Error {
    HasTail,
}

pub struct FlushBuffer<const H: usize, const T: usize> {
    data: Vec<u8>,
    has_tail: bool,
}

impl<const H: usize, const T: usize> FlushBuffer<H, T> {
    pub fn new() -> Self {
        Self {
            data: vec![0; H],
            has_tail: false,
        }
    }

    pub fn header_mut(&mut self) -> &mut [u8] {
        &mut self.data[..H]
    }

    pub fn header(&self) -> &[u8] {
        &self.data[..H]
    }

    pub fn tail(&self) -> Option<&[u8]> {
        if self.has_tail {
            let start = self.data.len() - T;
            Some(&self.data[start..T])
        } else {
            None
        }
    }

    pub fn tail_mut(&mut self) -> Option<&mut [u8]> {
        if self.has_tail {
            let start = self.data.len() - T;
            Some(&mut self.data[start..T])
        } else {
            None
        }
    }

    pub fn data(&self) -> &[u8] {
        if self.has_tail {
            let end = self.data.len() - T;
            &self.data[H..end]
        } else {
            &self.data[H..]
        }
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        if self.has_tail {
            let end = self.data.len() - T;
            &mut self.data[H..end]
        } else {
            &mut self.data[H..]
        }
    }

    pub fn len(&self) -> usize {
        if self.has_tail {
            let end = self.data.len() - T;
            end - H
        } else {
            self.data.len() - H
        }
    }

    pub fn write_tail(&mut self, tail: [u8; T]) {
        self.data.extend_from_slice(&tail[..]);
        self.has_tail = true;
    }

    pub fn push_bytes(&mut self, data: &[u8]) -> Result<(), Error> {
        if self.has_tail {
            Err(Error::HasTail)
        } else {
            self.data.extend_from_slice(&data[..]);
            Ok(())
        }
    }

    pub fn push(&mut self, data: u8) -> Result<(), Error> {
        if self.has_tail {
            Err(Error::HasTail)
        } else {
            self.data.push(data);
            Ok(())
        }
    }

    pub fn pushable_vec(&mut self) -> Result<&mut Vec<u8>, Error> {
        if self.has_tail {
            Err(Error::HasTail)
        } else {
            Ok(&mut self.data)
        }
    }

    pub fn truncate(&mut self, offset: usize) {
        self.data.truncate(H + offset);
        self.has_tail = false;
    }
}
