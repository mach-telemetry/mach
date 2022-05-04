use std::alloc::{alloc, alloc_zeroed, dealloc, Layout};
use std::convert::TryInto;
use std::mem::{align_of, size_of, ManuallyDrop};

/// This is a workaround to Box<[u8]> that stores the size of the slice in the heap.
/// The layout of Box<[u8]> stores the slice of data in the heap and a pointer and the size of the
/// slice in the stack. The pointer is * [u8] which results in an unsized pointer type and a
/// resulting fat pointer (*const [u8], usize). We can't convert a fat pointer to a [u8; 8] without
/// losing information;
///
/// Here, we store the size of the byte slice in the heap before the actual byte array so we only
/// need a *u8 which is sized and can convert into [u8; 8] without losing data (because it's in the
/// heap.
pub struct Bytes(*const u8);

impl Clone for Bytes {
    fn clone(&self) -> Self {
        Bytes::from_slice(self.bytes())
    }
}

impl Bytes {
    pub fn len(&self) -> usize {
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(self.0, 8) };
        usize::from_be_bytes(slice.try_into().unwrap())
    }

    pub fn bytes(&self) -> &[u8] {
        let sz = self.len();
        unsafe {
            let ptr = self.0.offset(8);
            std::slice::from_raw_parts(ptr, sz)
        }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        let sz = self.len();
        unsafe {
            let ptr = self.0.offset(8);
            std::slice::from_raw_parts_mut(ptr as *mut u8, sz)
        }
    }

    pub fn as_raw_bytes(&self) -> &[u8] {
        let len = self.len();
        unsafe { std::slice::from_raw_parts(self.0, len + size_of::<usize>()) }
    }

    pub fn from_slice(data: &[u8]) -> Self {
        let usz = size_of::<usize>();
        let len = data.len();
        let total_len = usz + len;
        let layout = Layout::from_size_align(total_len, align_of::<u8>()).unwrap();
        let ptr = unsafe { alloc(layout) };
        let sl = unsafe { std::slice::from_raw_parts_mut(ptr, total_len) };
        sl[..usz].copy_from_slice(&len.to_be_bytes());
        sl[usz..].copy_from_slice(data);
        Bytes(ptr)
    }

    pub fn zeros(len: usize) -> Self {
        let usz = size_of::<usize>();
        let total_len = usz + len;
        let layout = Layout::from_size_align(total_len, align_of::<u8>()).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };
        let sl = unsafe { std::slice::from_raw_parts_mut(ptr, total_len) };
        sl[..usz].copy_from_slice(&len.to_be_bytes());
        Bytes(ptr)
    }

    pub fn into_raw(self) -> *const u8 {
        let me = ManuallyDrop::new(self);
        me.0
    }

    /// # Safety
    /// make sure entry is a valid pointer from into_raw
    pub unsafe fn from_raw(p: *const u8) -> Self {
        Self(p)
    }

    pub fn into_sample_entry(self) -> [u8; 8] {
        (self.into_raw() as u64).to_be_bytes()
    }

    /// # Safety
    /// make sure entry is a valid pointer from into_sample_entry
    pub unsafe fn from_sample_entry(entry: [u8; 8]) -> Self {
        Self::from_raw(u64::from_be_bytes(entry) as *const u8)
    }

    pub fn from_raw_bytes(bytes: &[u8]) -> (Self, usize) {
        let usz = size_of::<usize>();
        let len = usize::from_be_bytes(bytes[..usz].try_into().unwrap());
        (Self::from_slice(&bytes[usz..len + usz]), usz + len)
    }
}

/// # Safety
/// Bytes is safe to send so long as the location is valid. Bytes should not be copy or clone to
/// prevent a copy from being made and any of the &mut methods being used
unsafe impl Send for Bytes {}

impl Drop for Bytes {
    fn drop(&mut self) {
        let usz = size_of::<usize>();
        let len = self.len();
        let total_len = usz + len;
        let layout = Layout::from_size_align(total_len, align_of::<u8>()).unwrap();
        unsafe { dealloc(self.0 as *mut u8, layout) };
    }
}
