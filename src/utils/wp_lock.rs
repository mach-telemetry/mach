use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
};

pub enum Error {
    InconsistentRead,
}

pub struct WriteGuard<'lock, T> {
    item: &'lock WpLock<T>,
}

impl<'lock, T> Deref for WriteGuard<'lock, T> {
    type Target = T;
    fn deref(&self) -> &T {
        let ptr = self.item.item.get();
        unsafe { ptr.as_ref().unwrap() }
    }
}

impl<'lock, T> DerefMut for WriteGuard<'lock, T> {
    fn deref_mut(&mut self) -> &mut T {
        let ptr = self.item.item.get();
        unsafe { ptr.as_mut().unwrap() }
    }
}

impl<'lock, T> Drop for WriteGuard<'lock, T> {
    fn drop(&mut self) {
        assert!(self.item.spin.swap(false, SeqCst));
    }
}

pub struct WpLock<T> {
    spin: AtomicBool,
    version: AtomicUsize,
    item: UnsafeCell<T>,
}

impl<T> WpLock<T> {
    pub fn new(item: T) -> Self {
        Self {
            spin: AtomicBool::new(false),
            version: AtomicUsize::new(0),
            item: UnsafeCell::new(item),
        }
    }

    pub unsafe fn get_ref(&self) -> &T {
        self.item.get().as_ref().unwrap()
    }

    pub unsafe fn get_mut_ref(&self) -> &mut T {
        self.item.get().as_mut().unwrap()
    }

    pub fn write(&self) -> WriteGuard<T> {
        assert!(!self.spin.swap(true, SeqCst)); // this MUST be false - one writer only
        self.version.fetch_add(1, SeqCst);
        WriteGuard { item: self }
    }

    // Safety: This is unsafe if the memory region that is accessed by the reader might be
    // dropped by a writer.
    pub unsafe fn read(&self) -> ReadGuard<T> {
        let v = self.version.load(SeqCst);
        while self.spin.load(SeqCst) {}
        ReadGuard {
            v,
            released: false,
            item: self,
        }
    }
}

pub struct ReadGuard<'lock, T> {
    item: &'lock WpLock<T>,
    v: usize,
    released: bool,
}

impl<'lock, T> ReadGuard<'lock, T> {
    pub fn release(mut self) -> Result<(), Error> {
        self.released = true;
        while self.item.spin.load(SeqCst) {}
        if self.v != self.item.version.load(SeqCst) {
            Err(Error::InconsistentRead)
        } else {
            Ok(())
        }
    }
}

impl<'lock, T> Drop for ReadGuard<'lock, T> {
    fn drop(&mut self) {
        assert!(self.released)
    }
}

impl<'lock, T> Deref for ReadGuard<'lock, T> {
    type Target = T;
    fn deref(&self) -> &T {
        let ptr = self.item.item.get();
        unsafe { ptr.as_ref().unwrap() }
    }
}

unsafe impl<T> Sync for WpLock<T> {}
unsafe impl<T> Send for WpLock<T> {}
