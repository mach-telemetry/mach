use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    ops::{Deref, DerefMut},
};

pub struct WriteGuard<'lock, T: ReadCopy> {
    item: &'lock WpLock<T>
}

impl<'lock, T: ReadCopy> Deref for WriteGuard<'lock, T> {
    type Target=T;
    fn deref(&self) -> &T {
        let ptr = self.item.item.get();
        unsafe {
            ptr.as_ref().unwrap()
        }
    }
}

impl<'lock, T: ReadCopy> DerefMut for WriteGuard<'lock, T> {
    fn deref_mut(&mut self) -> &mut T {
        let ptr = self.item.item.get();
        unsafe {
            ptr.as_mut().unwrap()
        }
    }
}

impl<'lock, T: ReadCopy> Drop for WriteGuard<'lock, T> {
    fn drop(&mut self) {
        assert!(self.item.spin.swap(false, SeqCst));
    }
}

pub struct WpLock<T: ReadCopy> {
    spin: AtomicBool,
    version: AtomicUsize,
    item: UnsafeCell<T>
}

impl<T: ReadCopy> WpLock<T> {
    pub fn new(item: T) -> Self {
        Self {
            spin: AtomicBool::new(false),
            version: AtomicUsize::new(0),
            item: UnsafeCell::new(item),
        }
    }

    pub unsafe fn as_ref(&self) -> &T {
        self.item.get().as_ref().unwrap()
    }

    pub fn write(&self) -> WriteGuard<T> {
        assert!(!self.spin.swap(true, SeqCst)); // this MUST be false - one writer only
        self.version.fetch_add(1, SeqCst);
        WriteGuard {
            item: self
        }
    }

    pub fn read(&self) -> Option<T::Copied> {
        let v = self.version.load(SeqCst);
        while self.spin.load(SeqCst) {}
        let copy = unsafe { self.item.get().as_ref().unwrap().read_copy() };
        while self.spin.load(SeqCst) {}
        if v != self.version.load(SeqCst) {
            None
        } else {
            Some(copy)
        }
    }
}

pub trait ReadCopy {
    type Copied;
    fn read_copy(&self) -> Self::Copied;
}
