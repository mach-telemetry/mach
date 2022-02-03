use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
};

/// SAFETY: Impl only if the struct has ZERO deallocations during &mut T. For example, Vec CANNOT
/// impl this trait because Vec may dealloc when growing
pub unsafe trait NoDealloc {}

pub enum Error {
    InconsistentRead,
}

pub struct WriteGuard<'lock, T: NoDealloc> {
    item: &'lock WpLock<T>,
}

impl<'lock, T: NoDealloc> Deref for WriteGuard<'lock, T> {
    type Target = T;
    fn deref(&self) -> &T {
        let ptr = self.item.item.get();
        unsafe { ptr.as_ref().unwrap() }
    }
}

impl<'lock, T: NoDealloc> DerefMut for WriteGuard<'lock, T> {
    fn deref_mut(&mut self) -> &mut T {
        let ptr = self.item.item.get();
        unsafe { ptr.as_mut().unwrap() }
    }
}

impl<'lock, T: NoDealloc> Drop for WriteGuard<'lock, T> {
    fn drop(&mut self) {
        assert!(self.item.spin.swap(false, SeqCst));
    }
}

pub struct WpLock<T: NoDealloc> {
    spin: AtomicBool,
    version: AtomicUsize,
    item: UnsafeCell<T>,
}

impl<T: NoDealloc> WpLock<T> {
    pub fn new(item: T) -> Self {
        Self {
            spin: AtomicBool::new(false),
            version: AtomicUsize::new(0),
            item: UnsafeCell::new(item),
        }
    }

    /// Safety: Ensure that access to T does not race with write or get_mut_ref
    pub unsafe fn unprotected_read(&self) -> &T {
        self.item.get().as_ref().unwrap()
    }

    /// Safety: Ensure that access to T does not race with any other method call
    pub unsafe fn unprotected_write(&self) -> &mut T {
        self.item.get().as_mut().unwrap()
    }

    pub fn protected_write(&self) -> WriteGuard<T> {
        assert!(!self.spin.swap(true, SeqCst)); // this MUST be false - one writer only
        self.version.fetch_add(1, SeqCst);
        WriteGuard { item: self }
    }

    pub fn protected_read(&self) -> ReadGuard<T> {
        let v = self.version.load(SeqCst);
        while self.spin.load(SeqCst) {}
        ReadGuard {
            v,
            released: false,
            item: self,
        }
    }
}

pub struct ReadGuard<'lock, T: NoDealloc> {
    item: &'lock WpLock<T>,
    v: usize,
    released: bool,
}

impl<'lock, T: NoDealloc> ReadGuard<'lock, T> {
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

impl<'lock, T: NoDealloc> Drop for ReadGuard<'lock, T> {
    fn drop(&mut self) {
        assert!(self.released)
    }
}

impl<'lock, T: NoDealloc> Deref for ReadGuard<'lock, T> {
    type Target = T;
    fn deref(&self) -> &T {
        let ptr = self.item.item.get();
        unsafe { ptr.as_ref().unwrap() }
    }
}

unsafe impl<T: NoDealloc> Sync for WpLock<T> {}
unsafe impl<T: NoDealloc> Send for WpLock<T> {}
