use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
};

/// SAFETY: Impl only if the struct has ZERO deallocations during &mut T. For example, Vec CANNOT
/// impl this trait because Vec may dealloc when growing
pub unsafe trait NoDealloc {}

#[derive(std::fmt::Debug)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{atomic::AtomicBool, Arc, Barrier};
    use std::thread;

    const DATA_SZ: usize = 10_000;

    struct TestData {
        data: [i32; DATA_SZ],
        offset: usize,
    }

    unsafe impl NoDealloc for TestData {}

    impl TestData {
        pub fn new() -> Self {
            TestData {
                data: [0; DATA_SZ],
                offset: 0,
            }
        }

        pub fn push(&mut self, x: i32) {
            self.data[self.offset] = x;
            self.offset += 1;
        }

        pub fn full(&self) -> bool {
            self.offset == DATA_SZ
        }

        pub fn empty(&self) -> bool {
            self.offset == 0
        }
    }

    #[test]
    #[should_panic]
    fn test_only_single_writer() {
        let locked_data = WpLock::new(TestData::new());
        let guard1 = locked_data.protected_write();
        let guard2 = locked_data.protected_write(); // should panic
    }

    #[test]
    fn test_unprotected_read_write() {
        let locked_data = WpLock::new(TestData::new());
        unsafe {
            assert!(locked_data.unprotected_read().empty());
        }

        for _ in 0..DATA_SZ {
            unsafe {
                locked_data.unprotected_write().push(33);
            }
        }

        unsafe {
            assert!(locked_data.unprotected_read().full());
        }
    }

    #[test]
    fn test_single_writer_writes() {
        let locked_data = WpLock::new(TestData::new());
        {
            let mut write_guard = locked_data.protected_write();
            for _ in 0..DATA_SZ {
                (*write_guard).push(23);
            }
        }

        let read_guard = locked_data.protected_read();
        assert!((*read_guard).full());
        read_guard
            .release()
            .expect("single-threaded read should be successful");
    }

    #[test]
    fn test_synchronized_write_read() {
        let locked_data = Arc::new(WpLock::new(TestData::new()));
        let barrier = Arc::new(Barrier::new(2));

        let writer_data = locked_data.clone();
        let writer_barr = barrier.clone();
        let writer = move || {
            writer_barr.wait();

            let mut guard = writer_data.protected_write();
            for _ in 0..DATA_SZ {
                (*guard).push(33);
            }

            writer_barr.wait();
        };

        let reader_data = locked_data.clone();
        let reader_barr = barrier.clone();
        let reader = move || {
            let guard = reader_data.protected_read();
            assert!((*guard).empty());
            guard.release().expect("read should be uncontended");

            reader_barr.wait();
            reader_barr.wait();

            let guard = reader_data.protected_read();
            assert!((*guard).full());
            guard.release().expect("read should be uncontended");
        };

        let writer_thr = thread::spawn(writer);
        let reader_thr = thread::spawn(reader);

        writer_thr.join().unwrap();
        reader_thr.join().unwrap();
    }

    #[test]
    fn test_writer_prioritized_access() {
        const N_WRITER: usize = 1;
        const N_READER: usize = 9;

        let locked_data = Arc::new(WpLock::new(TestData::new()));
        let completed = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::new();

        let writer_data = locked_data.clone();
        let writer_completed = completed.clone();
        let writer = move || {
            for _ in 0..DATA_SZ {
                let mut guard = writer_data.protected_write();
                (*guard).push(33);
            }

            writer_completed.store(true, SeqCst);
        };

        handles.push(thread::spawn(writer));

        for _ in 0..N_READER {
            let reader_data = locked_data.clone();
            let writer_completed = completed.clone();

            handles.push(thread::spawn(move || {
                let mut buff = [0; DATA_SZ];
                let mut success = 0;
                let mut failure = 0;
                loop {
                    let guard = reader_data.protected_read();
                    buff.copy_from_slice(&(*guard).data);
                    match guard.release() {
                        Ok(..) => success += 1,
                        Err(..) => failure += 1,
                    }
                    match writer_completed.load(SeqCst) {
                        true => break,
                        false => {}
                    }
                }

                assert!(
                    failure > success * 10,
                    "read success {}, failure: {}",
                    success,
                    failure
                ); // highly unlikely to be false

                for _ in 0..10_000 {
                    let guard = reader_data.protected_read();
                    guard.release().expect("uncontended read should succeed");
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
