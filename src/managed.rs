
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::{Relaxed, Acquire, Release}}
    },
    ptr::NonNull,
    ops::Deref,
};
use crossbeam_queue::SegQueue;

const MAX_REFCOUNT: usize = (i32::MAX) as usize;

type InnerPtr<T> = NonNull<ManagedInner<T>>;
type InnerQueue<T> = Arc<SegQueue<InnerPtr<T>>>;

struct ManagedInner<T> {
    count: AtomicUsize,
    data: T
}

impl<T> Deref for ManagedInner<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.data
    }
}

pub struct ManagedPtr<T> {
    ptr: InnerPtr<T>,
    manager: Manager<T>,
}

impl<T> ManagedPtr<T> {
    fn inner(&self) -> &ManagedInner<T> {
        unsafe { self.ptr.as_ref() }
    }

    fn from_inner(ptr: InnerPtr<T>, manager: Manager<T>) -> Self {
        Self {
            ptr,
            manager
        }
    }

    pub unsafe fn get_mut(this: &Self) -> &mut T {
        &mut (*this.ptr.as_ptr()).data
    }

    pub fn raw_ptr(this: &Self) -> * const T {
        unsafe {
            &(*this.ptr.as_ptr()).data
        }
    }
}

impl<T> Clone for ManagedPtr<T> {
    fn clone(&self) -> Self {
        let old = self.inner().count.fetch_add(1, Relaxed);
        if old > MAX_REFCOUNT {
            panic!("Too many references");
        }
        Self::from_inner(self.ptr, self.manager.clone())
    }
}

impl<T> Drop for ManagedPtr<T> {
    fn drop(&mut self) {
        if self.inner().count.fetch_sub(1, Release) != 1 {
            return
        }
        self.inner().count.load(Acquire);
        self.manager.push(self.ptr);
    }
}

impl<T> Deref for ManagedPtr<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.inner().deref()
    }
}

pub struct Manager<T> {
    inner: InnerQueue<T>
}

impl<T> Clone for Manager<T> {
    fn clone(&self) -> Self {
        Manager {
            inner: self.inner.clone()
        }
    }
}

impl<T> Manager<T> {
    pub fn new() -> Self {
        Manager {
            inner: Arc::new(SegQueue::new())
        }
    }

    pub fn manage(&self, data: T) {
        let b = box ManagedInner {
            count: AtomicUsize::new(0),
            data,
        };
        self.inner.push(Box::leak(b).into());
    }

    fn push(&self, inner: InnerPtr<T>) {
        self.inner.push(inner)
    }

    pub fn take(&self) -> Option<ManagedPtr<T>> {
        let ptr = self.inner.pop()?;
        unsafe { assert_eq!(ptr.as_ref().count.fetch_add(1, Relaxed), 0); }
        Some(ManagedPtr::from_inner(ptr, self.clone()))
    }

    pub fn take_or_add(&self, f: fn() -> T) -> ManagedPtr<T> {
        let ptr = match self.inner.pop() {
            Some(x) => x,
            None => {
                let b = box ManagedInner {
                    count: AtomicUsize::new(0),
                    data: f(),
                };
                Box::leak(b).into()
            }
        };
        unsafe { assert_eq!(ptr.as_ref().count.fetch_add(1, Relaxed), 0); }
        ManagedPtr::from_inner(ptr, self.clone())
    }
}
