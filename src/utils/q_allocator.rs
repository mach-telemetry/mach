use std::{
    sync::{Arc, {atomic::{AtomicUsize, Ordering::SeqCst}}},
    ptr::NonNull,
    ops::{Deref, DerefMut},
};
use crossbeam::queue::SegQueue;

#[derive(Clone)]
pub struct QueueAllocator<T> {
    q: Arc<SegQueue<NonNull<Inner<T>>>>,
    allocator: fn() -> T,
}

impl<T> QueueAllocator<T> {
    pub fn new(allocator: fn() -> T) -> Self {
        QueueAllocator {
            q: Arc::new(SegQueue::new()),
            allocator,
        }
    }

    pub fn allocate(&self) -> Qrc<T> {
        let ptr: NonNull<Inner<T>> = match self.q.pop() {
            Some(ptr) => ptr,
            None => {
                let inner = (self.allocator)();
                NonNull::new(Box::into_raw(Box::new(Inner {
                    inner,
                    refs: AtomicUsize::new(0),
                    queue: self.q.clone(),
                }))).unwrap()
            }
        };

        let res = Qrc {
            ptr
        };

        res.increment_count();
        res
    }
}

struct Inner<T> {
    inner: T,
    refs: AtomicUsize,
    queue: Arc<SegQueue<NonNull<Inner<T>>>>
}

pub struct Qrc<T> {
    ptr: NonNull<Inner<T>>,
}

impl<T> Qrc<T> {
    fn inner(&self) -> &Inner<T> {
        unsafe { self.ptr.as_ref() }
    }

    fn inner_mut(&mut self) -> &mut Inner<T> {
        unsafe { self.ptr.as_mut() }
    }

    fn increment_count(&self) {
        self.inner().refs.fetch_add(1, SeqCst);
    }

    fn inner_as_mut(&mut self) -> &mut T {
        &mut self.inner_mut().inner
    }

    fn inner_as_ref(&self) -> &T {
        &self.inner().inner
    }
}

impl<T> Clone for Qrc<T> {
    fn clone(&self) -> Self {
        self.increment_count();
        Qrc {
            ptr: self.ptr
        }
    }

}

impl<T> Deref for Qrc<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.inner_as_ref()
    }
}

impl<T> DerefMut for Qrc<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner_as_mut()
    }
}


impl<T> Drop for Qrc<T> {
    fn drop(&mut self) {
        if 1 == self.inner().refs.fetch_sub(1, SeqCst) {
            self.inner().queue.push(self.ptr)
        }
    }
}

