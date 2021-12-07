use std::{
    ptr,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

struct Node<T> {
    next: *mut Node<T>,
    data: Box<[T]>,
}

impl<T: Default + Clone> Node<T> {
    fn new(sz: usize) -> Self {
        Node {
            next: ptr::null_mut::<Node<T>>(),
            data: vec![Default::default(); sz].into_boxed_slice(), // TODO: MaybeUninit
        }
    }

    fn set_next(&mut self, ptr: *mut Node<T>) {
        self.next = ptr
    }
}

impl<T> Drop for Node<T> {
    fn drop(&mut self) {
        // If this node is dropped, recursively drop the next node if there is one
        if !self.next.is_null() {
            unsafe {
                drop(Box::from_raw(self.next));
            }
        }
    }
}

struct InnerList<T> {
    head: *mut Node<T>,
    tail: *mut Node<T>, // RawPtr makes InnerList neither Sync nor Send
    cap: usize,
    len: AtomicUsize,
}

impl<T: Default + Clone> InnerList<T> {
    fn new() -> Self {
        let boxed_node = Box::new(Node::new(8));
        let raw = Box::into_raw(boxed_node);
        InnerList {
            // Head never changes
            head: raw,

            // Tail and cap are only accessed by a single writer
            tail: raw,
            cap: 8,

            // Len accessed by multiple threads (e.g. writer and snapshotting thread)
            len: AtomicUsize::new(0),
        }
    }

    /// Pushes an item. It is unsafe to call this without calling begin_write
    fn push(&mut self, item: T) {
        let l = self.len.fetch_add(1, SeqCst);
        if l == self.cap {
            self.grow();
        }
        let node: &mut Node<T> = unsafe { self.tail.as_mut().unwrap() };
        let offset = l - (self.cap - node.data.len());
        node.data[offset] = item;
    }

    /// If the list is full, grows the list by allocating a new node, doubling the list's size
    fn grow(&mut self) {
        // Allocate a new node that would double the list's size
        let boxed_node = Box::new(Node::new(self.cap));
        let new_tail = Box::into_raw(boxed_node);

        // Swap the old and new node, and set old node's next
        unsafe {
            let old_tail = self.tail.as_mut().unwrap();
            self.tail = new_tail;
            old_tail.set_next(new_tail);
        }

        // List has doubled in size
        self.cap *= 2;
    }
}

impl<T> Drop for InnerList<T> {
    fn drop(&mut self) {
        let boxed_node: Box<Node<T>> = unsafe { Box::from_raw(self.head) };
        drop(boxed_node);
    }
}

#[derive(Clone)]
pub struct List<T> {
    inner: Arc<InnerList<T>>,
    writers: Arc<AtomicUsize>,
}

impl<T: Default + Clone> List<T> {
    pub fn new() -> Self {
        List {
            inner: Arc::new(InnerList::new()),
            writers: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn writer(&self) -> ListWriter<T> {
        if self.writers.fetch_add(1, SeqCst) > 0 {
            panic!("Multiple list writers!");
        }
        ListWriter {
            inner: self.inner.clone(),
            writers: self.writers.clone(),
        }
    }

    pub fn snapshot(&self) -> ListReader<T> {
        ListReader {
            inner: self.inner.clone(),
            len: self.inner.len.load(SeqCst),
            //len: self.inner.len
        }
    }
}

unsafe impl<T> Send for List<T> {}
unsafe impl<T> Sync for List<T> {}

pub struct ListWriter<T> {
    inner: Arc<InnerList<T>>,
    writers: Arc<AtomicUsize>,
}

impl<T: Default + Clone> ListWriter<T> {
    pub fn push(&mut self, item: T) {
        unsafe {
            Arc::get_mut_unchecked(&mut self.inner).push(item);
        }
    }
}

impl<T> Drop for ListWriter<T> {
    fn drop(&mut self) {
        self.writers.fetch_sub(1, SeqCst);
    }
}

unsafe impl<T> Send for ListWriter<T> {}
unsafe impl<T> Sync for ListWriter<T> {}

pub struct ListReader<T> {
    inner: Arc<InnerList<T>>,
    len: usize,
}

impl<T> ListReader<T> {
    pub fn iter(&self) -> Iter<T> {
        Iter {
            inner: self,
            node: self.inner.head,
            idx: 0,
            offset: 0,
        }
    }

    pub fn _len(&self) -> usize {
        self.len
    }

    pub fn _first(&self) -> Option<&T> {
        if self.len == 0 {
            None
        } else {
            unsafe { Some(&self.inner.head.as_ref().unwrap().data[0]) }
        }
    }

    pub fn _last(&self) -> Option<&T> {
        if self.len == 0 {
            None
        } else {
            Some(self._value_at_index(self.len - 1))
        }
    }

    pub fn _value_at_index(&self, idx: usize) -> &T {
        assert!(idx < self.len, "idx {} for list of len {}", idx, self.len);

        let mut cidx = 0;

        // Safety: Len limits access. Head is always non-null.
        unsafe {
            let mut node = self.inner.head.as_ref().unwrap();
            while cidx < idx {
                let end = cidx + node.data.len();
                if end > idx {
                    break;
                } else {
                    node = node.next.as_ref().unwrap();
                    cidx = end;
                }
            }
            &node.data[idx - cidx]
        }
    }
}

pub struct Iter<'a, T> {
    inner: &'a ListReader<T>,
    node: *const Node<T>, // current node we're going over
    idx: usize,           // index into list
    offset: usize,        // offset into node
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.inner.len {
            None
        } else {
            // Safety: len limits how far down the list can be accessed. Any non-head next ptr <
            // len are non-null and initialized by push() and grow().  Head which is always
            // non-null and initialized taking care of len = 0.  self.idx == self.len == 0 takes
            // care of this edge case as well.
            let node = unsafe { self.node.as_ref().unwrap() };
            let item = &node.data[self.offset];
            self.offset += 1;
            self.idx += 1;

            // Move on to the next node if offset is at end of the node, and idx < len, then there
            // is another node after this (per push(), grow()).
            if self.offset == node.data.len() && self.idx < self.inner.len {
                self.node = node.next;
                self.offset = 0;
            }
            Some(item)
        }
    }
}
unsafe impl<T> Send for ListReader<T> {}
unsafe impl<T> Sync for ListReader<T> {}

#[cfg(test)]
mod test {
    use super::*;
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    #[test]
    fn push() {
        let list = List::new();
        let clone = list.clone();

        let h = thread::spawn(move || {
            let mut writer = clone.writer();
            for i in 0..10 {
                writer.push(i);
            }
        });
        h.join().unwrap();
        assert_eq!(list.inner.cap, 16);
        assert_eq!(list.inner.len.load(SeqCst), 10);
        let v: Vec<usize> = list.snapshot().iter().map(|x| *x).collect();
        assert_eq!(v.as_slice(), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    #[should_panic]
    fn invalid_multiple_writers() {
        let list: List<usize> = List::new();
        let _w = list.writer();
        let _w2 = list.writer();
    }

    #[test]
    fn nonconcurrent_multiple_writers() {
        let list: List<usize> = List::new();
        let _w = list.writer();
        drop(_w);
        let _w2 = list.writer();
    }

    #[test]
    fn begin_iter_end() {
        let list = List::new();
        let mut writer = list.writer();
        for i in 0..5 {
            writer.push(i);
        }
        let snapshot = list.snapshot();
        for i in 5..10 {
            writer.push(i);
        }
        let v: Vec<usize> = snapshot.iter().map(|x| *x).collect();
        assert_eq!(v.as_slice(), &[0, 1, 2, 3, 4]);
    }

    //#[test]
    //fn index() {
    //    let list = List::new();
    //    let clone = list.clone();

    //    let h = thread::spawn(move || {
    //        let mut writer = clone.writer();
    //        for i in 0..23 {
    //            writer.push(i);
    //        }
    //    });
    //    h.join().unwrap();
    //    let reader = list.snapshot();
    //    for i in 0..23 {
    //        assert_eq!(i, *reader.value_at_index(i));
    //    }
    //    assert_eq!(0, *reader.first().unwrap());
    //    assert_eq!(22, *reader.last().unwrap()); // last item inserted is a 22!
    //}

    #[test]
    fn multithread() {
        let b1 = Arc::new(Barrier::new(2));
        let b2 = Arc::new(Barrier::new(2));
        let list: List<usize> = List::new();
        let list2 = list.clone();
        let b1c = b1.clone();
        let b2c = b2.clone();
        let handle = thread::spawn(move || {
            let mut writer = list2.writer();
            for i in 0..5 {
                writer.push(i);
            }
            b1c.wait();
            b2c.wait();
            for i in 5..10 {
                writer.push(i);
            }
        });

        b1.wait();
        let v: Vec<usize> = list.snapshot().iter().map(|x| *x).collect();
        b2.wait();
        assert_eq!(v.as_slice(), &[0, 1, 2, 3, 4]);
        handle.join().unwrap();
    }

    #[test]
    fn drop_test() {
        let list = List::new();
        let mut w = list.writer();
        for i in 0..10 {
            w.push(i);
        }
    }
}
