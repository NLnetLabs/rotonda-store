use std::slice;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, Ordering};

#[derive(Debug)]
pub struct OnceBox<T> {
    ptr: AtomicPtr<T>,
}

impl<T> OnceBox<T> {
    pub fn new() -> Self {
        Self {
            ptr: AtomicPtr::new(null_mut()),
        }
    }

    pub fn is_null(&self) -> bool {
        let ptr = self.ptr.load(Ordering::Relaxed);
        ptr == null_mut()
    }

    pub fn get(&self) -> Option<&T> {
        let ptr = self.ptr.load(Ordering::Relaxed);
        if ptr == null_mut() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    }

    pub fn get_or_set(&self, value: T) -> (&T, bool) {
        let mut its_us = false;
        let ptr = Box::leak(Box::new(value));
        let res = match self.ptr.compare_exchange(
            null_mut(),
            ptr,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(current) => {
                // We set the new value, return it.
                assert!(current.is_null());
                its_us = true;
                ptr as *const _
            }
            Err(current) => {
                // `current` is the real value we need to drop our value.
                assert!(!current.is_null());
                let _ = unsafe { Box::from_raw(ptr) };
                current as *const _
            }
        };
        (unsafe { &*res }, its_us)
    }

    pub fn get_or_init(&self, create: impl FnOnce() -> T) -> &T {
        if let Some(res) = self.get() {
            return res;
        }
        let ptr = Box::leak(Box::new(create()));
        let res = match self.ptr.compare_exchange(
            null_mut(),
            ptr,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(current) => {
                // We set the new value, return it.
                assert!(current.is_null());
                ptr as *const _
            }
            Err(current) => {
                // `current` is the real value we need to drop our value.
                assert!(!current.is_null());
                let _ = unsafe { Box::from_raw(ptr) };
                current as *const _
            }
        };
        unsafe { &*res }
    }
}

impl<T> Drop for OnceBox<T> {
    fn drop(&mut self) {
        let ptr = self.ptr.swap(null_mut(), Ordering::Relaxed);
        if !ptr.is_null() {
            let _ = unsafe { Box::from_raw(ptr) };
        }
    }
}


pub struct OnceBoxSlice<T> {
    ptr: AtomicPtr<OnceBox<T>>,
    size: usize
}

impl<T> OnceBoxSlice<T> {
    pub fn new(size: usize) -> Self {
        Self {
            ptr: AtomicPtr::new(null_mut()),
            size
        }
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        let ptr = self.ptr.load(Ordering::Relaxed);
        if ptr == null_mut() {
            None
        } else {
            let slice = unsafe { slice::from_raw_parts(ptr, self.size) };
            slice.get(idx).and_then(|inner| inner.get())
        }
    }

    pub fn get_or_init(
        &self, idx: usize, create: impl FnOnce() -> T
    ) -> &T {
        let slice = self.get_or_make_slice();
        slice[idx].get_or_init(create)
    }

    fn get_or_make_slice(&self) -> &[OnceBox<T>] {
        let ptr = self.ptr.load(Ordering::Relaxed);
        if ptr != null_mut() {
            return unsafe { slice::from_raw_parts(ptr, self.size) }
        }

        // Create a slice, set it, get again.
        let mut vec = Vec::with_capacity(self.size);
        for _ in 0..self.size { vec.push(OnceBox::new()) }
        // Convert Vec<[OnceBox<T>] -> Box<[OnceBox<T>] -> &mut [OnceBox<T>]
        //  -> *mut OnceBox<T>
        let ptr = Box::leak(vec.into_boxed_slice()).as_mut_ptr();
        let res = match self.ptr.compare_exchange(
            null_mut(),
            ptr,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(current) => {
                // We set the new value, return it.
                assert!(current.is_null());
                ptr
            }
            Err(current) => {
                // There was a value already: current. Drop our new thing and
                // return current.
                assert!(!current.is_null());
                let _ = unsafe {
                    Box::from_raw(slice::from_raw_parts_mut(ptr, self.size))
                };
                current
            }
        };

        unsafe { slice::from_raw_parts(res, self.size) }
    }
}

impl<T> Drop for OnceBoxSlice<T> {
    fn drop(&mut self) {
        let ptr = self.ptr.swap(null_mut(), Ordering::Relaxed);
        if !ptr.is_null() {
            let _ = unsafe {
                Box::from_raw(
                    slice::from_raw_parts_mut(ptr, self.size)
                )
            };
        }
    }
}

