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
