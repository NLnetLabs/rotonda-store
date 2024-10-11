use std::thread;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};


pub struct OnceBox<T> {
    ptr: AtomicPtr<T>
}

impl<T> OnceBox<T> {
    pub fn null() -> Self {
        Self {
            ptr: AtomicPtr::new(null_mut())
        }
    }

    pub fn get(&self) -> Option<&T> {
        let ptr = self.ptr.load(Ordering::Relaxed);
        if ptr == null_mut() {
            None
        }
        else {
            Some(unsafe { &*ptr })
        }
    }

    pub fn get_or_set(&self, value: T) -> &T {
        let ptr = Box::leak(Box::new(value));
        let res = match self.ptr.compare_exchange(
            null_mut(), ptr, Ordering::SeqCst, Ordering::Acquire
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

    pub fn get_or_create(&self, create: impl FnOnce() -> Box<T>) -> &T {
        if let Some(res) = self.get() {
            return res
        }
        let ptr = Box::leak(create());
        let res = match self.ptr.compare_exchange(
            null_mut(), ptr, Ordering::SeqCst, Ordering::Acquire
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

