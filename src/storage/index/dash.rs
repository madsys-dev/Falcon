use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use crate::c::ffi::*;
use crate::config::NVM_ADDR;
use crate::storage::table::TupleId;
use crate::Result;
use libc::c_void;

#[derive(Debug, Clone)]
pub struct Dash<T> {
    phantom: PhantomData<T>,
    root: *mut c_void,
}
unsafe impl<T> Send for Dash<T> {}
unsafe impl<T> Sync for Dash<T> {}

const MASK: u64 = (1 << 62) - 1;
impl<T> Dash<T> {
    pub fn new() -> Self {
        Dash {
            root: unsafe { dash_create() },
            phantom: PhantomData,
        }
    }
    pub fn insert(&self, key: u64, value: TupleId) -> Option<u64> {
        assert!(value.get_address() > 1000);
        unsafe { dash_insert(self.root, key, value.get_address()) };
        Some(0)
    }
    pub fn update(&self, key: u64, value: TupleId) -> Option<u64> {
        // println!("{:x}", value.get_address());
        assert!(value.get_address() > 1000);

        unsafe { dash_update(self.root, key, value.get_address()) };
        Some(0)
    }
    pub fn remove(&self, key: &u64) -> Option<u64> {
        unsafe { dash_remove(self.root, *key) };
        Some(0)
    }
    pub fn get(&self, key: &u64) -> Option<TupleId> {
        let v = unsafe { dash_find(self.root, *key) };
        if v < NVM_ADDR {
            return None;
        }
        Some(TupleId {
            page_start: AtomicU64::new(v),
        })
    }
}
