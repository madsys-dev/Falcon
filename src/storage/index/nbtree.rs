
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;

use libc::c_void;
use crate::c::ffi::*;
use crate::Result;
use crate::storage::table::TupleId;

#[derive(Debug, Clone)]
pub struct NBTree<T> {
    phantom: PhantomData<T>,
    root: *mut c_void,
}
unsafe impl<T> Send for NBTree<T> {}
unsafe impl<T> Sync for NBTree<T> {}

const MASK:u64 = (1 << 62)-1;
impl<T> NBTree<T> {
    pub fn new() -> Self {
        NBTree {
            root: unsafe{btree_create()},
            phantom: PhantomData,
        }
    }
    pub fn insert(&self, key: u64, value: TupleId) -> Option<u64> {
        unsafe{btree_insert(self.root, key, value.get_address())};
        Some(0)
    }
    pub fn remove(&self, key: &u64) -> Option<u64> {
        unsafe{btree_remove(self.root, *key)};
        Some(0)
    }
    pub fn get(&self, key: &u64) -> Option<TupleId> {
        let v = unsafe{btree_find(self.root, *key)};
        Some(TupleId{page_start: AtomicU64::new(v & MASK)})
    }
    pub fn init(&self, thread_id: i32) {
        unsafe{btree_init_for_thread(thread_id)};
    }
}