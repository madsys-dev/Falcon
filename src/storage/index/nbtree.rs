
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;

use libc::{c_void, c_int};

use crate::c::ffi::*;
use crate::customer_config::BTREE_FILE_PATH;
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

    pub fn range(&self, start: &u64, end: &u64) -> Vec<TupleId> {
        let mut result = Vec::new();
        unsafe {
            let mut item = btree_scan(self.root, *start, *end);
            while item > 0 {
                // println!("receive {:x}", item);

                result.push(TupleId{page_start: AtomicU64::new(item & MASK)});
                item = btree_next(self.root, *start, *end);
            }
        }
        
        return result;
    }
    pub fn last(&self, start: &u64, end: &u64) -> Option<TupleId> {
        unsafe {
            let item = btree_last(self.root, *start, *end);
            if item == 0 {
                return None;
            }
            return Some(TupleId{page_start: AtomicU64::new(item & MASK)});
        }
    }
}

#[cfg(feature = "nbtree")]
pub fn init_index(thread_id: i32) {
    println!("-----init index {}----", thread_id);
    unsafe{btree_init_for_thread(thread_id as c_int)};
}