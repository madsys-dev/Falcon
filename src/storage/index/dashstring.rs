use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use crate::c::ffi::*;
use crate::storage::table::TupleId;
use crate::Result;
use libc::{c_int, c_void};
use std::ffi::{CString, NulError};

#[derive(Debug, Clone)]
pub struct DashString {
    root: *mut c_void,
}
unsafe impl Send for DashString {}
unsafe impl Sync for DashString {}

const MASK: u64 = (1 << 62) - 1;
impl DashString {
    pub fn new() -> Self {
        DashString {
            root: unsafe { dashstring_create() },
        }
    }
    pub fn insert<S>(&self, key: S, length: usize, value: TupleId) -> Option<u64>
    where
        S: Into<String>,
    {
        let ckey = std::ffi::CString::new(key.into()).expect("CString::new failed");
        unsafe {
            dashstring_insert(
                self.root,
                ckey.as_ptr(),
                length as c_int,
                value.get_address(),
            )
        };
        Some(0)
    }
    pub fn update<S>(&self, key: S, length: usize, value: TupleId) -> Option<u64>
    where
        S: Into<String>,
    {
        let ckey = std::ffi::CString::new(key.into()).expect("CString::new failed");
        unsafe {
            dashstring_update(
                self.root,
                ckey.as_ptr(),
                length as c_int,
                value.get_address(),
            )
        };
        Some(0)
    }
    pub fn get<S>(&self, key: S, length: usize) -> Option<TupleId>
    where
        S: Into<String>,
    {
        let ckey = std::ffi::CString::new(key.into()).unwrap();
        let v = unsafe { dashstring_find(self.root, ckey.as_ptr(), length as c_int) };
        Some(TupleId {
            page_start: AtomicU64::new(v & MASK),
        })
    }
    pub fn remove<S>(&self, key: S, length: usize) -> Option<TupleId>
    where
        S: Into<String>,
    {
        let ckey = std::ffi::CString::new(key.into()).unwrap();
        let v = unsafe { dashstring_remove(self.root, ckey.as_ptr(), length as c_int) };
        Some(TupleId {
            page_start: AtomicU64::new(v & MASK),
        })
    }
}
