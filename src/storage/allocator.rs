use crate::config::*;
use crate::mvcc_config::THREAD_COUNT;
// use crate::index::bplus_tree::BplusTree;
use crate::storage::nvm_file::{NVMTableStorage, PageId};
use crate::storage::table::TupleId;
use crate::utils::persist::persist_array::PersistArray;
use crate::ycsb::u64_rand_new;
// use crate::util::persist::persist_bitmap::PersistBitmap;
use crate::{Error, Result};
use crossbeam::queue::SegQueue;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct Page {
    // pub page_id: AtomicU32,
    pub page_start: AtomicU64,
    pub max_tuple: u32,
    pub offset: AtomicU32,
}

impl Page {
    pub fn new(page_start: u64, max_tuple: u32) -> Self {
        Page {
            // page_id: AtomicU32::new(page_id),
            page_start: AtomicU64::new(page_start),
            offset: AtomicU32::new(0),
            max_tuple,
        }
    }
    pub fn alloc(&self) -> Result<u32> {
        let cur = self.offset.load(Ordering::Relaxed);
        if cur >= self.max_tuple {
            return Err(Error::NoSpace);
        }
        let x = self.offset.fetch_add(1, Ordering::Relaxed);
        if x >= self.max_tuple {
            return Err(Error::NoSpace);
        }
        Ok(x)
    }
}
#[derive(Debug)]
pub struct DualPageAllocator {
    page_list: Vec<Page>,
    cur_page: AtomicUsize,
    free_list: SegQueue<TupleId>,
    meta_page: PersistArray<u64>,
}
impl DualPageAllocator {
    pub fn new(meta_page: Address, max_tuple: u32) -> Self {
        let mut allocator = DualPageAllocator {
            page_list: Vec::new(),
            cur_page: AtomicUsize::new(0),
            free_list: SegQueue::new(),
            meta_page: PersistArray::<u64>::reload(meta_page),
        };
        allocator.page_list.push(Page::new(0, max_tuple));
        allocator.page_list.push(Page::new(0, max_tuple));

        allocator.allocate_new_page(0);
        allocator.allocate_new_page(1);

        println!("create page count {}", allocator.meta_page.len());
        allocator
    }
    fn allocate_new_page(&self, current: usize) {
        if !self
            .cur_page
            .compare_exchange(current, current ^ 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
        let page = self.page_list.get(current & 1).unwrap();

        let mut storage = NVMTableStorage::global_mut();
        let page_start = storage.alloc_page().unwrap().page_start;
        // println!("page allocate {}", page_start);

        unsafe {
            self.meta_page.push(page_start);
            page.page_start.store(page_start, Ordering::SeqCst);
            page.offset.store(0, Ordering::Relaxed);
        }
    }

    pub fn allocate_tuple(&self, size: u64) -> Result<TupleId> {
        loop {
            let cur = self.cur_page.load(Ordering::Relaxed);
            let page = self.page_list.get(cur).unwrap();
            match page.alloc() {
                Ok(tuple_id) => {
                    return Ok(TupleId::new(
                        page.page_start.load(Ordering::Relaxed),
                        tuple_id as u64,
                        size,
                    ));
                }
                _ => {}
            }

            match self.free_list.pop() {
                Some(tid) => {
                    return Ok(tid);
                }
                _ => {}
            }
            self.allocate_new_page(cur);
        }
    }
    pub fn free_tuple(&self, tid: TupleId) {
        self.free_list.push(tid);
    }
}

#[derive(Debug)]
pub struct LocalPageAllocator {
    page_start: Address,
    offset: u32,
    max_tuple: u32,
    free_list: SegQueue<TupleId>,
    meta_page: PersistArray<u64>,
    free_pages: SegQueue<Address>,
}
impl LocalPageAllocator {
    pub fn new(meta_page: Address, max_tuple: u32) -> Self {
        let mut allocator = LocalPageAllocator {
            page_start: 0,
            offset: 0,
            max_tuple: max_tuple,
            free_list: SegQueue::new(),
            meta_page: PersistArray::<u64>::reload(meta_page),
            free_pages: SegQueue::new(),
        };

        // let mut storage = NVMTableStorage::global_mut();
        // for _ in 0..1000 {
        //     allocator
        //         .free_pages
        //         .push(storage.alloc_page().unwrap().page_start);
        // }
        allocator.allocate_new_page();
        // println!("create page count {}", allocator.meta_page.len());
        allocator
    }
    pub fn pre_alloc(&mut self, count: u64) {
        let mut storage = NVMTableStorage::global_mut();
        for _ in 0..count {
            self.free_pages
                .push(storage.alloc_page().unwrap().page_start);
        }
    }
    fn allocate_new_page(&mut self) {
        let page_start = self.free_pages.pop().unwrap_or_else(|| {
            let mut storage = NVMTableStorage::global_mut();
            // println!("page used");
            storage.alloc_page().unwrap().page_start
        });
        // println!("page allocate {:?}", PageId::get_page_id(page_start));

        unsafe {
            self.meta_page.push(page_start);
            self.page_start = page_start;
            self.offset = 0;
        }
    }

    pub fn allocate_tuple(&mut self, size: u64) -> Result<TupleId> {
        loop {
            if self.offset < self.max_tuple {
                let tuple_id = TupleId::new(self.page_start, self.offset as u64, size);
                self.offset += 1;
                return Ok(tuple_id);
            }

            match self.free_list.pop() {
                Some(tid) => {
                    return Ok(tid);
                }
                _ => {}
            }
            self.allocate_new_page();
        }
    }
    #[cfg(feature = "append")]
    pub fn allocate_append_tuple(&mut self, size: u64, thread_id: usize) -> Result<TupleId> {
        loop {
            if self.offset < self.max_tuple {
                let tuple_id = TupleId::new(self.page_start, self.offset as u64, size);
                self.offset += 1;
                return Ok(tuple_id);
            }

            match self.free_list.pop() {
                Some(tid) => {
                    return Ok(tid);
                }
                _ => {}
            }
            let page_start = self.page_start + THREAD_COUNT as u64 * PAGE_SIZE as u64;
            // println!("page allocate {}", page_start);

            unsafe {
                self.meta_page.push(page_start);
                self.page_start = page_start;
                self.offset = 0;
            };
        }
    }
    pub fn free_tuple(&self, tid: u64) {
        self.free_list.push(TupleId {
            page_start: AtomicU64::new(tid),
        });
    }
}
