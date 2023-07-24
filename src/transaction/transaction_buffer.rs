use crossbeam::queue::ArrayQueue;

use crate::config::{Address, MAX_PAGE_COUNT};
use crate::config::{PAGE_SIZE, U64_OFFSET};
use crate::storage::catalog::Catalog;
use crate::storage::delta::TupleDelta;
use crate::storage::nvm_file::{NVMTableStorage, PageId};
use crate::storage::table::TupleId;
use crate::utils::file;
use crate::utils::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};

use super::transaction::Transaction;


#[cfg(feature = "ilog")]
const MANAGER_PAGE_SIZE: u64 = 2048 - 4 * U64_OFFSET;
#[cfg(feature = "ilog")]
const DRAM_PAGE_SIZE: u64 = PAGE_SIZE * 20 - 4 * U64_OFFSET;
#[cfg(not(feature = "ilog"))]
const MANAGER_PAGE_SIZE: u64 = PAGE_SIZE - 4 * U64_OFFSET;
const DATA_SIZE: u64 = 1280;
const COMMITED_OFFSET: u64 = U64_OFFSET;
const NEXT_OFFSET: u64 = COMMITED_OFFSET + U64_OFFSET;
const STATE: u64 = NEXT_OFFSET + U64_OFFSET;
const EMPTY: u64 = STATE + U64_OFFSET;
type BufferAddress = u64;

#[derive(Debug)]
pub struct TransactionBuffer {
    thread_id: u64,
    offset: u64,
    pub address: Address,
    #[cfg(feature = "ilog")]
    pub cur_page: Vec<u8>,
    #[cfg(feature = "ilog")]
    pub d_address: Address,
    #[cfg(feature = "ilog")]
    pub d_offset: u64,
}

impl TransactionBuffer {
    pub fn init(&mut self) {
        self.offset = EMPTY;
        unsafe {
            *(self.address as *mut u64) = self.offset;
            *((self.address + COMMITED_OFFSET) as *mut u64) = self.offset;
            *((self.address + NEXT_OFFSET) as *mut u64) = 0;
            *((self.address + STATE) as *mut u64) = 0;
        }
        #[cfg(all(feature = "ilog", feature = "mvcc"))]
        {
            self.d_address = self.cur_page.as_ptr() as u64;
        }
    }
    pub fn new(catalog: impl Deref<Target = Catalog>, thread_id: u64) -> Self {
        let mut storage = NVMTableStorage::global_mut();
        let page = storage.alloc_page().unwrap();

        catalog.set_transaction_page_start(thread_id, page.page_start);
        let mut manager = TransactionBuffer {
            thread_id: thread_id,
            offset: 0,
            address: page.page_start,
            #[cfg(feature = "ilog")]
            cur_page: vec![0; DRAM_PAGE_SIZE as usize],
            #[cfg(feature = "ilog")]
            d_address: 0,
            #[cfg(feature = "ilog")]
            d_offset: 0,
        };
        manager.init();
        manager
    }
    pub fn free(&mut self) {
        #[cfg(feature = "ilog")]
        {
            self.offset = 0;
            return;
        }
        let mut storage = NVMTableStorage::global_mut();
        storage.free_page_list(PageId::get_page_id(self.address))
    }
    pub fn reload(catalog: &Catalog, thread_id: u64) -> Self {
        let mut managers: Vec<TransactionBuffer> = Vec::new();
        let mut address = catalog.get_transaction_page_start(thread_id);

        while address != 0 {
            let mut manager = TransactionBuffer {
                thread_id: thread_id,
                offset: 0,
                address: address,
                #[cfg(feature = "ilog")]
                cur_page: vec![0; MANAGER_PAGE_SIZE as usize],
                #[cfg(feature = "ilog")]
                d_address: 0,
                #[cfg(feature = "ilog")]
                d_offset: 0,
            };
            assert!(PageId::get_page_id(address).id < MAX_PAGE_COUNT);
            manager.offset = manager.get_offset();

            address = manager.get_next();
            managers.push(manager);
        }
        managers.reverse();
        for manager in &managers {
            let mut offset = manager.offset;
            let commit = manager.get_committed_offset();

            if offset == commit && offset != EMPTY {
                break;
            }
            let mut deltas: Vec<TupleDelta> = Vec::new();
            if offset + DATA_SIZE > MANAGER_PAGE_SIZE {
                offset = EMPTY;
            }
            while offset != commit {
                if offset + DATA_SIZE > MANAGER_PAGE_SIZE {
                    offset = EMPTY;
                }
                let delta = TupleDelta::reload(manager.address + offset).unwrap();
                // println!("delta: {}, {}, {}", thread_id, manager.address, offset);

                assert!(delta.len() < DATA_SIZE);

                if offset == commit {
                    break; //commit == EMPTY
                }
                offset += delta.len() + U64_OFFSET;
                // println!("{}", delta.len());

                deltas.push(delta);
            }
            // deltas.reverse();

            for delta in &deltas {
                delta.rollback(manager.get_committed());
            }
        }

        catalog.set_transaction_page_start(thread_id, 0);
        let mut storage = NVMTableStorage::global_mut();
        for manager in &managers {
            let page = PageId::get_page_id(manager.address);
            storage.free_page_list(page);
        }
        drop(storage);

        TransactionBuffer::new(catalog, thread_id)
    }
    pub fn get_thread_id(&self) -> u64 {
        return self.thread_id;
    }

    pub fn get_offset(&self) -> Address {
        unsafe { *(self.address as *const u64) }
    }
    pub fn get_timestamp(&self) -> u64 {
        Catalog::get_transaction_ts(self.thread_id)
    }
    pub fn get_committed_offset(&self) -> Address {
        unsafe { *((self.address + COMMITED_OFFSET) as *const u64) }
    }
    pub fn get_next(&self) -> u64 {
        unsafe { *((self.address + NEXT_OFFSET) as *const u64) }
    }
    pub fn get_committed(&self) -> u64 {
        unsafe { *((self.address + STATE) as *const u64) }
    }
    pub fn set_committed(&self, committed: u64) {
        unsafe {
            io::write(self.address + STATE, committed);
        }
    }
    pub fn set_next(&self, address: Address) {
        unsafe {
            io::write(self.address + NEXT_OFFSET, address);
        }
    }
    pub fn add_timestamp(&self) -> u64 {
        let ts = self.get_timestamp();
        unsafe {
            *(self.address as *mut u64) = self.offset;

            Catalog::update_ts(self.thread_id, ts + 1);
            #[cfg(feature = "clwb_txn")]
            {
                io::clwb(self.address as *const u8);
                file::sfence();
            }
        }
        ts + 1
    }
    pub fn save_redo(&mut self, len: u64) {
        self.offset += len;
        self.commit(false);
    }

    pub fn add_delta(&mut self, len: u64) {
        assert!(len <= DATA_SIZE);
        #[cfg(not(feature = "ilog"))]
        {
            self.offset += len;
        }
        // let u = unsafe { &*(self.address as *const AtomicU64) };
        // u.store(self.offset, Ordering::Relaxed);
        // file::sfence();
        // unsafe {
        //     io::clwb(self.address as *const u8);
        // }
        // }
        #[cfg(all(feature = "ilog", feature = "mvcc"))]
        {
            self.d_offset += len;
        }
    }
    pub fn begin(&mut self) {
        let u = unsafe { &*(self.address as *const AtomicU64) };

        u.store(self.offset, Ordering::Relaxed);
        // println!("begin old {}", u.load(Ordering::Acquire), );

        // file::sfence();
        self.set_committed(0);
        file::sfence();
    }
    pub fn alloc(&mut self) -> BufferAddress {
        if self.offset + DATA_SIZE >= MANAGER_PAGE_SIZE {
            #[cfg(not(feature = "ilog"))]
            {
                let mut storage = NVMTableStorage::global_mut();
                let page = storage.alloc_page().unwrap();
                // println!("buffer allocate {}", page.page_start);
                // if self.get_next() > 0 {
                //     storage.free_page_list(PageId::get_page_id(self.get_next()));
                // }

                let cur_page = self.address;

                self.address = page.page_start;
                // println!("new page");
                self.init();
            }
            self.offset = EMPTY;

            // self.set_next(cur_page);
        }
        self.address + self.offset
    }
    pub fn commit(&mut self, committed: bool) {
        let committed_address = self.address + COMMITED_OFFSET;
        let u = unsafe { &*(committed_address as *const AtomicU64) };
        // if committed {
        //     println!("commit old {}, new {}, total: {}", u.load(Ordering::Acquire), self.offset, self.offset - u.load(Ordering::Acquire));
        // }

        u.store(self.offset, Ordering::Relaxed);
        //T
        // file::sfence();
        if committed {
            self.set_committed(1);
        }
        // unsafe {
        //     io::clwb(committed_address as *const u8);
        // }
    }
    #[cfg(all(feature = "ilog", feature = "mvcc"))]
    pub fn alloc_dram(&mut self) -> BufferAddress {
        if self.d_address + DATA_SIZE >= DRAM_PAGE_SIZE {
            self.d_offset = 0;
        }
        self.d_address + self.d_offset
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NVM_ADDR;

    #[test]
    fn test_manager() {
        NVMTableStorage::init_test_database();
        let catalog = Catalog::new(NVM_ADDR);
        let mut manager = TransactionBuffer::new(&catalog, 0);
        let offset = manager.alloc();
        manager.add_delta(10);
        let offset_2 = manager.alloc();
        assert_eq!(offset + 10, offset_2);
    }

    #[test]
    fn test_manager_page() {
        NVMTableStorage::init_test_database();
        let catalog = Catalog::new(NVM_ADDR);
        let mut manager = TransactionBuffer::new(&catalog, 0);
        let offset = manager.alloc() - manager.address;
        manager.add_delta(PAGE_SIZE / 2);
        manager.add_delta(PAGE_SIZE / 2);
        let offset_2 = manager.alloc();
        assert_eq!(offset, offset_2 - manager.address);
    }
}
