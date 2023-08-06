#[cfg(feature = "dash")]
use crate::c::ffi::init;
#[cfg(feature = "nbtree")]
use crate::c::ffi::init_btree_file;
use crate::config::*;
use crate::customer_config::NVM_FILE_PATH;
#[cfg(feature = "dash")]
use crate::customer_config::INDEX_FILE_PATH;
#[cfg(feature = "nbtree")]
use crate::customer_config::BTREE_FILE_PATH;
use crate::utils::file;
use crate::utils::persist::persist_bitmap::PersistBitmap;
use crate::{Error, Result};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use std::path::Path;

const ROOT_PAGE_SIZE: u64 = PAGE_SIZE;
const CATELOG_PAGE_SIZE: u64 = PAGE_SIZE;

const ROOT_ADDR: u64 = NVM_ADDR + PAGE_SIZE;
const PAGE_ADDR_START: u64 = ROOT_ADDR + PAGE_SIZE;

#[derive(Debug)]
pub struct PageId {
    pub page_start: u64,
    pub id: u64,
}

impl PageId {
    pub fn new(id: u64) -> PageId {
        PageId {
            page_start: id * PAGE_SIZE + PAGE_ADDR_START,
            id: id,
        }
    }
    pub fn get_page_start(id: u64) -> u64 {
        id * PAGE_SIZE + PAGE_ADDR_START
    }
    pub fn get_page_id(address: Address) -> PageId {
        PageId {
            page_start: address,
            id: (address - PAGE_ADDR_START) / PAGE_SIZE,
        }
    }
}

#[derive(Debug)]
pub struct NVMTableStorage {
    // base: *mut u8,
    page_bitmap: PersistBitmap<'static>,
}
static STORAGE: OnceCell<RwLock<NVMTableStorage>> = OnceCell::new();

impl NVMTableStorage {
    pub fn new(file_path: impl AsRef<Path>, data_size: u64) -> Result<Self> {
        let mut file_size = data_size;
        if file_size == 0 {
            file_size = MAX_PAGE_COUNT * PAGE_SIZE + ROOT_PAGE_SIZE + CATELOG_PAGE_SIZE;
        }
        let base = file::mmap(file_path.as_ref(), file_size)?;
        // unsafe{ptr::write_bytes(base, 0, file_size as usize);}

        let page_bitmap = PersistBitmap::from_slice(unsafe {
            std::slice::from_raw_parts_mut(base, PAGE_SIZE as usize)
        });
        let s = NVMTableStorage { page_bitmap };
        Ok(s)
    }

    pub fn init_database() {
        let storage = NVMTableStorage::new("test_database.db", 0).unwrap();
        STORAGE.set(RwLock::new(storage)).unwrap();
    }
    pub fn init_test_database() {
        // log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        // debug!("INFO");
        // debug!("DEBUG");
        #[cfg(feature = "dash")]
        unsafe {
            let path = std::ffi::CString::new(INDEX_FILE_PATH).expect("CString::new failed");
            init(path.as_ptr());
        }
        #[cfg(feature = "nbtree")]
        unsafe {
            let path = std::ffi::CString::new(BTREE_FILE_PATH).expect("CString::new failed");
            init_btree_file(path.as_ptr());
        }
        if STORAGE.get().is_none() {
            #[cfg(feature = "native")]
            let file_name = "_test_persist";
            #[cfg(feature = "nvm_server")]
            let file_name = NVM_FILE_PATH;

            let _ = std::fs::remove_file(file_name);
            let storage = NVMTableStorage::new(file_name, 0).unwrap();
            STORAGE.set(RwLock::new(storage)).unwrap();
        }
    }
    pub fn reload_test_database() {
        // log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        // debug!("INFO");
        // debug!("DEBUG");
        #[cfg(feature = "dash")]
        unsafe {
            let path = std::ffi::CString::new(INDEX_FILE_PATH).expect("CString::new failed");
            init(path.as_ptr());
        }
        #[cfg(feature = "nbtree")]
        unsafe {
            let path = std::ffi::CString::new(BTREE_FILE_PATH).expect("CString::new failed");
            init_btree_file(path.as_ptr());
        }
        if STORAGE.get().is_none() {
            #[cfg(feature = "native")]
            let file_name = "_test_persist";
            #[cfg(feature = "nvm_server")]
            let file_name = NVM_FILE_PATH;
            // let _ = std::fs::remove_file(file_name);
            let storage = NVMTableStorage::new(file_name, 0).unwrap();
            STORAGE.set(RwLock::new(storage)).unwrap();
        }
    }
    pub fn global(
    ) -> parking_lot::lock_api::RwLockReadGuard<'static, parking_lot::RawRwLock, NVMTableStorage>
    {
        STORAGE.get().unwrap().read()
    }

    pub fn global_mut(
    ) -> parking_lot::lock_api::RwLockWriteGuard<'static, parking_lot::RawRwLock, NVMTableStorage>
    {
        STORAGE.get().unwrap().write()
    }

    /// Allocate a new page.
    pub fn alloc_page(&mut self) -> Result<PageId> {
        let page_id = self.page_bitmap.alloc().ok_or(Error::NoSpace)?;
        // debug!("alloc page: {}", page_id);
        // if page_id > 50000 {
        //     println!("page allocate {}", page_id);

        // }

        Ok(PageId::new(page_id))
    }

    pub fn free_page_list(&mut self, page: PageId) {
        self.page_bitmap.free(page.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create() {
        let _ = std::fs::remove_file("/tmp/test_database.db");
        let mut storage = NVMTableStorage::new("/tmp/test_database.db", 0).unwrap();
        assert_eq!(storage.alloc_page().unwrap().id, 0);
        assert_eq!(storage.alloc_page().unwrap().id, 1);
        let _ = std::fs::remove_file("/tmp/test_database.db");
    }
}
