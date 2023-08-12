use super::timestamp::TimeStamp;
use crate::config::{Address, CATALOG_ADDRESS};
use crate::config::{PAGE_SIZE, U64_OFFSET};
use crate::mvcc_config::TRANSACTION_COUNT;
use crate::range;
use crate::storage::nvm_file::NVMTableStorage;
use crate::storage::schema::TableSchema;
use crate::storage::table::Table;
use crate::storage::variable_table::VariableTable;
use crate::transaction::clog::Clog;
use crate::transaction::snapshot::SnapShot;
use crate::transaction::transaction::Transaction;
use crate::transaction::transaction_buffer::TransactionBuffer;
use crate::utils::persist::persist_struct::PersistStruct;
use crate::utils::{file, io};
use crate::Result;
use once_cell::sync::OnceCell;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use std::time::SystemTime;

pub const TABLE_ID: Range<u64> = range!(0, 1);
pub const TABLE_NAME: Range<u64> = range!(TABLE_ID.end, 15);
pub const TABLE_ADDRESS: Range<u64> = range!(TABLE_NAME.end, U64_OFFSET);
pub const SCHEMA_OFFSET: u64 = TABLE_ADDRESS.end;

/// | len(u64) | id(u8) | name ([str, 15]) | address (u64) | schema ([u8] table_schema) ｜
#[derive(Copy, Clone, Debug)]
pub struct TableDes {
    data: PersistStruct,
}

impl TableDes {
    pub fn new(address: Address) -> Self {
        println!("Build des at {}", address);
        TableDes {
            data: PersistStruct::new(address, SCHEMA_OFFSET),
        }
    }
    pub fn reload(address: Address) -> Self {
        TableDes {
            data: PersistStruct::reload(address, SCHEMA_OFFSET),
        }
    }

    pub fn _address(&self) -> u64 {
        self.data._address()
    }
    pub fn id(&self) -> u8 {
        self.get_meta_data(TABLE_ID)
    }
    pub fn get_meta_data<T: Copy + Display>(&self, parameter: Range<u64>) -> T {
        self.data.get_meta_data(parameter)
    }
    pub fn set_meta_data<T: Display>(&self, parameter: Range<u64>, value: T) {
        self.data.set_meta_data(parameter, value)
    }
    pub fn len(&self) -> u64 {
        self.data.len()
    }
    pub fn save(&mut self, bytes: &[u8]) {
        self.data.save(bytes);
    }
    pub fn table_name(&self) -> String {
        let vec = self.data.range(TABLE_NAME.start, TABLE_NAME.end);
        String::from_utf8(vec.to_vec())
            .unwrap()
            .trim_end_matches(char::from(0))
            .to_string()
        // self.get_meta_data(TABLE_NAME)
    }
    pub fn set_table_name(&self, name: &str) {
        let s = name.to_string();
        let mut name_bytes = s.clone().into_bytes();
        self.data
            .copy_from_slice(TABLE_NAME.start, name_bytes.as_slice());
    }
    pub fn table_address(&self) -> u64 {
        self.get_meta_data(TABLE_ADDRESS)
    }

    pub fn get_schema(&self) -> TableSchema {
        TableSchema::from_bytes(self.data.data())
    }
    pub fn save_schema(&mut self, schema: &[u8]) {
        assert_eq!(self.data._new(), true);

        self.save(schema);
    }
    pub fn to_table(&self, id: u32) -> Table {
        println!("gen table {}", self.table_address());
        let start = SystemTime::now();
        let table = Table::reload(self.get_schema(), self.table_address(), id);
        let end = SystemTime::now();
        println!(
            "Rebuild table uses {} nano seconds",
            end.duration_since(start).unwrap().as_nanos()
        );
        table
    }
}

pub const TRANSACTION_PAGE_ADDRESS: Range<u64> =
    range!(0, 2 * U64_OFFSET * TRANSACTION_COUNT as u64);
pub const TRANSACTION_TS_ADDRESS: Range<u64> = range!(
    TRANSACTION_PAGE_ADDRESS.end,
    2 * U64_OFFSET * TRANSACTION_COUNT as u64
);

pub const CATELOG_HEADER: u64 = TRANSACTION_TS_ADDRESS.end;
pub const CLOG_MAX_PAGES: u64 = 0;
pub const CLOG_SPACE_SIZE: u64 = CLOG_MAX_PAGES * U64_OFFSET + U64_OFFSET;

/// The data part of the Catalog is a variable-length table that holds the table's metadata.
/// Catelog stores a HashMap in memory for quick lookups of table
#[derive(Debug)]
pub struct Catalog {
    address: Address,
    table_index: RwLock<HashMap<String, Arc<Table>>>,
    table_space: RwLock<VariableTable>,
    clog: Clog,
    snapshot: SnapShot,
}
unsafe impl Send for Catalog {}
unsafe impl Sync for Catalog {}

pub static CATALOG: OnceCell<Catalog> = OnceCell::new();

impl Catalog {
    pub fn get_address(&self) -> Address {
        self.address
    }
    pub fn new(address: Address) -> Self {
        Catalog {
            address,
            table_index: RwLock::new(HashMap::new()),
            table_space: RwLock::new(VariableTable::new(
                address + CATELOG_HEADER,
                PAGE_SIZE - CATELOG_HEADER - CLOG_SPACE_SIZE,
            )),
            clog: Clog::new(),
            snapshot: SnapShot::new(),
        }
    }

    pub fn reload(address: Address) {
        let start = SystemTime::now();

        let catalog = Catalog {
            address,
            table_index: RwLock::new(HashMap::new()),
            table_space: RwLock::new(VariableTable::reload(address + CATELOG_HEADER)),
            clog: Clog::new(),
            snapshot: SnapShot::new(),
        };
        let table_space = catalog.table_space.read().unwrap();
        let headers = table_space.get_all_headers();
        let mut table_index = catalog.table_index.write().unwrap();
        let end = SystemTime::now();
        println!(
            "Reload table des uses {} nano seconds",
            end.duration_since(start).unwrap().as_nanos()
        );
        for header in headers {
            if header.free_size == 0 {
                let table_des = TableDes::reload(header.data_address);
                let table = table_des.to_table(table_des.id() as u32);
                // println!("111 {}, {}", table_des.table_name(), table_des.table_name().len());
                table_index.insert(table_des.table_name(), Arc::new(table));
            }
        }
        drop(table_space);
        drop(table_index);
        if CATALOG.get().is_none() {
            CATALOG.set(catalog).unwrap();
        }
    }

    pub fn add_table(&self, name: &str, schema: TableSchema) -> Result {
        let mut table_space = self.table_space.write().unwrap();
        let mut table_index = self.table_index.write().unwrap();

        assert!(!table_index.contains_key(name));
        let schema_bytes = schema.to_bytes();
        let table_meta_len = U64_OFFSET + SCHEMA_OFFSET + schema_bytes.len() as u64;
        let (id, address) = table_space.allocate(table_meta_len).unwrap();
        let mut storage = NVMTableStorage::global_mut();
        let table_address = storage.alloc_page().unwrap().page_start;
        drop(storage);
        let table = Table::new(schema, table_address, id as u32);
        println!("create table id = {}, name = {}", id, name);

        let mut table_des = TableDes::new(address);

        table_des.set_table_name(name);
        table_des.set_meta_data(TABLE_ID, id);
        table_des.save_schema(&schema_bytes.as_slice());
        table_des.set_meta_data(TABLE_ADDRESS, table_address);
        file::sfence();
        table_space.set_header_by_id(id, address, 0);
        table_index.insert(String::from(name), Arc::new(table));

        Ok(())
    }

    // pub fn get_mut_table(&mut self, table_name: &str) -> Option<&mut Table> {
    //     self.table_index.get_mut(table_name)
    // }
    pub fn get_table(&self, table_name: &str) -> Arc<Table> {
        self.table_index
            .read()
            .unwrap()
            .get(table_name)
            .cloned()
            .unwrap()
    }
    // pub fn get_table_mut(&self, table_name: &str) -> Arc<&mut Table>  {
    //     self.table_index.write().unwrap().get_mut(table_name).unwrap()
    // }
    #[cfg(feature = "cc_cfg_occ")]
    pub fn get_clog(&self) -> &Clog {
        &self.clog
    }
    pub fn get_snapshot(&self) -> &SnapShot {
        &self.snapshot
    }
    // pub fn get_snapshot_mut(&mut self) -> &mut SnapShot {
    //     &mut self.snapshot
    // }
    pub fn set_transaction_page_start(&self, thread_id: u64, address: Address) {
        let iter = self.address + thread_id * U64_OFFSET;
        unsafe {
            io::write(iter, address);
        }
        file::sfence();
    }
    pub fn get_transaction_page_start(&self, thread_id: u64) -> u64 {
        let iter = CATALOG_ADDRESS + thread_id * U64_OFFSET;
        unsafe { io::read(iter) }
    }

    #[inline]
    pub fn update_ts(thread_id: u64, ts: u64) {
        let iter = CATALOG_ADDRESS + TRANSACTION_TS_ADDRESS.start + thread_id * U64_OFFSET;
        let u = unsafe { &*(iter as *const AtomicU64) };
        u.store(ts, Ordering::Relaxed);
        #[cfg(feature = "clwb_txn")]
        {
            unsafe {
                io::clwb(iter as *const u8);
                file::sfence();
            }
        }
    }

    #[inline]
    pub fn get_transaction_ts(thread_id: u64) -> u64 {
        let iter = CATALOG_ADDRESS + TRANSACTION_TS_ADDRESS.start + thread_id * U64_OFFSET;
        let u = unsafe { &*(iter as *const AtomicU64) };
        u.load(Ordering::Relaxed)
    }

    pub fn init_catalog() {
        if CATALOG.get().is_none() {
            CATALOG.set(Catalog::new(CATALOG_ADDRESS)).unwrap();
        }
    }
    pub fn global() -> &'static Catalog {
        CATALOG.get().unwrap()
    }
    // pub fn global_mut() -> std::sync::RwLockWriteGuard<'static, Catalog>  {
    //     CATALOG.get().unwrap().write().unwrap()
    // }
    pub fn set_primary_key(&self, table_name: &str, key: usize) {
        let table = self.get_table(table_name);
        let mut storage = NVMTableStorage::global_mut();
        let table_address = storage.alloc_page().unwrap().page_start;
        drop(storage);
        //TODO Reload
        let mut new_table = Table::new(table.schema.clone(), table.meta_page._address(), table.id);
        println!("create table id = {}, name = {}", table.id, table_name);

        for key in table.get_index_key() {
            new_table.add_index(*key).unwrap();
        }
        new_table.set_primary_key(key).unwrap();

        let mut table_index = self.table_index.write().unwrap();
        table_index.insert(String::from(table_name), Arc::new(new_table));
    }
    pub fn set_range_primary_key(&self, table_name: &str, key: usize) {
        let table = self.get_table(table_name);
        let mut storage = NVMTableStorage::global_mut();
        let table_address = storage.alloc_page().unwrap().page_start;
        drop(storage);
        //TODO Reload
        let mut new_table = Table::new(table.schema.clone(), table.meta_page._address(), table.id);
        println!("create table id = {}, name = {}", table.id, table_name);

        for key in table.get_index_key() {
            new_table.add_index(*key).unwrap();
        }
        new_table.set_range_primary_key(key).unwrap();
        // new_table.set_primary_key(table.get_primary_key()).unwrap();

        let mut table_index = self.table_index.write().unwrap();
        table_index.insert(String::from(table_name), Arc::new(new_table));
    }
    pub fn add_index_by_name(&self, table_name: &str, key: &str) {
        let table = self.get_table(table_name);
        let mut storage = NVMTableStorage::global_mut();
        let table_address = storage.alloc_page().unwrap().page_start;
        drop(storage);
        //TODO Reload
        let mut new_table = Table::new(table.schema.clone(), table_address, table.id);
        println!("create table id = {}, name = {}", table.id, table_name);
        for key in table.get_index_key() {
            new_table.add_index(*key).unwrap();
        }
        new_table.add_index_by_name(key).unwrap();
        new_table.set_primary_key(table.get_primary_key()).unwrap();

        let mut table_index = self.table_index.write().unwrap();
        table_index.insert(String::from(table_name), Arc::new(new_table));
    }
    pub fn add_range_index_by_name(&self, table_name: &str, key: &str) {
        let table = self.get_table(table_name);
        let mut storage = NVMTableStorage::global_mut();
        let table_address = storage.alloc_page().unwrap().page_start;
        drop(storage);
        //TODO Reload
        let mut new_table = Table::new(table.schema.clone(), table_address, table.id);
        println!("create table id = {}, name = {}", table.id, table_name);
        for key in table.get_index_key() {
            new_table.add_index(*key).unwrap();
        }
        new_table.add_range_index_by_name(key).unwrap();
        new_table.set_primary_key(table.get_primary_key()).unwrap();

        let mut table_index = self.table_index.write().unwrap();
        table_index.insert(String::from(table_name), Arc::new(new_table));
    }
    /// after index created
    #[cfg(feature = "buffer_pool")]
    pub fn set_pool_size(&self, table_name: &str, size: usize) {
        let table = self.get_table(table_name);
        let mut storage = NVMTableStorage::global_mut();
        let table_address = storage.alloc_page().unwrap().page_start;
        drop(storage);
        //TODO Reload
        let mut new_table = Table::new(table.schema.clone(), table_address, table.id);
        for key in table.get_index_key() {
            if table_name == "ORDER-LINE" || table_name == "NEW-ORDER" || table_name == "usertable" {
                new_table.add_range_index(*key).unwrap();
            }
            else {
                new_table.add_index(*key).unwrap();

            }
        }
        new_table.set_pool_size(size);
        new_table.set_primary_key(table.get_primary_key()).unwrap();
        let mut table_index = self.table_index.write().unwrap();
        table_index.insert(String::from(table_name), Arc::new(new_table));
    }

    #[cfg(feature = "cc_cfg_occ")]
    pub fn finished(&self, tid: u64) -> bool {
        let ts = TimeStamp { tid };
        SnapShot::is_finished(ts, &self.clog)
    }

    #[inline]
    pub fn get_min_txn() -> u64 {
        let mut min_txn = Catalog::get_transaction_ts(0);
        for i in 1..TRANSACTION_COUNT {
            min_txn = min(min_txn, Catalog::get_transaction_ts(i as u64));
        }
        min_txn
    }
    pub fn reload_timestamp(&self) {
        let mut max_txn = Catalog::get_transaction_ts(0);
        for i in 1..TRANSACTION_COUNT {
            max_txn = max(max_txn, Catalog::get_transaction_ts(i as u64));
        }
        println!("reload timestamp {}", max_txn);
        self.snapshot.reload_clock(max_txn + 1);
    }
    pub fn redo_transaction(&self) {
        for i in 0..TRANSACTION_COUNT {
            TransactionBuffer::reload(self, i as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NVM_ADDR;
    use crate::storage::schema::ColumnType;
    #[test]
    fn test_table_name() {
        NVMTableStorage::init_test_database();
        let table = TableDes::new(NVM_ADDR);
        table.set_meta_data(TABLE_NAME, "test");
        let s = table.table_name();
        assert_eq!(24, table.len());
        assert_eq!("test", s);
    }

    #[test]
    fn test_schema() {
        NVMTableStorage::init_test_database();
        let mut schema = TableSchema::new();
        schema.push(ColumnType::Int64, "a");
        schema.push(ColumnType::Double, "b");

        let mut table = TableDes::new(NVM_ADDR);
        table.save_schema(schema.to_bytes().as_slice());
        let r_schema = table.get_schema();
        let columns = r_schema.columns();
        assert_eq!(columns[0].type_, ColumnType::Int64);
        assert_eq!(columns[1].type_, ColumnType::Double);
    }

    #[test]
    fn test_add_table() {
        NVMTableStorage::init_test_database();
        let catalog = Catalog::new(NVM_ADDR);
        let mut schema = TableSchema::new();
        schema.push(ColumnType::Int64, "a");
        schema.push(ColumnType::Double, "b");
        catalog.add_table("test", schema).unwrap();
    }
    #[test]
    fn test_reload() {
        NVMTableStorage::init_test_database();
        let catalog = Catalog::new(NVM_ADDR);
        let mut schema = TableSchema::new();
        let table_name = "test";
        schema.push(ColumnType::Int64, "a");
        schema.push(ColumnType::Double, "b");
        catalog.add_table(table_name, schema).unwrap();
        Catalog::reload(NVM_ADDR);
        let reloaded_catalog = Catalog::global();
        let table = reloaded_catalog.get_table(&String::from(table_name));
        let columns = table.schema.columns();
        assert_eq!(columns[0].type_, ColumnType::Int64);
        assert_eq!(columns[1].type_, ColumnType::Double);
    }
}
