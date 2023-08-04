use crate::config::*;
// use crate::index::bplus_tree::BplusTree;
use crate::mvcc_config::*;
use crate::storage::delta::*;
use crate::storage::row::{Tuple, TupleError, DELETE_FLAG};
use crate::storage::schema::*;
use crate::storage::timestamp::TimeStamp;
use crate::transaction::snapshot::SnapShotEntity;
use crate::utils::io;
use crate::utils::persist::persist_array::PersistArray;
// use crate::util::persist::persist_bitmap::PersistBitmap;
use super::global::Timer;
#[cfg(feature = "dash")]
use super::index::dash::Dash;
#[cfg(feature = "dash")]
use super::index::dashstring::DashString;
// use super::index::nbtree::NBTree;
use super::row::BufferDataVec;
use super::row::COMMIT_MASK;
use crate::storage::allocator::{DualPageAllocator, LocalPageAllocator, Page};
use crate::{Error, Result};
use crossbeam_epoch::Guard;
use dashmap::DashMap;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::convert::TryInto;
use std::str;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use concurrent_map::ConcurrentMap;
use bztree::BzTree;
use super::global::*;
use super::row::BufferVec;
pub mod buffer;
pub mod crud;
pub mod index;

#[cfg(feature = "index_bplus_tree")]
type Index<T> = BplusTree<T, TupleId>;
#[cfg(feature = "rust_hash")]
type Index<T> = HashMap<T, TupleId>;
#[cfg(feature = "rust_dash")]
type Index<T> = DashMap<T, TupleId>;
#[cfg(feature = "nbtree")]
type Index<u64> = NBTree<u64>;
#[cfg(feature = "dash")]
type Index<u64> = Dash<u64>;

#[cfg(feature = "rust_map")]
type RangeIndex<T> = BzTree<T, TupleId>;

#[cfg(feature = "local_allocator")]
type TupleAllocator = LocalPageAllocator;
#[cfg(feature = "center_allocator")]
type TupleAllocator = DualPageAllocator;
// #[derive(Debug)]
pub enum TableIndex {
    Int64(Index<u64>),
    #[cfg(feature = "dash")]
    String(DashString),
    #[cfg(not(feature = "dash"))]
    String(Index<String>),
    Int64R(RangeIndex<u64>),
    StringR(RangeIndex<String>),
    
    None,
}

impl std::fmt::Debug for TableIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            &TableIndex::Int64(index) => {index.fmt(f)}
            &TableIndex::String(index) => {index.fmt(f)}
            _ => {f.write_str("index debug not support")}
            
        }
	}
}
#[derive(Debug)]
pub enum IndexType {
    Int64(u64),
    String(String),
}
#[derive(Debug)]
pub struct TupleId {
    pub page_start: AtomicU64,
}

impl Eq for TupleId {}

impl std::hash::Hash for TupleId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.get_address().hash(state);
    }
}

impl PartialEq for TupleId {
    fn eq(&self, other: &Self) -> bool {
        self.get_address() == other.get_address()
    }
}

impl Clone for TupleId {
    fn clone(&self) -> Self {
        Self {
            page_start: AtomicU64::new(self.get_address()),
        }
    }
}

impl TupleId {
    pub fn new(page_start: u64, id: u64, size: u64) -> Self {
        TupleId {
            page_start: AtomicU64::new(page_start + id * size),
        }
    }
    pub fn from_address(page_start: u64) -> Self {
        TupleId {
            page_start: AtomicU64::new(page_start),
        }
    }
    pub fn get_address(&self) -> u64 {
        self.page_start.load(Ordering::Relaxed)
    }
    pub fn update(&self, data: u64) {
        // println!("{:x}", data);
        self.page_start.store(data, Ordering::Relaxed);
    }
    pub fn cas(&self, old: u64, new: u64) -> u64 {
        // println!("{:x}", data);
        let result = self
            .page_start
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst);
        match result {
            Ok(u) => return u,
            Err(u) => return u,
        }
    }

    // #[cfg(feature = "buffer_pool")]
    // pub fn is_pool_id(&self) -> bool {
    //     return (self.page_start & POW_2_63) > 0;
    // }
    // #[cfg(feature = "buffer_pool")]
    // pub fn pool_id(&self) -> usize {
    //     return (self.page_start ^ POW_2_63) as usize;
    // }
}

/// meta_page(array)
/// |len (8bytes) | page1(8bytes) | page2(8bytes) ……|
///
/// tuple_page
/// --------------------------------------------------------------------
/// |                     bitmap(max_tuple bits)                       |
/// --------------------------------------------------------------------
/// |                              bitmap                              |
/// --------------------------------------------------------------------
/// |  bitmap  | tuple1(tuple_size bytes) | tuple2(tuple_size bytes)|……｜
///
#[derive(Debug)]
pub struct Table {
    pub schema: TableSchema,
    pub id: u32,
    pub meta_page: PersistArray<u64>,
    #[cfg(feature = "local_allocator")]
    allocator: Vec<RwLock<TupleAllocator>>,
    #[cfg(feature = "center_allocator")]
    allocator: TupleAllocator,
    primary_key: AtomicUsize,
    #[cfg(not(feature = "lock_index"))]
    index: HashMap<usize, TableIndex>,
    #[cfg(feature = "lock_index")]
    index: HashMap<usize, RwLock<TableIndex>>,
    // index_key: RwLock<Vec<usize>>,
    pub tuple_size: u64,
    max_tuple: u64,
    tuple_start: u64,
    #[cfg(feature = "buffer_pool")]
    pub pool_size: usize,
    #[cfg(feature = "buffer_pool")]
    pub buffer_pool: Vec<BufferVec>,
    #[cfg(feature = "buffer_pool")]
    pub buffer_pointer: Vec<RwLock<usize>>,
    // #[cfg(feature = "buffer_pool")]
    // pub tuple2pool: DashMap<TupleId, usize>,
    // #[cfg(feature = "append")]
    // pub tid2address: DashMap<TupleId, Address>,
}

impl Table {
    // A tuple takes up space bits of its own space + 1 bit of bitmap space
    fn max_tuple(size: u64) -> u64 {
        PAGE_SIZE / size
    }
    // Calculate the starting bytes of the tuple, aligned 16bytes
    fn tuple_start(max_tuple: u64) -> u64 {
        ((max_tuple + 127) / 128) * 16
    }

    fn align16_tuple_size(tuple_size: u64) -> u64 {
        let size = ((tuple_size + 15) >> 4) << 4;
        // if size < TUPLE_SIZE_BASE {
        //     return TUPLE_SIZE_BASE;
        // }
        return size;
    }

    // new&reload
    pub fn new(schema: TableSchema, address: Address, id: u32) -> Self {
        println!("Build table at {:X}", address);
        let tuple_size = Table::align16_tuple_size(schema.tuple_size() as u64);
        let max_tuple = Table::max_tuple(tuple_size);
        let tuple_start = 0;
        let meta_page = PersistArray::new(address);
        #[cfg(feature = "buffer_pool")]
        let mut buffer_pointer: Vec<RwLock<usize>> = Vec::new();
        #[cfg(feature = "buffer_pool")]
        for i in 0..TRANSACTION_COUNT {
            buffer_pointer.push(RwLock::new(i * 100));
        }
        let mut table = Table {
            tuple_size,
            id,
            schema,
            primary_key: AtomicUsize::new(0),
            index: HashMap::new(),
            #[cfg(feature = "local_allocator")]
            allocator: std::iter::from_fn(|| {
                Some(RwLock::new(TupleAllocator::new(address, max_tuple as u32)))
            })
            .take(TRANSACTION_COUNT as usize)
            .collect(),
            #[cfg(feature = "center_allocator")]
            allocator: TupleAllocator::new(address, max_tuple as u32),
            // index_key: RwLock::new(Vec::new()),
            meta_page: meta_page,
            max_tuple,
            tuple_start,
            #[cfg(feature = "buffer_pool")]
            pool_size: 0,
            #[cfg(feature = "buffer_pool")]
            buffer_pool: Vec::new(),
            #[cfg(feature = "buffer_pool")]
            buffer_pointer,
            // #[cfg(feature = "buffer_pool")]
            // tuple2pool: DashMap::new(),
            // #[cfg(feature = "append")]
            // tid2address: DashMap::new(),
        };
        #[cfg(feature = "buffer_pool")]
        table.set_pool_size(POOL_SIZE);
        table
    }
    pub fn reload(schema: TableSchema, address: Address, id: u32) -> Self {
        println!("reload table at {}", address);
        let tuple_size = schema.tuple_size() as u64 + U64_OFFSET;
        let max_tuple = Table::max_tuple(tuple_size);
        let tuple_start = Table::tuple_start(max_tuple);
        let meta_page = PersistArray::reload(address);
        #[cfg(feature = "buffer_pool")]
        let mut buffer_pointer: Vec<RwLock<usize>> = Vec::new();
        #[cfg(feature = "buffer_pool")]
        for i in 0..TRANSACTION_COUNT {
            buffer_pointer.push(RwLock::new(i * 100));
        }
        let mut table = Table {
            tuple_size: schema.tuple_size() as u64,
            id,
            schema,
            primary_key: AtomicUsize::new(0),
            index: HashMap::new(),
            // index_key: RwLock::new(Vec::new()),
            #[cfg(feature = "local_allocator")]
            allocator: std::iter::from_fn(|| {
                Some(RwLock::new(TupleAllocator::new(address, max_tuple as u32)))
            })
            .take(TRANSACTION_COUNT as usize)
            .collect(),
            #[cfg(feature = "center_allocator")]
            allocator: TupleAllocator::new(address, max_tuple as u32),
            meta_page: meta_page,
            max_tuple,
            tuple_start,
            #[cfg(feature = "buffer_pool")]
            pool_size: 0,
            #[cfg(feature = "buffer_pool")]
            buffer_pool: Vec::new(),
            #[cfg(feature = "buffer_pool")]
            buffer_pointer,
        };
        println!("reload page count {}", table.meta_page.len());
        #[cfg(feature = "buffer_pool")]
        table.set_pool_size(POOL_SIZE);
        // table.reload_table();
        table
    }

    // TODO_RESTORE
    fn reload_table(&self) {
        #[cfg(feature = "local_allocator")]
        {
            let len = self.meta_page.len();
            let mut committed_tid = 0;
            let mut tuples: Vec<TupleId> = Vec::new();
            for i in 0..len {
                let page_start = self.meta_page.get(i).unwrap();
                for id in 0..self.max_tuple {
                    let tuple_id = TupleId::new(page_start, id, self.tuple_size);
                    let tuple = Tuple::reload(self.get_address(&tuple_id));
                    let flag = tuple.delete_flag();
                    if flag == 0 {
                        continue;
                    }
                    let tid = tuple.lock_tid();
                    if (flag & COMMIT_MASK) != 0 && tid > committed_tid {
                        committed_tid = tid;
                    }
                    if tid <= committed_tid {
                        //committed
                    } else {
                        tuples.push(tuple_id);
                    }
                }
            }
            for tuple_id in &tuples {
                let tuple = Tuple::reload(self.get_address(tuple_id));
                let tid = tuple.lock_tid();
                if tid <= committed_tid {
                    //committed
                } else {
                    //uncommitted
                    self.remove_tuple(tuple_id, 0).unwrap();
                }
            }
        }
    }

    fn reload_page(&self, page_id: u64, page_address: Address) -> Page {
        let mut free_count = self.max_tuple;

        for offset in 0..self.tuple_start {
            let a: u8 = unsafe { io::read(page_address + offset) };
            if a == 0xFF {
            } else {
                if a == 0 {
                    free_count += 8;
                } else {
                    for i in 0..8 {
                        if (1 << i) & a == 0 {
                            free_count += 1;
                        }
                    }
                }
            }
            if offset == page_address {
                break;
            }
        }
        Page::new(page_address, self.max_tuple as u32)
    }

    pub fn set_primary_key(&mut self, key: usize) -> Result {
        // let mut p = self.primary_key.write().unwrap();
        self.primary_key
            .store(key, std::sync::atomic::Ordering::SeqCst);
        if self.index.contains_key(&key) {
            return Ok(());
        }
        self.add_index(key)
    }
    pub fn set_range_primary_key(&mut self, key: usize) -> Result {
        // let mut p = self.primary_key.write().unwrap();
        self.primary_key
            .store(key, std::sync::atomic::Ordering::SeqCst);
        if self.index.contains_key(&key) {
            return Ok(());
        }
        self.add_range_index(key)
    }
    pub fn get_primary_key(&self) -> usize {
        self.primary_key.load(Ordering::Relaxed)
    }
    pub fn add_index_by_name(&mut self, key: &str) -> Result {
        self.add_index(self.schema.search_by_name(key).unwrap())
    }
    pub fn add_index(&mut self, key: usize) -> Result {
        if self.index.contains_key(&key) {
            return Ok(());
        }
        let key_type = self.schema.columns()[key].type_;
        println!("add_index_on {}", self.schema.columns()[key].name);
        let index = &mut self.index;
        match key_type {
            ColumnType::Int64 => {
                #[cfg(not(feature = "lock_index"))]
                index.insert(key, TableIndex::Int64(Index::<u64>::new()));
                #[cfg(feature = "lock_index")]
                index.insert(key, RwLock::new(TableIndex::Int64(Index::<u64>::new())));
            }
            ColumnType::String { len: _ } => {
                #[cfg(not(any(feature = "lock_index", feature = "dash")))]
                index.insert(key, TableIndex::String(Index::<String>::new()));
                #[cfg(feature = "dash")]
                index.insert(key, TableIndex::String(DashString::new()));
                #[cfg(feature = "lock_index")]
                index.insert(key, RwLock::new(TableIndex::String(Index::<String>::new())));
            }
            _ => return Err(Error::Tuple(TupleError::IndexTypeNotSupported)),
        }
        // let mut index_key = self.index_key.write().unwrap();
        // index_key.push(key);
        Ok(())
    }
    pub fn add_range_index_by_name(&mut self, key: &str) -> Result {
        self.add_range_index(self.schema.search_by_name(key).unwrap())
    }
    pub fn add_range_index(&mut self, key: usize) -> Result {
        if self.index.contains_key(&key) {
            return Ok(());
        }
        let key_type = self.schema.columns()[key].type_;
        println!("add_range_ndex_on {}", self.schema.columns()[key].name);
        let index = &mut self.index;
        match key_type {
            ColumnType::Int64 => {
                #[cfg(feature = "rust_map")]
                index.insert(key, TableIndex::Int64R(RangeIndex::<u64>::default()));
            }
            ColumnType::String { len: _ } => {
                #[cfg(feature = "rust_map")]
                index.insert(key, TableIndex::StringR(RangeIndex::<String>::default()));
            }
            _ => return Err(Error::Tuple(TupleError::IndexTypeNotSupported)),
        }
        // let mut index_key = self.index_key.write().unwrap();
        // index_key.push(key);
        Ok(())
    }
    #[cfg(not(feature = "lock_index"))]
    pub fn get_index_key(&self) -> std::collections::hash_map::Keys<usize, TableIndex> {
        self.index.keys()
    }
    #[cfg(feature = "lock_index")]
    pub fn get_index_key(&self) -> std::collections::hash_map::Keys<usize, RwLock<TableIndex>> {
        self.index.keys()
    }
    #[cfg(feature = "nbtree")]
    pub fn init_index(&self, thread_id: i32) {
        for (_, index) in &self.index {
            match index {
                TableIndex::Int64(index) => {
                    index.init(thread_id);
                }
                _ => {}
            };
        }
    }
    pub fn pre_alloc(&self, count: u64) {
        for tid in 0..TRANSACTION_COUNT {
            let mut allocator = self.allocator.get(tid).unwrap().write();
            allocator.pre_alloc(count);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::nvm_file::NVMTableStorage;

    use super::*;

    #[test]
    // #[ignore]
    fn test_add_get() {
        let mut schema = TableSchema::new();
        schema.push(ColumnType::Int64, "a");
        schema.push(ColumnType::Int64, "b");

        // let mut tuple = Tuple::new(0, "257,4", &schema).expect("");

        // let mut tuple2 = Tuple::new("257$41", &schema).expect("");

        // for d in tuple2.as_bytes() {
        //     print!("{} ", d);
        // }
        // println!();
        NVMTableStorage::init_test_database();
        let mut storage = NVMTableStorage::global_mut();
        let page_id = storage.alloc_page().unwrap();
        drop(storage);

        println!("table restore {}", page_id.page_start);

        let mut table = Table::new(schema, page_id.page_start, 0);

        println!("table ok {}", page_id.page_start);

        let tuple_id = table.allocate_tuple(0).unwrap();
        let tuple_id2 = table.allocate_tuple(0).unwrap();

        let tuple_address = table.get_address(&tuple_id);
        let ts = {
            let mut t = TimeStamp::default();
            t.tid = 0;
            t
        };
        let _ = Tuple::new(tuple_address, "257,4", &table.schema, ts).expect("");
        let tuple_address = table.get_address(&tuple_id2);

        let t2 = Tuple::new(tuple_address, "258,4", &table.schema, ts).expect("");

        table.set_primary_key(0).unwrap();
        table
            .index_insert(IndexType::Int64(257), &tuple_id)
            .unwrap();
        table.index_insert_by_tuple(&tuple_id2, &t2).unwrap();
        let index_tuple_id = table.search_tuple_id(&IndexType::Int64(257)).unwrap();
        assert_eq!(index_tuple_id.get_address(), tuple_id.get_address());

        let index_tuple_id = table.search_tuple_id(&IndexType::Int64(258)).unwrap();
        assert_eq!(index_tuple_id.get_address(), tuple_id2.get_address());

        // let _t = table.get_tuple(u).unwrap();
        // table.fix_tuple(u, 0, 0, &[0]).unwrap();
    }
}
