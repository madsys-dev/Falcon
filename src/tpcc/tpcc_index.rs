use crate::storage::table::TupleId;
use once_cell::sync::OnceCell;
use std::collections::HashMap;

#[derive(Debug)]
pub struct TpccIndex {
    tuple_index: HashMap<u64, TupleId>,
}

static mut WAREHOUSES_INDEX: OnceCell<TpccIndex> = OnceCell::new();
static mut DISTRICTS_INDEX: OnceCell<TpccIndex> = OnceCell::new();
static mut CUSTOMERS_INDEX: OnceCell<TpccIndex> = OnceCell::new();
static mut ITEMS_INDEX: OnceCell<TpccIndex> = OnceCell::new();
static mut ORDERS_INDEX: OnceCell<TpccIndex> = OnceCell::new();
static mut ORDER_LINES_INDEX: OnceCell<TpccIndex> = OnceCell::new();
static mut NEW_ORDERS_INDEX: OnceCell<TpccIndex> = OnceCell::new();
static mut STOCKS_INDEX: OnceCell<TpccIndex> = OnceCell::new();

impl TpccIndex {
    pub fn new() -> Self {
        TpccIndex {
            tuple_index: HashMap::new(),
        }
    }

    pub fn add(&mut self, key: u64, tid: TupleId) {
        assert!(!self.tuple_index.contains_key(&key));
        self.tuple_index.insert(key, tid);
    }
    pub fn get(&self, key: u64) -> TupleId {
        assert!(self.tuple_index.contains_key(&key));
        self.tuple_index.get(&key).unwrap().clone()
    }
    pub fn init_tpcc() {
        unsafe {
            if WAREHOUSES_INDEX.get().is_none() {
                WAREHOUSES_INDEX.set(TpccIndex::new()).unwrap();
            }
            if DISTRICTS_INDEX.get().is_none() {
                DISTRICTS_INDEX.set(TpccIndex::new()).unwrap();
            }
            if CUSTOMERS_INDEX.get().is_none() {
                CUSTOMERS_INDEX.set(TpccIndex::new()).unwrap();
            }
            if ITEMS_INDEX.get().is_none() {
                ITEMS_INDEX.set(TpccIndex::new()).unwrap();
            }
            if ORDERS_INDEX.get().is_none() {
                ORDERS_INDEX.set(TpccIndex::new()).unwrap();
            }
            if ORDER_LINES_INDEX.get().is_none() {
                ORDER_LINES_INDEX.set(TpccIndex::new()).unwrap();
            }
            if NEW_ORDERS_INDEX.get().is_none() {
                NEW_ORDERS_INDEX.set(TpccIndex::new()).unwrap();
            }
            if STOCKS_INDEX.get().is_none() {
                STOCKS_INDEX.set(TpccIndex::new()).unwrap();
            }
        }
    }
    pub fn warehouses_index() -> &'static mut Self {
        unsafe { WAREHOUSES_INDEX.get_mut().expect("tpcc is not initialized") }
    }
    pub fn districts_index() -> &'static mut Self {
        unsafe { DISTRICTS_INDEX.get_mut().expect("tpcc is not initialized") }
    }
    pub fn customers_index() -> &'static mut Self {
        unsafe { CUSTOMERS_INDEX.get_mut().expect("tpcc is not initialized") }
    }
    pub fn items_index() -> &'static mut Self {
        unsafe { ITEMS_INDEX.get_mut().expect("tpcc is not initialized") }
    }
    pub fn orders_index() -> &'static mut Self {
        unsafe { ORDERS_INDEX.get_mut().expect("tpcc is not initialized") }
    }
    pub fn order_lines_index() -> &'static mut Self {
        unsafe {
            ORDER_LINES_INDEX
                .get_mut()
                .expect("tpcc is not initialized")
        }
    }
    pub fn new_orders_index() -> &'static mut Self {
        unsafe { NEW_ORDERS_INDEX.get_mut().expect("tpcc is not initialized") }
    }
    pub fn stocks_index() -> &'static mut Self {
        unsafe { STOCKS_INDEX.get_mut().expect("tpcc is not initialized") }
    }
}
