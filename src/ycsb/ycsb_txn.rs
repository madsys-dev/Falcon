use crate::{
    storage::{
        global::{Timer, READING},
        table::{IndexType, Table},
    },
    transaction::{transaction::Transaction, transaction_buffer::TransactionBuffer},
};

use super::ycsb_query::YcsbQuery;
use super::{Operation, Properties};
use crate::Result;

#[derive(Debug)]
pub struct YcsbTxn<'a> {
    props: Properties,
    pub txn: Transaction<'a>,
    pub read_buf: u8,
    pub result: Result<()>,
    #[cfg(feature = "txn_clock")]
    pub timer: Timer,
}

impl<'a> YcsbTxn<'a> {
    pub fn new(props: Properties, buffer: &'a mut TransactionBuffer) -> Self {
        YcsbTxn {
            props: props,
            txn: Transaction::new(buffer, false),
            result: Ok(()),
            read_buf: 0,
            #[cfg(feature = "txn_clock")]
            timer: Timer::new(),
        }
    }
    pub fn read(&mut self, table: &'a Table, key: u64, column: usize) -> bool {
        #[cfg(not(feature = "ycsb_read_tuple"))]
        {
            match table.search_tuple_id(&IndexType::Int64(key)) {
                Ok(tid) => {
                    // #[cfg(feature = "txn_clock")]
                    // self.timer.start(READING);
                    if tid.get_address() < crate::config::NVM_ADDR {
                        return false;
                    }
                    match self.txn.read_column(table, &tid, column) {
                        Ok(vec) => {
                            self.read_buf = vec.data[0];
                            // #[cfg(feature = "txn_clock")]
                            // self.timer.end(READING, READING);
                            return true;
                        }
                        _ => {
                            return false;
                        }
                    }
                }
                _ => {
                    println!("{}, not on index", key);
                    return false;
                }
            }
        }
        #[cfg(feature = "ycsb_read_tuple")]
        {
            match table.search_tuple_id(&IndexType::Int64(key)) {
                Ok(tid) => match self.txn.read(table, &tid) {
                    Ok(vec) => {
                        self.read_buf = vec.data[0];
                        return true;
                    }
                    _ => {
                        return false;
                    }
                },
                _ => {
                    println!("{}, not on index", key);
                    return false;
                }
            }
        }
        true
    }
    pub async fn async_read(&mut self, table: &'a Table, key: u64, column: usize) -> bool {
        match table.search_tuple_id(&IndexType::Int64(key)) {
            Ok(tid) => {
                self.txn.prefetch_read(table, &tid).await;
                match self.txn.read(table, &tid) {
                    Ok(vec) => {
                        self.read_buf = vec.get_column_by_id(&table.schema, column)[80];
                        return true;
                    }
                    _ => {
                        return false;
                    }
                }
            }
            _ => {
                println!("not on index");
                return false;
            }
        }
    }

    pub fn scan(&self, table: &Table, start_key: u64, record_count: u64) -> bool {
        true
    }

    pub fn update(&mut self, table: &'a Table, key: u64, column: usize, data: &[u8]) -> bool {
        match table.search_tuple_id(&IndexType::Int64(key)) {
            if tid.get_address() < crate::config::NVM_ADDR {
                return false;
            }
            Ok(tid) => match self.txn.update(table, &tid, column, data) {
                Ok(_) => return true,
                Err(e) => {
                    self.result = Err(e);
                    return false;
                }
            },
            _ => {
                println!("tuple id not found, key:{}", key);
                return false;
            }
        }
    }
    pub async fn async_update(
        &mut self,
        table: &'a Table,
        key: u64,
        column: usize,
        data: &[u8],
    ) -> bool {
        match table.search_tuple_id(&IndexType::Int64(key)) {
            Ok(tid) => {
                self.txn.prefetch_read(table, &tid).await;
                match self.txn.update(table, &tid, column, data) {
                    Ok(_) => return true,
                    Err(e) => {
                        self.result = Err(e);
                        return false;
                    }
                }
            }
            _ => {
                // println!("tuple id not found, key:{}", padded_key);
                return false;
            }
        }
    }

    pub fn insert(&mut self, table: &'a Table, value: &str) -> bool {
        self.txn.insert(table, value);
        true
    }

    fn delete(&self, table: &str, key: &str, thread_id: u64) -> bool {
        todo!()
    }

    pub fn begin(&mut self) {
        #[cfg(feature = "txn_clock")]
        self.timer.start(READING);
        self.txn.begin();
    }

    pub fn commit(&mut self) -> bool {
        if self.txn.commit() {
            #[cfg(feature = "txn_clock")]
            self.timer.end(READING, READING);
            return true;
        }
        false
    }

    pub fn abort(&mut self) {
        self.txn.abort();
    }

    pub fn run_txn(&mut self, table: &'a Table, query: &YcsbQuery) -> bool {
        self.txn.read_only = true;
        for req in &query.requests {
            if req.op != Operation::Read {
                self.txn.read_only = false;
                break;
            }
        }
        self.begin();
        self.result = Ok(());
        for req in &query.requests {
            match req.op {
                Operation::Read => {
                    if !self.read(table, req.key, req.column) {
                        self.txn.abort();
                        return false;
                    }
                }
                Operation::Update => {
                    if !self.update(table, req.key, req.column, req.value.as_bytes()) {
                        self.txn.abort();
                        return false;
                    }
                }
                Operation::Scan => {
                    if !self.scan(table, req.key, self.props.workload.scan_len) {
                        self.txn.abort();
                        return false;
                    }
                }
                _ => {
                    self.txn.abort();
                    return false;
                }
            }
        }
        self.commit()
    }
    pub async fn run_txn_async(&mut self, table: &'a Table, query: &YcsbQuery) -> bool {
        self.txn.read_only = true;
        for req in &query.requests {
            if req.op != Operation::Read {
                self.txn.read_only = false;
                break;
            }
        }
        self.txn.begin();
        self.result = Ok(());
        for req in &query.requests {
            match req.op {
                Operation::Read => {
                    if !self.async_read(table, req.key, req.column).await {
                        self.txn.abort();
                        return false;
                    }
                }
                Operation::Update => {
                    if !self
                        .async_update(table, req.key, req.column, req.value.as_bytes())
                        .await
                    {
                        self.txn.abort();
                        return false;
                    }
                }
                Operation::Scan => {
                    if !self.scan(table, req.key, self.props.workload.scan_len) {
                        self.txn.abort();
                        return false;
                    }
                }
                _ => {
                    self.txn.abort();
                    return false;
                }
            }
        }
        self.txn.commit()
    }
}
