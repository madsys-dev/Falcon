use std::collections::HashMap;

use crate::config::Address;
use crate::config::POW_2_63;
use crate::storage::catalog::*;
use crate::storage::global::*;
use crate::storage::row::Tuple;
use crate::storage::row::*;
use crate::storage::table::{Table, TupleId};
use crate::storage::timestamp::TimeStamp;
use crate::transaction::access::{AccessStruct, WriteSetStruct};
use crate::transaction::clog::*;
use crate::transaction::snapshot::SnapShotEntity;
use crate::transaction::transaction_buffer::*;
use crate::transaction::*;
use crate::utils::executor::mem;
use crate::utils::file;
use crate::Result;

use super::access::FlushCache;

#[derive(Debug)]
pub struct Transaction<'a> {
    pub ts: TimeStamp,
    // #[cfg(feature = "cc_cfg_occ")]
    // clog: &'a Clog,
    pub txn_buffer: &'a mut TransactionBuffer,
    pub snapshot: SnapShotEntity,
    read_set: Vec<AccessStruct<'a>>,
    write_set: Vec<WriteSetStruct<'a>>,
    pub read_only: bool,
    thread_id: usize,
    cur_min_txn: u64,
    #[cfg(feature = "hot_unflush")]
    flush_cache: FlushCache,
    #[cfg(feature = "clock")]
    pub timer: Timer,
}

impl<'a> Transaction<'a> {
    pub fn new(buffer: &'a mut TransactionBuffer, read_only: bool) -> Transaction<'a> {
        // #[cfg(feature = "cc_cfg_occ")]
        // let clog = Catalog::global().get_clog();
        Transaction {
            thread_id: buffer.get_thread_id() as usize,
            ts: {
                let mut t = TimeStamp::default();
                t.tid = get_tid(buffer.get_thread_id(), buffer.get_timestamp());
                t
            },
            // #[cfg(feature = "cc_cfg_occ")]
            // clog,
            txn_buffer: buffer,
            snapshot: SnapShotEntity::new(),
            read_set: Vec::new(),
            write_set: Vec::new(),
            read_only,
            cur_min_txn: 1,
            #[cfg(feature = "hot_unflush")]
            flush_cache: FlushCache::new(),
            #[cfg(feature = "clock")]
            timer: Timer::new(),
        }
    }

    pub fn begin(&mut self) {
        let snapshot = Catalog::global().get_snapshot();
        snapshot.get_snapshot(&mut self.snapshot);
        self.ts.tid = snapshot.new_txn(self.thread_id as u64);
        #[cfg(feature = "read_ts")]
        {
            self.snapshot.clock = self.ts.tid;
        }
        // #[cfg(feature = "cc_cfg_occ")]
        // self.clog.save(self.ts, 0);

        // if self.read_only {
        //     self.cur_min_txn = Catalog::get_min_txn();
        //     self.ts.tid = self.cur_min_txn;
        // }
        if self.ts.tid - self.cur_min_txn > 2000 {
            self.cur_min_txn = Catalog::get_min_txn();
        }
        self.txn_buffer.begin();
    }

    pub fn finish(&mut self, commited: bool) {
        // #[cfg(feature = "local_cc_cfg_occ")]
        // {
        //     let snapshot = Catalog::global().get_snapshot();
        //     snapshot.finish_txn(self.ts, commited);
        // }
        // println!("== {}, commit = {}", self.ts.tid, commited);

        Catalog::update_ts(self.thread_id as u64, self.ts.tid);
        for access in &self.read_set {
            access.finish(self.thread_id, self.cur_min_txn);
        }
        for access in &self.write_set {
            access.finish(self.thread_id, self.cur_min_txn);
        }
        self.write_set.clear();
        self.read_set.clear();
    }

    pub fn validation(&mut self) -> bool {
        for access in &self.read_set {
            #[cfg(not(feature = "buffer_pool"))]
            let tuple = access.table.get_tuple(&access.tuple_id);
            #[cfg(feature = "buffer_pool")]
            let tuple = access
                .table
                .get_tuple_buffer(
                    &access.tuple_id,
                    self.thread_id,
                    self.ts.tid,
                    self.cur_min_txn,
                    #[cfg(feature = "clock")]
                    &mut self.timer,
                )
                .0
                .data
                .read();
            if !access.valid(&tuple.ts()) {
                return false;
            }
            let mut ts = TimeStamp::default();
            ts.tid = tuple.lock_tid();

            if !self.snapshot.access(ts, self.ts.tid, self.cur_min_txn) {
                return false;
            }
        }
        true
    }

    pub fn commit(&mut self) -> bool {
        // println!("111");

        #[cfg(feature = "clock")]
        let mut k = 0;

        #[cfg(not(feature = "update_direct"))]
        for ws in &self.write_set {
            #[cfg(feature = "ilog")]
            let delta_address = self.txn_buffer.alloc();
            #[cfg(feature = "ilog")]
            let column_id = ws.column_id;

            #[cfg(feature = "local_cc_cfg_2pl")]
            let mut counter = 0;
            #[cfg(feature = "local_cc_cfg_2pl")]
            if !ws.is_insert {
                for access_item in &self.read_set {
                    // println!("read_tuple_id0: {}, update_tuple_id: {}",  access_item.tuple_id.get_address(), ws.tuple_id.get_address());
                    #[cfg(feature = "buffer_pool")]
                    {
                        if ws.tuple_id.get_address() < POW_2_63 {
                            ws.table.get_tuple_buffer(
                                &ws.tuple_id,
                                self.thread_id as usize,
                                self.ts.tid,
                                self.cur_min_txn,
                            );
                        }
                        // println!("read_tuple_id1: {}, update_tuple_id: {}",  access_item.tuple_id.get_address(), ws.tuple_id.get_address());
                        if access_item.tuple_id.get_address() == ws.tuple_id.get_address()
                            && access_item.table.id == ws.table.id
                        {
                            counter += 1;
                        }
                    }
                    #[cfg(not(feature = "buffer_pool"))]
                    {
                        if access_item.tuple_id.get_address() == ws.tuple_id.get_address() {
                            counter += 1;
                        }
                    }
                }
            }

            #[cfg(feature = "clock")]
            self.timer.start(UPDATING);

            if !ws.is_insert
                && ws
                    .table
                    .update_tuple_ts(
                        &ws.tuple_id,
                        &self.snapshot,
                        self.ts,
                        self.thread_id as usize,
                        self.cur_min_txn,
                        #[cfg(feature = "clock")]
                        &mut self.timer,
                        #[cfg(feature = "ilog")]
                        delta_address,
                        #[cfg(feature = "ilog")]
                        column_id,
                        #[cfg(feature = "ilog")]
                        ws.data.as_slice(),
                        #[cfg(feature = "ilog")]
                        &mut self.txn_buffer,
                        #[cfg(feature = "local_cc_cfg_2pl")]
                        counter,
                    )
                    .is_err()
            {
                // println!("222");
                self.abort();
                return false;
            } else {
                #[cfg(feature = "clock")]
                {
                    self.timer.add_tmp(UPDATING, k);
                    k += 1;
                }
            }
        }

        #[cfg(all(not(feature = "cc_cfg_occ"), feature = "update_direct"))]
        {
            for ws in &self.write_set {
                let tuple = ws.table.get_tuple(&ws.tuple_id);
                tuple.set_lock_tid(0);
                tuple.set_ts(self.ts);
            }
        }

        #[cfg(feature = "serializable")]
        {
            #[cfg(feature = "cc_cfg_occ")]
            if !self.read_only && !self.validation() {
                self.abort();
                return false;
            }
        }
        #[cfg(feature = "zen")]
        let mut address = 0;

        #[cfg(feature = "clock")]
        let mut k = 0;
        #[cfg(not(feature = "update_direct"))]
        for ws in &mut self.write_set {
            // debug!("txn abort tuple_id {}", ws.tuple_id.id);
            if ws.is_insert {
                // #[cfg(feature = "cc_cfg_mvto")]
                // ws.do_update(0, 0, self.thread_id, self.cur_min_txn);
                #[cfg(feature = "clock")]
                {
                    k += 1;
                }
                continue;
            }
            #[cfg(feature = "zen")]
            {
                address = ws.do_update(
                    self.txn_buffer.alloc(),
                    ws.column_id,
                    self.thread_id,
                    self.cur_min_txn,
                    #[cfg(feature = "clock")]
                    &mut self.timer,
                    #[cfg(feature = "clock")]
                    k,
                );
            }

            #[cfg(not(feature = "zen"))]
            {
                let delta_address = self.txn_buffer.alloc();
                #[cfg(all(feature = "mvcc", feature = "ilog"))]
                let d_delta_address = self.txn_buffer.alloc_dram();
                let column_id = ws.column_id;
                #[cfg(not(feature = "append"))]
                self.txn_buffer.add_delta(ws.do_update(
                    delta_address,
                    #[cfg(all(feature = "mvcc", feature = "ilog"))]
                    d_delta_address,
                    column_id,
                    self.thread_id,
                    self.cur_min_txn,
                    #[cfg(feature = "clock")]
                    &mut self.timer,
                    #[cfg(feature = "clock")]
                    k,
                ));
                #[cfg(feature = "append")]
                ws.do_update(
                    delta_address,
                    #[cfg(all(feature = "mvcc", feature = "ilog"))]
                    d_delta_address,
                    column_id,
                    self.thread_id,
                    self.cur_min_txn,
                    #[cfg(feature = "clock")]
                    &mut self.timer,
                    #[cfg(feature = "clock")]
                    k,
                );
            }
            #[cfg(feature = "clock")]
            {
                k += 1;
            }
        }
        #[cfg(feature = "zen")]
        {
            if address != 0 {
                let tuple = Tuple::reload(address);
                tuple.commit();
            }
        }
        // file::sfence();
        self.txn_buffer.commit(true);
        self.finish(true);
        true
    }

    pub fn abort(&mut self) {
        for ws in &self.write_set {
            // debug!("txn abort tuple_id {}", ws.tuple_id.id);
            ws.abort(
                self.thread_id,
                self.cur_min_txn,
                #[cfg(feature = "clock")]
                &mut self.timer,
            );
        }
        self.finish(false);
    }

    pub fn insert(&mut self, table: &'a Table, data: &str) -> TupleId {
        let tuple_id = table.allocate_tuple(self.thread_id).unwrap();
        //println!("insert id {}", tuple_id.id);
        let tuple_address = table.get_address(&tuple_id);

        // println!("insert table, {}, {}", table.id, data);

        self.write_set.push(WriteSetStruct {
            tuple_id: tuple_id.clone(),
            ts: self.ts,
            table,
            is_insert: true,
            column_id: 0,
            #[cfg(feature = "buffer_direct")]
            pointer: 0,
            #[cfg(feature = "hot_unflush")]
            flush: true,
            data: Vec::new(),
        });
        let tuple = &Tuple::new(tuple_address, data, &table.schema, self.ts).unwrap();
        table.index_insert_by_tuple(&tuple_id, &tuple).unwrap();
        tuple.clwb_len(table.schema.tuple_size() as u64);
        // debug!("txn {} insert tuple_id {}", self.ts, tuple_id.id);

        tuple_id.clone()
    }
    pub fn alloc(&mut self, table: &'a Table) -> Tuple {
        let tuple_id = table.allocate_tuple(self.thread_id).unwrap();
        //println!("insert id {}", tuple_id.id);
        let tuple_address = table.get_address(&tuple_id);

        //println!("insert tuple address {}", tuple_address);

        self.write_set.push(WriteSetStruct {
            tuple_id,
            ts: self.ts,
            table,
            is_insert: true,
            column_id: 0,
            #[cfg(feature = "buffer_direct")]
            pointer: 0,
            #[cfg(feature = "hot_unflush")]
            flush: true,
            data: Vec::new(),
        });
        let tuple = Tuple::reload(tuple_address);
        tuple.set_ts(self.ts);
        tuple.set_next(0);
        tuple.set_lock_tid(self.ts.tid);
        tuple
    }

    pub async fn prefetch_read(&mut self, table: &Table, tuple_id: &TupleId) {
        let address = table.get_address(tuple_id);
        mem::prefetch_read(address).await
    }
    pub fn read(&mut self, table: &'a Table, tuple_id: &TupleId) -> Result<TupleVec> {
        #[cfg(feature = "clock")]
        self.timer.start(READING);

        #[cfg(not(feature = "buffer_pool"))]
        let tuple_nvm = table.get_tuple(tuple_id);
        // tuple_nvm.apply_next(self.clog);
        #[cfg(feature = "buffer_pool")]
        let tuple_nvm = table
            .get_tuple_buffer(
                tuple_id,
                self.thread_id,
                self.ts.tid,
                self.cur_min_txn,
                #[cfg(feature = "clock")]
                &mut self.timer,
            )
            .0
            .data
            .read();
        // println!("load ok");
        let mut tuple_ts0 = tuple_nvm.ts();

        let mut tuple = tuple_nvm.read(table.tuple_size as usize);

        // println!("read ok");
        // return Ok(tuple);

        // println!("read");

        let mut latest = true;

        loop {
            // println!("txn {} read {}", self.ts.tid, tuple_ts.tid);
            // println!("{}", tuple_ts);
            let mut tuple_ts = tuple.get_ts();
            while latest && tuple_ts0 != tuple_ts {
                tuple_ts0 = tuple_nvm.ts();
                tuple = tuple_nvm.read(table.tuple_size as usize);
                tuple_ts = tuple.get_ts();
            }

            #[cfg(all(feature = "serializable", feature = "cc_cfg_2pl"))]
            {
                if !self.read_only {
                    if tuple_nvm.lock_read(self.ts.tid).is_err() {
                        return Err(TupleError::AcquireReadLockFalse.into());
                    } else {
                        break;
                    }
                }
            }
            if !self.read_only && tuple_nvm.lock_tid() != 0 && tuple_nvm.lock_tid() < self.ts.tid {
                return Err(TupleError::TupleChanged { conflict_tid: 0 }.into());
            }
            #[cfg(all(feature = "serializable"))]
            if self
                .snapshot
                .access(tuple_ts, self.ts.tid, self.cur_min_txn)
            {
                #[cfg(feature = "cc_cfg_mvto")]
                if latest && !self.read_only {
                    tuple_nvm.set_read_ts(self.ts.tid);
                    file::sfence();
                }
                break;
            }
            // debug!("txn {} read old {}", self.ts.tid, tuple_ts.tid);

            #[cfg(all(feature = "serializable", not(feature = "cc_cfg_mvto")))]
            {
                if !self.read_only {
                    #[cfg(feature = "clock")]
                    self.timer.end(READING, READING);
                    return Err(TupleError::PreValidationFailed.into());
                }
            }
            #[cfg(not(feature = "mvcc"))]
            return Err(TupleError::TupleChanged { conflict_tid: 0 }.into());
            // if latest {
            //     println!("{}, {}, {}, {:x}, {:x}", self.ts.tid, tuple_id.get_address(), tuple_ts, tuple_nvm.next_address(), tuple.next);
            // }
            if !tuple.next(&table) {
                #[cfg(feature = "clock")]
                self.timer.end(READING, READING);
                return Err(TupleError::TupleNotExists.into());
            }
            latest = false;
        }
        // TODO tuple_id for different table
        #[cfg(not(feature = "update_direct"))]
        {
            for ws in &self.write_set {
                if ws.tuple_id.eq(&tuple_id) && ws.table.id == table.id {
                    // println!("read {} {}", table.id, ws.column_id);

                    let offset = table.schema.get_column_offset(ws.column_id).start;
                    // let column_type = table.schema.get_column_type((tid >> 50) as usize);
                    // println!("{}", offset);
                    // println!("{}", tuple.data.len());
                    // println!("{}", column_type.len());
                    unsafe {
                        std::ptr::copy(
                            ws.data.as_ptr(),
                            tuple.data.as_mut_ptr().add(offset),
                            ws.data.len(),
                        );
                    }
                }
            }
        }
        #[cfg(feature = "clock")]
        self.timer.end(READING, READING);
        #[cfg(feature = "serializable")]
        if !self.read_only {
            #[cfg(not(feature = "buffer_pool"))]
            self.read_set
                .push(AccessStruct::new(table, tuple_id.clone(), tuple.ts()));
            #[cfg(feature = "buffer_pool")]
            self.read_set
                .push(AccessStruct::new(table, tuple_id.clone(), tuple.ts()));
        }

        Ok(tuple)
    }

    pub fn update(
        &mut self,
        table: &'a Table,
        tuple_id: &TupleId,
        update_column_id: usize,
        update_data: &[u8],
    ) -> Result<()> {
        // #[cfg(feature = "clock")]
        // self.timer.start(UPDATING);
        // let delta_len = U64_OFFSET + DELTA_DATA_OFFSET + update_data.len() as u64 + U64_OFFSET;
        #[cfg(feature = "update_direct")]
        {
            let delta_address = self.txn_buffer.alloc();
            #[cfg(feature = "clock")]
            self.timer.start(UPDATING);
            match table.fix_tuple(
                tuple_id,
                delta_address,
                update_column_id,
                update_data,
                &self.snapshot,
                self.ts,
                self.thread_id as usize,
                self.cur_min_txn,
                #[cfg(feature = "clock")]
                &mut self.timer,
            ) {
                Ok(delta_len) => {
                    self.txn_buffer.add_delta(delta_len);
                    // self.write_set
                    //     .push(WriteSetStruct::new(table, tuple_id, self.ts, false));
                    self.write_set.push(WriteSetStruct::new(
                        table,
                        tuple_id.clone(),
                        self.ts,
                        false,
                        update_column_id,
                        Vec::from(update_data),
                    ));
                    // debug!("txn {} update tuple_id {}", self.ts, tuple_id.page_start);
                    // #[cfg(feature = "clock")]
                    // self.timer.end(UPDATING, UPDATING);
                    #[cfg(feature = "clock")]
                    {
                        self.timer.add_tmp(UPDATING, 0);
                        self.timer.total(UPDATING, 0, UPDATING);
                    }
                    return Ok(());
                }
                Err(e) => {
                    // #[cfg(feature = "clock")]
                    // self.timer.end(UPDATING, UPDATING);
                    return Err(e);
                }
            }
        }
        #[cfg(feature = "update_local")]
        {
            // match table.update_tuple_ts(
            //     &tuple_id,
            //     &self.snapshot,
            //     self.ts,
            //     self.thread_id as usize,
            //     self.cur_min_txn,
            //     #[cfg(feature = "clock")]
            //     &mut self.timer,
            // ) {
            //     Ok(_) => {
            // let flush = table.id != 1 && table.id != 2;
            #[cfg(feature = "hot_unflush")]
            let flush = self.flush_cache.access(tuple_id.get_address());
            self.write_set.push(WriteSetStruct::new(
                table,
                tuple_id.clone(),
                self.ts,
                false,
                update_column_id,
                #[cfg(feature = "hot_unflush")]
                flush,
                Vec::from(update_data),
            ));
            // #[cfg(feature = "clock")]
            // self.timer.end(UPDATING, UPDATING);
            return Ok(());
            //     }
            //     Err(e) => {
            //         #[cfg(feature = "clock")]
            //         self.timer.end(UPDATING, UPDATING);
            //         return Err(e);
            //     }
            // }
        }
        #[cfg(feature = "buffer_direct")]
        {
            match table.update_tuple_buffer(
                tuple_id,
                &self.snapshot,
                update_column_id,
                update_data,
                self.ts,
                self.thread_id as usize,
                self.cur_min_txn,
                #[cfg(feature = "clock")]
                &mut self.timer,
            ) {
                Ok(pointer) => {
                    self.write_set.insert(
                        tuple_id.page_start | ((update_column_id as u64) << 50),
                        WriteSetStruct::new(
                            table,
                            tuple_id,
                            self.ts,
                            false,
                            #[cfg(feature = "buffer_direct")]
                            pointer,
                            Vec::from(update_data),
                        ),
                    );
                    #[cfg(feature = "clock")]
                    self.timer.end(UPDATING, UPDATING);
                    return Ok(());
                }
                Err(e) => {
                    #[cfg(feature = "clock")]
                    self.timer.end(UPDATING, UPDATING);

                    return Err(e);
                }
            }
        }
    }

    pub fn read_column(
        &mut self,
        table: &'a Table,
        tuple_id: &TupleId,
        column_id: usize,
    ) -> Result<TupleColumnVec> {
        // #[cfg(feature = "clock")]
        // self.timer.start(READING);
        assert!(tuple_id.get_address() > 0);
        #[cfg(not(feature = "buffer_pool"))]
        let tuple_nvm = table.get_tuple(tuple_id);
        // tuple_nvm.apply_next(self.clog);
        #[cfg(feature = "buffer_pool")]
        let tuple_nvm = table
            .get_tuple_buffer(
                tuple_id,
                self.thread_id,
                self.ts.tid,
                self.cur_min_txn,
                #[cfg(feature = "clock")]
                &mut self.timer,
            )
            .0
            .data
            .read();
        // println!("load ok");
        let mut tuple_ts0 = tuple_nvm.ts();

        let mut tuple = tuple_nvm.column_read(table.schema.get_column_offset(column_id));
        // println!("read ok");
        // return Ok(tuple);

        // println!("read");

        let mut latest = true;

        loop {
            let mut tuple_ts = tuple.get_ts();
            while latest && tuple_ts0 != tuple_ts {
                tuple_ts0 = tuple_nvm.ts();
                tuple = tuple_nvm.column_read(table.schema.get_column_offset(column_id));
                tuple_ts = tuple.get_ts();
            }

            #[cfg(all(feature = "serializable", feature = "cc_cfg_2pl"))]
            {
                if !self.read_only {
                    if tuple_nvm.lock_read(self.ts.tid).is_err() {
                        return Err(TupleError::AcquireReadLockFalse.into());
                    } else {
                        break;
                    }
                }
            }
            // println!("lock_tid {}", tuple_nvm.lock_tid());
            if !self.read_only && tuple_nvm.lock_tid() != 0 && tuple_nvm.lock_tid() < self.ts.tid {
                return Err(TupleError::TupleChanged { conflict_tid: 0 }.into());
            }
            #[cfg(all(feature = "serializable"))]
            if self
                .snapshot
                .access(tuple_ts, self.ts.tid, self.cur_min_txn)
            {
                #[cfg(feature = "cc_cfg_mvto")]
                if latest && !self.read_only {
                    tuple_nvm.set_read_ts(self.ts.tid);
                    file::sfence();
                }
                break;
            }

            // debug!("txn {} read old {}", self.ts.tid, tuple_ts.tid);
            #[cfg(all(feature = "serializable", feature = "cc_cfg_occ"))]
            {
                if !self.read_only {
                    // println!("txn {} read {}", self.ts.tid, tuple_ts.tid);
                    // println!("2222");
                    #[cfg(feature = "clock")]
                    self.timer.end(READING, READING);
                    return Err(TupleError::PreValidationFailed.into());
                }
            }
            #[cfg(not(feature = "mvcc"))]
            return Err(TupleError::TupleChanged { conflict_tid: 0 }.into());
            // if latest {
            //     println!("{}, {}, {}, {:x}, {:x}", self.ts.tid, tuple_id.get_address(), tuple_ts, tuple_nvm.next_address(), tuple.next);
            // }
            if !tuple.next(&table) {
                #[cfg(feature = "clock")]
                self.timer.end(READING, READING);
                // println!("1111");
                return Err(TupleError::TupleNotExists.into());
            }
            latest = false;
        }
        #[cfg(not(feature = "update_direct"))]
        {
            for ws in &self.write_set {
                if ws.tuple_id.eq(&tuple_id) {
                    let offset = table.schema.get_column_offset(ws.column_id).start;
                    // let column_type = table.schema.get_column_type((tid >> 50) as usize);
                    // println!("{}", offset);
                    // println!("{}", tuple.data.len());
                    // println!("{}", column_type.len());
                    unsafe {
                        std::ptr::copy(
                            ws.data.as_ptr(),
                            tuple.data.as_mut_ptr().add(offset),
                            ws.data.len(),
                        );
                    }
                }
            }
        }
        // #[cfg(feature = "clock")]
        // self.timer.end(READING, READING);

        #[cfg(feature = "serializable")]
        if !self.read_only {
            #[cfg(not(feature = "buffer_pool"))]
            self.read_set
                .push(AccessStruct::new(table, tuple_id.clone(), tuple.ts()));
            #[cfg(feature = "buffer_pool")]
            self.read_set
                .push(AccessStruct::new(table, tuple_id.clone(), tuple.ts()));
        }
        Ok(tuple)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::nvm_file::*;
    use crate::storage::schema::*;
    use crate::storage::table::IndexType;
    #[test]
    fn test_insert_and_update_and_read() {
        let thread_id = 0;
        let mut schema = TableSchema::new();
        schema.push(ColumnType::Int64, "a");
        schema.push(ColumnType::Int64, "b");
        println!("Build schema");

        NVMTableStorage::init_test_database();

        Catalog::init_catalog();
        let catalog = Catalog::global();
        let mut buffer = TransactionBuffer::new(catalog, thread_id);
        let table_name = "table1";
        catalog.add_table(table_name, schema).unwrap();

        catalog.set_primary_key(table_name, 1);
        let table = &catalog.get_table(table_name);

        // txn1 add t1, t2

        let mut transaction1 = Transaction::new(&mut buffer, false);
        transaction1.begin();
        println!("txn1 begin");

        let tuple1_id = transaction1.insert(table, "233,666");
        println!("insert t1 233 666");
        let tuple2_id = transaction1.insert(table, "666,233");
        println!("insert t2 666 233");
        println!("tuple1 insert address: {:x}", tuple1_id.get_address());
        println!("tuple2 insert address: {:x}", tuple2_id.get_address());

        transaction1.commit();

        // let tuple_id = table.search_tuple_id(&IndexType::Int64(666)).unwrap();
        // let tuple_id2 = tuple_id.clone();

        // let mut transaction2 = Transaction::new(&mut buffer, false);
        // transaction2.begin();
        // transaction2.read_column(table, &tuple_id, 0).unwrap();
        // transaction2.read_column(table, &tuple_id2, 0).unwrap();
        // transaction2.commit();
        // return ;
        // txn2 update t1
        let mut transaction2 = Transaction::new(&mut buffer, false);
        transaction2.begin();
        let tuple1_id = table.search_tuple_id(&IndexType::Int64(666)).unwrap();
        transaction2
            .update(table, &tuple1_id, 0, &(666 as u64).to_le_bytes())
            .unwrap();
        println!("transaction id {} update t1 666 666", transaction2.ts.tid);

        assert!(transaction2.commit());
        let tuple1_id = table.search_tuple_id(&IndexType::Int64(666)).unwrap();
        println!("tuple1 new address: {:x}", tuple1_id.get_address());
        #[cfg(feature = "clock")]
        println!("time on buffer switch: {} ns", transaction2.timer.clock[0]);

        // txn3 update t2  ->  abort
        let mut transaction3 = Transaction::new(&mut buffer, false);
        transaction3.begin();
        transaction3
            .update(table, &tuple2_id, 0, &(233 as u64).to_le_bytes())
            .unwrap();
        transaction3.abort();
        #[cfg(feature = "clock")]
        println!("time on buffer read: {} ns", transaction3.timer.clock[1]);

        // txn4 read t1, t2
        // txn4 update t2 666 233 -> 233 233
        // txn4 read t2
        let tuple1_id = table.search_tuple_id(&IndexType::Int64(666)).unwrap();
        println!("txn4 read tuple 1: {:x}", tuple1_id.get_address());

        let mut transaction4 = Transaction::new(&mut buffer, false);
        transaction4.begin();
        let c1 = transaction4.read_column(table, &tuple1_id, 0).unwrap();
        println!("read t1 = {:?}", c1.data);

        assert_eq!(c1.data[0], 154);

        let tuple1 = transaction4.read(table, &tuple1_id).unwrap();

        println!("read t1 ok");

        let tuple2 = transaction4.read(table, &tuple2_id).unwrap();

        transaction4
            .update(table, &tuple2_id, 0, &(233 as u64).to_le_bytes())
            .unwrap();

        // let mut buffer2 = TransactionBuffer::new(catalog, 1);
        // let mut transaction5 = Transaction::new(&mut buffer2, false);
        // transaction5.begin();
        // transaction5.read_column(table, &tuple2_id, 0).unwrap();

        let mut tuple2_1 = transaction4.read(table, &tuple2_id).unwrap();
        println!("update t2 233 233");

        assert!(transaction4.commit());
        println!("transaction4 commit");

        // txn2 update t1
        // let mut transaction2 = Transaction::new(&mut buffer, false);
        // transaction2.begin();
        // let tuple1_id = table.search_tuple_id(&IndexType::Int64(666)).unwrap();
        // transaction2
        //     .update(table, &tuple1_id, 0, &(666 as u64).to_le_bytes())
        //     .unwrap();
        // println!("update t1 666 666");

        // assert!(transaction2.commit());
        // assert t1, t2
        let t = tuple1._to_vec();
        println!("t1 666 666: ");
        for u in &t {
            print!("{} ", u);
        }
        println!("");

        assert_eq!(t[0], 154);
        assert_eq!(t[8], 154);
        let t = tuple2._to_vec();
        println!("t2: ");
        for u in &t {
            print!("{} ", u);
        }
        println!("");

        assert_eq!(t[0], 154);
        assert_eq!(t[8], 233);
        // assert t2'.version_list
        let checkpoints = [[233, 233], [154, 233]];
        let mut i = 0;
        loop {
            let t = tuple2_1._to_vec();
            assert_eq!(t[0], checkpoints[i][0]);
            assert_eq!(t[8], checkpoints[i][1]);
            println!("version: ");
            for u in t {
                print!("{} ", u);
            }
            println!("");

            if !tuple2_1.next(&table) {
                break;
            }
            i = i + 1;
        }
    }
}
