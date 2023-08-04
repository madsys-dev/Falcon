use std::ops::IndexMut;

use crate::config::Address;
use crate::storage::catalog::Catalog;
use crate::storage::global::Timer;
use crate::storage::global::UPDATING;
use crate::storage::table::{Table, TupleId};
use crate::storage::timestamp::TimeStamp;
use crate::utils::file;
use crate::{Error, Result};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub struct AccessStruct<'a> {
    pub table: &'a Table,
    pub tuple_id: TupleId,
    pub ts: TimeStamp,
}
pub const DELETE_COLUMN_FLAG: usize = 100000;
impl<'a> AccessStruct<'a> {
    pub fn new(table: &'a Table, tuple_id: TupleId, ts: TimeStamp) -> Self {
        // println!("access {}", tuple_id.get_address());
        AccessStruct {
            table,
            tuple_id,
            ts,
        }
    }

    pub fn valid(&self, ts: &TimeStamp) -> bool {
        ts.tid == self.ts.tid
    }

    pub fn finish(&self, thread_id: usize, cur_min_txn: u64) {
        #[cfg(feature = "cc_cfg_2pl")]
        {
            #[cfg(not(feature = "buffer_pool"))]
            let tuple = self.table.get_tuple(&self.tuple_id);
            #[cfg(feature = "buffer_pool")]
            let tuple = self
                .table
                .get_tuple_buffer(&self.tuple_id, thread_id, self.ts.tid, cur_min_txn)
                .0
                .data
                .read();
            // println!("{} release lock ", self.tuple_id.get_address());
            tuple.release_read_lock();
        }
    }
}

#[derive(Debug)]
pub struct WriteSetStruct<'a> {
    pub tuple_id: TupleId,
    pub ts: TimeStamp,
    pub table: &'a Table,
    pub is_insert: bool,
    pub data: Vec<u8>,
    pub column_id: usize,
    #[cfg(feature = "hot_unflush")]
    pub flush: bool,
    #[cfg(feature = "buffer_direct")]
    pub pointer: usize,
}

impl<'a> WriteSetStruct<'a> {
    pub fn new(
        table: &'a Table,
        tuple_id: TupleId,
        ts: TimeStamp,
        is_insert: bool,
        column_id: usize,
        #[cfg(feature = "buffer_direct")] pointer: usize,
        #[cfg(feature = "hot_unflush")] flush: bool,
        data: Vec<u8>,
    ) -> Self {
        WriteSetStruct {
            tuple_id: tuple_id.clone(),
            ts,
            table,
            is_insert,
            column_id,
            #[cfg(feature = "buffer_direct")]
            pointer,
            #[cfg(feature = "hot_unflush")]
            flush,
            data,
        }
    }
    #[cfg(not(feature = "update_direct"))]
    pub fn do_update(
        &mut self,
        address: Address,
        #[cfg(all(feature = "mvcc", feature = "ilog"))] d_delta_address: Address,
        column_id: usize,
        thread_id: usize,
        cur_min_txn: u64,
        #[cfg(feature = "clock")] timer: &mut Timer,
        #[cfg(feature = "clock")] tmp_index: usize,
    ) -> u64 {
        use std::sync::atomic::Ordering;

        use crate::utils::file;

        #[cfg(feature = "clock")]
        timer.start(UPDATING);

        if !self.is_insert && self.column_id != DELETE_COLUMN_FLAG {
            let mut ret = self
                .table
                .fix_tuple(
                    &self.tuple_id,
                    address,
                    #[cfg(all(feature = "mvcc", feature = "ilog"))]
                    d_delta_address,
                    column_id,
                    self.data.as_slice(),
                    self.ts,
                    thread_id,
                    cur_min_txn,
                    #[cfg(feature = "buffer_direct")]
                    self.pointer,
                    #[cfg(feature = "clock")]
                    timer,
                    #[cfg(feature = "hot_unflush")]
                    self.flush,
                )
                .unwrap();
            #[cfg(not(feature = "buffer_pool"))]
            {
                #[cfg(feature = "append")]
                {
                    self.tuple_id = TupleId::from_address(ret);
                }

                let tuple = self.table.get_tuple(&self.tuple_id);
                #[cfg(not(feature = "cc_cfg_2pl"))]
                assert_eq!(tuple.lock_tid(), self.ts.tid);
                tuple.set_ts_tid(self.ts.tid);
                // T
                // file::sfence();
                // debug!("cn {} {}", tuple._address(), self.ts.tid);
                // tuple.set_lock_tid(0);
            }
            #[cfg(feature = "buffer_pool")]
            {
                self.tuple_id = TupleId::from_address(ret);

                let vec = self
                    .table
                    .get_tuple_buffer(
                        &self.tuple_id,
                        thread_id,
                        self.ts.tid,
                        cur_min_txn,
                        #[cfg(feature = "clock")]
                        timer,
                    )
                    .0;
                let tuple = vec.data.read();
                // #[cfg(not(feature = "cc_cfg_2pl"))]
                // assert_eq!(tuple.lock_tid(), self.ts.tid);
                tuple.set_ts_tid(self.ts.tid);
                // println!("{:?}", tuple.get_data(self.table.tuple_size));

                // file::sfence();
                // tuple.set_lock_tid(0);
                return vec.nvm_id.load(Ordering::Relaxed);
            }

            // let tuple_nvm = Tuple::reload(tuple.n)
            // }
            #[cfg(feature = "clock")]
            {
                timer.total(UPDATING, tmp_index, UPDATING);
            }
            // println!("ret {}", ret);
            // #[cfg(feature = "append")]
            // { ret = 0; }
            return ret;
        }
        // println!("ret 1111");
        else if !self.is_insert {
            let mut ret = self
                .table
                .del_tuple(
                    &self.tuple_id,
                    address,
                    #[cfg(all(feature = "mvcc", feature = "ilog"))]
                    d_delta_address,
                    self.ts,
                    thread_id,
                    cur_min_txn,
                    #[cfg(feature = "buffer_direct")]
                    self.pointer,
                    #[cfg(feature = "clock")]
                    timer,
                    #[cfg(feature = "hot_unflush")]
                    self.flush,
                )
                .unwrap();
            #[cfg(not(feature = "buffer_pool"))]
            {
                #[cfg(feature = "append")]
                {
                    self.tuple_id = TupleId::from_address(ret);
                }

                let tuple = self.table.get_tuple(&self.tuple_id);
                #[cfg(not(feature = "cc_cfg_2pl"))]
                assert_eq!(tuple.lock_tid(), self.ts.tid);
                tuple.set_ts_tid(self.ts.tid);
                self.table.remove_tuple(&self.tuple_id, thread_id).unwrap();
                // T
                // file::sfence();
                // debug!("cn {} {}", tuple._address(), self.ts.tid);
                // tuple.set_lock_tid(0);
            }
            
            #[cfg(feature = "buffer_pool")]
            {
                self.tuple_id = TupleId::from_address(ret);

                let vec = self
                    .table
                    .get_tuple_buffer(
                        &self.tuple_id,
                        thread_id,
                        self.ts.tid,
                        cur_min_txn,
                        #[cfg(feature = "clock")]
                        timer,
                    )
                    .0;
                let tuple = vec.data.read();
                // #[cfg(not(feature = "cc_cfg_2pl"))]
                // assert_eq!(tuple.lock_tid(), self.ts.tid);
                tuple.set_ts_tid(self.ts.tid);
                // println!("{:?}", tuple.get_data(self.table.tuple_size));

                // file::sfence();
                // tuple.set_lock_tid(0);
                return vec.nvm_id.load(Ordering::Relaxed);
            }

            // let tuple_nvm = Tuple::reload(tuple.n)
            // }
            #[cfg(feature = "clock")]
            {
                timer.total(UPDATING, tmp_index, UPDATING);
            }
            // println!("ret {}", ret);
            // #[cfg(feature = "append")]
            // { ret = 0; }
            return ret;
        }
        return self.table.get_address(&self.tuple_id);
    }

    pub fn finish(&self, thread_id: usize, cur_min_txn: u64) {
        #[cfg(not(feature = "buffer_pool"))]
        let tuple: crate::storage::row::Tuple = self.table.get_tuple(&self.tuple_id);
        #[cfg(feature = "buffer_pool")]
        if self.is_insert {
            let tuple = self.table.get_tuple(&self.tuple_id);
            tuple.clwb_len(self.table.tuple_size);
            tuple.set_lock_tid(0);
            return;
        }
        #[cfg(feature = "buffer_pool")]
        let tuple = self
            .table
            .get_tuple_buffer(&self.tuple_id, thread_id, self.ts.tid, cur_min_txn)
            .0
            .data
            .read();
        #[cfg(feature = "cc_cfg_2pl")]
        {
            tuple.release_write_lock();
        }
        #[cfg(not(feature = "cc_cfg_2pl"))]
        {
            // println!("{} {}", tuple.lock_tid(), self.ts.tid);
            if tuple.lock_tid() == self.ts.tid {
                // println!("unlock {:x} {}", self.tuple_id.get_address(), self.ts.tid);
                // println!("{:?}", tuple.get_data(self.table.tuple_size));
                tuple.set_lock_tid(0);
            }
        }
        #[cfg(all(feature = "clwb_tuple", not(feature = "buffer_pool")))]
        if self.is_insert {
            tuple.clwb_len(self.table.tuple_size);
            return;
        }
    }
    pub fn abort(
        &self,
        thread_id: usize,
        cur_min_txn: u64,
        #[cfg(feature = "clock")] timer: &mut Timer,
    ) {
        if self.is_insert {
            self.table.remove_tuple(&self.tuple_id, thread_id).unwrap();
            return;
        }
        #[cfg(feature = "update_direct")]
        {
            let tuple = self.table.get_tuple(&self.tuple_id);
            tuple.apply_next();
        }
        #[cfg(not(feature = "cc_cfg_2pl"))]
        {
            #[cfg(not(feature = "buffer_pool"))]
            let tuple = self.table.get_tuple(&self.tuple_id);
            #[cfg(feature = "buffer_pool")]
            let tuple = self
                .table
                .get_tuple_buffer(
                    &self.tuple_id,
                    thread_id,
                    self.ts.tid,
                    cur_min_txn,
                    #[cfg(feature = "clock")]
                    timer,
                )
                .0
                .data
                .read();

            // tuple.set_ts_tid(self.ts.tid);
            // println!("abort {} {} {}", tuple._address(), tuple.lock_tid(), self.ts.tid);

            // if tuple.lock_tid() == self.ts.tid {
            //     // println!("an {} {}", tuple._address(), self.ts.tid);
            //     tuple.set_lock_tid(0);
            // }
        }
    }
}

pub const CountStart: usize = 50;
pub const FMASK: u64 = (1 << 50) - 1;
#[cfg(feature = "tpcc")]
pub const GROUP: u64 = 1 << 12;
#[cfg(not(feature = "tpcc"))]
pub const GROUP: u64 = 1 << 12;
#[cfg(feature = "tpcc")]
pub const SET: usize = 32;
#[cfg(not(feature = "tpcc"))]
pub const SET: usize = 32;

#[derive(Debug)]
pub struct FlushCache {
    pub tuple_id: Vec<[Address; SET]>,
    pub counter: usize,
}

impl FlushCache {
    pub fn new() -> Self {
        FlushCache {
            tuple_id: vec![[0; SET]; GROUP as usize],
            counter: 0,
        }
    }
    pub fn access(&mut self, access: Address) -> bool {
        let k = ((access / 100) % 1_000_000_007) & (GROUP - 1);
        // println!("access {}", access);
        let caches = &mut self.tuple_id[k as usize];
        for cache in caches {
            if access == *cache & FMASK {
                let count = *cache >> CountStart;
                if count < 10000 {
                    *cache += FMASK + 1;
                }
                return false;
            }
        }
        let caches = &mut self.tuple_id[k as usize];
        for _ in 0..2 {
            if self.counter == SET {
                self.counter = 0;
            }

            let cache = caches[self.counter];
            let count = cache >> CountStart;
            if count == 0 {
                caches[self.counter] = access;
                self.counter += 1;
                return true;
            }
            caches[self.counter] -= FMASK + 1;
            self.counter += 1;
        }
        return true;
    }
}
