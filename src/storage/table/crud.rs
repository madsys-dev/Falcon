use crate::{
    mvcc_config::delta::DELTA_DATA_OFFSET,
    storage::row::TUPLE_HEADER,
    transaction::transaction_buffer::TransactionBuffer,
    utils::file::{self, sfence},
};

use super::*;
impl Table {
    #[inline]
    pub fn get_address(&self, tuple_id: &TupleId) -> Address {
        tuple_id.get_address()
    }

    pub fn get_tuple(&self, tuple_id: &TupleId) -> Tuple {
        assert!(tuple_id.get_address() & POW_2_63 == 0);
        return Tuple::reload(self.get_address(&tuple_id));
    }
    pub fn allocate_tuple(&self, tid: usize) -> Result<TupleId> {
        #[cfg(feature = "center_allocator")]
        {
            self.allocator.allocate_tuple(self.tuple_size)
        }
        #[cfg(feature = "local_allocator")]
        {
            let mut allocator = self.allocator.get(tid).unwrap().write();
            allocator.allocate_tuple(self.tuple_size)
        }
    }
    #[cfg(feature = "append")]
    pub fn allocate_append_tuple(&self, tid: usize) -> Result<TupleId> {
        let mut allocator = self.allocator.get(tid).unwrap().write();
        allocator.allocate_append_tuple(self.tuple_size, tid)
    }
    pub fn remove_tuple(&self, tuple_id: &TupleId, thread_id: usize) -> Result {
        #[cfg(feature = "center_allocator")]
        self.allocator.free_tuple(tuple_id);
        #[cfg(feature = "local_allocator")]
        self.allocator
            .get(thread_id)
            .unwrap()
            .read()
            .free_tuple(tuple_id.get_address());
        return self.index_remove_by_tuple(tuple_id, &self.get_tuple(tuple_id));
        // return Ok(());
    }

    #[cfg(feature = "buffer_pool")]
    pub fn remove_tuple_buffer(&self, tuple_id: &TupleId, thread_id: usize, tuple: &RwLockReadGuard<BufferDataVec>,) -> Result {
        #[cfg(feature = "center_allocator")]
        self.allocator.free_tuple(tuple_id);
        #[cfg(feature = "local_allocator")]
        self.allocator
            .get(thread_id)
            .unwrap()
            .read()
            .free_tuple(tuple_id.get_address());
        return self.index_remove_by_tuple_buffer(tuple);
        // return Ok(());
    }
    /// S1: get_old_value
    /// S2: check_metadata TODO
    /// S3: gen_delta
    /// S4: CAS next_address  false -> abort; true -> S5
    /// S3: update new_value
    pub fn fix_tuple(
        &self,
        tuple_id: &TupleId,
        new_address: Address,
        #[cfg(all(feature = "mvcc", feature = "ilog"))] d_delta_address: Address,
        column_id: usize,
        new_data: &[u8],
        #[cfg(feature = "update_direct")] snapshot: &SnapShotEntity,
        ts: TimeStamp,
        thread_id: usize,
        cur_min_txn: u64,
        #[cfg(feature = "buffer_direct")] pool_id: usize,
        #[cfg(feature = "clock")] timer: &mut Timer,
        #[cfg(feature = "hot_unflush")] flush: bool,
    ) -> Result<u64> {
        #[cfg(not(feature = "buffer_pool"))]
        let tuple = self.get_tuple(tuple_id);
        #[cfg(feature = "buffer_pool")]
        let vec = self
            .get_tuple_buffer(
                tuple_id,
                thread_id,
                ts.tid,
                cur_min_txn,
                #[cfg(feature = "clock")]
                timer,
            )
            .0;
        // #[cfg(feature = "buffer_pool")]
        // let tuple_pool = vec.data.write();
        #[cfg(all(feature = "buffer_pool", feature = "delta"))]
        let tuple = vec.data.read();
        #[cfg(feature = "buffer_pool")]
        let tuple_address = vec.nvm_id.load(Ordering::Relaxed);
        let column_offset = self.schema.get_column_offset(column_id);
        #[cfg(feature = "delta")]
        let old_data = tuple.get_data_by_column(column_offset.clone());
        // #[cfg(all(not(feature = "delta"), not(feature = "tpcc")))]
        // let old_data = tuple.get_data(TUPLE_HEADER as u64 + 8);
        #[cfg(all(not(feature = "delta"), not(feature = "buffer_pool")))]
        let old_data = tuple.get_data(self.tuple_size);
        #[cfg(all(feature = "buffer_pool", feature = "delta"))]
        drop(tuple);
        #[cfg(feature = "buffer_pool")]
        let tuple = Tuple::reload(tuple_address);
        self.update_tuple(
            &tuple,
            // #[cfg(feature = "buffer_pool")] tuple_address,
            tuple_id,
            #[cfg(not(feature = "buffer_pool"))]
            old_data,
            #[cfg(feature = "buffer_pool")]
            new_data,
            new_address,
            #[cfg(all(feature = "mvcc", feature = "ilog"))]
            d_delta_address,
            new_data,
            column_offset.start as u64,
            #[cfg(feature = "update_direct")]
            snapshot,
            ts,
            thread_id,
            cur_min_txn,
            #[cfg(feature = "buffer_direct")]
            pool_id,
            #[cfg(feature = "clock")]
            timer,
            #[cfg(feature = "hot_unflush")]
            flush,
        )
    }
    pub fn update_tuple_ts(
        &self,
        tuple_id: &TupleId,
        snapshot: &SnapShotEntity,
        ts: TimeStamp,
        thread_id: usize,
        cur_min_txn: u64,
        #[cfg(feature = "clock")] timer: &mut Timer,
        #[cfg(feature = "ilog")] new_address: Address,
        #[cfg(feature = "ilog")] start: usize,
        #[cfg(feature = "ilog")] new_data: &[u8],
        #[cfg(feature = "ilog")] buffer: &mut TransactionBuffer,
        #[cfg(feature = "local_cc_cfg_2pl")] counter: u64,
    ) -> Result<TimeStamp> {
        #[cfg(not(feature = "buffer_pool"))]
        let tuple = self.get_tuple(tuple_id);
        #[cfg(feature = "buffer_pool")]
        let tuple = self
            .get_tuple_buffer(
                tuple_id,
                thread_id,
                ts.tid,
                cur_min_txn,
                #[cfg(feature = "clock")]
                timer,
            )
            .0
            .data
            .read();

        #[cfg(feature = "clock")]
        timer.start(READING);
        #[cfg(feature = "ilog")]
        let mut delta = TupleDelta::new(new_address, 0, tuple_id.get_address()).unwrap();
        #[cfg(feature = "ilog")]
        {
            #[cfg(feature = "delta")]
            delta.set_meta_data(delta::DELTA_COLUMN_OFFSET, start as u32);
            #[cfg(feature = "travel")]
            delta.set_meta_data(delta::DELTA_COLUMN_OFFSET, 0 as u32);

            delta.set_meta_data(delta::DELTA_TABLE_ID, self.id);
            delta.set_meta_data(delta::TID, tuple.ts());

            // delta.save(new_data);
            // file::sfence();
            // assert_eq!(delta.len(), 140);
        }
       
        let lock_tid = tuple.lock_tid().clone();

        #[cfg(feature = "cc_cfg_occ")]
        {
            if lock_tid != 0 && lock_tid != ts.tid {
                return Err(TupleError::TupleChanged {
                    conflict_tid: lock_tid,
                }
                .into());
            }
            if !snapshot.access(TimeStamp { tid: lock_tid }, ts.tid, cur_min_txn) {
                return Err(TupleError::TupleChanged {
                    conflict_tid: lock_tid,
                }
                .into());
            }
        }

        #[cfg(feature = "cc_cfg_to")]
        {
            if lock_tid != 0 && lock_tid != ts.tid {
                // println!("lock {:x} {} {}", tuple_id.get_address(), lock_tid, ts.tid);
                return Err(TupleError::TupleChanged {
                    conflict_tid: lock_tid,
                }
                .into());
            }
            let tuple_ts = tuple.ts();

            if tuple_ts.read_ts > ts.tid || tuple_ts.tid > ts.tid {
                // println!("read_ts {} {} {}", tuple_ts.read_ts, tuple_ts.tid, ts.tid);
                return Err(TupleError::TupleChanged {
                    conflict_tid: tuple_ts.read_ts,
                }
                .into());
            }
        }
        #[cfg(feature = "cc_cfg_2pl")]
        {
            if tuple.lock_write(ts.tid, counter).is_ok() {
                return Ok(ts);
            } else {
                return Err(TupleError::AcquireWriteLockFalse.into());
            }
        }
        #[cfg(not(feature = "cc_cfg_2pl"))]
        {
            let cas_result = tuple.cas_lock_tid(0, ts.tid);
            #[cfg(feature = "clock")]
            timer.end(READING, READING);
            // println!("lock {:x} {} {}", tuple_id.get_address(), lock_tid, ts.tid);

            if cas_result != 0 && cas_result != ts.tid {
                return Err(TupleError::TupleChanged {
                    conflict_tid: cas_result,
                }
                .into());
            }
            if tuple.ts().tid > ts.tid {
                // tuple.set_lock_tid(0);
                return Err(TupleError::TupleChanged {
                    conflict_tid: cas_result,
                }
                .into());
            }
            #[cfg(feature = "ilog")]
            {
                delta.save(new_data);
                buffer.save_redo(delta.len() + U64_OFFSET);
            }
            Ok(ts)
        }
        // tuple.set_ts(ts);
    }
    
    pub fn update_tuple(
        &self,
        tuple: &Tuple,
        tuple_id: &TupleId,
        old_data: &[u8],
        new_address: Address,
        #[cfg(all(feature = "mvcc", feature = "ilog"))] d_delta_address: Address,
        new_data: &[u8],
        start: u64,
        #[cfg(feature = "update_direct")] snapshot: &SnapShotEntity,
        ts: TimeStamp,
        thread_id: usize,
        cur_min_txn: u64,
        #[cfg(feature = "buffer_direct")] new_pointer: usize,
        #[cfg(feature = "clock")] timer: &mut Timer,
        #[cfg(feature = "hot_unflush")] flush: bool,
    ) -> Result<u64> {
        #[cfg(not(feature = "buffer_pool"))]
        let old_next_address = tuple.next_address().clone();
        #[cfg(not(feature = "buffer_pool"))]
        let tuple_ts = tuple.ts().clone();
        #[cfg(feature = "buffer_pool")]
        let (old_vec, pointer) = self.get_tuple_buffer(
            tuple_id,
            thread_id,
            ts.tid,
            cur_min_txn,
            #[cfg(feature = "clock")]
            timer,
        );
        #[cfg(feature = "update_direct")]
        {
            #[cfg(feature = "cc_cfg_to")]
            {
                let tuple_ts = tuple.ts();
                if tuple_ts.tid > ts.tid && tuple_ts.tid != POW_2_63 | ts.tid {
                    return Err(TupleError::TupleChanged {
                        conflict_tid: tuple_ts.tid,
                    }
                    .into());
                }
                if tuple_ts.read_ts > ts.tid {
                    return Err(TupleError::TupleChanged {
                        conflict_tid: tuple_ts.read_ts,
                    }
                    .into());
                }
                let cas_result = tuple.cas_lock_tid(0, ts.tid);
                if cas_result != 0 {
                    return Err(TupleError::TupleChanged {
                        conflict_tid: cas_result,
                    }
                    .into());
                }
            }
            #[cfg(feature = "cc_cfg_occ")]
            if !snapshot.access(tuple_ts, ts.tid, cur_min_txn) {
                return Err(TupleError::TupleChanged {
                    conflict_tid: tuple_ts.tid,
                }
                .into());
            }
            #[cfg(feature = "cc_cfg_2pl")]
            {
                if tuple.lock_write(ts.tid).is_err() {
                    return Err(TupleError::AcquireWriteLockFalse.into());
                }
            }
        }

        // println!("old_next_address: {}, new_address: {}", old_next_address, new_address);
        #[cfg(all(not(feature = "ilog"), not(feature = "append")))]
        let mut delta =
            TupleDelta::new(new_address, old_next_address, tuple_id.get_address()).unwrap();
        #[cfg(not(feature = "append"))]
        {
            #[cfg(not(feature = "ilog"))]
            {
                #[cfg(feature = "delta")]
                delta.set_meta_data(delta::DELTA_COLUMN_OFFSET, start as u32);
                #[cfg(feature = "travel")]
                delta.set_meta_data(delta::DELTA_COLUMN_OFFSET, 0 as u32);

                delta.set_meta_data(delta::DELTA_TABLE_ID, self.id);

                // #[cfg(not(feature = "local_cc_cfg_2pl"))]
                // if ts.tid < tuple_ts.tid {
                //     println!("{}, {}, {}", tuple_id.get_address(), ts.tid, tuple_ts.tid);
                // }
                #[cfg(feature = "local_cc_cfg_to")]
                assert!(ts.tid >= tuple_ts.tid);
                delta.set_meta_data(delta::TID, tuple_ts.tid);

                delta.save(old_data);
                //T file::sfence();
                delta.clwb();
                tuple.set_next(new_address);
            }

            #[cfg(all(feature = "mvcc", feature = "ilog"))]
            {
                let mut delta =
                    TupleDelta::new(d_delta_address, old_next_address, tuple_id.get_address())
                        .unwrap();
                delta.set_meta_data(delta::DELTA_COLUMN_OFFSET, start as u32);
                delta.set_meta_data(delta::DELTA_TABLE_ID, self.id);
                delta.set_meta_data(delta::TID, tuple_ts.tid);
                delta.save(old_data);
                file::sfence();
                tuple.set_next(d_delta_address);
            }

            // println!("old_next_address: {}, new_address: {}", old_next_address, new_address);
            // #[cfg(feature = "cc_cfg_occ")]
            // {
            //     let cas_result = tuple.set_ts_and_next(tuple_ts, ts, old_next_address, new_address);
            //     if cas_result != tuple_ts.tid {
            //         return Err(TupleError::TupleChanged {
            //             conflict_tid: cas_result,
            //         }
            //         .into());
            //     }
            // }
            //T file::sfence();
            #[cfg(feature = "cc_cfg_to")]
            {
                let mut ts1 = ts.clone();
                ts1.tid |= POW_2_63;
                // if tuple.lock_tid() != ts.tid
                // { debug!("l {} {}", tuple._address(), tuple.lock_tid());}
                // assert_eq!(tuple.lock_tid(), ts.tid);

                #[cfg(not(feature = "buffer_pool"))]
                tuple.set_ts_tid(ts1.tid);
                #[cfg(feature = "buffer_pool")]
                old_vec.data.read().set_ts_tid(ts1.tid);
            }
            #[cfg(not(feature = "cc_cfg_to"))]
            {
                tuple.set_ts_tid(ts.tid);
            }
            tuple.update_data_by_column(start as u64, new_data);
            // tuple.set_lock_tid(0);
            #[cfg(feature = "hot_unflush")]
            if flush {
                // let range = self.schema.get_column_offset(0);
                // let key = tuple.get_data_by_column(range);
                // let k:u64 = u64::from_le_bytes(key.try_into().unwrap());
                // if k > 60000 {
                // println!("");
                tuple.clwb_update(self.tuple_size, start, start + new_data.len() as u64);
                // }
            }
            #[cfg(not(feature = "hot_unflush"))]
            tuple.clwb_update(self.tuple_size, start, start + new_data.len() as u64);
            // println!("new_address: {}", tuple.next_address());
        }
        #[cfg(feature = "buffer_pool")]
        {
            let start_p = self.pool_size / TRANSACTION_COUNT * thread_id;
            let end_p = self.pool_size / TRANSACTION_COUNT * (thread_id + 1);
            #[cfg(not(feature = "mvcc"))]
            if start_p <= pointer && pointer < end_p {
                let mut data = old_vec.data.write();
                data.update_data_by_column(start, new_data);

                data.set_next(0);

                old_vec.clock.store(ts.tid, Ordering::Relaxed);
                let new_tuple_id = self.allocate_tuple(thread_id).unwrap();
                // println!("- insert id {:X}", new_tuple_id.get_address());
                let tuple_address = self.get_address(&new_tuple_id);
                // println!("-- insert id {:x}, {:?}", tuple_address, tuple.get_data(self.tuple_size));
                Tuple::by_buffer(tuple_address, data.get_data(self.tuple_size));
                old_vec.nvm_id.store(tuple_address, Ordering::Relaxed);
                return Ok(pointer as u64 | POW_2_63);
            }

            #[cfg(not(feature = "buffer_direct"))]
            let (new_vec, new_pointer) = self.buffer_replace(thread_id, cur_min_txn);
            #[cfg(feature = "buffer_direct")]
            let new_vec = self.buffer_pool.get(new_pointer).unwrap();
            let mut data = new_vec.data.write();
            new_vec.clock.store(ts.tid, Ordering::Relaxed);
            data.set_lock_tid(ts.tid);

            #[cfg(not(feature = "buffer_direct"))]
            {
                // if pointer/POOL_PER_TRANSACTION != thread_id {
                data.copy_from_buffer(ts, &old_vec, self.tuple_size as usize);

                data.update_data_by_column(start, new_data);
                data.set_ts_tid(ts.tid);
                data.set_next(new_address);

                new_vec
                    .nvm_id
                    .store(old_vec.nvm_id.load(Ordering::Relaxed), Ordering::Relaxed);
                // assert_ne!(data.ts().tid, old_vec.data.read().ts().tid);
                // assert_eq!(old_vec.data.read().lock_tid(), ts.tid);
                file::sfence();
            }
            // println!("--- save {}, {}", new_pointer, ts);
            drop(data);
            let data = new_vec.data.read();
            self.update_tuple_buffer_id_on_index(
                new_pointer as u64 | POW_2_63,
                &data,
                pointer as u64 | POW_2_63,
            )
            .unwrap();
            #[cfg(feature = "zen")]
            {
                let new_tuple_id = self.allocate_tuple(thread_id).unwrap();
                // println!("- insert id {:X}", tuple_id.page_start);
                let tuple_address = self.get_address(&new_tuple_id);
                // println!("-- insert id {:X}", tuple_id.page_start);
                Tuple::by_buffer(tuple_address, data.get_data(self.tuple_size));
                // println!("--- insert id {:x}", new_tuple_id.page_start.load(Ordering::Relaxed));
                new_vec.nvm_id.store(tuple_address, Ordering::Relaxed);
                // self.tid2address.insert(tuple_id.clone(), tuple_address);

                return Ok(new_pointer as u64 | POW_2_63);
            }
        }
        #[cfg(all(not(feature = "buffer_pool"), feature = "append"))]
        {
            // #[cfg(not(feature = "tpcc"))]
            // let new_tuple_id = self.allocate_append_tuple(thread_id).unwrap();
            // #[cfg(feature = "tpcc")]
            let new_tuple_id = self.allocate_tuple(thread_id).unwrap();

            let tuple_address = self.get_address(&new_tuple_id);
            let new_tuple = Tuple::reload(tuple_address);
            // assert_eq!(TUPLE_HEADER, 248);

            // assert_eq!(old_data.len(), 8);
            // println!("{:?}", old_data);
            // assert_eq!(TUPLE_HEADER + old_data.len(), self.tuple_size as usize);

            new_tuple.update_data_by_column(TUPLE_HEADER as u64, old_data);
            new_tuple.update_data_by_column(start as u64, new_data);

            new_tuple.set_ts(tuple.ts());
            new_tuple.set_next(tuple._address());
            // new_tuple.set_lock_tid(0);

            new_tuple.set_lock_tid(ts.tid);
            file::sfence();
            #[cfg(feature = "clock")]
            timer.start(BUFFER);
            // if self.id == 6 {
            //     println!("{:?}, old: {}, new: {}", new_tuple_id, tuple._address(), tuple_address);
            // }
            self.update_tuple_id_on_index(tuple_address, &tuple)
                .unwrap_or(());

            #[cfg(feature = "clock")]
            timer.end(BUFFER, BUFFER);
            new_tuple.clwb_len(self.tuple_size);
            // assert_eq!(tuple_address, new_tuple._address());
            return Ok(tuple_address);
        }
        #[cfg(not(feature = "append"))]
        return Ok(old_data.len() as u64 + DELTA_DATA_OFFSET as u64 + U64_OFFSET);
        #[cfg(not(feature = "zen"))]
        Ok(0)
    }

    /// update delete flag
    pub fn del_tuple(
        &self,
        tuple_id: &TupleId,
        new_address: Address,
        #[cfg(all(feature = "mvcc", feature = "ilog"))] d_delta_address: Address,
        #[cfg(feature = "update_direct")] snapshot: &SnapShotEntity,
        ts: TimeStamp,
        thread_id: usize,
        cur_min_txn: u64,
        #[cfg(feature = "buffer_direct")] pool_id: usize,
        #[cfg(feature = "clock")] timer: &mut Timer,
        #[cfg(feature = "hot_unflush")] flush: bool,
    ) -> Result<u64> {
        #[cfg(not(feature = "buffer_pool"))]
        let tuple = self.get_tuple(tuple_id);
        #[cfg(feature = "buffer_pool")]
        let vec = self
            .get_tuple_buffer(
                tuple_id,
                thread_id,
                ts.tid,
                cur_min_txn,
                #[cfg(feature = "clock")]
                timer,
            )
            .0;
        #[cfg(all(feature = "buffer_pool", feature = "delta"))]
        let tuple = vec.data.read();
        #[cfg(feature = "buffer_pool")]
        let tuple_address = vec.nvm_id.load(Ordering::Relaxed);
        #[cfg(feature = "delta")]
        let old_data = tuple.delete_flag().to_le_bytes();
        #[cfg(all(not(feature = "delta"), not(feature = "buffer_pool")))]
        let old_data = tuple.get_data(self.tuple_size);
        let new_data: u64 = 1;
        #[cfg(all(feature = "buffer_pool", feature = "delta"))]
        drop(tuple);
        #[cfg(feature = "buffer_pool")]
        let tuple = Tuple::reload(tuple_address);
    
        self.update_tuple(
            &tuple,
            tuple_id,
            #[cfg(not(feature = "buffer_pool"))]
            &old_data,
            #[cfg(feature = "buffer_pool")]
            &new_data.to_le_bytes(),
            new_address,
            #[cfg(all(feature = "mvcc", feature = "ilog"))]
            d_delta_address,
            &new_data.to_le_bytes(),
            DELETE_FLAG.start,
            #[cfg(feature = "update_direct")]
            snapshot,
            ts,
            thread_id,
            cur_min_txn,
            #[cfg(feature = "buffer_direct")]
            pool_id,
            #[cfg(feature = "clock")]
            timer,
            #[cfg(feature = "hot_unflush")]
            flush,
        )
    }

    #[cfg(feature = "buffer_direct")]
    pub fn update_tuple_buffer(
        &self,
        tuple_id: &TupleId,
        snapshot: &SnapShotEntity,
        column_id: usize,
        new_data: &[u8],
        ts: TimeStamp,
        thread_id: usize,
        cur_min_txn: u64,
        #[cfg(feature = "clock")] timer: &mut Timer,
    ) -> Result<usize> {
        let start = self.schema.get_column_offset(column_id).start as u64;

        let (old_vec, pointer) = self.get_tuple_buffer(
            tuple_id,
            thread_id,
            ts.tid,
            cur_min_txn,
            #[cfg(feature = "clock")]
            timer,
        );

        let tuple = old_vec.data.read();

        let lock_tid = tuple.lock_tid().clone();

        #[cfg(feature = "cc_cfg_occ")]
        if !snapshot.access(TimeStamp { tid: lock_tid }, ts.tid, cur_min_txn) {
            return Err(TupleError::TupleChanged {
                conflict_tid: lock_tid,
            }
            .into());
        }
        #[cfg(feature = "cc_cfg_to")]
        {
            if lock_tid != 0 && lock_tid != ts.tid {
                // println!("lock {} {}", lock_tid, ts.tid);
                return Err(TupleError::TupleChanged {
                    conflict_tid: lock_tid,
                }
                .into());
            }
            let tuple_ts = tuple.ts();

            if tuple_ts.read_ts > ts.tid || tuple_ts.tid > ts.tid {
                // println!("read_ts {} {}", tuple_ts.read_ts, ts.tid);
                return Err(TupleError::TupleChanged {
                    conflict_tid: tuple_ts.read_ts,
                }
                .into());
            }
        }

        let (new_vec, new_pointer) = self.buffer_replace(thread_id, cur_min_txn);

        let mut data = new_vec.data.write();

        data.copy_from_buffer(ts, &old_vec);
        data.update_data_by_column(start, new_data);
        data.set_ts_tid(ts);
        data.set_next(pointer as u64);
        data.set_lock_tid(0);

        new_vec.clock.store(ts.tid, Ordering::Relaxed);
        new_vec.nvm_id.store(tuple_id.page_start, Ordering::Relaxed);
        Ok(new_pointer)
    }

}
