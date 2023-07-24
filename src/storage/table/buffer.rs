use crate::{storage::catalog::Catalog, utils::file};

use super::*;

#[cfg(feature = "buffer_pool")]
impl Table {
    pub fn set_pool_size(&mut self, size: usize) {
        self.pool_size = size;
        self.buffer_pool = std::iter::from_fn(|| Some(BufferVec::new(self.tuple_size as usize)))
            .take(size as usize)
            .collect();
        let mut buffer_pointer: Vec<RwLock<usize>> = Vec::new();
        #[cfg(feature = "buffer_pool")]
        for i in 0..TRANSACTION_COUNT {
            buffer_pointer.push(RwLock::new(i * size / TRANSACTION_COUNT));
        }
        self.buffer_pointer = buffer_pointer;
    }
    pub fn new_with_pool(
        schema: TableSchema,
        address: Address,
        size: usize,
        tuple_id: u32,
    ) -> Self {
        let mut table = Table::new(schema, address, tuple_id);
        table.set_pool_size(size);
        table
    }
    pub fn clear_buffer(&self) {
        for vec in &self.buffer_pool {
            vec.nvm_id.store(0, Ordering::Relaxed);
        }
    }
    pub fn buffer_replace(&self, thread_id: usize, cur_min_txn: u64) -> (&BufferVec, usize) {
        let start = self.pool_size / TRANSACTION_COUNT * thread_id;
        let end = self.pool_size / TRANSACTION_COUNT * (thread_id + 1);
        let mut pointer = self.buffer_pointer.get(thread_id as usize).unwrap().write();
        let mut count = 0;
        let mut cur_min = cur_min_txn;
        loop {
            if *pointer >= end {
                *pointer = start;
                count += 1;
                cur_min += 10000; //
                                  // if count > 10 {
                                  //     println!("count {} {} {}", self.id, count, cur_min);
                                  //     // cur_min = Catalog::get_min_txn();
                                  // }
            }
            // debug!("try replece {} {} {}", thread_id, self.schema.columns()[0].name, *pointer);
            let vec = self.buffer_pool.get(*pointer).unwrap();

            // if count > 10 {
            //     println!("try replace {}, {} {:?}", self.id, cur_min_txn, vec.clock);
            // }
            if vec.replace(cur_min) {
                let data = vec.data.read();
                if vec.nvm_id.load(Ordering::Relaxed) > 0 {
                    data.set_lock_tid(POW_2_63);
                    self.update_tuple_buffer_id_on_index(
                        vec.nvm_id.load(Ordering::Relaxed),
                        &data,
                        (*pointer) as u64 | POW_2_63,
                    )
                    .unwrap_or(());
                    vec.nvm_id.store(0, Ordering::Relaxed);
                }

                *pointer += 1;
                return (&vec, *pointer - 1);
            }
            *pointer += 1;
        }
    }
    pub fn get_primary_k(&self, tuple: &Tuple) -> u64 {
        let range = self
            .schema
            .get_column_offset(self.primary_key.load(Ordering::Relaxed));
        let key = tuple.get_data_by_column(range);
        return u64::from_le_bytes(key.try_into().unwrap());
    }
    pub fn get_tuple_buffer(
        &self,
        tuple_id: &TupleId,
        thread_id: usize,
        tid: u64,
        cur_min_txn: u64,
        #[cfg(feature = "clock")] timer: &mut Timer,
    ) -> (&BufferVec, usize) {
        #[cfg(feature = "clock")]
        timer.start(BUFFER);
        // println!("get buffer {:x}", tuple_id.get_address());
        if tuple_id.get_address() & POW_2_63 > 0 {
            let pool_id = (tuple_id.get_address() ^ POW_2_63) as usize;
            // println!("hit buffer {} ", pool_id);

            let vec = self.buffer_pool.get(pool_id).unwrap();
            let clock = vec.clock.load(Ordering::Relaxed);
            // println!("--- hit buffer check clock = {} nvm_id: {:x} = {:x}", clock, vec.nvm_id.load(Ordering::Relaxed), tuple_id.get_address());
            // println!("{:?}", vec.data.read().get_data(self.tuple_size));
            if clock < tid {
                unsafe {
                    vec.clock
                        .compare_exchange(clock, tid, Ordering::Relaxed, Ordering::Relaxed)
                        .unwrap_unchecked();
                }
            }

            // println!("--- read buffer {}, {}", pool_id, vec.data.read().ts());
            #[cfg(feature = "clock")]
            timer.end(BUFFER, BUFFER_HIT);
            return (vec, pool_id);
        }
        assert_ne!(tuple_id.get_address() & POW_2_63, POW_2_63);
        let tuple = self.get_tuple(tuple_id);

        let (vec, pointer) = self.buffer_replace(thread_id, cur_min_txn);
        let mut data = vec.data.write();
        let result = self.update_tuple_id_on_index(pointer as u64 | POW_2_63, &tuple);
        // file::sfence();

        // TODO MULTI VERSION vec.active.store(true, Ordering::Relaxed);
        vec.nvm_id.store(tuple._address(), Ordering::Relaxed);
        data.copy_from_nvm(&tuple, self.tuple_size as usize);

        vec.clock.store(tid, Ordering::Relaxed);
        match result {
            Ok(()) => {
                // println!("buffer not conflict");
            }
            Err(Error::Tuple(TupleError::TupleChanged {
                conflict_tid: new_tuple_id,
            })) => {
                tuple_id.update(new_tuple_id);
                // let key = tuple.get_data_by_column(self.schema.get_column_offset(0));

                // println!("buffer conflict table_id: {}, tid: {}, key: {},{} {}", self.id, tid, &u64::from_le_bytes(key.try_into().unwrap()), tuple._address(), new_tuple_id);

                return self.get_tuple_buffer(tuple_id, thread_id, tid, cur_min_txn);
            }
            _ => {}
        }
        // tuple.set_lock_tid(0);
        tuple_id.update(pointer as u64 | POW_2_63);
        #[cfg(feature = "clock")]
        timer.end(BUFFER, BUFFER_SWAP);
        return (vec, pointer);
    }
}
