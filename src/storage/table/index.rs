use super::*;

impl Table {
    #[cfg(feature = "lock_index")]
    pub fn get_index_read_guard(&self, columns: usize) -> RwLockReadGuard<TableIndex> {
        let index = self.index.get(&columns).unwrap();

        let mut table_index = index.try_read();

        while index.is_none() {
            index = index.try_read();
        }

        index.unwrap()
    }
    #[cfg(feature = "lock_index")]
    pub fn get_index_write_guard(&self, columns: usize) -> RwLockWriteGuard<TableIndex> {
        let index = self.index.get(&columns).unwrap();

        let mut table_index = index.try_write();

        while index.is_none() {
            index = index.try_write();
        }

        index.unwrap()
    }

    pub fn search_tuple_id_on_index(&self, key: &IndexType, columns: usize) -> Result<TupleId> {
        // let index = self.index.read().unwrap();
        // println!("key:{:?}, columns:{:?}, index:{:?}",key,columns,self.index);

        let result: Result<TupleId>;

        #[cfg(not(feature = "lock_index"))]
        {
            let table_index = self.index.get(&columns).unwrap();
            match (table_index, key) {
                (TableIndex::Int64(index), IndexType::Int64(u)) => match index.get(u) {
                    Some(v) => {
                        result = Ok(v.clone());
                    }
                    None => {
                        return Err(Error::Tuple(TupleError::KeyNotMatched));
                    }
                },
                #[cfg(feature = "dash")]
                (TableIndex::String(index), IndexType::String(u)) => {
                    let u = u.trim_end_matches(char::from(0));
                    match index.get(u, u.len()) {
                        Some(v) => {
                            result = Ok(v.clone());
                        }
                        None => {
                            return Err(Error::Tuple(TupleError::KeyNotMatched));
                        }
                    }
                }
                #[cfg(not(any(feature = "nbtree", feature = "dash")))]
                (TableIndex::String(index), IndexType::String(u)) => match index.get(u) {
                    Some(v) => {
                        result = Ok(v.clone());
                    }
                    None => {
                        return Err(Error::Tuple(TupleError::KeyNotMatched));
                    }
                },
                #[cfg(any(feature = "nbtree"))]
                (TableIndex::String(index), IndexType::String(u)) => {
                    return Err(Error::Tuple(TupleError::IndexNotBuilt))
                }
                (TableIndex::None, _) => return Err(Error::Tuple(TupleError::IndexNotBuilt)),
                _ => return Err(Error::Tuple(TupleError::KeyNotMatched)),
            };
        }

        #[cfg(feature = "lock_index")]
        {
            let table_index = self.get_index_read_guard(columns);
            match (&*index, key) {
                (TableIndex::Int64(index), IndexType::Int64(u)) => match index.get(u) {
                    Some(v) => {
                        result = Ok(*v);
                    }
                    None => {
                        return Err(Error::Tuple(TupleError::KeyNotMatched));
                    }
                },
                (TableIndex::String(index), IndexType::String(u)) => match index.get(u) {
                    Some(v) => {
                        result = Ok(*v);
                    }
                    None => {
                        return Err(Error::Tuple(TupleError::KeyNotMatched));
                    }
                },
                (TableIndex::None, _) => return Err(Error::Tuple(TupleError::IndexNotBuilt)),
                _ => return Err(Error::Tuple(TupleError::KeyNotMatched)),
            };
        }

        match result {
            Ok(tid) => {
                // unsafe {
                //     intrinsics::prefetch_read_data(&tid.page_start(), 3);
                // }
                // #[cfg(feature = "buffer_pool")]
                // if tid.is_pool_id() {
                //     let vec = self.buffer_pool.get(tid.pool_id()).unwrap();
                //     while vec.clock.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_err()
                //     {
                //         if vec.dirty.load(Ordering::Relaxed) {
                //             return Err(Error::Tuple(TupleError::TupleChanged{conflict_tid: 0}));
                //         }
                //     }
                // }
                return Ok(tid);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    //YCSB
    // pub fn update_tuple_id_on_index(&self, key: &IndexType, new_address: u64) -> Result<> {
    //     // let index = self.index.read().unwrap();
    //     // println!("key:{:?}, columns:{:?}, index:{:?}",key,columns,self.index);

    //     let result: Result<TupleId>;

    //     let table_index = self.index.get(&self.get_primary_key()).unwrap();
    //     match (table_index, key) {
    //         (TableIndex::Int64(index), IndexType::Int64(u)) => match index.get(u) {
    //             Some(v) => {
    //                 v.update(new_address);
    //             }
    //             None => {
    //                 return Err(Error::Tuple(TupleError::KeyNotMatched));
    //             }
    //         },
    //         (TableIndex::String(index), IndexType::String(u)) => match index.get(u) {
    //             Some(v) => {
    //                 v.update(new_address);
    //             }
    //             None => {
    //                 return Err(Error::Tuple(TupleError::KeyNotMatched));
    //             }
    //         },
    //         (TableIndex::None, _) => return Err(Error::Tuple(TupleError::IndexNotBuilt)),
    //         _ => return Err(Error::Tuple(TupleError::KeyNotMatched)),
    //     };
    //     return Ok(());
    // }

    pub fn update_tuple_id_on_index(&self, new_address: u64, tuple: &Tuple) -> Result {
        for (column_id, table_index) in self.index.iter() {
            // println!("{} {}\n", self.id, column_id);

            let range = self.schema.get_column_offset(*column_id);
            // let pair = self.index.get(key).unwrap();
            let key = tuple.get_data_by_column(range);
            // println!("update index {:?}, {:x}", key, new_address);
            match table_index {
                #[cfg(not(feature = "dash"))]
                TableIndex::Int64(index) => {
                    match index.get(&u64::from_le_bytes(key.try_into().unwrap())) {
                        Some(v) => {
                            let result = v.cas(tuple._address(), new_address);
                            if result != tuple._address() {
                                return Err(Error::Tuple(TupleError::TupleChanged {
                                    conflict_tid: result,
                                }));
                            }
                        }
                        None => {
                            return Err(Error::Tuple(TupleError::KeyNotMatched));
                        }
                    }
                }
                #[cfg(feature = "dash")]
                TableIndex::Int64(index) => {
                    match index.update(
                        u64::from_le_bytes(key.try_into().unwrap()),
                        TupleId::from_address(new_address),
                    ) {
                        Some(v) => {}
                        None => {
                            return Err(Error::Tuple(TupleError::KeyNotMatched));
                        }
                    }
                }
                #[cfg(not(any(feature = "dash", feature = "nbtree")))]
                TableIndex::String(index) => {
                    match index.get(&String::from(str::from_utf8(key).unwrap())) {
                        Some(v) => {
                            v.update(new_address);
                        }
                        None => {
                            return Err(Error::Tuple(TupleError::KeyNotMatched));
                        }
                    }
                }
                #[cfg(feature = "dash")]
                TableIndex::String(index) => {
                    let key = String::from(str::from_utf8(key).unwrap());
                    let key = key.trim_end_matches(char::from(0));

                    match index.get(key.clone(), key.len()) {
                        Some(v) => {
                            v.update(new_address);
                        }
                        None => {
                            return Err(Error::Tuple(TupleError::KeyNotMatched));
                        }
                    }
                }
                #[cfg(feature = "nbtree")]
                TableIndex::String(index) => {
                    match index.get(&String::from(str::from_utf8(key).unwrap())) {
                        Some(v) => {
                            v.update(new_address);
                        }
                        None => {
                            return Err(Error::Tuple(TupleError::KeyNotMatched));
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    pub fn update_tuple_buffer_id_on_index(
        &self,
        new_address: u64,
        tuple: &RwLockReadGuard<BufferDataVec>,
        pool_address: u64,
    ) -> Result {
        for (column_id, table_index) in self.index.iter() {
            let range = self.schema.get_column_offset(*column_id);
            // println!("{} {}\n", range.start, range.end);
            // let pair = self.index.get(key).unwrap();
            let key = tuple.get_data_by_column(range);
            // println!("update index {:?}, {:x}", key, new_address);

            match table_index {
                #[cfg(not(feature = "dash"))]
                TableIndex::Int64(index) => {
                    match index.get(&u64::from_le_bytes(key.try_into().unwrap())) {
                        Some(v) => {
                            if v.get_address() == pool_address {
                                v.update(new_address);
                            }
                            // else {
                            //     println!("{}, {}", v.get_address(), pool_address)
                            // }
                        }
                        None => {
                            return Err(Error::Tuple(TupleError::KeyNotMatched));
                        }
                    }
                }
                #[cfg(feature = "dash")]
                TableIndex::Int64(index) => {
                    match index.update(
                        u64::from_le_bytes(key.try_into().unwrap()),
                        TupleId::from_address(new_address),
                    ) {
                        Some(v) => {}
                        None => {
                            return Err(Error::Tuple(TupleError::KeyNotMatched));
                        }
                    }
                }
                #[cfg(feature = "dash")]
                TableIndex::String(index) => {
                    let key = String::from(str::from_utf8(key).unwrap());
                    let key = key.trim_end_matches(char::from(0));

                    match index.get(key.clone(), key.len()) {
                        Some(v) => {
                            v.update(new_address);
                        }
                        None => {
                            return Err(Error::Tuple(TupleError::KeyNotMatched));
                        }
                    }
                }
                #[cfg(not(any(feature = "nbtree", feature = "dash")))]
                TableIndex::String(index) => {
                    match index.get(&String::from(str::from_utf8(key).unwrap())) {
                        Some(v) => {
                            v.update(new_address);
                        }
                        None => {
                            return Err(Error::Tuple(TupleError::KeyNotMatched));
                        }
                    }
                }
                _ => {
                    return Err(Error::Tuple(TupleError::KeyNotMatched));
                }
            }
        }
        Ok(())
    }

    pub fn index_insert(&self, key: IndexType, value: &TupleId) -> Result {
        // let k = self.primary_key.read().unwrap();
        self.index_insert_on_index(
            key,
            value,
            self.primary_key.load(std::sync::atomic::Ordering::SeqCst),
        )
    }
    pub fn index_insert_on_index(&self, key: IndexType, value: &TupleId, columns: usize) -> Result {
        #[cfg(not(feature = "lock_index"))]
        {
            let table_index = self.index.get(&columns).unwrap();
            match (table_index, key) {
                (TableIndex::Int64(index), IndexType::Int64(u)) => {
                    index.insert(u, value.clone()).unwrap();
                }
                #[cfg(not(any(feature = "nbtree", feature = "dash")))]
                (TableIndex::String(index), IndexType::String(u)) => {
                    index.insert(u, value.clone()).unwrap();
                }
                #[cfg(feature = "dash")]
                (TableIndex::String(index), IndexType::String(u)) => {
                    let u = u.trim_end_matches(char::from(0));
                    index.insert(u.clone(), u.len(), value.clone()).unwrap();
                }
                #[cfg(any(feature = "nbtree"))]
                (TableIndex::String(index), IndexType::String(u)) => {
                    return Err(Error::Tuple(TupleError::IndexNotBuilt))
                }
                (TableIndex::None, _) => return Err(Error::Tuple(TupleError::IndexNotBuilt)),
                _ => return Err(Error::Tuple(TupleError::KeyNotMatched)),
            }
        }
        #[cfg(feature = "lock_index")]
        {
            let mut table_index = self.get_index_write_guard(columns);
            match (&mut *index, key) {
                (TableIndex::Int64(index), IndexType::Int64(u)) => {
                    index.insert(u, value).unwrap();
                }
                (TableIndex::String(index), IndexType::String(u)) => {
                    index.insert(u, value).unwrap();
                }
                (TableIndex::None, _) => return Err(Error::Tuple(TupleError::IndexNotBuilt)),
                _ => return Err(Error::Tuple(TupleError::KeyNotMatched)),
            }
        }
        Ok(())
    }

    pub fn index_insert_by_tuple(&self, tuple_id: &TupleId, tuple: &Tuple) -> Result {
        // let index = self.index.read().unwrap();
        // let index_key = self.index_key.read().unwrap();
        // #[cfg(feature = "buffer_pool")]
        // self.tuple2pool.insert(tuple_id.clone(), 0);
        // #[cfg(feature = "zen")]
        // self.tid2address.insert(tuple_id.clone(), tuple_id.get_address());

        for (column_id, table_index) in self.index.iter() {
            let range = self.schema.get_column_offset(*column_id);
            // println!("{} {}\n", range.start, range.end);
            // let pair = self.index.get(key).unwrap();
            let key = tuple.get_data_by_column(range);

            #[cfg(not(feature = "lock_index"))]
            {
                // let table_index = self.index.get(column_id).unwrap();

                match table_index {
                    TableIndex::Int64(index) => {
                        index.insert(
                            u64::from_le_bytes(key.try_into().unwrap()),
                            tuple_id.clone(),
                        );
                    }
                    #[cfg(feature = "dash")]
                    TableIndex::String(index) => {
                        let key = str::from_utf8(key).unwrap();
                        let key = key.to_string();
                        let key = key.trim_end_matches(char::from(0));
                        // println!("{}, {}", key, key.len());

                        index.insert(key.clone(), key.len(), tuple_id.clone());
                    }
                    #[cfg(not(any(feature = "nbtree", feature = "dash")))]
                    TableIndex::String(index) => {
                        index.insert(String::from(str::from_utf8(key).unwrap()), tuple_id.clone());
                    }
                    _ => {
                        return Err(Error::Tuple(TupleError::IndexNotBuilt));
                    }
                }
            }
            #[cfg(feature = "lock_index")]
            {
                let mut table_index = self.get_index_write_guard(*column_id);

                match &mut *table_index {
                    TableIndex::Int64(index) => {
                        index.insert(u64::from_le_bytes(key.try_into().unwrap()), tuple_id);
                    }
                    TableIndex::String(index) => {
                        index.insert(String::from(str::from_utf8(key).unwrap()), tuple_id);
                    }
                    _ => {
                        // return Err(Error::Tuple(TupleError::IndexNotBuilt)),
                    }
                }
            }
        }

        Ok(())
    }
    pub fn index_remove_by_tuple(&self, tuple_id: &TupleId, tuple: &Tuple) -> Result {
        // let index = self.index.read().unwrap();
        // let index_key = self.index_key.read().unwrap();
        for (column_id, table_index) in self.index.iter() {
            let range = self.schema.get_column_offset(*column_id);
            // println!("{} {}\n", range.start, range.end);
            // let pair = self.index.get(key).unwrap();
            let key = tuple.get_data_by_column(range);
            #[cfg(not(feature = "lock_index"))]
            {
                match table_index {
                    TableIndex::Int64(index) => {
                        index.remove(&u64::from_le_bytes(key.try_into().unwrap()));
                    }
                    #[cfg(not(any(feature = "nbtree", feature = "dash")))]
                    TableIndex::String(index) => {
                        index.remove(&String::from(str::from_utf8(key).unwrap()));
                    }
                    #[cfg(feature = "dash")]
                    TableIndex::String(index) => {
                        let key = String::from(str::from_utf8(key).unwrap());
                        let key = key.trim_end_matches(char::from(0));
                        index.remove(key.clone(), key.len());
                    }
                    _ => {
                        return Err(Error::Tuple(TupleError::IndexNotBuilt));
                    }
                }
            }
            #[cfg(feature = "lock_index")]
            {
                let mut table_index = self.get_index_write_guard(*column_id);
                match &mut *table_index {
                    TableIndex::Int64(index) => {
                        index.remove(&u64::from_le_bytes(key.try_into().unwrap()));
                    }
                    TableIndex::String(index) => {
                        index.remove(&String::from(str::from_utf8(key).unwrap()));
                    }
                    _ => {
                        // return Err(Error::Tuple(TupleError::IndexNotBuilt)),
                    }
                }
            }
        }

        Ok(())
    }
    pub fn search_tuple_id(&self, key: &IndexType) -> Result<TupleId> {
        let k = self.primary_key.load(std::sync::atomic::Ordering::SeqCst); //read().unwrap();
        self.search_tuple_id_on_index(key, k)
    }
}
