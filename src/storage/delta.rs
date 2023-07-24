use crate::config::Address;
use crate::mvcc_config::delta::*;
use crate::storage::row::TupleError;
use crate::transaction::transaction::Transaction;
use crate::utils::persist::persist_struct::PersistStruct;
use std::fmt::Display;
use std::ops::Range;
use std::result::Result;

use super::row::Tuple;

/// ```ignore
/// |----------------------------------------------------|
/// |         len(u64)         |        tid (u64)        |
/// |----------------------------------------------------|
/// |   next_delta_address(u64) |  table_id(u32)         |
/// |----------------------------------------------------|
/// |  delta_column_offset (u32)  |  delta_data([u8])  |
/// |----------------------------------------------------|
/// ```

#[derive(Copy, Clone, Debug)]
pub struct TupleDelta {
    pub delta_data: PersistStruct,
}

impl TupleDelta {
    pub fn new(
        address: Address,
        next_delta: u64,
        tuple_address: Address,
    ) -> Result<Self, TupleError> {
        let mut delta = TupleDelta {
            delta_data: PersistStruct::new(address, DELTA_DATA_OFFSET),
        };

        delta.set_meta_data(NEXT_DELTA_ADDRESS, next_delta);
        delta.set_meta_data(TUPLE_ADDRESS, tuple_address);

        Ok(delta)
    }
    pub fn reload(address: Address) -> Result<Self, TupleError> {
        let delta = TupleDelta {
            delta_data: PersistStruct::reload(address, DELTA_DATA_OFFSET),
        };
        Ok(delta)
    }
    pub fn clwb(&self) {
        #[cfg(feature = "clwb_delta")]
        self.delta_data.clwb();
    }
    pub fn _address(&self) -> Address {
        self.delta_data._address()
    }
    pub fn _data_address(&self) -> Address {
        self.delta_data._data_address()
    }
    pub fn get_meta_data<T: Copy + Display>(&self, parameter: Range<u64>) -> T {
        self.delta_data.get_meta_data(parameter)
    }
    pub fn set_meta_data<T: Display>(&mut self, parameter: Range<u64>, value: T) {
        self.delta_data.set_meta_data(parameter, value)
    }
    pub fn len(&self) -> u64 {
        self.delta_data.len()
    }
    pub fn save(&mut self, bytes: &[u8]) {
        self.delta_data.save(bytes);
    }
    pub fn data_len(&self) -> u64 {
        self.delta_data.data_len()
    }
    pub fn data(&self) -> &[u8] {
        self.delta_data.data()
    }
    pub fn rollback(&self, committed: u64) {
        let tuple_address: u64 = self.get_meta_data(TUPLE_ADDRESS);

        let tuple = Tuple::reload(tuple_address);
        let next_address = tuple.next_address();
        let ts = tuple.ts();
        let index: u32 = self.get_meta_data(DELTA_COLUMN_OFFSET);
        if committed == 1 {
            tuple.update_data_by_column(index as u64, self.data());
        }
        tuple.set_ts_and_next(ts, self.get_meta_data(TID), next_address, 0);
        tuple.set_lock_tid(0);
    }
}

pub struct TupleDeltaIterator {
    delta: TupleDelta,
}

impl Iterator for TupleDeltaIterator {
    type Item = TupleDelta;

    fn next(&mut self) -> Option<Self::Item> {
        let next_delta_address = self.delta.get_meta_data(NEXT_DELTA_ADDRESS);
        if next_delta_address != 0 {
            let d = TupleDelta::reload(self.delta._address()).unwrap();
            self.delta = TupleDelta::reload(next_delta_address).unwrap();
            return Some(d);
        }
        None
    }
}

// #[test]
// fn list_test() {
//     /*let mut a = ListNode::new(1, None);
//     a.add(3);
//     a.add(4);
//     a.add(5);
//     a.add(9);
//     println!("{}", a);*/
// }
