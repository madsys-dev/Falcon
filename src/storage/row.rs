use crate::config::Address;
use crate::config::POW_2_63;
use crate::config::TUPLE_SIZE;
use crate::config::U64_OFFSET;
use crate::mvcc_config::*;
use crate::range;
use crate::storage::delta::TupleDelta;
use crate::storage::schema::{ColumnType, TableSchema};
use crate::storage::timestamp::TimeStamp;
use crate::transaction::clog::*;
use crate::transaction::TxStatus;
use crate::utils::file;
use crate::utils::file::sfence;
use crate::utils::io;
use crate::utils::persist::persist_struct::PersistStruct;
use std::fmt::Display;
use std::fs::File;
use std::io::Write;
use std::mem::size_of;
use std::ops::Range;
use std::ptr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[cfg(feature = "read_ts")]
use crate::storage::timestamp::TS_READ_TS;

pub const TID: Range<u64> = range!(0, size_of::<TimeStamp>() as u64);
pub const NEXT_DELTA_ADDRESS: Range<u64> = range!(TID.end, U64_OFFSET);
pub const DELETE_FLAG: Range<u64> = range!(NEXT_DELTA_ADDRESS.end, U64_OFFSET);
pub const LOCK_TID: Range<u64> = range!(DELETE_FLAG.end, U64_OFFSET);
#[cfg(all(feature = "align", not(feature = "large")))]
pub const TUPLE_HEADER: usize = crate::mvcc_config::YCSB_SIZE-8 as usize;
#[cfg(all(feature = "align", feature = "large"))]
pub const TUPLE_HEADER: usize = 248;
#[cfg(not(feature = "align"))]
pub const TUPLE_HEADER: usize = LOCK_TID.end as usize;
pub const DELETE_MASK: u64 = 1;
pub const COMMIT_MASK: u64 = 2;

/// ```ignore
/// |--------------------------------------------------------------------|
/// |   len(u64)   |        delete_flag (u64 table_id + tuple_id)        |  
/// |--------------------------------------------------------------------|
/// |                 ts (u64) + first_delta_address(u64)                |
/// |--------------------------------------------------------------------|
/// |                          data([u8])                                |
/// |--------------------------------------------------------------------|
/// ```
#[derive(Default)]
pub struct Tuple {
    data: PersistStruct,
}

impl Tuple {
    pub fn new(
        address: Address,
        data: &str,
        schema: &TableSchema,
        ts: TimeStamp,
    ) -> Result<Self, TupleError> {
        let mut t = Tuple {
            data: PersistStruct::new_without_length(address, TUPLE_HEADER as u64),
        };
        // let tuple_len = schema.tuple_size();
        t.set_ts(ts);
        t.set_next(0);
        t.set_lock_tid(0);
        t.set_delete_flag();

        let mut len = TUPLE_HEADER as u64;
        //TODO check data_list, schema
        for (field, col) in data.split(",").zip(schema.columns().iter()) {
            len += t.push(len, &col.type_, field);
        }
        Ok(t)
    }
    pub fn clwb(&self) {
        #[cfg(feature = "clwb_tuple")]
        self.data.clwb();
    }
    pub fn clwb_len(&self, len: u64) {
        #[cfg(feature = "clwb_tuple")]
        self.data.clwb_len(len);
    }
    pub fn clwb_update(&self, len: u64, start: u64, end: u64) {
        #[cfg(feature = "clwb_tuple")]
        {
            // assert_eq!(start&255, 0);
            // assert_eq!(end&255, 0);
            // #[cfg(not(feature = "clwb_fence"))]
            // file::sfence();
            #[cfg(feature = "hot_unflush")]
            if end - start <= 64 && (self._address() | 255) + end > 256 {
                return;
            }
            let mut iter: u64 = start;
            let mut count = 0;
            // println!("flush {} {} {}", self._address(), start, end);
            while iter < end {
                unsafe {
                    count += 1;
                    io::clwb((iter + self._address()) as *const u8);
                }
                iter += 64;
            }
            unsafe {
                io::clwb(self._address() as *const u8);
            }
            #[cfg(feature = "clwb_fence")]
            file::sfence();
            // assert_eq!(count, 4);
        }
    }
    pub fn reload(address: Address) -> Self {
        Tuple {
            data: PersistStruct::reload_without_length(address, TUPLE_HEADER as u64),
        }
    }

    pub fn push(&mut self, offset: u64, data_type: &ColumnType, data: &str) -> u64 {
        match data_type {
            ColumnType::Int64 => {
                // println!("{}, {}", offset, data);
                let value = data.parse::<u64>().unwrap();
                self.data.copy_from_slice(offset, &value.to_le_bytes());
                return 8;
            }
            ColumnType::Double => {
                let value = data.parse::<f64>().unwrap();
                self.data.copy_from_slice(offset, &value.to_le_bytes());
                return 8;
            }
            ColumnType::String { len } => {
                let value = data.as_bytes();
                let data_len = value.len();
                if data_len > *len {
                    println!("{}, {}", data_len, *len);
                }
                assert!(data_len <= *len);

                self.data.copy_from_slice(offset, value);

                if data_len < *len {
                    let zeros = vec![0u8; *len - data_len];
                    self.data.copy_from_slice(offset + data_len as u64, &zeros);
                }
                return *len as u64;
            }
        }
    }
    pub fn save(&mut self, offset: u64, data: &[u8]) {
        self.data
            .copy_from_slice(offset + TUPLE_HEADER as u64, data);
    }
    pub fn save_u64(&mut self, offset: u64, data: u64) {
        self.save(offset, &data.to_le_bytes());
    }
    pub fn save_f64(&mut self, offset: u64, data: f64) {
        self.save(offset, &data.to_le_bytes());
    }

    pub fn _address(&self) -> Address {
        self.data._address()
    }

    pub fn next_address(&self) -> Address {
        self.data.get_meta_data(NEXT_DELTA_ADDRESS)
    }

    pub fn ts(&self) -> TimeStamp {
        self.data.get_meta_data(TID)
    }

    pub fn lock_tid(&self) -> u64 {
        self.data.get_meta_data(LOCK_TID)
    }
    pub fn delete_flag(&self) -> u64 {
        self.data.get_meta_data(DELETE_FLAG)
    }

    pub fn set_next(&self, next_address: Address) -> bool {
        self.data.set_meta_data(NEXT_DELTA_ADDRESS, next_address);
        true
    }
    pub fn set_ts(&self, ts: TimeStamp) -> bool {
        // debug!("tuple update ts {} {}", self._address(), ts.tid);
        self.data.set_meta_data(TID, ts);
        true
    }
    pub fn set_ts_tid(&self, tid: u64) -> bool {
        // debug!("tuple update ts1 {} {}", self._address(), tid);
        self.data.set_meta_data(TID, tid);
        true
    }

    pub fn set_lock_tid(&self, tid: u64) -> bool {
        let address: u64 = (self._address() + LOCK_TID.start) as u64;
        let ts = unsafe { &*(address as *const AtomicU64) };
        ts.store(tid, Ordering::Relaxed);
        // self.data.set_meta_data(LOCK_TID, ts);
        true
    }

    pub fn set_delete_flag(&self) -> bool {
        self.data.set_meta_data(DELETE_FLAG, DELETE_MASK);
        true
    }
    pub fn cas_lock_tid(&self, old_ts: u64, new_ts: u64) -> u64 {
        //  debug!("tuple lock {} {}", self._address(), new_ts);

        let address: u64 = (self._address() + LOCK_TID.start) as u64;
        let ts = unsafe { &*(address as *const AtomicU64) };
        let result = ts.compare_exchange(old_ts, new_ts, Ordering::Relaxed, Ordering::Relaxed);

        match result {
            Ok(u) => return u,
            Err(u) => return u,
        }
    }

    #[cfg(feature = "read_ts")]
    pub fn set_read_ts(&self, read_ts: u64) {
        // debug!("read {} {}", self._address(), read_ts);
        let address: u64 = (self._address() + TID.start + TS_READ_TS.start) as u64;
        let ts = unsafe { &*(address as *const AtomicU64) };
        loop {
            let old_ts = ts.load(Ordering::Relaxed);
            if old_ts >= read_ts {
                break;
            }
            if ts
                .compare_exchange(old_ts, read_ts, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }
    pub fn set_ts_and_next(
        &self,
        old_ts: TimeStamp,
        new_ts: TimeStamp,
        old_next_address: Address,
        new_next_address: Address,
    ) -> u64 {
        #[cfg(feature = "update_direct")]
        {
            let old: u128 = (old_ts.tid as u128) | (old_next_address as u128) << 64;
            let new: u128 = (new_ts.tid as u128) | (new_next_address as u128) << 64;
            let address: u128 = self._address() as u128;

            assert_eq!(address & 15, 0);
            unsafe {
                let cur = core::arch::x86_64::cmpxchg16b(
                    address as *mut u128,
                    old,
                    new,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                let old_tid = (cur & ((1u128 << 64) - 1)) as u64;
                return old_tid;
            }
        }

        #[cfg(not(feature = "update_direct"))]
        {
            self.set_next(new_next_address);
            sfence();
            // assert_eq!(self.lock_tid(), new_ts.tid);
            self.set_ts(new_ts);
            return old_ts.tid;
        }
    }

    pub fn get_data_by_column(&self, column_range: Range<usize>) -> &[u8] {
        self.data
            .range(column_range.start as u64, column_range.end as u64)
    }
    pub fn update_data_by_column(&self, column_start: u64, new_data: &[u8]) {
        self.data.copy_from_slice(column_start, new_data);
    }

    pub fn read(&self, len: usize) -> TupleVec {
        TupleVec::new(self, len)
    }
    pub fn column_read(&self, range: Range<usize>) -> TupleColumnVec {
        TupleColumnVec::new(self, range)
    }
    pub fn _to_vec(&self) -> Vec<u8> {
        self.data.to_vec()
    }
    pub fn get_data(&self, end: u64) -> &[u8] {
        self.data.range(TUPLE_HEADER as u64, end)
    }
    pub fn apply_next(&self) -> bool {
        let next_address = self.next_address();
        let ts = self.ts();
        if next_address == 0 {
            return false;
        }

        let delta = TupleDelta::reload(next_address).unwrap();

        let index = delta.get_meta_data(delta::DELTA_COLUMN_OFFSET);

        self.update_data_by_column(index, delta.data());
        #[cfg(feature = "cc_cfg_occ")]
        self.set_ts_and_next(
            ts,
            delta.get_meta_data(delta::TID),
            next_address,
            delta.get_meta_data(delta::NEXT_DELTA_ADDRESS),
        );
        // #[cfg(not(feature = "cc_cfg_clog"))]
        // {
        //     self.set_lock_tid(ts.tid);
        //     self.set_next(delta.get_meta_data(delta::NEXT_DELTA_ADDRESS));
        //     self.set_ts(delta.get_meta_data(delta::TID));
        //     self.set_lock_tid(0);
        // }

        true
    }

    pub fn by_buffer(start: Address, buffer: &[u8]) {
        let data = PersistStruct::new_without_length(start, TUPLE_HEADER as u64);
        // println!("--- {:X}, {}", start, buffer.len());
        data.copy_from_slice(0, buffer);
        let address: u64 = (start + LOCK_TID.start) as u64;
        let ts = unsafe { &*(address as *const AtomicU64) };
        ts.store(0, Ordering::Relaxed);
        #[cfg(feature = "clwb_tuple")]
        data.clwb_len(buffer.len() as u64);
    }

    pub fn commit(&self) {
        let address: u64 = (self._address() + DELETE_FLAG.start) as u64;
        let u = unsafe { &*(address as *const AtomicU64) };
        u.fetch_or(COMMIT_MASK, Ordering::Relaxed);
        unsafe {
            io::clwb(address as *const u8);
        }
    }
    #[cfg(all(feature = "cc_cfg_2pl", feature = "read_ts"))]
    pub fn lock_read(&self, reader: u64) -> Result<(), TupleError> {
        let address: u64 = (self._address() + TID.start + TS_READ_TS.start) as u64;
        let ts = unsafe { &*(address as *const AtomicU64) };

        let tid = ts.fetch_add(1, Ordering::SeqCst);

        if tid & POW_2_63 > 0 {
            if tid == reader {
                return Ok(());
            }
            let tid = ts.fetch_sub(1, Ordering::SeqCst);
            // println!("lock by {:x}", tid);
            return Err(TupleError::AcquireReadLockFalse);
        }
        Ok(())
    }
    #[cfg(all(feature = "cc_cfg_2pl", feature = "read_ts"))]
    pub fn release_read_lock(&self) {
        let address: u64 = (self._address() + TID.start + TS_READ_TS.start) as u64;
        let ts = unsafe { &*(address as *const AtomicU64) };
        let tid = ts.load(Ordering::Relaxed);
        if tid < POW_2_63 && tid > 0 {
            //may be write by itself
            ts.fetch_sub(1, Ordering::SeqCst);
        }
    }
    #[cfg(all(feature = "cc_cfg_2pl", feature = "read_ts"))]
    pub fn lock_write(&self, txn_id: u64, counter: u64) -> Result<(), TupleError> {
        let address: u64 = (self._address() + TID.start + TS_READ_TS.start) as u64;
        let ts = unsafe { &*(address as *const AtomicU64) };
        if ts
            .compare_exchange(
                counter,
                txn_id | POW_2_63,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            return Ok(());
        }
        let tid = ts.load(Ordering::Relaxed);
        // println!("{:x} {:x}", self._address(), tid);
        if tid == txn_id | POW_2_63 {
            return Ok(());
        }
        return Err(TupleError::AcquireWriteLockFalse);
    }
    #[cfg(all(feature = "cc_cfg_2pl", feature = "read_ts"))]
    pub fn release_write_lock(&self) {
        // println!("{:x} release write_lock", self._address());
        let address: u64 = (self._address() + TID.start + TS_READ_TS.start) as u64;
        let ts = unsafe { &*(address as *const AtomicU64) };
        ts.store(0, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct TupleVec {
    pub next: u64,
    pub ts: TimeStamp,
    pub data: Vec<u8>,
}

impl TupleVec {
    pub fn new(tuple: &Tuple, len: usize) -> Self {
        TupleVec {
            ts: tuple.ts(),
            next: tuple.next_address(),
            data: tuple.data.to_vec_len(len),
        }
    }
    pub fn from_buffer(buffer: &BufferDataVec) -> Self {
        TupleVec {
            ts: buffer.ts(),
            next: buffer.next_address(),
            data: buffer.data.iter().copied().collect(),
        }
    }
    pub fn ts(&self) -> TimeStamp {
        self.ts.clone()
    }
    fn delta(&mut self, delta: &TupleDelta) {
        let index: u32 = delta.get_meta_data(delta::DELTA_COLUMN_OFFSET);
        let len = delta.data_len();
        let delta_address = delta._data_address();

        unsafe {
            ptr::copy(
                delta_address as *const u8,
                self.data.as_mut_ptr().add(index as usize),
                len as usize,
            );
        }

        self.ts = delta.get_meta_data(delta::TID);
    }
    pub fn next(&mut self, table: &Table) -> bool {
        if self.next == 0 {
            return false;
        }
        #[cfg(not(feature = "append"))]
        {
            match TupleDelta::reload(self.next) {
                Ok(delta) => {
                    self.next = delta.get_meta_data(delta::NEXT_DELTA_ADDRESS);

                    self.delta(&delta);
                }
                _ => {
                    return false;
                }
            }
        }
        #[cfg(feature = "append")]
        {
            let tuple = Tuple::reload(self.next);
            self.next = tuple.next_address();
            self.data = tuple.data.to_vec_len(self.data.len());
            self.ts = tuple.ts();
        }
        // #[cfg(not(feature = "buffer_pool"))]
        // {
        //     #[cfg(not(feature = "append"))]
        //     {
        //         let delta = TupleDelta::reload(self.next).unwrap();
        //         self.next = delta.get_meta_data(delta::NEXT_DELTA_ADDRESS);
        //         self.delta(&delta);
        //     }
        //     #[cfg(feature = "append")]
        //     {
        //         let tuple = Tuple::reload(self.next);
        //         self.next = tuple.next_address();
        //         self.data = tuple.data.to_vec_len(self.data.len());
        //         self.ts = tuple.ts();
        //     }
        // }

        // #[cfg(feature = "buffer_pool")]
        // {
        //     match table
        //         .buffer_pool
        //         .get(self.next as usize)
        //         {
        //             Some(item) => {
        //                 let buffer = item.data
        //                 .read();
        //             self.ts = buffer.ts();
        //             self.next = buffer.next_address();
        //             self.data = buffer.data.iter().copied().collect();
        //             }
        //             _ => {
        //                 println!("{}, {}", self.ts, self.next);
        //             }
        //         }

        // }
        true
    }
    pub fn get_column_by_id(&self, schema: &TableSchema, id: usize) -> &[u8] {
        let range = schema.get_column_offset(id);
        &self.data[range]
    }
    pub fn _to_vec(&self) -> Vec<u8> {
        self.data[TUPLE_HEADER..].into()
    }
    pub fn _next(&self) -> u64 {
        self.next
    }
    pub fn get_ts(&self) -> TimeStamp {
        self.ts
    }
}

#[derive(Debug)]
pub struct TupleColumnVec {
    pub next: u64,
    pub ts: TimeStamp,
    pub offset: Range<usize>,
    pub data: Vec<u8>,
}

impl TupleColumnVec {
    pub fn new(tuple: &Tuple, range: Range<usize>) -> Self {
        TupleColumnVec {
            ts: tuple.ts(),
            next: tuple.next_address(),
            offset: range.clone(),
            data: tuple.get_data_by_column(range).to_vec(),
        }
    }
    pub fn from_buffer(buffer: &BufferDataVec, range: Range<usize>) -> Self {
        TupleColumnVec {
            ts: buffer.ts(),
            next: buffer.next_address(),
            offset: range.clone(),
            data: buffer.get_data_by_column(range).to_vec(),
        }
    }
    pub fn ts(&self) -> TimeStamp {
        self.ts.clone()
    }
    fn delta(&mut self, delta: &TupleDelta) {
        let index: u32 = delta.get_meta_data(delta::DELTA_COLUMN_OFFSET);
        let len = delta.data_len();
        let delta_address = delta._data_address();
        if index as usize == self.offset.start {
            unsafe {
                ptr::copy(
                    delta_address as *const u8,
                    self.data.as_mut_ptr(),
                    len as usize,
                );
            }
        }

        self.ts = delta.get_meta_data(delta::TID);
    }
    pub fn next(&mut self, table: &Table) -> bool {
        if self.next == 0 {
            return false;
        }
        #[cfg(not(feature = "append"))]
        {
            // println!("next tv2 {}", self.next);

            match TupleDelta::reload(self.next) {
                Ok(delta) => {
                    self.next = delta.get_meta_data(delta::NEXT_DELTA_ADDRESS);
                    self.delta(&delta);
                }
                _ => {
                    return false;
                }
            }
        }
        #[cfg(feature = "append")]
        {
            let tuple = Tuple::reload(self.next);
            self.next = tuple.next_address();
            self.data = tuple.get_data_by_column(self.offset.clone()).to_vec();
            self.ts = tuple.ts();
        }

        // #[cfg(feature = "buffer_pool")]
        // {
        //     let buffer = table
        //         .buffer_pool
        //         .get(self.next as usize)
        //         .unwrap()
        //         .data
        //         .read();
        //     self.ts = buffer.ts();
        //     self.next = buffer.next_address();
        //     self.data = buffer.get_data_by_column(self.offset.clone()).to_vec();
        // }
        true
    }
    pub fn get_column_by_id(&self, schema: &TableSchema, id: usize) -> &[u8] {
        let range = schema.get_column_offset(id);
        &self.data[range]
    }
    pub fn _next(&self) -> u64 {
        self.next
    }
    pub fn get_ts(&self) -> TimeStamp {
        self.ts
    }
}

use bitvec::access::BitSafe;
use bytes::Buf;
use parking_lot::RwLock;
use std::convert::TryInto;
use thiserror::Error;

use super::catalog::Catalog;
use super::catalog::CATALOG;
use super::table::Table;

#[derive(Debug, Error)]
pub enum TupleError {
    #[error("Can't parse int64")]
    ParseIntError(#[from] core::num::ParseIntError),

    #[error("Can't parse double")]
    ParseDoubleError(#[from] core::num::ParseFloatError),

    #[error("There is no next tuple")]
    NoNextTuple,

    #[error("tuple changed after delta gen")]
    TupleChanged { conflict_tid: u64 },

    #[error("tuple don't exist")]
    TupleNotExists,

    #[error("column not exists")]
    ColumnNotExists,

    #[error("txn need to abort")]
    PreValidationFailed,

    #[error("need to a new page")]
    NoSpace,

    #[error("primary key type not supported")]
    IndexTypeNotSupported,

    #[error("index have not built")]
    IndexNotBuilt,

    #[error("index key not matched")]
    KeyNotMatched,

    #[error("AcquireReadLockFalse")]
    AcquireReadLockFalse,

    #[error("AcquireWriteLockFalse")]
    AcquireWriteLockFalse,
}

#[derive(Debug)]
pub struct BufferDataVec {
    data: Vec<u8>,
}
impl BufferDataVec {
    pub fn new(tuple_size: usize) -> Self {
        BufferDataVec {
            data: vec![0; tuple_size],
        }
    }
    pub fn save(&mut self, offset: u64, data: &[u8]) {
        let start = offset as usize + TUPLE_HEADER;
        self.data[start..].copy_from_slice(data);
    }
    pub fn save_u64(&mut self, offset: u64, data: u64) {
        self.save(offset, &data.to_le_bytes());
    }
    pub fn _address(&self) -> *const u8 {
        self.data.as_ptr()
    }
    pub fn get_meta_data<T: Copy + Display>(&self, parameter: Range<u64>) -> T {
        unsafe {
            // println!("get meta data {}", parameter.start);

            let value = ptr::read(self._address().add(parameter.start as usize) as *const T);

            // println!("get meta data ok");
            value
        }
    }
    pub fn set_meta_data<T: Display>(&self, parameter: Range<u64>, value: T) {
        unsafe {
            // println!("get meta data {}", parameter.start);
            ptr::write(
                self._address().add(parameter.start as usize) as *mut T,
                value,
            );
            // println!("save meta data ok");
            // file::sfence();
        };
    }

    pub fn next_address(&self) -> Address {
        self.get_meta_data(NEXT_DELTA_ADDRESS)
    }

    pub fn ts(&self) -> TimeStamp {
        self.get_meta_data(TID)
    }
    pub fn lock_tid(&self) -> u64 {
        self.get_meta_data(LOCK_TID)
    }
    pub fn delete_flag(&self) -> u64 {
        self.get_meta_data(DELETE_FLAG)
    }

    pub fn set_next(&self, next_address: Address) -> bool {
        self.set_meta_data(NEXT_DELTA_ADDRESS, next_address);
        true
    }
    pub fn set_ts(&self, ts: TimeStamp) -> bool {
        // println!("update ts {} to {}", self.ts().tid, ts.tid);
        self.set_meta_data(TID, ts);
        true
    }
    pub fn set_ts_tid(&self, tid: u64) -> bool {
        // debug!("tuple update ts1 {} {}", self._address(), tid);
        self.set_meta_data(TID, tid);
        true
    }
    pub fn set_lock_tid(&self, ts: u64) -> bool {
        self.set_meta_data(LOCK_TID, ts);
        true
    }
    pub fn cas_lock_tid(&self, old_ts: u64, new_ts: u64) -> u64 {
        let ts = unsafe { &*(self._address().add(LOCK_TID.start as usize) as *const AtomicU64) };
        let result = ts.compare_exchange(old_ts, new_ts, Ordering::Relaxed, Ordering::Relaxed);
        match result {
            Ok(u) => return u,
            Err(u) => return u,
        }
    }

    #[cfg(feature = "read_ts")]
    pub fn set_read_ts(&self, read_ts: u64) {
        let ts = unsafe {
            &*(self._address().add((TID.start + TS_READ_TS.start) as usize) as *const AtomicU64)
        };
        loop {
            let old_ts = ts.load(Ordering::Relaxed);
            if old_ts >= read_ts {
                break;
            }
            if ts
                .compare_exchange(old_ts, read_ts, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
    pub fn set_ts_and_next(
        &self,
        old_ts: TimeStamp,
        new_ts: TimeStamp,
        old_next_address: Address,
        new_next_address: Address,
    ) -> u64 {
        self.set_next(new_next_address);
        self.set_ts_tid(new_ts.tid);
        // sfence();
        return old_ts.tid;
    }

    pub fn get_data_by_column(&self, column_range: Range<usize>) -> &[u8] {
        &self.data[column_range]
    }
    pub fn update_data_by_column(&mut self, column_start: u64, new_data: &[u8]) {
        self.data[(column_start as usize)..(column_start as usize + new_data.len())]
            .copy_from_slice(new_data);
    }

    pub fn read(&self, len: usize) -> TupleVec {
        TupleVec::from_buffer(self)
    }
    pub fn column_read(&self, range: Range<usize>) -> TupleColumnVec {
        TupleColumnVec::from_buffer(self, range)
    }
    pub fn _to_vec(&self) -> Vec<u8> {
        self.data.to_vec()
    }

    pub fn apply_next(&mut self) -> bool {
        let next_address = self.next_address();
        let ts = self.ts();
        if next_address == 0 {
            return false;
        }

        let delta = TupleDelta::reload(next_address).unwrap();

        let index = delta.get_meta_data(delta::DELTA_COLUMN_OFFSET);

        self.update_data_by_column(index, delta.data());

        self.set_ts_and_next(
            ts,
            delta.get_meta_data(delta::TID),
            next_address,
            delta.get_meta_data(delta::NEXT_DELTA_ADDRESS),
        );

        true
    }
    pub fn copy_from_nvm(&mut self, tuple: &Tuple, tuple_len: usize) {
        // for d in tuple.data.to_vec_len(tuple_len) {
        //     print!("{} ", d)
        // }
        // println!();

        self.data[0..tuple_len].copy_from_slice(tuple.data.to_vec_len(tuple_len).as_slice());
        // println!("load tuple with ts {}", self.ts().tid);

        self.set_lock_tid(0);
        self.set_next(0);
        self.set_ts(TimeStamp::default());
    }
    pub fn get_data(&self, size: u64) -> &[u8] {
        &self.data[0..size as usize]
    }
    pub fn copy_from_buffer(&mut self, ts: TimeStamp, buffer: &BufferVec, size: usize) {
        let tuple = buffer.data.read();
        #[cfg(not(feature = "cc_cfg_2pl"))]
        tuple.set_lock_tid(ts.tid);
        self.data[0..size].copy_from_slice(tuple.get_data(size as u64));
    }
    #[cfg(all(feature = "cc_cfg_2pl", feature = "read_ts"))]
    pub fn lock_write(&self, txn_id: u64, counter: u64) -> Result<(), TupleError> {
        let address: u64 =
            unsafe { (self._address().add((TID.start + TS_READ_TS.start) as usize)) as u64 };
        let ts = unsafe { &*(address as *const AtomicU64) };
        if ts
            .compare_exchange(
                counter,
                txn_id | POW_2_63,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            // println!("acquire lock by {}", txn_id);
            return Ok(());
        }
        if ts.load(Ordering::Relaxed) == (txn_id | POW_2_63) {
            return Ok(());
        }

        // let tid = ts.load(Ordering::Relaxed);
        // println!("{:x} {:x}", self._address(), tid);

        return Err(TupleError::AcquireWriteLockFalse);
    }
    #[cfg(all(feature = "cc_cfg_2pl", feature = "read_ts"))]
    pub fn lock_read(&self, reader: u64) -> Result<(), TupleError> {
        let address: u64 =
            unsafe { (self._address().add((TID.start + TS_READ_TS.start) as usize)) as u64 };
        let ts = unsafe { &*(address as *const AtomicU64) };

        let tid = ts.fetch_add(1, Ordering::SeqCst);

        if tid & POW_2_63 > 0 {
            if tid == reader {
                return Ok(());
            }
            let tid = ts.fetch_sub(1, Ordering::SeqCst);

            return Err(TupleError::AcquireReadLockFalse);
        }
        Ok(())
    }
    #[cfg(all(feature = "cc_cfg_2pl", feature = "read_ts"))]
    pub fn release_read_lock(&self) {
        let address: u64 =
            unsafe { (self._address().add((TID.start + TS_READ_TS.start) as usize)) as u64 };
        let ts = unsafe { &*(address as *const AtomicU64) };
        let tid = ts.load(Ordering::Relaxed);
        if tid < POW_2_63 && tid > 0 {
            //may be write by itself
            ts.fetch_sub(1, Ordering::SeqCst);
        }
        // println!("release read lock");
    }
    #[cfg(all(feature = "cc_cfg_2pl", feature = "read_ts"))]
    pub fn release_write_lock(&self) {
        // println!("{:x} release write_lock", self._address());
        let address: u64 =
            unsafe { (self._address().add((TID.start + TS_READ_TS.start) as usize)) as u64 };
        let ts = unsafe { &*(address as *const AtomicU64) };
        ts.store(0, Ordering::SeqCst);
    }
}
#[derive(Debug)]
pub struct BufferVec {
    pub clock: AtomicU64, // clock + active + copy
    pub nvm_id: AtomicU64,
    pub data: RwLock<BufferDataVec>, //dirty
}
impl BufferVec {
    pub fn new(tuple_size: usize) -> Self {
        BufferVec {
            clock: AtomicU64::new(0),
            nvm_id: AtomicU64::new(0),
            data: RwLock::new(BufferDataVec::new(tuple_size)),
        }
    }
    pub fn replace(&self, cur_min_txn: u64) -> bool {
        let c = self.clock.load(Ordering::Relaxed);
        // println!("{:X}", c);
        if c > cur_min_txn {
            return false;
        }
        if self.data.try_write().is_none() {
            return false;
        }
        if self
            .clock
            .compare_exchange(c, POW_2_63, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        // if self.active.load(Ordering::Relaxed) {
        //     return false;
        // }
        true
    }
}
