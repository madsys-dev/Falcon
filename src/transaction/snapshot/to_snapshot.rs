use crate::config::POW_2_63;
use crate::storage::row::TupleError;
use crate::storage::timestamp::TimeStamp;
use crate::transaction::TxStatus;
use crate::{storage::catalog::Catalog, transaction::clog::Clog};
use chrono::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};

const ABORT: u64 = 1;
#[derive(Debug)]
pub struct ToSnapShot {
    clock: AtomicU64,
}
// use real clock instead of share counter for better scalibility
impl ToSnapShot {
    pub fn new() -> Self {
        ToSnapShot {
            clock: AtomicU64::new(2),
        }
    }
    pub fn new_txn(&self, thread_id: u64) -> u64 {
        // self.clock.fetch_add(1, Ordering::Relaxed)
        ((Local::now().timestamp_nanos() as u64) << 6) | thread_id
    }
    pub fn reload_clock(&self, new_clock: u64) {
        self.clock.store(new_clock, Ordering::Relaxed)
    }

    pub fn finish_txn(&self, _: TimeStamp, _: bool) {
        // self.clock.fetch_add(1, Ordering::Relaxed);
    }
    pub fn is_finished(txn_id: TimeStamp, clog: &Clog) -> bool {
        let stat = clog.get(txn_id);
        if stat == ABORT || stat > txn_id.tid {
            return true;
        }
        false
    }
    pub fn get_snapshot(&self, snapshot: &mut ToSnapShotEntity) {
        snapshot.clock = (Local::now().timestamp_nanos() as u64) << 6;
        // snapshot.clock =self.clock.load(Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct ToSnapShotEntity {
    pub clock: u64,
}
impl ToSnapShotEntity {
    pub fn new() -> Self {
        ToSnapShotEntity { clock: 0 }
    }
    pub fn access(&self, tuple_ts: TimeStamp, txn_id: u64, _: u64) -> bool {
        if tuple_ts.tid == txn_id || tuple_ts.tid == txn_id | POW_2_63 {
            return true;
        }
        tuple_ts.tid <= self.clock
    }
}
