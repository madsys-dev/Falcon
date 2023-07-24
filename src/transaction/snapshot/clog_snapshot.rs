use crate::storage::timestamp::TimeStamp;
use crate::transaction::TxStatus;
use crate::{storage::catalog::Catalog, transaction::clog::Clog};
use std::sync::atomic::{AtomicU64, Ordering};
const ABORT: u64 = 1;
#[derive(Debug)]
pub struct ClogMvccSnapShot {
    clock: AtomicU64,
}
impl ClogMvccSnapShot {
    pub fn new() -> Self {
        ClogMvccSnapShot {
            clock: AtomicU64::new(1),
        }
    }
    pub fn new_txn(&self, _: u64) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
        // 10
    }
    pub fn reload_clock(&self, new_clock: u64) {
        self.clock.store(new_clock, Ordering::Relaxed)
    }
    pub fn finish_txn(&self, txn_id: TimeStamp, commited: bool) {
        let clog = Catalog::global().get_clog();
        if commited {
            clog.save(txn_id, self.clock.fetch_add(1, Ordering::Relaxed));
        } else {
            clog.save(txn_id, ABORT);
        }
    }
    pub fn is_finished(txn_id: TimeStamp, clog: &Clog) -> bool {
        let stat = clog.get(txn_id);
        if stat == ABORT || stat > txn_id.tid {
            return true;
        }
        false
    }
    pub fn get_snapshot(&self, snapshot: &mut ClogMvccSnapShotEntity) {
        snapshot.clock = self.clock.load(Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct ClogMvccSnapShotEntity {
    pub clock: u64,
}
impl ClogMvccSnapShotEntity {
    pub fn new() -> Self {
        ClogMvccSnapShotEntity { clock: 0 }
    }
    pub fn access(&self, tuple_ts: TimeStamp, txn_ts: u64, cur_min_txn: u64) -> bool {
        // println!("{}, {}", tuple_ts.tid, txn_ts);
        if tuple_ts.tid == txn_ts {
            return true;
        }
        if tuple_ts.tid < cur_min_txn {
            return true;
        }
        let clog = Catalog::global().get_clog();
        let ts = clog.get(tuple_ts);
        if ts == 0 {
            return false;
        }
        if ts == ABORT {
            return true;
        }
        ts <= self.clock
    }
}
