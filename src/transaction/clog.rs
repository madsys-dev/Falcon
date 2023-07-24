use crate::config::Address;
use crate::config::PAGE_SIZE;
use crate::config::U64_OFFSET;
use crate::storage::timestamp::TimeStamp;
use crossbeam::queue::SegQueue;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

const MAX_TXN_COUNT_PER_PAGE: u64 = PAGE_SIZE / U64_OFFSET;
const MAX_CLOG_SIZE: u64 = 32;

#[derive(Debug)]
pub struct ClogPage {
    page: Vec<AtomicU64>,
}
impl ClogPage {
    pub fn new() -> Self {
        ClogPage {
            page: std::iter::from_fn(|| Some(AtomicU64::new(0)))
                .take(MAX_TXN_COUNT_PER_PAGE as usize)
                .collect(),
        }
    }
    fn set(&self, tid: u64, status: u64) {
        self.page
            .get(tid as usize)
            .unwrap()
            .store(status, Ordering::Relaxed);
    }
    fn get(&self, tid: u64) -> u64 {
        self.page.get(tid as usize).unwrap().load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct Clog {
    page: ClogPage, //Vec<ClogPage>,
}

impl Clog {
    pub fn new() -> Clog {
        Clog {
            // pages: std::iter::from_fn(|| Some(ClogPage::new()))
            //     .take(MAX_CLOG_SIZE as usize)
            //     .collect(),
            page: ClogPage::new(),
        }
    }
    pub fn get(&self, ts: TimeStamp) -> u64 {
        // let page_id = 0;//(ts.tid / MAX_TXN_COUNT_PER_PAGE) % MAX_CLOG_SIZE;
        let page_offset = ts.tid % MAX_TXN_COUNT_PER_PAGE;
        // let page = self.pages.get(page_id as usize).unwrap();
        self.page.get(page_offset)
    }
    pub fn save(&self, ts: TimeStamp, status: u64) {
        // let page_id = 0;//(ts.tid / MAX_TXN_COUNT_PER_PAGE) % MAX_CLOG_SIZE;
        let page_offset = ts.tid % MAX_TXN_COUNT_PER_PAGE;
        // println!("{}", page_id);
        // let page = self.pages.get(page_id as usize).unwrap();
        self.page.set(page_offset, status);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NVM_ADDR;
    use crate::storage::catalog::Catalog;
    use crate::storage::nvm_file::NVMTableStorage;

    #[test]
    fn test_clog_rw() {
        let clog = Clog::new();
        let mut ts0 = TimeStamp::default();
        let mut ts1 = TimeStamp::default();

        ts0.tid = 0;
        ts1.tid = 1;

        clog.save(ts0, 1);
        clog.save(ts1, 0);
        assert_eq!(clog.get(ts0), 1);
        assert_eq!(clog.get(ts1), 0);

        // let clog_reload = Clog::reload(page_id.page_start);
        // assert_eq!(clog_reload.get(ts0), 1);
        // assert_eq!(clog_reload.get(ts1), 0);
    }

    #[test]
    #[cfg(feature = "cc_cfg_occ")]
    fn test_clog_in_catalog() {
        NVMTableStorage::init_test_database();
        let catalog = Catalog::new(NVM_ADDR);
        let clog = catalog.get_clog();
        let mut ts0 = TimeStamp::default();
        let mut ts1 = TimeStamp::default();

        ts0.tid = 0;
        ts1.tid = 1;
        clog.save(ts0, 1);
        clog.save(ts1, 0);
        assert_eq!(clog.get(ts0), 1);
        assert_eq!(clog.get(ts1), 0);
    }
}
