use std::mem::size_of;

use crate::storage::row::TUPLE_HEADER;

pub const NVM_ADDR: u64 = 0x1_000_000_000;
pub const CATALOG_ADDRESS: u64 = NVM_ADDR + PAGE_SIZE;
// pub const CLWB_SIZE: u64 = 64;
#[cfg(feature = "native")]
pub const MAX_PAGE_COUNT: u64 = 16000;
#[cfg(feature = "native")]
pub const PAGE_SIZE: u64 = 0x20000; // 800000
#[cfg(feature = "nvm_server")]
pub const MAX_PAGE_COUNT: u64 = 400000;
#[cfg(feature = "nvm_server")]
pub const PAGE_SIZE: u64 = 0x200000; // 800000
pub const U64_OFFSET: u64 = 8;
pub const USIZE_OFFSET: u64 = size_of::<usize>() as u64;
pub type Address = u64;

#[cfg(not(feature = "tpcc"))]
pub const TUPLE_SIZE: usize = 1056;
#[cfg(feature = "tpcc")]
pub const TUPLE_SIZE: usize = 512;

pub const POOL_SIZE: usize = 1 * 1024 * 1024;
pub const POOL_PERC: usize = 4;
pub const ADDRESS_MASK: u64 = (1u64 << 48) - 1;
pub const POW_2_63: u64 = 1u64 << 63;

// It's better if TPCC_WAREHOUSE is not less than THREAD_COUNT.
pub const TPCC_WAREHOUSE: u64 = 2048;
pub const YCSB_TOTAL: u64 = 256 * 1024 * 1024;
pub const NVM_FILE_PATH: &str = "/mnt/pmem0/jzc/_test_persist";
