#![deny(unused_must_use)]
#![feature(asm)]
#![feature(core_intrinsics)]
#![feature(stdsimd)]
#![feature(stmt_expr_attributes)]
#![feature(option_result_unwrap_unchecked)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate libc;

use jemallocator::Jemalloc;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(any(feature = "dash", feature = "nbtree"))]
pub mod c;
pub mod config;
pub mod customer_config;
pub mod mvcc_config;
pub mod storage;
pub mod tpcc;
pub mod transaction;
pub mod utils;
pub mod ycsb;

use thiserror::Error;

/// The error type which is returned from this crate.
#[derive(Debug, Error)]
pub enum Error {
    #[error("no space")]
    NoSpace,
    #[error("entry not found")]
    NotFound,
    #[error("already exist")]
    Exist,
    #[error("transaction needs abort")]
    TxNeedAbort,
    #[error("transaction conflict")]
    TxConflict,
    #[error("io error")]
    IO(#[from] std::io::Error),
    #[error("tuple error")]
    Tuple(#[from] crate::storage::row::TupleError),
}

/// A specialized `Result` type for this crate.
pub type Result<T = ()> = std::result::Result<T, Error>;

// #[macro_use]
// extern crate lazy_static;
// use std::collections::HashMap;
// use std::sync::RwLock;

// lazy_static! {
//     static ref TupleRwMutex: HashMap<u64, RwLock<u8>> = HashMap::new();
// }
