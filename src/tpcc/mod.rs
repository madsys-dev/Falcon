pub mod tpcc;
pub mod tpcc_index;
pub mod tpcc_init;
pub mod tpcc_isolation;
pub mod tpcc_query;
pub mod tpcc_test;
pub mod tpcc_txn_asycn;
pub mod tpcc_txn_sycn;
use crate::{mvcc_config::TRANSACTION_COUNT, tpcc::tpcc::IS_FULL_SCHEMA};
use once_cell::sync::OnceCell;
use rand::{prelude::ThreadRng, *};

/// [left, right]

pub fn u64_rand(rng: &mut ThreadRng, lower_bound: u64, upper_bound: u64) -> u64 {
    // let mut rng = rand::thread_rng();
    rng.gen_range(lower_bound, upper_bound + 1)
}
pub fn f64_rand(rng: &mut ThreadRng, lower_bound: f64, upper_bound: f64, precision: f64) -> f64 {
    // let mut rng = rand::thread_rng();
    rng.gen_range(
        (lower_bound / precision) as u64,
        (upper_bound / precision) as u64 + 1,
    ) as f64
        * precision
}
pub fn char_rand_new(rng: &mut ThreadRng) -> char {
    u64_rand(rng, 48, 122) as u8 as char
}

pub fn string_rand(rng: &mut ThreadRng, len_lower_bound: u64, len_upper_bound: u64) -> String {
    let len = u64_rand(rng, len_lower_bound, len_upper_bound);
    let mut s = String::new();
    s.reserve(len as usize);
    for _ in 0..len {
        s.push(char_rand_new(rng));
    }
    s
}
pub fn bool_rand(rng: &mut ThreadRng, percentage: u64) -> bool {
    rng.gen_range(0, 100) < percentage
}

pub fn num_char_rand(rng: &mut ThreadRng) -> char {
    u64_rand(rng, '0' as u64, '9' as u64) as u8 as char
}

pub fn zip_rand(rng: &mut ThreadRng) -> String {
    let mut s = String::new();
    s.reserve(4);

    for _ in 0..4 {
        s.push(num_char_rand(rng));
    }
    s + "11111"
}
pub fn data_rand(rng: &mut ThreadRng) -> String {
    let mut s = string_rand(rng, 26, 50);

    if bool_rand(rng, 10) {
        let pos = u64_rand(rng, 0, s.len() as u64 - 8) as usize;
        s = String::from(&s[0..pos]) + "ORIGINAL" + &s[pos + 8..s.len()];
    }
    s
}
pub fn credit_rand(rng: &mut ThreadRng) -> String {
    if bool_rand(rng, 10) {
        String::from("BC")
    } else {
        String::from("GC")
    }
}
pub fn nurand(rng: &mut ThreadRng, a: u64, x: u64, y: u64) -> u64 {
    static mut C255: OnceCell<u64> = OnceCell::new();
    static mut C1023: OnceCell<u64> = OnceCell::new();
    static mut C8191: OnceCell<u64> = OnceCell::new();
    let c: &u64;
    unsafe {
        if a == 255 {
            c = C255.get_or_init(|| u64_rand(rng, 0, a));
        } else if a == 1023 {
            c = C1023.get_or_init(|| u64_rand(rng, 0, a));
        } else if a == 8191 {
            c = C8191.get_or_init(|| u64_rand(rng, 0, a));
        } else {
            c = &0;
            assert!(false);
        }
    }
    (((u64_rand(rng, 0, a) | u64_rand(rng, x, y)) + c) % (y - x + 1)) + x
}
pub fn permutation_rand(rng: &mut ThreadRng, lower_bound: u64, upper_bound: u64) -> Vec<u64> {
    let mut v = Vec::new();
    v.reserve((upper_bound - lower_bound + 1) as usize);

    for i in lower_bound..upper_bound + 1 {
        v.push(i);
    }
    let len = v.len();
    for i in 1..len {
        let pos = u64_rand(rng, 0, i as u64) as usize;
        let t = v[i];
        v[i] = v[pos];
        v[pos] = t;
    }
    v
}
pub const LAST_NAMES: [&str; 10] = [
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
];
pub fn lastname(num: u64) -> String {
    let mut s = String::new();
    s.reserve(16);
    s.push_str(LAST_NAMES[(num / 100 % 10) as usize]);
    s.push_str(LAST_NAMES[(num / 10 % 10) as usize]);
    s.push_str(LAST_NAMES[(num % 10) as usize]);
    s
}
pub fn append_u64(s: &mut String, u: u64) {
    if !s.is_empty() {
        s.push(',');
    }
    s.push_str(&u.to_string());
}
pub fn append_f64(s: &mut String, f: f64) {
    if !s.is_empty() {
        s.push(',');
    }
    s.push_str(&f.to_string());
}
pub fn append_string(s: &mut String, t: String) {
    if !s.is_empty() {
        s.push(',');
    }
    s.push_str(&t);
}
#[cfg(feature = "nvm_server")]
pub const WAREHOUSES: u64 = crate::config::TPCC_WAREHOUSE;
#[cfg(feature = "native")]
pub const WAREHOUSES: u64 = 8 as u64;
#[cfg(feature = "nvm_server")]
pub const ITEMS: u64 = 100000;
#[cfg(feature = "native")]
pub const ITEMS: u64 = 100;
pub const STOCKS_PER_WAREHOUSE: u64 = ITEMS;
pub const DISTRICTS_PER_WAREHOUSE: u64 = 10;
pub const CUSTOMERS_PER_DISTRICT: u64 = 3000;
pub const HISTORIES_PER_CUSTOMER: u64 = 1;
pub const ORDERS_PER_DISTRICT: u64 = CUSTOMERS_PER_DISTRICT;
pub const MAX_LINES_PER_ORDER: u64 = 15;
pub fn item_key(iid: u64) -> u64 {
    iid
}
pub fn warehouse_key(wid: u64) -> u64 {
    wid
}
pub fn district_key(wid: u64, did: u64) -> u64 {
    wid * DISTRICTS_PER_WAREHOUSE + did
}
pub fn customer_key(wid: u64, did: u64, cid: u64) -> u64 {
    district_key(wid, did) * CUSTOMERS_PER_DISTRICT + cid
}
pub fn cid_from_key(c_key: u64) -> u64 {
    c_key % CUSTOMERS_PER_DISTRICT
}
pub fn customer_last_key(c_last: String, wid: u64, did: u64) -> String {
    let mut C_LAST_LEN: usize = 32;
    // if IS_FULL_SCHEMA {C_LAST_LEN = 16};

    let mut key = format!("{}{}{}", c_last, wid, did);
    if key.len() < C_LAST_LEN {
        key.reserve(C_LAST_LEN - key.len());
    }
    // key.push(' ');
    // key.push_str(&wid.to_string());
    // key.push(' ');
    // key.push_str(&did.to_string());
    while key.len() < C_LAST_LEN {
        key.push('\0');
    }

    key
}
pub fn order_key(wid: u64, did: u64, oid: u64) -> u64 {
    district_key(wid, did) * ORDERS_PER_DISTRICT + oid
}
pub fn new_order_key(wid: u64, did: u64, oid: u64) -> u64 {
    order_key(wid, did, oid)
}
pub fn order_line_key(wid: u64, did: u64, oid: u64, olid: u64) -> u64 {
    order_key(wid, did, oid) * MAX_LINES_PER_ORDER + olid
}
pub fn stock_key(wid: u64, iid: u64) -> u64 {
    wid * STOCKS_PER_WAREHOUSE + iid
}
