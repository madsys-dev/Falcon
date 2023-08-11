pub mod ycsb_query;
pub mod ycsb_test;
pub mod ycsb_txn;

use std::collections::HashMap;

use rand::prelude::ThreadRng;
use rand::Rng;

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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Operation {
    Read,
    Update,
    Insert,
    Scan,
    Delete,
}

#[derive(Debug, Clone, Copy)]
pub struct YCSBWorkload {
    pub read_perc: f64,
    pub write_perc: f64,
    pub scan_perc: f64,
    pub scan_len: u64,
    pub insert_perc: f64,
    #[cfg(feature = "ycsb_mvcc")]
    pub ro_perc: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct Properties {
    pub init_parallelism: u64,
    pub table_size: u64,
    pub zipf_theta: f64,
    pub workload: YCSBWorkload,
    pub part_per_txn: u64,
    pub perc_multi_part: u64,
    pub req_per_query: usize,
    pub field_per_tuple: u64,
    pub virtual_part_cnt: u64,
    pub first_part_local: bool,
    pub max_key_length: usize,
    pub max_field_length: usize,
}

impl Properties {
    pub fn default() -> Self {
        let mut prop = Properties {
            init_parallelism: 1,
            #[cfg(feature = "nvm_server")]
            table_size: crate::customer_config::YCSB_TOTAL,
            #[cfg(feature = "native")]
            table_size: 100000,
            zipf_theta: 0.6,
            #[cfg(feature = "ycsb_wh")]
            workload: YCSBWorkload {
                read_perc: 0.1,
                write_perc: 0.9,
                scan_perc: 0.0,
                scan_len: 20,
                insert_perc: 0.0,
                #[cfg(feature = "ycsb_mvcc")]
                ro_perc: 0.0,
            },
            #[cfg(feature = "ycsb_a")]
            workload: YCSBWorkload {
                read_perc: 0.5,
                write_perc: 0.5,
                scan_perc: 0.0,
                scan_len: 20,
                insert_perc: 0.0,
                #[cfg(feature = "ycsb_mvcc")]
                ro_perc: 0.0,
            },
            #[cfg(feature = "ycsb_b")]
            workload: YCSBWorkload {
                read_perc: 0.95,
                write_perc: 0.05,
                scan_perc: 0.0,
                scan_len: 20,
                insert_perc: 0.0,
            },
            #[cfg(feature = "ycsb_c")]
            workload: YCSBWorkload {
                read_perc: 1.0,
                write_perc: 0.0,
                scan_perc: 0.0,
                scan_len: 20,
                insert_perc: 0.0,
            },
            #[cfg(feature = "ycsb_d")]
            workload: YCSBWorkload {
                read_perc: 0.95,
                write_perc: 0.0,
                scan_perc: 0.0,
                scan_len: 100,
                insert_perc: 0.05,
            },
            #[cfg(feature = "ycsb_e")]
            workload: YCSBWorkload {
                read_perc: 0.0,
                write_perc: 0.0,
                scan_perc: 0.95,
                scan_len: 100,
                insert_perc: 0.05,
            },
            #[cfg(feature = "ycsb_f")]
            workload: YCSBWorkload {
                read_perc: 0.5,
                write_perc: 0.5,
                scan_perc: 0.0,
                scan_len: 20,
                insert_perc: 0.0,
                #[cfg(feature = "ycsb_mvcc")]
                ro_perc: 0.0,
            },

            part_per_txn: 1,
            perc_multi_part: 1,
            req_per_query: 1,
            #[cfg(all(feature = "align", feature = "a256"))]
            field_per_tuple: 1,
            #[cfg(all(feature = "align", feature = "a512"))]
            field_per_tuple: 1,
            #[cfg(all(feature = "align", feature = "a64"))]
            field_per_tuple: 4,
            #[cfg(all(feature = "align", feature = "a128"))]
            field_per_tuple: 2,
            #[cfg(not(feature = "align"))]
            field_per_tuple: 1,
            virtual_part_cnt: 1,
            first_part_local: true,
            max_key_length: 8,

            #[cfg(all(feature = "align", feature = "a256"))]
            max_field_length: 256,
            #[cfg(all(feature = "align", feature = "a512"))]
            max_field_length: 512,
            #[cfg(all(feature = "align", feature = "a64"))]
            max_field_length: 64,
            #[cfg(all(feature = "align", feature = "a128"))]
            max_field_length: 128,
            #[cfg(all(not(feature = "align"), feature = "nvm_server"))]
            max_field_length: 1000,
            #[cfg(feature = "native")]
            max_field_length: 10,
        };
        prop
    }
    pub fn zeta(n: u64, theta: f64) -> f64 {
        let mut sum: f64 = 0.0;
        for i in 1..(n + 1) {
            sum += f64::powf(1.0 / (i as f64), theta);
        }
        return sum;
    }
}

pub fn u64_rand_new(rng: &mut ThreadRng, lower_bound: u64, upper_bound: u64) -> u64 {
    // let mut rng = rand::thread_rng();
    rng.gen_range(lower_bound, upper_bound + 1)
}
pub fn f64_rand_new(
    rng: &mut ThreadRng,
    lower_bound: f64,
    upper_bound: f64,
    precision: f64,
) -> f64 {
    // let mut rng = rand::thread_rng();
    rng.gen_range(
        (lower_bound / precision) as u64,
        (upper_bound / precision) as u64 + 1,
    ) as f64
        * precision
}
pub fn char_rand_new(rng: &mut ThreadRng) -> char {
    u64_rand_new(rng, 48, 122) as u8 as char
}

pub fn string_rand(rng: &mut ThreadRng, len_lower_bound: u64, len_upper_bound: u64) -> String {
    let len = u64_rand_new(rng, len_lower_bound, len_upper_bound);
    let mut s = String::new();
    s.reserve(len as usize);
    for _ in 0..len {
        s.push(char_rand_new(rng));
    }
    s
}
