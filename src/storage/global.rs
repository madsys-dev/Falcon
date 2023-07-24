use std::time::{Duration, Instant, SystemTime};

use once_cell::sync::OnceCell;

use crate::{mvcc_config::THREAD_COUNT, tpcc::u64_rand};

pub const BUFFER_SWAP: usize = 0;
pub const BUFFER: usize = 0;
pub const BUFFER_HIT: usize = 1;
pub const READING: usize = 2;
pub const UPDATING: usize = 3;

const SAMPLE_SIZE: usize = 10000;
const CLOCK_COUNT: usize = 4;
const COUNTER_COUNT: usize = 4;
const TAMPLE: usize = 100;
#[derive(Clone, Copy, Debug)]
pub struct Timer {
    pub clock: [u128; CLOCK_COUNT],
    pub counter: [u64; COUNTER_COUNT],
    pub start: [Instant; CLOCK_COUNT],
    pub sample: [u128; SAMPLE_SIZE],
    pub tmp: [u128; TAMPLE],
}

impl Timer {
    pub fn new() -> Timer {
        Timer {
            clock: [0; CLOCK_COUNT],
            counter: [0; COUNTER_COUNT],
            start: [Instant::now(); CLOCK_COUNT],
            sample: [0; SAMPLE_SIZE],
            tmp: [0; TAMPLE],
        }
    }
    #[inline]
    pub fn clear(&mut self) {
        self.clock = [0; CLOCK_COUNT];
        self.counter = [0; COUNTER_COUNT];
    }
    #[inline]
    pub fn start(&mut self, count: usize) {
        self.start[count] = Instant::now();
    }
    #[inline]
    pub fn end(&mut self, start_count: usize, count: usize) {
        let now = Instant::now();
        let dur = now.duration_since(self.start[start_count]);
        self.clock[count] += dur.as_nanos();
        self.counter[count] += 1;

        // println!("{}", dur.as_nanos());
        #[cfg(any(feature = "latency_read", feature = "txn_clock"))]
        if count == READING {
            let mut r = self.counter[count] as usize;
            if r >= SAMPLE_SIZE {
                let mut rng = rand::thread_rng();
                r = u64_rand(&mut rng, 0, self.counter[count] - 1) as usize;
            }
            if r < SAMPLE_SIZE {
                self.sample[r] = dur.as_nanos();
            }
        }
    }
    #[inline]
    pub fn add_tmp(&mut self, start_count: usize, tmp_index: usize) {
        let now = Instant::now();
        let dur = now.duration_since(self.start[start_count]);
        self.tmp[tmp_index] += dur.as_nanos();
    }
    #[inline]
    pub fn total(&mut self, start_count: usize, tmp_index: usize, count: usize) {
        let now = Instant::now();
        let dur = now.duration_since(self.start[start_count]);
        let total = dur.as_nanos() + self.tmp[tmp_index];
        self.clock[count] += total;
        self.counter[count] += 1;

        #[cfg(feature = "latency_write")]
        if count == UPDATING {
            let mut r = self.counter[count] as usize;
            // println!("{}, {}", r, total);
            if r >= SAMPLE_SIZE {
                let mut rng = rand::thread_rng();
                r = u64_rand(&mut rng, 0, self.counter[count] - 1) as usize;
            }
            if r < SAMPLE_SIZE {
                self.sample[r] = total;
            }
        }
        self.tmp[tmp_index] = 0;
    }

    #[inline]
    pub fn add_counter(&mut self, count: usize, num: u64) {
        self.counter[count] += num;
    }
    // #[inline]
    // pub fn get_as_ms(&mut self, count: usize) -> f64 {
    //     (self.clock[count] / 1000_000) as f64
    // }

    #[inline]
    pub fn get_as_ms(&mut self, count: usize) -> f64 {
        self.clock[count] as f64 / self.counter[count] as f64
    }
}
