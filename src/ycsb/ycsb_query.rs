use std::collections::{HashMap, HashSet};

use rand::prelude::ThreadRng;

use crate::storage::schema::Column;

use super::{f64_rand_new, u64_rand_new, Operation, Properties};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
pub struct YcsbRequest {
    pub op: Operation,
    pub key: u64,
    pub value: String,
    pub column: usize,
    pub scan_len: u64,
}
impl YcsbRequest {
    pub fn new(op: Operation, key: u64, value: String, column: usize) -> Self {
        YcsbRequest {
            op,
            key,
            value,
            column,
            scan_len: 0,
        }
    }
    pub fn new_scan(op: Operation, key: u64, value: String, column: usize, scan_len: u64) -> Self {
        YcsbRequest {
            op,
            key,
            value,
            column,
            scan_len,
        }
    }
}
pub struct YcsbQuery<'a> {
    pub requests: Vec<YcsbRequest>,
    zeta_2_theta: f64,
    denom: f64,
    rng: ThreadRng,
    counter: &'a AtomicU64,
}

impl<'a> YcsbQuery<'a> {
    pub fn new(prop: &Properties, counter: &'a AtomicU64) -> Self {
        let mut requests = Vec::new();
        requests.reserve(prop.req_per_query);
        for _ in 0..prop.req_per_query {
            requests.push(YcsbRequest::new(Operation::Read, 0, String::from(""), 0));
            #[cfg(feature = "ycsb_f")]
            requests.push(YcsbRequest::new(Operation::Read, 0, String::from(""), 0));
        }
        let zeta_2_theta = Properties::zeta(2, prop.zipf_theta);

        let mut ycsb = YcsbQuery {
            requests,
            zeta_2_theta,
            denom: Properties::zeta(prop.table_size, prop.zipf_theta),
            rng: rand::thread_rng(),
            counter,
        };
        ycsb.gen_request(prop);
        ycsb
    }

    pub fn zipf(&mut self, n: u64, theta: f64) -> u64 {
        let zetan = self.denom;

        let u = f64_rand_new(&mut self.rng, 0.0, 1.0, 0.000_000_000_000_001);
        let uz = u * zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + f64::powf(0.5, theta) {
            return 1;
        }
        let alpha = 1.0 / (1.0 - theta);

        let eta =
            (1.0 - f64::powf(2.0 / n as f64, 1.0 - theta)) / (1.0 - self.zeta_2_theta / zetan);
        let mut v = ((n as f64) * f64::powf(eta * u - eta + 1.0, alpha)) as u64;
        if v >= n {
            v = n - 1;
        }
        v
    }
    pub fn gen_request(&mut self, prop: &Properties) {
        let mut v = HashSet::<u64>::new();
        let dv = vec!['a' as u8; prop.max_field_length];
        let value = String::from_utf8(dv).unwrap();
        let mut ro = false;
        #[cfg(feature = "ycsb_mvcc")]
        {
            let op = f64_rand_new(&mut self.rng, 0.0, 1.0, 0.01);
            if op <= prop.workload.ro_perc {
                ro = true;
            }
        }

        for i in 0..prop.req_per_query {
            let op = f64_rand_new(&mut self.rng, 0.0, 1.0, 0.01);

            #[cfg(feature = "ycsb_d")]
            let mut key = self.counter.load(Ordering::Relaxed) - self.zipf(self.counter.load(Ordering::Relaxed), prop.zipf_theta);
            #[cfg(feature = "ycsb_e")]
            let mut key = self.zipf(self.counter.load(Ordering::Relaxed) - prop.workload.scan_len, prop.zipf_theta);
            #[cfg(not(all(feature = "ycsb_d", feature = "ycsb_e")))]
            let mut key = self.zipf(prop.table_size, prop.zipf_theta);
            while v.contains(&key) {
                #[cfg(feature = "ycsb_d")]
                {key = self.counter.load(Ordering::Relaxed) - self.zipf(self.counter.load(Ordering::Relaxed), prop.zipf_theta);}
                #[cfg(feature = "ycsb_e")]
                {key = self.zipf(self.counter.load(Ordering::Relaxed) - prop.workload.scan_len, prop.zipf_theta);}
                #[cfg(not(all(feature = "ycsb_d", feature = "ycsb_e")))]
                {key = self.zipf(prop.table_size, prop.zipf_theta);}
            }
            v.insert(key);
            let column = u64_rand_new(&mut self.rng, 1, prop.field_per_tuple) as usize;

            key += 1;
            // TODO fix ycsb_f in revision
            #[cfg(feature = "ycsb_f")]
            {
                if ro || op <= prop.workload.read_perc {
                    self.requests[i*2] = YcsbRequest::new(Operation::Read, key, "0".to_string(), column);
                    self.requests[i * 2 + 1] = YcsbRequest::new(Operation::Nop, key, "0".to_string(), column);

                } 
                else {
                    self.requests[i * 2] =
                        YcsbRequest::new(Operation::Read, key, "0".to_string(), column);
                    self.requests[i * 2 + 1] =
                        YcsbRequest::new(Operation::Update, key, value.clone(), column);
                }
            }
            #[cfg(not(feature = "ycsb_f"))]
            if ro || op <= prop.workload.read_perc {
                self.requests[i] = YcsbRequest::new(Operation::Read, key, "0".to_string(), column);
            } else if op <= prop.workload.read_perc + prop.workload.write_perc {
                self.requests[i] = YcsbRequest::new(Operation::Update, key, value.clone(), column);
            } else if op <= prop.workload.read_perc + prop.workload.write_perc + prop.workload.scan_perc {
                self.requests[i] = YcsbRequest::new_scan(Operation::Scan, key, "0".to_string(), column, u64_rand_new(&mut self.rng, 1, prop.workload.scan_len));
            } else {
                self.requests[i] = YcsbRequest::new(Operation::Insert, self.counter.fetch_add(1, Ordering::Relaxed), "0".to_string(), column);
            }
        }
        // self.requests.sort_by(|a, b| b.key.cmp(&a.key));
    }
}
#[cfg(test)]
mod test {

    use super::Properties;
    use std::sync::atomic::AtomicU64;
    use super::YcsbQuery;
    #[test]
    fn test_zipf() {
        let prop = Properties::default();
        let counter = AtomicU64::new(0);
        let mut query = YcsbQuery::new(&prop, &counter);
        for i in 0..10_000_000 {
            let key = query.zipf(prop.table_size, prop.zipf_theta);
            println!("{}", key);
        }
    }
}
