use std::collections::{HashMap, HashSet};

use rand::prelude::ThreadRng;

use crate::storage::schema::Column;

use super::{f64_rand_new, u64_rand_new, Operation, Properties};

pub struct YcsbRequest {
    pub op: Operation,
    pub key: u64,
    pub value: String,
    pub column: usize,
}
impl YcsbRequest {
    pub fn new(op: Operation, key: u64, value: String, column: usize) -> Self {
        YcsbRequest {
            op,
            key,
            value,
            column,
        }
    }
}
pub struct YcsbQuery {
    pub requests: Vec<YcsbRequest>,
    zeta_2_theta: f64,
    denom: f64,
    rng: ThreadRng,
}

impl YcsbQuery {
    pub fn new(prop: &Properties) -> Self {
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

            let mut key = self.zipf(prop.table_size, prop.zipf_theta);
            while v.contains(&key) {
                key = self.zipf(prop.table_size, prop.zipf_theta)
            }
            v.insert(key);
            let column = u64_rand_new(&mut self.rng, 1, prop.field_per_tuple) as usize;

            key += 1;
            // TODO fix ycsb_f in revision
            #[cfg(feature = "ycsb_f")]
            {
                self.requests[i * 2] =
                    YcsbRequest::new(Operation::Read, key, "0".to_string(), column);
                self.requests[i * 2 + 1] =
                    YcsbRequest::new(Operation::Update, key, value.clone(), column);
            }
            #[cfg(not(feature = "ycsb_f"))]
            if ro || op <= prop.workload.read_perc {
                self.requests[i] = YcsbRequest::new(Operation::Read, key, "0".to_string(), column);
            } else if op <= prop.workload.read_perc + prop.workload.write_perc {
                self.requests[i] = YcsbRequest::new(Operation::Update, key, value.clone(), column);
            } else if op <= prop.workload.read_perc + prop.workload.write_perc + prop.workload.scan_perc {
                self.requests[i] = YcsbRequest::new(Operation::Scan, key, "0".to_string(), column);
            } else {
                self.requests[i] = YcsbRequest::new(Operation::Insert, key, "0".to_string(), column);
            }
        }
        // self.requests.sort_by(|a, b| b.key.cmp(&a.key));
    }
}
#[cfg(test)]
mod test {
    use super::Properties;

    use super::YcsbQuery;
    #[test]
    fn test_zipf() {
        let prop = Properties::default();
        let mut query = YcsbQuery::new(&prop);
        for i in 0..10_000_000 {
            let key = query.zipf(prop.table_size, prop.zipf_theta);
            println!("{}", key);
        }
    }
}
