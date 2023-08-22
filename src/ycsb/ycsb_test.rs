#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};
    use std::fs::OpenOptions;
    use std::str::FromStr;
    use std::sync::atomic::AtomicU64;
    use std::thread;
    use std::time::{Duration, SystemTime};

    use crate::config::{CATALOG_ADDRESS, POOL_PERC};
    use crate::customer_config::YCSB_TOTAL;
    use crate::mvcc_config::{TEST_THREAD_COUNT, THREAD_COUNT, TRANSACTION_COUNT};
    use crate::storage::catalog::{self, Catalog};
    // use crate::storage::index::nbtree::NBTree;
    use crate::storage::nvm_file::NVMTableStorage;
    use crate::storage::schema::{ColumnType, TableSchema};
    use crate::storage::table::{IndexType, Table, TupleId};
    use crate::tpcc::{string_rand, u64_rand};
    use crate::transaction::transaction::Transaction;
    use crate::transaction::transaction_buffer::TransactionBuffer;
    use crate::utils::executor::executor::Executor;
    use crate::ycsb::ycsb_query::YcsbQuery;
    use crate::ycsb::ycsb_txn::YcsbTxn;
    use crate::ycsb::Properties;
    use crate::Error;
    use bitvec::macros::internal::funty::IsNumber;
    use std::io::Write;
    use std::sync::{mpsc, Arc, Barrier};

    fn init_all(p: Properties) {
        NVMTableStorage::init_test_database();
    }
    fn init_data(prop: Properties) {
        Catalog::init_catalog();
        let mut schema = TableSchema::new();
        schema.push(ColumnType::Int64, "KEY");
        for i in 0..prop.field_per_tuple {
            schema.push(
                ColumnType::String {
                    len: prop.max_field_length as usize,
                },
                format!("F{}", i).as_str(),
            );
        }
        let catalog = Catalog::global();
        catalog.add_table("usertable", schema).unwrap();
        #[cfg(feature = "ycsb_e")]
        catalog.set_range_primary_key("usertable", 0);
        #[cfg(not(feature = "ycsb_e"))]
        catalog.set_primary_key("usertable", 0);

        #[cfg(feature = "buffer_pool")]
        catalog.set_pool_size("usertable", prop.table_size as usize / POOL_PERC);
        let mut handles = Vec::with_capacity(THREAD_COUNT);
        let barrier = Arc::new(Barrier::new(THREAD_COUNT));

        for i in 0..THREAD_COUNT {
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                let catalog = Catalog::global();
                let table = catalog.get_table("usertable");
                let mut buffer = TransactionBuffer::new(catalog, i as u64);
                let mut ycsb_txn = YcsbTxn::new(prop, &mut buffer);
                #[cfg(feature = "nbtree")]
                crate::storage::index::nbtree::init_index(i as i32);
                b.wait();
                for key in 0..prop.table_size {
                    if (key as usize) % THREAD_COUNT == i {
                        let mut value = (key + 1).to_string();
                        for _ in 0..prop.field_per_tuple {
                            value.push(',');
                            value.push_str(String::from("123").as_str());
                        }
                        ycsb_txn.begin();
                        ycsb_txn.insert_init(&table, value.as_str());
                        assert!(ycsb_txn.commit());
                        if key % 1000000 == 0 {
                            println!("{}", key);
                        }
                    }
                }
                println!("insert finish");
                b.wait();
                let mut k = prop.table_size as usize / POOL_PERC / THREAD_COUNT;
                for key in 0..prop.table_size {
                    if (key as usize) % THREAD_COUNT == i && k > 0 {
                        k -= 1;
                        ycsb_txn.begin();
                        ycsb_txn.read(&table, key + 1, 1);
                        ycsb_txn.commit();
                    }
                }
                println!("warmup finished");
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn basic_test() {
        let props = Properties::default();
        init_all(props.clone());

        let mut input = String::from_str("0").unwrap();

        for _ in 0..props.field_per_tuple {
            input.push_str(",1235");
        }
        let catalog = Catalog::global();
        let mut buffer = TransactionBuffer::new(catalog, 0);
        let mut ycsb_txn = YcsbTxn::new(props, &mut buffer);
        let table = catalog.get_table("usertable");
        ycsb_txn.begin();
        ycsb_txn.insert_init(&table, &input);
        assert!(ycsb_txn.commit());

        ycsb_txn.begin();
        assert!(ycsb_txn.read(&table, 0, 0));
        assert!(ycsb_txn.commit());

        ycsb_txn.begin();
        assert!(ycsb_txn.update(&table, 0, 1, "q".as_bytes()));
        assert!(ycsb_txn.commit());
    }

    #[test]
    fn ycsb_test_sync() {
        let mut props = Properties::default();
        // ycsb_test_reload();
        // reload(true);
        // #[cfg(feature = "ycsb_size")]
        // let size_set = [0x40, 0x80, 0x100, 0x200, 0x400, 0x1000, 0x4000, 0x10000, 0x40000, 0x100000];
   
        init_all(props);
        init_data(props);
        let catalog = Catalog::global();
        let total_time = Duration::new(10, 0);
        let (tx0, rx) = mpsc::channel();
        let mut name = "latency.txt";

        #[cfg(feature = "latency_read")]
        {
            #[cfg(feature = "n2db_local")]
            {
                #[cfg(not(feature = "ycsb_read_tuple"))]
                {
                    name = "read-n2db-latency.txt";
                }
                #[cfg(feature = "ycsb_read_tuple")]
                {
                    name = "read-n2db-1k.txt";
                }
            }
            #[cfg(feature = "zen_local")]
            {
                #[cfg(not(feature = "ycsb_read_tuple"))]
                {
                    name = "read-zen-latency.txt";
                }
                #[cfg(feature = "ycsb_read_tuple")]
                {
                    name = "read-zen-1k.txt";
                }
            }
        }
        #[cfg(feature = "latency_write")]
        {
            #[cfg(not(feature = "buffer_pool"))]
            {
                #[cfg(not(feature = "append"))]
                {
                    name = "write-delta-latency.txt";
                }
                #[cfg(feature = "append")]
                {
                    name = "write-n2db-latency.txt";
                }
            }
            #[cfg(feature = "zen_local")]
            {
                name = "write-zen-latency.txt";
            }
            #[cfg(feature = "wbl_local")]
            {
                name = "write-ndbp-latency.txt";
            }
            #[cfg(feature = "update_direct")]
            {
                name = "write-nd-latency.txt";
            }
        }
        let _ = std::fs::remove_file(name);

        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(name)
            .unwrap();
        #[cfg(not(feature = "ycsb_mvcc"))]
        let theta_set = [0.99, 0.0];
        #[cfg(feature = "ycsb_size")]
        let theta_set = [0.0];
        #[cfg(feature = "ycsb_mvcc")]
        let theta_set = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
        
        let key_counter = Arc::new(AtomicU64::new(YCSB_TOTAL));
        let mut offset: usize = THREAD_COUNT;
        for theta in theta_set {
            #[cfg(feature = "ycsb_mvcc")]
            {
                props.zipf_theta = 0.95;
                props.workload.ro_perc = theta;
            }
            #[cfg(not(feature = "ycsb_mvcc"))]
            {
                props.zipf_theta = theta;
            }
            let table = catalog.get_table("usertable");
            let barrier = Arc::new(Barrier::new(TEST_THREAD_COUNT));
            let b_tree_offset = offset;
            for i in 0..TEST_THREAD_COUNT {
                let b = barrier.clone();
                let tx = tx0.clone();
                let counter = key_counter.clone();
                thread::spawn(move || {
                    let mut buffer = TransactionBuffer::new(catalog, (i) as u64);
                    let mut ycsb_txn = YcsbTxn::new(props, &mut buffer);
                    let table = catalog.get_table("usertable");
                    println!("test start {}", i);
                    let mut num = 0;
                    let mut total = 0;
                    let mut query = YcsbQuery::new(&props, &counter);
                    let mut rng = rand::thread_rng();
                    let mut retry = 0;
                    #[cfg(feature = "nbtree")]
                    crate::storage::index::nbtree::init_index((i+b_tree_offset) as i32);
                    #[cfg(feature = "append")]
                    table.pre_alloc(100);
                    b.wait();
                    let start = SystemTime::now();

                    loop {
                        query.gen_request(&props);
                        let mut duration = std::time::Duration::new(0, 5000);

                        total += 1;
                        retry = 0;
                        while !ycsb_txn.run_txn(&table, &query) {
                            total += 1;
                            retry += 1;
                            // #[cfg(feature = "cc_cfg_occ")]
                            // loop {
                            //     // total += 1;
                            //     match ycsb_txn.result {
                            //         Err(Error::Tuple(
                            //             crate::storage::row::TupleError::TupleChanged {
                            //                 conflict_tid: tid,
                            //             },
                            //         )) => {
                            //             if tid == 0 || catalog.finished(tid) {
                            //                 break;
                            //             }
                            //             std::thread::sleep(duration);
                            //             duration = duration + duration_base;
                            //             let end = SystemTime::now();
                            //             // println!("{}", tid);
                            //             if end.duration_since(start).unwrap().ge(&total_time) {
                            //                 break;
                            //             }
                            //         }
                            //         _ => {
                            //             break;
                            //         }
                            //     }
                            // }
                            // #[cfg(not(feature = "cc_cfg_occ"))]
                            {
                                std::thread::sleep(duration);
                                // if retry < 10 {
                                //     duration = duration + duration_base;
                                // }
                                let end = SystemTime::now();
                                if end.duration_since(start).unwrap().ge(&total_time) {
                                    break;
                                }
                            }
                        }
                        num += 1;
                        let end = SystemTime::now();
                        if end.duration_since(start).unwrap().ge(&total_time) {
                            println!("read_buff {} {}", i, ycsb_txn.read_buf);
                            break;
                        }
                    }
                    #[cfg(all(not(feature = "clock"), not(feature = "txn_clock")))]
                    tx.send((num, total)).unwrap();
                    #[cfg(feature = "clock")]
                    tx.send((
                        num,
                        total,
                        ycsb_txn.txn.timer.get_as_ms(0),
                        ycsb_txn.txn.timer.get_as_ms(1),
                        ycsb_txn.txn.timer.get_as_ms(2),
                        ycsb_txn.txn.timer.get_as_ms(3),
                        ycsb_txn.txn.timer.sample,
                    ))
                    .unwrap();

                    #[cfg(feature = "txn_clock")]
                    tx.send((
                        num,
                        total,
                        ycsb_txn.timer.get_as_ms(2),
                        ycsb_txn.timer.sample,
                    ))
                    .unwrap();
                    // buffer.free();
                });
            }
            offset += TEST_THREAD_COUNT;
            let mut num = 0;
            let mut total = 0;
            let mut swapt = 0.0;
            let mut hit = 0.0;
            let mut read = 0.0;
            let mut update = 0.0;
            let mut txn = 0.0;
            let mut vec = Vec::<u128>::new();
            for i in 0..TEST_THREAD_COUNT {
                #[cfg(feature = "clock")]
                {
                    let (num0, total0, swapt0, hit0, read0, update0, sample0) = rx.recv().unwrap();
                    num += num0;
                    total += total0;
                    swapt += swapt0;
                    hit += hit0;
                    read += read0;
                    update += update0;
                    vec.extend_from_slice(&sample0);
                    println!(
                        "{}: {} txns committed, swap: {}, hit: {}",
                        i, num0, swapt0, hit0
                    );
                    vec.sort();
                }
                #[cfg(feature = "txn_clock")]
                {
                    let (num0, total0, txn0, sample0) = rx.recv().unwrap();
                    num += num0;
                    total += total0;
                    txn += txn0;
                    vec.extend_from_slice(&sample0);
                    vec.sort();
                }
                #[cfg(all(not(feature = "clock"), not(feature = "txn_clock")))]
                {
                    let (num0, total0) = rx.recv().unwrap();
                    num += num0;
                    total += total0;
                    println!("{}: {} of {} txns committed", i, num0, total0);
                }
            }
            #[cfg(all(not(feature = "clock"), not(feature = "txn_clock")))]
            println!(
                "total theta = {}, txn {} of {} txns committed per second",
                theta,
                num / 10,
                total / 10
            );
            #[cfg(feature = "clock")]
            println!(
                "total theta = {}, txn {} of {} txns committed per second, avg_swap: {}, avg_hit: {}, avg_read: {:.3}, avg_update: {:.3}, 10%: {:3}, 90%: {:3}",
                theta,
                num / 10,
                total / 10,
                swapt as f64 / TEST_THREAD_COUNT as f64,
                hit as f64 / TEST_THREAD_COUNT as f64,
                read as f64 / TEST_THREAD_COUNT as f64,
                update as f64 / TEST_THREAD_COUNT as f64,
                vec.get(TEST_THREAD_COUNT * 1000).unwrap(),
                vec.get(TEST_THREAD_COUNT * 9500).unwrap(),
            );
            #[cfg(feature = "txn_clock")]
            println!(
                "total theta = {}, txn {} of {} txns committed per second, avg_txn: {:.3}, 10%: {:3}, 95%: {:3}, 99%: {:3}",
                theta,
                num / 10,
                total / 10,
                txn as f64 / TEST_THREAD_COUNT as f64,
                vec.get(TEST_THREAD_COUNT * 1000).unwrap(),
                vec.get(TEST_THREAD_COUNT * 9500).unwrap(),
                vec.get(TEST_THREAD_COUNT * 9900).unwrap(),
            );
            #[cfg(any(feature = "latency_read", feature = "latency_write"))]
            for v in vec {
                f.write(v.to_string().as_bytes()).unwrap();
                f.write(b"\n").unwrap();
            }
        }
    }
    pub fn reload(init: bool) {
        if init {
            NVMTableStorage::reload_test_database();
        }
        let start0 = SystemTime::now();
        Catalog::reload(CATALOG_ADDRESS);
        let catalog = Catalog::global();

        let end = SystemTime::now();
        println!(
            "Reload catalog uses {} nano seconds",
            end.duration_since(start0).unwrap().as_nanos()
        );
        let start = SystemTime::now();
        catalog.set_primary_key("usertable", 0);
        let end = SystemTime::now();
        println!(
            "Reload index uses {} nano seconds",
            end.duration_since(start).unwrap().as_nanos()
        );
        //TODO redo
        let start = SystemTime::now();
        catalog.redo_transaction();
        let end = SystemTime::now();
        println!(
            "Redo Transacion uses {} nano seconds",
            end.duration_since(start).unwrap().as_nanos()
        );
        let start = SystemTime::now();
        catalog.reload_timestamp();
        let end = SystemTime::now();
        println!(
            "Reload timestamp uses {} nano seconds",
            end.duration_since(start).unwrap().as_nanos()
        );
        println!(
            "Recovery uses {} nano seconds",
            end.duration_since(start0).unwrap().as_nanos()
        );
    }
    #[test]
    pub fn ycsb_test_reload() {
        let mut props = Properties::default();
        reload(true);
        // init_all(props);
        // init_data(props);
        let catalog = Catalog::global();
        let total_time = Duration::new(10, 0);
        let (tx0, rx) = mpsc::channel();
        let key_counter = Arc::new(AtomicU64::new(YCSB_TOTAL));

        for i in 0..THREAD_COUNT {
            let tx = tx0.clone();
            let counter = key_counter.clone();
            thread::spawn(move || {
                let mut buffer = TransactionBuffer::new(catalog, (i) as u64);
                let mut ycsb_txn = YcsbTxn::new(props, &mut buffer);
                let table = catalog.get_table("usertable");
                println!("test start {}", i);
                let mut num = 0;
                let mut total = 0;
                let mut query = YcsbQuery::new(&props, &counter);
                let start = SystemTime::now();
                let mut rng = rand::thread_rng();
                let mut retry = 0;
                #[cfg(feature = "append")]
                table.pre_alloc(100);
                for _ in 0..2235 {
                    query.gen_request(&props);
                    let mut duration = std::time::Duration::new(0, 5000);

                    total += 1;
                    retry = 0;
                    while !ycsb_txn.run_txn(&table, &query) {
                        std::thread::sleep(duration);
                        let end = SystemTime::now();
                        if end.duration_since(start).unwrap().ge(&total_time) {
                            break;
                        }
                    }
                    // println!("commit {}", ycsb_txn.txn.txn_buffer.get_offset());

                    num += 1;
                }

                tx.send((num, total)).unwrap();
                // buffer.free();
            });
        }

        let mut num = 0;
        let mut total = 0;
        for i in 0..THREAD_COUNT {
            let (num0, total0) = rx.recv().unwrap();
            num += num0;
            total += total0;
        }
        reload(false);
    }
}
