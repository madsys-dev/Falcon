#[cfg(test)]
mod tests {
    use crate::storage::catalog::Catalog;
    use crate::storage::global::{Timer, READING};
    use crate::storage::nvm_file::NVMTableStorage;
    use crate::tpcc::tpcc::*;
    use crate::tpcc::*;
    // use crate::tpcc::tpcc_index::TpccIndex;
    use crate::mvcc_config::{TEST_THREAD_COUNT, THREAD_COUNT, TRANSACTION_COUNT};
    use crate::tpcc::tpcc_init;
    use crate::tpcc::tpcc_query::*;
    use crate::tpcc::{tpcc_txn_asycn, tpcc_txn_sycn};
    use crate::transaction::transaction::Transaction;
    use crate::transaction::transaction_buffer::TransactionBuffer;
    use crate::utils::executor::executor::Executor;
    use log4rs;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::{mpsc, Barrier};
    use std::time::{Duration, SystemTime};
    use std::{thread, time};

    #[test]
    fn tpcc_test_sync() {
        // log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        // debug!("INFO");
        // debug!("DEBUG");
        NVMTableStorage::init_test_database();
        Catalog::init_catalog();
        if IS_FULL_SCHEMA {
            tpcc_init::init_schema("config/schema_file/TPCC_full_schema.txt");
        } else {
            tpcc_init::init_schema("config/schema_file/TPCC_short_schema.txt");
        }
        init_tables();
        // let threads::Vec<JoinHandle> = Vec::new();
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("latency.txt")
            .unwrap();
        let (tx0, rx) = mpsc::channel();
        // let senders = Vec::new();
        println!("test start");

        let catalog = Catalog::global();
        let total_time = Duration::new(10, 0);

        // for i in 0..TRANSACTION_COUNT {
        //     let mut buffer = TransactionBuffer::new(catalog, i as u64);
        //     let mut txn = Transaction::new(&mut buffer, false);
        //     txn.begin();
        //     txn.commit();
        // }
        let barrier = Arc::new(Barrier::new(TEST_THREAD_COUNT));
        for i in 0..TEST_THREAD_COUNT {
            let tx = tx0.clone();
            let b = barrier.clone();

            thread::spawn(move || {
                let mut buffer = TransactionBuffer::new(catalog, i as u64);
                println!("test prepare {} {}", i, buffer.address);
                let mut rng = rand::thread_rng();
                let mut txn = Transaction::new(&mut buffer, false);

                #[cfg(feature = "txn_clock")]
                let mut timer = Timer::new();
                
                let tablelist = TableList::new(&catalog);

                #[cfg(feature = "nbtree")]
                crate::storage::index::nbtree::init_index((i+THREAD_COUNT) as i32);
                let mut num = 0;
                let mut total = 0;
                println!("test start {} ", i);
                b.wait();
                let start = SystemTime::now();

                loop {
                    let pr = u64_rand(&mut rng, 1, 100);
                    if pr <= 43 {
                        let query = TpccQuery::gen_payment(&mut rng, i as u64);
                        
                        loop {
                            total = total + 1;

                            #[cfg(feature = "txn_clock")]
                            timer.start(READING);
                            // println!("{} payment", i);
                            if tpcc_txn_sycn::run_payment(&mut txn, &tablelist, &query) {
                                num = num + 1;
    
                                #[cfg(feature = "payment_clock")]
                                timer.end(READING, READING);
                                break;
                            }
                            let end = SystemTime::now();
                            if end.duration_since(start).unwrap().ge(&total_time) {
                                break;
                            }
                        }
                        
                    } else if pr <= 88 {
                        let query = TpccQuery::gen_new_order(&mut rng, i as u64);
                        loop {
                            total = total + 1;

                            #[cfg(feature = "txn_clock")]
                            timer.start(READING);
                            // println!("{} new_order", i);

                            if tpcc_txn_sycn::run_new_order(&mut txn, &tablelist, &query) {
                                num = num + 1;

                                #[cfg(feature = "new_order_clock")]
                                timer.end(READING, READING);
                                break;
                            }
                            let end = SystemTime::now();
                            if end.duration_since(start).unwrap().ge(&total_time) {
                                break;
                            }
                        }
                    } else if pr <= 92 {
                        let query = TpccQuery::gen_stock_level(&mut rng, i as u64);
                        loop {
                            total = total + 1;

                            #[cfg(feature = "txn_clock")]
                            timer.start(READING);
                            // println!("{} stock_level", i);

                            if tpcc_txn_sycn::run_stock_level(&mut txn, &tablelist, &query) {
                                num = num + 1;
    
                                #[cfg(feature = "stock_level_clock")]
                                timer.end(READING, READING);
                                break;
                            }
                            let end = SystemTime::now();
                            if end.duration_since(start).unwrap().ge(&total_time) {
                                break;
                            }
                        }
                            
                    } else if pr <= 96 {
                        let query = TpccQuery::gen_order_status(&mut rng, i as u64);
                        loop {
                            // println!("{} order_status", i);
                            total = total + 1;

                            #[cfg(feature = "txn_clock")]
                            timer.start(READING);
                            if tpcc_txn_sycn::run_order_status(&mut txn, &tablelist, &query) {
                                num = num + 1;
                                #[cfg(feature = "order_status_clock")]
                                timer.end(READING, READING);
                                break;
                            }
                            let end = SystemTime::now();
                            if end.duration_since(start).unwrap().ge(&total_time) {
                                break;
                            }
                        }
                    } else {
                        let query = TpccQuery::gen_deliver(&mut rng, i as u64);
                        loop {
                            total = total + 1;
                            // println!("{} deliver", i);

                            #[cfg(feature = "txn_clock")]
                            timer.start(READING);
                            if tpcc_txn_sycn::run_deliver(&mut txn, &tablelist, &query) {
                                num = num + 1;
                                #[cfg(feature = "deliver_clock")]
                                timer.end(READING, READING);
                                break;
                            }
                            let end = SystemTime::now();
                            if end.duration_since(start).unwrap().ge(&total_time) {
                                break;
                            }

                        }
                        
                    }
                    
                    let end = SystemTime::now();
                    if end.duration_since(start).unwrap().ge(&total_time) {
                        break;
                    }
                }
                #[cfg(all(not(feature = "txn_clock")))]
                tx.send((num, total)).unwrap();
                // #[cfg(feature = "clock")]
                // tx.send((
                //     num,
                //     total,
                //     txn.timer.get_as_ms(0),
                //     txn.timer.get_as_ms(1),
                //     txn.timer.get_as_ms(2),
                //     txn.timer.get_as_ms(3),
                //     txn.timer.sample,
                // ))
                // .unwrap();
                // println!("txn {} commit {} of {}", i, num, total);
                #[cfg(feature = "txn_clock")]
                tx.send((num, total, timer.get_as_ms(2), timer.sample))
                    .unwrap();
            });
        }

        let mut num = 0;
        let mut total = 0;
        let mut swapt = 0.0;
        let mut hit = 0.0;
        let mut read = 0.0;
        let mut update = 0.0;
        let mut txn = 0.0;
        let mut vec = Vec::<u128>::new();
        for i in 0..TEST_THREAD_COUNT {
            #[cfg(feature = "txn_clock")]
            {
                let (num0, total0, txn0, sample0) = rx.recv().unwrap();
                num += num0;
                total += total0;
                txn += txn0;
                vec.extend_from_slice(&sample0);
                vec.sort();
            }
            #[cfg(not(feature = "txn_clock"))]
            {
                let (num0, total0) = rx.recv().unwrap();
                num += num0;
                total += total0;
                println!("{}: {} of {} txns committed", i, num0, total0);
            }
        }
        #[cfg(all(not(feature = "clock"), not(feature = "txn_clock")))]
        println!(
            "total txn {} of {} txns committed per second",
            num / 10,
            total / 10
        );
        #[cfg(feature = "clock")]
        println!(
            "total txn {} of {} txns committed per second, avg_swap: {}, avg_hit: {}, avg_read: {:.3}, avg_update: {:.3}, 10%: {:3}, 90%: {:3}",
            num / 10,
            total / 10,
            swapt as f64 / TEST_THREAD_COUNT as f64,
            hit as f64 / TEST_THREAD_COUNT as f64,
            read as f64 / TEST_THREAD_COUNT as f64,
            update as f64 / TEST_THREAD_COUNT as f64,
            vec.get(THREAD_COUNT * 1000).unwrap(),
            vec.get(THREAD_COUNT * 9500).unwrap(),
        );
        #[cfg(feature = "txn_clock")]
        println!(
            "total txn {} of {} txns committed per second, avg_txn: {:.3}, 10%: {:3}, 95%: {:3}, 99%: {:3}",
            num / 10,
            total / 10,
            txn as f64 / TEST_THREAD_COUNT as f64,
            vec.get(TEST_THREAD_COUNT * 1000).unwrap(),
            vec.get(TEST_THREAD_COUNT * 9500).unwrap(),
            vec.get(TEST_THREAD_COUNT * 9900).unwrap(),
        );
        // #[cfg(feature = "clock")]
        // for v in vec {
        //     f.write(v.to_string().as_bytes()).unwrap();
        //     f.write(b"\n").unwrap();
        // }
    }

    // #[test]
    // fn tpcc_test_async() {
    //     NVMTableStorage::init_test_database();
    //     Catalog::init_catalog();
    //     if IS_FULL_SCHEMA {
    //         tpcc_init::init_schema("config/schema_file/TPCC_full_schema.txt");
    //     } else {
    //         tpcc_init::init_schema("config/schema_file/TPCC_short_schema.txt");
    //     }
    //     init_tables();
    //     // let threads::Vec<JoinHandle> = Vec::new();

    //     let (tx0, rx) = mpsc::channel();
    //     // let senders = Vec::new();
    //     println!("test start");

    //     let catalog = Catalog::global();
    //     let total_time = Duration::new(10, 0);
    //     let tpthread = TRANSACTION_COUNT / THREAD_COUNT;
    //     for i in 0..THREAD_COUNT {
    //         let tx1 = tx0.clone();
    //         thread::spawn(move || {
    //             let (executor, spawner) = Executor::new();
    //             for t_cnt in (i * tpthread)..(i * tpthread + tpthread) {
    //                 let tx = tx1.clone();
    //                 spawner.spawn(async move {
    //                     let mut buffer = TransactionBuffer::new(catalog, t_cnt as u64);
    //                     println!("test start {} {}", t_cnt, buffer.address);
    //                     let mut rng = rand::thread_rng();

    //                     let mut txn = Transaction::new(&mut buffer, false);
    //                     let tablelist = TableList::new(&catalog);

    //                     let mut num = 0;
    //                     let mut total = 0;
    //                     let start = SystemTime::now();
    //                     loop {

    //                         total = total + 1;
    //                         let mut commited: bool = false;
    //                         if u64_rand(&mut rng, 1, 88) > 45 {
    //                             let query = TpccQuery::gen_payment(&mut rng, t_cnt as u64);
    //                             commited =
    //                                 tpcc_txn_asycn::run_payment(&mut txn, &tablelist, &query).await;
    //                         } else {
    //                             let query = TpccQuery::gen_new_order(&mut rng, t_cnt as u64);

    //                             commited =
    //                                 tpcc_txn_asycn::run_new_order(&mut txn, &tablelist, &query)
    //                                     .await;
    //                         }
    //                         if commited {
    //                             num = num + 1;
    //                         }

    //                         let end = SystemTime::now();
    //                         if end.duration_since(start).unwrap().ge(&total_time) {
    //                             break;
    //                         }
    //                     }
    //                     tx.send((num, total)).unwrap();
    //                 });
    //             }

    //             drop(spawner);
    //             executor.run();
    //         });
    //     }
    //     let mut num = 0;
    //     let mut total = 0;
    //     for i in 0..TRANSACTION_COUNT {
    //         let (num0, total0) = rx.recv().unwrap();
    //         num += num0;
    //         total += total0;
    //         println!("{}: {} of {} txns committed", i, num0, total0);
    //     }
    //     println!(
    //         "total new order {} of {} txns committed",
    //         num / 10,
    //         total / 10
    //     );
    // }
}
