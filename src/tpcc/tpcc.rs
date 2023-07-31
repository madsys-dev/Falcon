use crate::mvcc_config::{THREAD_COUNT, TRANSACTION_COUNT};
use crate::storage::catalog::Catalog;
use crate::tpcc::tpcc_index::TpccIndex;
use crate::tpcc::*;
use crate::transaction::transaction::*;
use crate::transaction::transaction_buffer::*;
use chrono::prelude::*;
use std::sync::mpsc;
use std::{thread, time};

//todo:full schema
pub const IS_FULL_SCHEMA: bool = true;
pub fn init_table_item(buffer: &mut TransactionBuffer) {
    let mut rng_mut = rand::thread_rng();
    let mut rng = &mut rng_mut;
    let mut txn = Transaction::new(buffer, false);
    let table = &Catalog::global().get_table("ITEM");
    // let index = TpccIndex::items_index();
    let mut s = String::new();
    s.reserve(40);
    txn.begin();

    for iid in 0..ITEMS {
        // println!("Insert item {}", iid);
        append_u64(&mut s, iid);
        append_u64(&mut s, u64_rand(rng, 1, 10000));
        append_string(&mut s, string_rand(rng, 14, 24));
        append_f64(&mut s, f64_rand(rng, 1.00, 100.00, 0.01));
        append_string(&mut s, data_rand(rng));
        txn.insert(table, &s);
        // index.add(item_key(iid), txn.insert(table, &s));
        s = String::new();
        s.reserve(40);
    }
    txn.commit();
}
pub fn init_table_warehouse(buffer: &mut TransactionBuffer, wid: u64) {
    let mut rng_mut = rand::thread_rng();
    let mut rng = &mut rng_mut;
    let mut txn = Transaction::new(buffer, false);
    let table = &Catalog::global().get_table("WAREHOUSE");
    // let index = TpccIndex::warehouses_index();
    let mut s = String::new();
    s.reserve(100);
    // // println!("Insert warehouse {}", wid);
    txn.begin();
    append_u64(&mut s, wid);
    append_string(&mut s, string_rand(rng, 6, 10));
    append_string(&mut s, string_rand(rng, 10, 20));
    append_string(&mut s, string_rand(rng, 10, 20));
    append_string(&mut s, string_rand(rng, 10, 20));
    append_string(&mut s, string_rand(rng, 2, 2));
    append_string(&mut s, zip_rand(rng));
    append_f64(&mut s, f64_rand(rng, 0.0000, 0.2000, 0.0001));
    append_f64(&mut s, 300000.00);
    txn.insert(table, &s);
    // index.add(warehouse_key(wid), txn.insert(table, &s));
    txn.commit();
}
pub fn init_table_stock(buffer: &mut TransactionBuffer, wid: u64) {
    let mut rng_mut = rand::thread_rng();
    let mut rng = &mut rng_mut;

    let mut txn = Transaction::new(buffer, false);
    let table = &Catalog::global().get_table("STOCK");
    // let index = TpccIndex::stocks_index();
    let mut s = String::new();
    s.reserve(40);
    txn.begin();
    if IS_FULL_SCHEMA {
        for sid in 0..STOCKS_PER_WAREHOUSE {
            // println!("Insert stock {},{}", wid, sid);
            append_u64(&mut s, stock_key(wid, sid));
            append_u64(&mut s, wid);
            append_u64(&mut s, u64_rand(rng, 10, 100));
            for _ in 0..10 {
                append_string(&mut s, string_rand(rng, 14, 24));
            }
            append_u64(&mut s, 0);
            append_u64(&mut s, 0);
            append_u64(&mut s, 0);
            append_string(&mut s, string_rand(rng, 50, 50));

            txn.insert(table, &s);
            // index.add(stock_key(wid, sid), txn.insert(table, &s));
            s = String::new();
            s.reserve(40);
        }
    } else {
        for sid in 0..STOCKS_PER_WAREHOUSE {
            // println!("Insert stock {},{}", wid, sid);
            append_u64(&mut s, stock_key(wid, sid));
            append_u64(&mut s, wid);
            append_u64(&mut s, u64_rand(rng, 10, 100));
            append_u64(&mut s, 0);
            txn.insert(table, &s);
            // index.add(stock_key(wid, sid), txn.insert(table, &s));
            s = String::new();
            s.reserve(40);
        }
    }
    txn.commit();
}
pub fn init_table_district(buffer: &mut TransactionBuffer, wid: u64) {
    let mut rng_mut = rand::thread_rng();
    let mut rng = &mut rng_mut;

    let mut txn = Transaction::new(buffer, false);
    let table = &Catalog::global().get_table("DISTRICT");
    // let index = TpccIndex::districts_index();
    let mut s = String::new();
    s.reserve(100);

    txn.begin();
    for did in 0..DISTRICTS_PER_WAREHOUSE {
        // println!("Insert district {},{}", wid, did);
        append_u64(&mut s, district_key(wid, did));
        append_u64(&mut s, wid);
        append_string(&mut s, string_rand(rng, 6, 10));
        append_string(&mut s, string_rand(rng, 10, 20));
        append_string(&mut s, string_rand(rng, 10, 20));
        append_string(&mut s, string_rand(rng, 10, 20));
        append_string(&mut s, string_rand(rng, 2, 2));
        append_string(&mut s, zip_rand(rng));
        append_f64(&mut s, f64_rand(rng, 0.0000, 0.2000, 0.0001));
        append_f64(&mut s, 30000.00);
        append_u64(&mut s, CUSTOMERS_PER_DISTRICT);
        txn.insert(table, &s);
        // index.add(district_key(wid, did), txn.insert(table, &s));
        s = String::new();
        s.reserve(100);
    }
    txn.commit();
}
pub fn init_table_customer(buffer: &mut TransactionBuffer, wid: u64, did: u64) {
    let mut rng_mut = rand::thread_rng();
    let mut rng = &mut rng_mut;

    let mut txn = Transaction::new(buffer, false);
    let table = &Catalog::global().get_table("CUSTOMER");
    // let index = TpccIndex::customers_index();
    let mut s = String::new();
    s.reserve(100);

    txn.begin();
    if IS_FULL_SCHEMA {
        for cid in 0..CUSTOMERS_PER_DISTRICT {
            append_u64(&mut s, customer_key(wid, did, cid));
            append_u64(&mut s, did);
            append_u64(&mut s, wid);
            // println!("Insert customer {},{},{}", wid, did, cid);
            //	16,string,C_FIRST
            append_string(&mut s, string_rand(rng, 8, 16));
            append_string(&mut s, String::from("OE"));
            if cid < 1000 {
                append_string(&mut s, customer_last_key(lastname(cid), wid, did));
            } else {
                append_string(
                    &mut s,
                    customer_last_key(lastname(nurand(rng, 255, 0, 999)), wid, did),
                );
            }
            // C_STREET_1
            append_string(&mut s, string_rand(rng, 20, 20));
            append_string(&mut s, string_rand(rng, 20, 20));
            append_string(&mut s, string_rand(rng, 20, 20));
            // 2,string,C_STATE
            append_string(&mut s, string_rand(rng, 2, 2));
            append_string(&mut s, string_rand(rng, 9, 9));
            append_string(&mut s, string_rand(rng, 16, 16));
            append_u64(&mut s, u64_rand(rng, 0, 10000));

            //C_CREDIT
            append_string(&mut s, credit_rand(rng));
            append_u64(&mut s, 50000);
            append_u64(&mut s, u64_rand(rng, 0, 5000));

            // 8,double,C_BALANCE
            append_f64(&mut s, -10.00);
            append_f64(&mut s, 10.00);
            append_u64(&mut s, 1);
            append_u64(&mut s, 1);
            append_string(&mut s, string_rand(rng, 500, 500));

            txn.insert(table, &s);
            // index.add(customer_key(wid, did, cid), txn.insert(table, &s));
            s = String::new();
            s.reserve(100);
        }
    } else {
        for cid in 0..CUSTOMERS_PER_DISTRICT {
            append_u64(&mut s, customer_key(wid, did, cid));
            append_u64(&mut s, did);
            append_u64(&mut s, wid);
            // println!("Insert customer {},{},{}", wid, did, cid);
            append_string(&mut s, String::from("OE"));
            if cid < 1000 {
                append_string(&mut s, customer_last_key(lastname(cid), wid, did));
            } else {
                append_string(
                    &mut s,
                    customer_last_key(lastname(nurand(rng, 255, 0, 999)), wid, did),
                );
            }
            append_string(&mut s, string_rand(rng, 2, 2));
            append_string(&mut s, credit_rand(rng));
            append_f64(&mut s, f64_rand(rng, 0.0000, 0.5000, 0.0001));
            append_f64(&mut s, -10.00);
            append_f64(&mut s, 10.00);
            append_u64(&mut s, 1);
            txn.insert(table, &s);
            // index.add(customer_key(wid, did, cid), txn.insert(table, &s));
            s = String::new();
            s.reserve(100);
        }
    }
    txn.commit();
}
pub fn init_table_history(buffer: &mut TransactionBuffer, wid: u64, did: u64, cid: u64) {
    let mut txn = Transaction::new(buffer, false);
    let table = &Catalog::global().get_table("HISTORY");
    let mut s = String::new();
    s.reserve(40);
    let mut rng_mut = rand::thread_rng();
    let mut rng = &mut rng_mut;
    txn.begin();
    if IS_FULL_SCHEMA {
        for _ in 0..HISTORIES_PER_CUSTOMER {
            // println!("Insert history {},{},{},{}", wid, did, cid, i);
            append_u64(&mut s, cid);
            append_u64(&mut s, did);
            append_u64(&mut s, wid);
            append_u64(&mut s, did);
            append_u64(&mut s, wid);
            append_u64(&mut s, 0);
            append_f64(&mut s, 10.00);
            append_string(&mut s, string_rand(rng, 24, 24));

            txn.insert(table, &s);
            s = String::new();
            s.reserve(40);
        }
    } else {
        for _ in 0..HISTORIES_PER_CUSTOMER {
            // println!("Insert history {},{},{},{}", wid, did, cid, i);
            append_u64(&mut s, cid);
            append_u64(&mut s, did);
            append_u64(&mut s, wid);
            append_u64(&mut s, 0);
            append_f64(&mut s, 10.00);
            txn.insert(table, &s);
            s = String::new();
            s.reserve(40);
        }
    }
    txn.commit();
}
pub fn init_table_order(buffer: &mut TransactionBuffer, wid: u64, did: u64) {
    let mut rng_mut = rand::thread_rng();
    let mut rng = &mut rng_mut;

    let mut txn = Transaction::new(buffer, false);
    let table = &Catalog::global().get_table("ORDER");
    let table1 = &Catalog::global().get_table("ORDER-LINE");
    let table2 = &Catalog::global().get_table("NEW-ORDER");
    // let index = TpccIndex::orders_index();
    // let table1 = TpccIndex::order_lines_index();
    // let index2 = TpccIndex::new_orders_index();
    let cids = permutation_rand(rng, 1, CUSTOMERS_PER_DISTRICT);
    let mut s = String::new();
    s.reserve(60);

    txn.begin();
    if IS_FULL_SCHEMA {
        for oid in 0..ORDERS_PER_DISTRICT {
            let cid = cids[oid as usize];
            // println!("Insert order {},{},{},{}", wid, did, cid, oid);
            append_u64(&mut s, order_key(wid, did, oid));
            append_u64(&mut s, cid);
            append_u64(&mut s, did);
            append_u64(&mut s, wid);
            append_u64(&mut s, 0);
            if oid < 2100 {
                append_u64(&mut s, u64_rand(rng, 1, 10));
            } else {
                append_u64(&mut s, 0);
            }
            let ol_cnt = u64_rand(rng, 5, 15);
            append_u64(&mut s, ol_cnt);
            append_u64(&mut s, 1);
            txn.insert(table, &s);
            // index.add(order_key(wid, did, oid), txn.insert(table, &s));
            s = String::new();
            for olid in 0..ol_cnt {
                append_u64(&mut s, order_key(wid, did, oid));
                append_u64(&mut s, did);
                append_u64(&mut s, wid);
                append_u64(&mut s, order_line_key(wid, did, oid, olid));
                //println!("Insert order line {},{},{},{}", wid, did, oid, olid);
                append_u64(&mut s, u64_rand(rng, 1, ITEMS) - 1);
                // OL_SUPPLY_W_ID
                append_u64(&mut s, wid);
                append_u64(&mut s, Local::now().timestamp_nanos() as u64);
                append_u64(&mut s, u64_rand(rng, 1, 100));
                append_f64(&mut s, 0.0);
                append_string(&mut s, string_rand(rng, 24, 24));

                txn.insert(table1, &s);
                // index1.add(order_line_key(wid, did, oid, olid), txn.insert(table1, &s));
                s = String::new();
                s.reserve(100);
            }
            if oid >= 2100 {
                append_u64(&mut s, order_key(wid, did, oid));
                append_u64(&mut s, did);
                append_u64(&mut s, wid);
                txn.insert(table2, &s);
                // index2.add(new_order_key(wid, did, oid), txn.insert(table2, &s));
                s = String::new();
                s.reserve(60);
            }
        }
    } else {
        for oid in 0..ORDERS_PER_DISTRICT {
            let cid = cids[oid as usize];
            // println!("Insert order {},{},{},{}", wid, did, cid, oid);
            append_u64(&mut s, order_key(wid, did, oid));
            append_u64(&mut s, cid);
            append_u64(&mut s, did);
            append_u64(&mut s, wid);
            append_u64(&mut s, 0);
            if oid < 2100 {
                append_u64(&mut s, u64_rand(rng, 1, 10));
            } else {
                append_u64(&mut s, 0);
            }
            let ol_cnt = u64_rand(rng, 5, 15);
            append_u64(&mut s, ol_cnt);
            append_u64(&mut s, 1);
            txn.insert(table, &s);
            // index.add(order_key(wid, did, oid), txn.insert(table, &s));
            s = String::new();
            for olid in 0..ol_cnt {
                append_u64(&mut s, order_key(wid, did, oid));
                append_u64(&mut s, did);
                append_u64(&mut s, wid);
                append_u64(&mut s, order_line_key(wid, did, oid, olid));
                //println!("Insert order line {},{},{},{}", wid, did, oid, olid);
                append_u64(&mut s, u64_rand(rng, 1, ITEMS) - 1);
                txn.insert(table1, &s);
                // table1.add_range_index(order_line_key(wid, did, oid, olid), txn.insert(table1, &s));
                s = String::new();
                s.reserve(60);
            }
            if oid >= 2100 {
                append_u64(&mut s, order_key(wid, did, oid));
                append_u64(&mut s, did);
                append_u64(&mut s, wid);
                txn.insert(table2, &s);
                // index2.add(new_order_key(wid, did, oid), txn.insert(table2, &s));
                s = String::new();
                s.reserve(60);
            }
        }
    }
    txn.commit();
}
pub fn init_warehouse(buffer: &mut TransactionBuffer, wid: u64) {
    init_table_warehouse(buffer, wid);
    init_table_district(buffer, wid);
    init_table_stock(buffer, wid);
    for did in 0..DISTRICTS_PER_WAREHOUSE {
        init_table_customer(buffer, wid, did);
        init_table_order(buffer, wid, did);
        for cid in 0..CUSTOMERS_PER_DISTRICT {
            init_table_history(buffer, wid, did, cid);
        }
    }
}
pub fn init_tables() {
    TpccIndex::init_tpcc();
    let catalog = Catalog::global();
    let mut buffer = TransactionBuffer::new(catalog, 0);
    init_table_item(&mut buffer);

    let (tx0, rx) = mpsc::channel();

    for i in 0..THREAD_COUNT {
        let tx = tx0.clone();
        thread::spawn(move || {
            let mut buffer = TransactionBuffer::new(catalog, i as u64);

            for wid in 0..WAREHOUSES {
                if (wid as usize) % THREAD_COUNT == i {
                    println!("{}", wid);
                    init_warehouse(&mut buffer, wid);
                }
            }
            tx.send(()).unwrap();
        });
    }
    for _ in 0..THREAD_COUNT {
        let _ = rx.recv().unwrap();
    }
}
