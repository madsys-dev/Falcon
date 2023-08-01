use crate::storage::catalog::Catalog;
use crate::storage::row::*;
use crate::storage::schema;
use crate::storage::table::*;
use crate::tpcc::tpcc_query::*;
use crate::tpcc::*;
use crate::transaction::transaction::*;
use crate::transaction::transaction_buffer::*;
use chrono::prelude::*;
use std::convert::TryInto;

pub fn run_new_order<'a>(
    txn: &mut Transaction<'a>,
    table_list: &'a TableList,
    new_order: &TpccQuery,
) -> bool {
    // let mut txn = Transaction::new(buffer, false);
    txn.begin();
    txn.read_only = false;
    let warehouses = &table_list.warehouses;
    let districts = &table_list.districts;
    let customers = &table_list.customers;
    let orders = &table_list.orders;
    let new_orders = &table_list.new_orders;
    let items = &table_list.items;
    let stocks = &table_list.stocks;
    let order_lines = &table_list.order_lines;
    let wid = new_order.wid;
    let did = new_order.did;
    let cid = new_order.cid;
    let entry_d = new_order.entry_d;
    let customer: TupleVec;
    match customers.search_tuple_id(&IndexType::Int64(customer_key(wid, did, cid))) {
        Ok(tid) => match txn.read(customers, &tid) {
            Ok(row) => {
                customer = row;
            }
            _ => {
                txn.abort();
                return false;
            }
        },
        _ => {
            txn.abort();
            return false;
        }
    }

    let schema = &customers.schema;
    let c_discount = f64::from_le_bytes(
        customer
            .get_column_by_id(schema, schema.search_by_name("C_DISCOUNT").unwrap())
            .try_into()
            .unwrap(),
    );
    let tid = warehouses.search_tuple_id(&IndexType::Int64(wid)).unwrap();
    let warehouse: TupleVec;
    match txn.read(warehouses, &tid) {
        Ok(row) => {
            warehouse = row;
        }
        _ => {
            txn.abort();
            return false;
        }
    }

    let schema = &warehouses.schema;
    let w_tax = f64::from_le_bytes(
        warehouse
            .get_column_by_id(schema, schema.search_by_name("W_TAX").unwrap())
            .try_into()
            .unwrap(),
    );
    let tid = districts
        .search_tuple_id(&IndexType::Int64(district_key(wid, did)))
        .unwrap();
    let district: TupleVec;
    match txn.read(districts, &tid) {
        Ok(row) => {
            district = row;
        }
        _ => {
            txn.abort();
            return false;
        }
    }
    let schema = &districts.schema;
    let column = schema.search_by_name("D_NEXT_O_ID").unwrap();
    let oid = u64::from_le_bytes(
        district
            .get_column_by_id(schema, column)
            .try_into()
            .unwrap(),
    );
    let d_tax = f64::from_le_bytes(
        district
            .get_column_by_id(schema, schema.search_by_name("D_TAX").unwrap())
            .try_into()
            .unwrap(),
    );
    let next_oid = oid + 1;
    match txn.update(districts, &tid, column, &next_oid.to_le_bytes()) {
        Ok(_) => {
            // println!("next_oid: {}", next_oid);
        }
        _ => {
            txn.abort();
            return false;
        }
    }
    // println!("---------------");

    let ol_cnt = new_order.items.len() as u64;
    let mut all_local = 1;
    for olid in 0..ol_cnt {
        if new_order.items[olid as usize].wid != wid {
            all_local = 0;
        }
    }
    {
        let mut tuple = txn.alloc(orders);
        tuple.save_u64(0, order_key(wid, did, oid));
        tuple.save_u64(8, cid);
        tuple.save_u64(16, did);
        tuple.save_u64(24, wid);
        tuple.save_u64(32, entry_d);
        tuple.save_u64(40, 0);
        tuple.save_u64(48, ol_cnt);
        tuple.save_u64(56, all_local);

        let mut tuple = txn.alloc(new_orders);
        tuple.save_u64(0, order_key(wid, did, oid));
        tuple.save_u64(8, did);
        tuple.save_u64(16, wid);
    }

    for olid in 0..ol_cnt {
        let item = &new_order.items[olid as usize];
        let ol_iid = item.iid;
        let ol_wid = item.wid;
        let ol_quantity = item.quantity;
        let item: TupleVec;
        // println!("11111 {}", ol_iid);

        match items.search_tuple_id(&IndexType::Int64(ol_iid)) {
            Ok(tid) => match txn.read(items, &tid) {
                Ok(row) => {
                    // println!("22222 {}, {}", ol_iid, tid.get_address());
                    item = row;
                }
                _ => {
                    txn.abort();
                    return false;
                }
            },
            _ => {
                txn.abort();
                return false;
            }
        }
        // println!("++++++++++++++++++++");

        let schema = &items.schema;
        let id = schema.search_by_name("I_PRICE").unwrap();
        // println!("get item {}", id);
        let i_price = f64::from_le_bytes(item.get_column_by_id(schema, id).try_into().unwrap());
        let stock: TupleVec;
        let s_tid: TupleId;
        match stocks.search_tuple_id(&IndexType::Int64(stock_key(ol_wid, ol_iid))) {
            Ok(tid) => {
                s_tid = tid.clone();
                match txn.read(stocks, &tid) {
                    Ok(row) => {
                        stock = row;
                    }
                    _ => {
                        txn.abort();
                        return false;
                    }
                }
            }
            _ => {
                txn.abort();
                return false;
            }
        }
        let schema = &stocks.schema;
        let column = schema.search_by_name("S_QUANTITY").unwrap();
        let s_quantity =
            u64::from_le_bytes(stock.get_column_by_id(schema, column).try_into().unwrap());
        let new_s_quantity = if s_quantity > ol_quantity {
            s_quantity - ol_quantity
        } else {
            s_quantity + 91 - ol_quantity
        };
        let _ol_amount = ol_quantity as f64 * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount); //Full schema

        match txn.update(stocks, &s_tid, column, &new_s_quantity.to_le_bytes()) {
            Ok(_) => {}
            _ => {
                txn.abort();
                return false;
            }
        }

        let mut tuple = txn.alloc(order_lines);
        tuple.save_u64(0, order_key(wid, did, oid));
        tuple.save_u64(8, did);
        tuple.save_u64(16, wid);
        tuple.save_u64(24, order_line_key(wid, did, oid, olid));
        tuple.save_u64(32, ol_iid);

        if IS_FULL_SCHEMA {
            tuple.save_u64(40, 0); //OL_SUPPLY_WID
            tuple.save_u64(48, Local::now().timestamp_nanos() as u64); //OL_DELIVERY_ID
            tuple.save_u64(56, s_quantity);
            tuple.save_f64(64, _ol_amount);
            tuple.save_u64(72, 0);
        }
        order_lines.index_insert(IndexType::Int64(order_line_key(wid, did, oid, ol_iid)), &TupleId::from_address(tuple._address())).unwrap();
    }
    // println!("add {} ol from {} to {}", next_oid, order_line_key(wid, did, oid, 0), order_line_key(wid, did, oid, ol_cnt));
    txn.commit()
}
pub fn run_payment<'a>(
    txn: &mut Transaction<'a>,
    table_list: &'a TableList,
    payment: &TpccQuery,
) -> bool {
    // let mut txn = Transaction::new(buffer, false);
    txn.begin();
    txn.read_only = false;

    let warehouses = &table_list.warehouses;
    let districts = &table_list.districts;
    let customers = &table_list.customers;
    let histories = &table_list.histories;
    let wid = payment.wid;
    let did = payment.did;
    let h_amount = payment.h_amount;
    let warehouse: TupleVec;
    let w_tid: TupleId;
    match warehouses.search_tuple_id(&IndexType::Int64(wid)) {
        Ok(tid) => match txn.read(warehouses, &tid) {
            Ok(row) => {
                w_tid = tid;
                warehouse = row;
            }
            _ => {
                txn.abort();
                return false;
            }
        },
        _ => {
            txn.abort();
            return false;
        }
    }
    let schema = &warehouses.schema;
    let column = schema.search_by_name("W_YTD").unwrap();
    let w_ytd = f64::from_le_bytes(
        warehouse
            .get_column_by_id(schema, column)
            .try_into()
            .unwrap(),
    );
    let next_w_ytd = w_ytd + h_amount;
    match txn.update(warehouses, &w_tid, column, &next_w_ytd.to_le_bytes()) {
        Ok(_) => {}
        _ => {
            txn.abort();
            return false;
        }
    }
    let district: TupleVec;
    let d_tid: TupleId;
    match districts.search_tuple_id(&IndexType::Int64(district_key(wid, did))) {
        Ok(tid) => match txn.read(districts, &tid) {
            Ok(row) => {
                d_tid = tid;
                district = row;
            }
            _ => {
                txn.abort();
                return false;
            }
        },
        _ => {
            txn.abort();
            return false;
        }
    }
    let schema = &districts.schema;
    let column = schema.search_by_name("D_YTD").unwrap();
    let d_ytd = f64::from_le_bytes(
        district
            .get_column_by_id(schema, column)
            .try_into()
            .unwrap(),
    );
    let next_d_ytd = d_ytd + h_amount;
    match txn.update(districts, &d_tid, column, &next_d_ytd.to_le_bytes()) {
        Ok(_) => {}
        _ => {
            txn.abort();
            return false;
        }
    }
    let c_wid = payment.c_wid;
    let c_did = payment.c_did;
    let by_last = payment.by_last;
    let customer: TupleVec;
    let c_tid: TupleId;
    let cid: u64;
    let schema = &customers.schema;
    if by_last {
        let c_last = payment.c_last.clone();
        match customers.search_tuple_id_on_index(
            &IndexType::String(customer_last_key(c_last, c_wid, c_did)),
            schema.search_by_name("C_LAST").unwrap(),
        ) {
            Ok(tid) => match txn.read(customers, &tid) {
                Ok(row) => {
                    c_tid = tid;
                    customer = row;
                }
                _ => {
                    txn.abort();
                    return false;
                }
            },
            _ => {
                txn.abort();
                return false;
            }
        }
        cid = cid_from_key(u64::from_le_bytes(
            customer
                .get_column_by_id(schema, schema.search_by_name("C_ID").unwrap())
                .try_into()
                .unwrap(),
        ));
    } else {
        cid = payment.cid;
        match customers.search_tuple_id(&IndexType::Int64(customer_key(c_wid, c_did, cid))) {
            Ok(tid) => match txn.read(customers, &tid) {
                Ok(row) => {
                    c_tid = tid;
                    customer = row;
                }
                _ => {
                    txn.abort();
                    return false;
                }
            },
            _ => {
                txn.abort();
                return false;
            }
        }
    }
    let column = schema.search_by_name("C_BALANCE").unwrap();
    let c_balance = f64::from_le_bytes(
        customer
            .get_column_by_id(schema, column)
            .try_into()
            .unwrap(),
    );
    let new_c_balance = c_balance + h_amount;

    match txn.update(customers, &c_tid, column, &new_c_balance.to_le_bytes()) {
        Ok(_) => {}
        _ => {
            txn.abort();
            return false;
        }
    }
    {
        let mut tuple = txn.alloc(histories);
        tuple.save_u64(0, cid);
        tuple.save_u64(8, c_did);
        tuple.save_u64(16, c_wid);
        if IS_FULL_SCHEMA {
            tuple.save_u64(24, did);
            tuple.save_u64(32, wid);
            tuple.save_u64(40, 0);
            tuple.save(48, &h_amount.to_le_bytes());
            tuple.save(56, &vec![0; 24]);
        } else {
            tuple.save_u64(24, 0);
            tuple.save(32, &h_amount.to_le_bytes());
        }
    }
    if txn.commit() {
        return true;
    }
    return false;
}
pub fn run_stock_level<'a>(
    txn: &mut Transaction<'a>,
    table_list: &'a TableList,
    stock_level: &TpccQuery,
) -> bool {
    txn.begin();
    txn.read_only = true;
    let districts = &table_list.districts;
    let order_lines = &table_list.order_lines;
    let stocks = &table_list.stocks;
    let district: TupleVec;
    let wid = stock_level.wid;
    let did = stock_level.did;

    // 1.获取街区表 d_next_o_id;
    let d_tid: TupleId;
    match districts.search_tuple_id(&IndexType::Int64(district_key(wid, did))) {
        Ok(tid) => match txn.read(districts, &tid) {
            Ok(row) => {
                d_tid = tid;
                district = row;
            }
            _ => {
                txn.abort();
                return false;
            }
        },
        _ => {
            txn.abort();
            return false;
        }
    }
    let schema = &districts.schema;
    let column: usize = schema.search_by_name("D_NEXT_O_ID").unwrap();
    let d_next_o_id = u64::from_le_bytes(
        district
            .get_column_by_id(schema, column)
            .try_into()
            .unwrap(),
    );
    // 2.获取街区内20个订单库存不足的去重货物种类数量
    let max_orderline_key = order_line_key(wid, did, d_next_o_id-1, MAX_LINES_PER_ORDER);
    let min_orderline_key = order_line_key(wid, did, d_next_o_id-20, 1);
    // println!("read {} old from {} to {}", d_next_o_id, min_orderline_key, max_orderline_key);

    let lines:Vec<TupleId>;
    match order_lines.range_tuple_id(&IndexType::Int64(min_orderline_key), &IndexType::Int64(max_orderline_key)) {
        Ok(l) => {
            lines = l;
        },
        _ => {
            txn.abort();
            return false;
        }
    }
    let mut itemid2quantity = std::collections::HashMap::<u64, u64>::new();
    let schema = &order_lines.schema;
    for olid in lines {
        match txn.read(order_lines, &olid) {
            Ok(row) => {
                
                // re-count the consumption of items in this warehouse
                let column: usize = schema.search_by_name("OL_W_ID").unwrap();
                let w_id = u64::from_le_bytes(
                    row.get_column_by_id(schema, column)
                        .try_into()
                        .unwrap(),
                );
                if w_id == wid {
                    let column: usize = schema.search_by_name("OL_QUANTITY").unwrap();
                    let quantity = u64::from_le_bytes(
                        row.get_column_by_id(schema, column)
                            .try_into()
                            .unwrap(),
                    );
                    let column: usize = schema.search_by_name("OL_I_ID").unwrap();
                    let item_id = u64::from_le_bytes(
                        row.get_column_by_id(schema, column)
                            .try_into()
                            .unwrap(),
                    );
                    if !itemid2quantity.contains_key(&item_id) {
                        itemid2quantity.insert(item_id, 0);
                    }
                    itemid2quantity.insert(item_id, itemid2quantity.get(&item_id).unwrap() + quantity);
                }
            }
            _ => {
                txn.abort();
                return false;
            }
        }
    }
    let mut scnt = 0;
    for (item_id, _) in itemid2quantity.iter() {
        let stock: TupleColumnVec;
        let schema = &stocks.schema;
        let column = schema.search_by_name("S_QUANTITY").unwrap();
        match stocks.search_tuple_id(&IndexType::Int64(stock_key(wid, *item_id))) {
            Ok(tid) => {
                match txn.read_column(stocks, &tid, column) {
                    Ok(row) => {
                        stock = row;
                    }
                    _ => {
                        txn.abort();
                        return false;
                    }
                }
            }
            _ => {
                txn.abort();
                return false;
            }
        }
        let s_quantity =
            u64::from_le_bytes(stock.data.as_slice().try_into().unwrap());
        if s_quantity < stock_level.threshold {
            scnt += 1;
        }
    }
    assert!(scnt >= 0);
    if txn.commit() {
        return true;
    }
    return false;
}