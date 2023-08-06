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
        tuple.save_u64(8, customer_key(wid, did, cid));
        tuple.save_u64(16, did);
        tuple.save_u64(24, wid);
        tuple.save_u64(32, entry_d);
        tuple.save_u64(40, 0);
        tuple.save_u64(48, ol_cnt);
        tuple.save_u64(56, all_local);
        orders.index_insert_by_tuple(&TupleId::from_address(tuple._address()), &tuple).unwrap();
        let mut tuple = txn.alloc(new_orders);
        tuple.save_u64(0, order_key(wid, did, oid));
        tuple.save_u64(8, did);
        tuple.save_u64(16, wid);
        new_orders.index_insert(IndexType::Int64(order_key(wid, did, oid)), &TupleId::from_address(tuple._address())).unwrap();

    }

    for olid in 0..ol_cnt {
        let item = &new_order.items[olid as usize];
        let ol_iid = item.iid;
        let ol_wid = item.wid;
        let ol_quantity = item.quantity;
        let item;
        // println!("11111 {}", ol_iid);

        let schema = &items.schema;
        let column = schema.search_by_name("I_PRICE").unwrap();

        match items.search_tuple_id(&IndexType::Int64(ol_iid)) {
            Ok(tid) => match txn.read_column(items, &tid, column) {
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

        // println!("get item {}", id);
        let i_price = f64::from_le_bytes(item.data.as_slice().try_into().unwrap());
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
        order_lines.index_insert(IndexType::Int64(order_line_key(wid, did, oid, olid)), &TupleId::from_address(tuple._address())).unwrap();
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
            &IndexType::String(customer_last_key(&c_last, c_wid, c_did)),
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
        tuple.save_u64(0, customer_key(wid, did, cid));
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
    let min_orderline_key = order_line_key(wid, did, d_next_o_id-20, 0);
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
                
                // 去重统计本仓库item的消耗量
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
    // 3.检查库存
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

pub fn run_order_status<'a>(
    txn: &mut Transaction<'a>,
    table_list: &'a TableList,
    order_status: &TpccQuery,
) -> bool {
    txn.begin();
    txn.read_only = true;
    let orders = &table_list.orders;
    let order_lines = &table_list.order_lines;
    let customers = &table_list.customers;
    let wid = order_status.wid;
    let did = order_status.did;
    let mut cid = order_status.cid;
    let by_last = order_status.by_last;
    let c_last = &order_status.c_last;
    let customer: TupleVec;
    let schema = &customers.schema;

    // 1.获取顾客信息
    if by_last {
        match customers.search_tuple_id_on_index(
            &IndexType::String(customer_last_key(c_last, wid, did)),
            schema.search_by_name("C_LAST").unwrap(),
        ) {
            Ok(tid) => match txn.read(customers, &tid) {
                Ok(row) => {                    customer = row;
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
        match customers.search_tuple_id(&IndexType::Int64(customer_key(wid, did, cid))) {
            Ok(tid) => match txn.read(customers, &tid) {
                Ok(row) => {
                    customer = row;
                    cid = customer_key(wid, did, cid);
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

    // 2.获取客户最后的一次订单
    let schema = &orders.schema;
    let mut oid = 0;
    // println!("1111");
    match orders.search_tuple_id_on_index(&IndexType::Int64(cid), schema.search_by_name("O_C_ID").unwrap()) {
        
        Ok(tid) => {
            // 
            match txn.read(orders, &tid) {
                Ok(row) => {
                    oid = u64::from_le_bytes(
                        row.get_column_by_id(schema, schema.search_by_name("O_ID").unwrap()).try_into().unwrap());
                    // 此处可根据row打印对应的order内容
                }
                _ => {
                    txn.abort();
                    return false;
                }
            }
        },
        _ => {
            // 没找到订单
            // println!("{}, {:?}", cid, order_status);
            txn.abort();
            return true;
        }
    }
    // println!("2222");

    // 3. 获取orderline信息
    let max_orderline_key = order_line_key(wid, did, oid, MAX_LINES_PER_ORDER);
    let min_orderline_key = order_line_key(wid, did, oid, 0);
    // println!("read {} old from {} to {}", d_next_o_id, min_orderline_key, max_orderline_key);

    let lines:Vec<TupleId>;
    match order_lines.range_tuple_id(&IndexType::Int64(min_orderline_key), &IndexType::Int64(max_orderline_key)) {
        Ok(l) => {
            lines = l;
            // assert!(lines.len() == 0 || lines.len() >= 5);
        },
        _ => {
            txn.abort();
            return false;
        }
    }
    let schema = &order_lines.schema;
    for olid in lines {
        match txn.read(order_lines, &olid) {
            Ok(row) => {       
            // 此处可根据row_id打印对应的orderline内容

                let column: usize = schema.search_by_name("OL_I_ID").unwrap();
                let item_id = u64::from_le_bytes(
                    row.get_column_by_id(schema, column)
                        .try_into()
                        .unwrap(),
                );
                assert!(item_id <= ITEMS);
            }
            _ => {
                txn.abort();
                return false;
            }
        }
    }

    if txn.commit() {
        return true;
    }
    return false;
}

pub fn run_deliver<'a>(
    txn: &mut Transaction<'a>,
    table_list: &'a TableList,
    deliver: &TpccQuery,
) -> bool {
    let orders = &table_list.orders;
    
    let order_lines = &table_list.order_lines;
    let new_orders = &table_list.new_orders;
    let customers = &table_list.customers;
    let wid = deliver.wid;


    for did in 0..DISTRICTS_PER_WAREHOUSE { 

        txn.begin();
        txn.read_only = false;
        let oid:u64;
        let no_tid: TupleId;
        // 1.查找最早的未交付订单
        let max_order_key = order_key(wid, did, ORDERS_PER_DISTRICT);
        let min_order_key = order_key(wid, did, 0);

        match new_orders.last_range_tuple_id(&IndexType::Int64(min_order_key), &IndexType::Int64(max_order_key)) {
            Ok(tid) => {
                no_tid = tid;
                match txn.read(new_orders, &no_tid) {
                    Ok(row) => {
                        let schema = &new_orders.schema;
                        oid = u64::from_le_bytes(row.get_column_by_id(schema, schema.search_by_name("NO_O_ID").unwrap()).try_into().unwrap());
                    }
                    _ => {
                        txn.abort();
                        return false;
                    }
                }
            },
            _ => {
                continue;
            }
        }

        // 2.更新订单表, 获知用户id
        let order: TupleVec;
        let o_tid: TupleId;
        let schema = &orders.schema;

        match orders.search_tuple_id(&IndexType::Int64(oid)) {
            Ok(tid) => match txn.read(orders, &tid) {
                Ok(row) => {
                    o_tid = tid;
                    order = row;
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

        let column = schema.search_by_name("O_C_ID").unwrap();
        let cid = u64::from_le_bytes(
            order
                .get_column_by_id(schema, column)
                .try_into()
                .unwrap(),
        );

        let column = schema.search_by_name("O_CARRIER_ID").unwrap();
        // TODO
        // println!("{:?}", o_tid);
        match txn.update(orders, &o_tid, column, &deliver.o_carrier_id.to_le_bytes()) {
            Ok(_) => {}
            _ => {
                txn.abort();
                return false;
            }
        }
        // 3. 更新orderline数据表
        let oid = oid % ORDERS_PER_DISTRICT;
        let max_orderline_key = order_line_key(wid, did, oid, MAX_LINES_PER_ORDER);
        let min_orderline_key = order_line_key(wid, did, oid, 0);
        // println!("read {} old from {} to {}", oid, min_orderline_key, max_orderline_key);

        let lines:Vec<TupleId>;
        match order_lines.range_tuple_id(&IndexType::Int64(min_orderline_key), &IndexType::Int64(max_orderline_key)) {
            Ok(l) => {
                lines = l;
                // println!("{:?}", lines);
            },
            _ => {
                txn.abort();
                return false;
            }
        }
        let schema = &order_lines.schema;
        let column: usize = schema.search_by_name("OL_DELIVERY_D").unwrap();
        let mut ol_total:f64 = 0.0;
        for olid in lines {
            match txn.read(order_lines, &olid) {
                Ok(row) => {
                    let ol_amount = f64::from_le_bytes(
                        row.get_column_by_id(schema, schema.search_by_name("OL_AMOUNT").unwrap())
                        .try_into().unwrap());
                    ol_total += ol_amount;
                }
                _ => {
                    txn.abort();
                    return false;
                }
            }
            match txn.update(order_lines, &olid, column, &deliver.ol_delivery_d.to_le_bytes()) {
                Ok(_) => {
                }
                _ => {
                    txn.abort();
                    return false;
                }
            }
        }
        //4. 更新客户表数据
        let schema = &customers.schema;
        let column: usize = schema.search_by_name("C_BALANCE").unwrap();
        let c_tid: TupleId;
        let customer: TupleVec;
        match customers.search_tuple_id(&IndexType::Int64(cid)) {
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
        #[cfg(not(feature = "append"))]
        {
            let c_balance = f64::from_le_bytes(
                customer
                    .get_column_by_id(schema, column)
                    .try_into()
                    .unwrap(),
            );
            match txn.update(customers, &c_tid, column, &(c_balance + ol_total).to_le_bytes()) {
                Ok(_) => {
                }
                _ => {
                    txn.abort();
                    return false;
                }
            }
            let column: usize = schema.search_by_name("C_DELIVERY_CNT").unwrap();
            let c_delevery = u64::from_le_bytes(
                customer
                    .get_column_by_id(schema, column)
                    .try_into()
                    .unwrap(),
            );
            match txn.update(customers, &c_tid, column, &(c_delevery + 1).to_le_bytes()) {
                Ok(_) => {
                }
                _ => {
                    txn.abort();
                    return false;
                }
            }
        }
        
        #[cfg(feature = "append")]
        {
            let mut bytes = Vec::<u8>::new();
            let c_balance = f64::from_le_bytes(
                customer
                    .get_column_by_id(schema, column)
                    .try_into()
                    .unwrap(),
            );
            bytes.extend_from_slice(&(c_balance + ol_total).to_le_bytes());
            
            let column: usize = schema.search_by_name("C_YTD_PAYMENT").unwrap();
            bytes.extend_from_slice(customer
                .get_column_by_id(schema, column));
            let column: usize = schema.search_by_name("C_PAYMENT_CNT").unwrap();
            bytes.extend_from_slice(customer
                .get_column_by_id(schema, column));
            let column: usize = schema.search_by_name("C_DELIVERY_CNT").unwrap();
            let c_delevery = u64::from_le_bytes(
                customer
                    .get_column_by_id(schema, column)
                    .try_into()
                    .unwrap(),
            );
            bytes.extend_from_slice(&(c_delevery + 1).to_le_bytes());
            match txn.update(customers, &c_tid, column, bytes.as_slice()) {
                Ok(_) => {
                }
                _ => {
                    txn.abort();
                    return false;
                }
            }
        }
        
        // 5. 删除neworder
        match txn.delete(new_orders, &no_tid) {
            Ok(_) => {
            }
            _ => {
                txn.abort();
                return false;
            }
        }
        // println!("deilver {} success", did);
        if !txn.commit() {
            return false;
        }
    }
    
    // println!("deilver fail");
    return true;
}