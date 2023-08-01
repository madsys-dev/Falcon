use bitvec::macros::internal::funty::IsNumber;

use crate::storage::catalog::Catalog;
use crate::storage::row::*;
use crate::storage::table::*;
use crate::tpcc::tpcc_query::*;
use crate::tpcc::*;
use crate::transaction::transaction::*;
use crate::transaction::transaction_buffer::*;
use crate::utils::executor::mem::prefetch_read;
use std::convert::TryInto;

pub async fn run_new_order<'a>(
    txn: &mut Transaction<'a>,
    table_list: &'a TableList,
    new_order: &TpccQuery,
) -> bool {
    // let mut txn = Transaction::new(buffer, false);
    txn.begin();
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
        Ok(tid) => {
            txn.prefetch_read(customers, &tid).await;
            match txn.read(customers, &tid) {
                Ok(row) => {
                    customer = row;
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
    let schema = &customers.schema;
    let c_discount = f64::from_le_bytes(
        customer
            .get_column_by_id(schema, schema.search_by_name("C_DISCOUNT").unwrap())
            .try_into()
            .unwrap(),
    );
    let tid = warehouses.search_tuple_id(&IndexType::Int64(wid)).unwrap();

    let warehouse: TupleVec;
    txn.prefetch_read(warehouses, &tid).await;
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
    txn.prefetch_read(districts, &tid).await;
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
        Ok(_) => {}
        _ => {
            txn.abort();
            return false;
        }
    }

    let ol_cnt = new_order.items.len() as u64;
    let mut all_local = 1;
    for olid in 0..ol_cnt {
        if new_order.items[olid as usize].wid != wid {
            all_local = 0;
        }
    }
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

    for olid in 0..ol_cnt {
        let item = &new_order.items[olid as usize];
        let ol_iid = item.iid;
        let ol_wid = item.wid;
        let ol_quantity = item.quantity;
        let item: TupleVec;
        match items.search_tuple_id(&IndexType::Int64(ol_iid)) {
            Ok(tid) => {
                txn.prefetch_read(items, &tid).await;

                match txn.read(items, &tid) {
                    Ok(row) => {
                        item = row;
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
        let schema = &items.schema;
        let i_price = f64::from_le_bytes(
            item.get_column_by_id(schema, schema.search_by_name("I_PRICE").unwrap())
                .try_into()
                .unwrap(),
        );
        let stock: TupleVec;
        let s_tid: TupleId;
        match stocks.search_tuple_id(&IndexType::Int64(stock_key(ol_wid, ol_iid))) {
            Ok(tid) => {
                s_tid = tid.clone();
                txn.prefetch_read(stocks, &tid).await;
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
        match txn.update(stocks, &s_tid, column, &new_s_quantity.to_le_bytes()) {
            Ok(_) => {}
            _ => {
                txn.abort();
                return false;
            }
        }
        let _ol_amount = ol_quantity as f64 * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount); //Full schema

        let mut tuple = txn.alloc(order_lines);
        tuple.save_u64(0, order_key(wid, did, oid));
        tuple.save_u64(8, did);
        tuple.save_u64(16, wid);
        tuple.save_u64(24, olid);
        tuple.save_u64(32, ol_iid);
    }

    txn.commit();
    true
}
pub async fn run_payment<'a>(
    txn: &mut Transaction<'a>,
    table_list: &'a TableList,
    payment: &TpccQuery,
) -> bool {
    txn.begin();
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
        Ok(tid) => {
            txn.prefetch_read(warehouses, &tid).await;
            match txn.read(warehouses, &tid) {
                Ok(row) => {
                    w_tid = tid;
                    warehouse = row;
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
        Ok(tid) => {
            txn.prefetch_read(districts, &tid).await;

            match txn.read(districts, &tid) {
                Ok(row) => {
                    d_tid = tid;
                    district = row;
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
            Ok(tid) => {
                txn.prefetch_read(customers, &tid).await;

                match txn.read(customers, &tid) {
                    Ok(row) => {
                        c_tid = tid;
                        customer = row;
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
        cid = cid_from_key(u64::from_le_bytes(
            customer
                .get_column_by_id(schema, schema.search_by_name("C_ID").unwrap())
                .try_into()
                .unwrap(),
        ));
    } else {
        cid = payment.cid;
        match customers.search_tuple_id(&IndexType::Int64(customer_key(c_wid, c_did, cid))) {
            Ok(tid) => {
                txn.prefetch_read(customers, &tid).await;
                match txn.read(customers, &tid) {
                    Ok(row) => {
                        c_tid = tid;
                        customer = row;
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
    let mut tuple = txn.alloc(histories);
    tuple.save_u64(0, cid);
    tuple.save_u64(8, c_did);
    tuple.save_u64(16, c_wid);
    tuple.save_u64(24, 0);
    tuple.save(32, &h_amount.to_le_bytes());
    txn.commit();
    true
}
