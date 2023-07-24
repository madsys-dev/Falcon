#[cfg(test)]
mod tests {
    use crate::storage::catalog::Catalog;
    use crate::storage::nvm_file::NVMTableStorage;
    use crate::storage::row::TupleError;
    use crate::storage::table::IndexType;
    use crate::tpcc::tpcc::*;
    // use crate::tpcc::tpcc_index::TpccIndex;
    use crate::tpcc::tpcc_init;
    use crate::tpcc::*;
    use crate::transaction::transaction::Transaction;
    use crate::transaction::transaction_buffer::TransactionBuffer;
    use crate::transaction::*;
    use crate::Error;
    use std::convert::TryInto;

    #[test]
    fn test_isolation() {
        NVMTableStorage::init_test_database();
        Catalog::init_catalog();
        if IS_FULL_SCHEMA {
            tpcc_init::init_schema("config/schema_file/TPCC_full_schema.txt");
        } else {
            tpcc_init::init_schema("config/schema_file/TPCC_short_schema.txt");
        }
        init_tables();
        let districts = &Catalog::global().get_table("DISTRICT");
        // let districts_index = TpccIndex::districts_index();

        // test1 

        let mut buffer0 = TransactionBuffer::new(Catalog::global(), 1);
        let mut buffer1 = TransactionBuffer::new(Catalog::global(), 2);
        let mut buffer2 = TransactionBuffer::new(Catalog::global(), 3);
        let mut buffer3 = TransactionBuffer::new(Catalog::global(), 4);
        let mut t0 = Transaction::new(&mut buffer0, false);
        t0.begin();
        let wid = 0;
        let did = 0;
        //let cid = 0;
        let tid = districts
            .search_tuple_id(&IndexType::Int64(district_key(wid, did)))
            .unwrap();
        let d0 = t0.read(districts, &tid).unwrap();
        t0.commit();
        let next_oid0 = u64::from_le_bytes(
            d0.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        let mut t1 = Transaction::new(&mut buffer1, false);
        t1.begin();
        t1.update(districts, &tid, 10, &(next_oid0 + 1).to_le_bytes())
            .unwrap();
        let mut t2 = Transaction::new(&mut buffer2, false);
        t2.begin();
        let d1 = t2.read(districts, &tid).unwrap();
        t2.commit();
        assert_eq!(t1.commit(), false);
        let mut t3 = Transaction::new(&mut buffer3, false);
        t3.begin();
        let d2 = t3.read(districts, &tid).unwrap();
        t3.commit();
        let next_oid1 = u64::from_le_bytes(
            d1.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        let next_oid2 = u64::from_le_bytes(
            d2.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        println!("next_oid2 {}", next_oid2);

        assert_eq!(next_oid0, next_oid1);
        assert_eq!(next_oid0, next_oid2);
        // test2
        // t0: read districts commit
        // t1: update districts
        // t2: read districts
        // t2: commit, t2 = t0
        // t1: abort
        // t3: read
        // t3: commit, t3 = t0
        let mut buffer0 = TransactionBuffer::new(Catalog::global(), 0);
        let mut buffer1 = TransactionBuffer::new(Catalog::global(), 1);
        let mut buffer2 = TransactionBuffer::new(Catalog::global(), 2);
        let mut buffer3 = TransactionBuffer::new(Catalog::global(), 3);
        let mut t0 = Transaction::new(&mut buffer0, false);
        t0.begin();
        let wid = 0;
        let did = 0;
        // let cid = 0;
        let tid = districts
            .search_tuple_id(&IndexType::Int64(district_key(wid, did)))
            .unwrap();

        let d0 = t0.read(districts, &tid).unwrap();
        t0.commit();
        let next_oid0 = u64::from_le_bytes(
            d0.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        // println!("{}", next_oid0);

        let mut t1 = Transaction::new(&mut buffer1, false);
        t1.begin();
        t1.update(districts, &tid, 10, &(next_oid0 + 1).to_le_bytes())
            .unwrap();
        let mut t2 = Transaction::new(&mut buffer2, false);
        t2.begin();
        let d1 = t2.read(districts, &tid).unwrap();
        t2.commit();
        // let next_oid1 = u64::from_le_bytes(
        //     d1.get_column_by_id(&districts.schema, 10)
        //         .try_into()
        //         .unwrap(),
        // );
        // println!("{}", next_oid0);
        // assert_eq!(next_oid0, next_oid1);
        t1.abort();
        // println!("{}", next_oid1);

        let mut t3 = Transaction::new(&mut buffer3, false);
        t3.begin();
        let d2 = t3.read(districts, &tid).unwrap();
        t3.commit();
        let next_oid1 = u64::from_le_bytes(
            d1.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        let next_oid2 = u64::from_le_bytes(
            d2.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        assert_eq!(next_oid0, next_oid1);
        assert_eq!(next_oid0, next_oid2);
        // test3
        let mut buffer0 = TransactionBuffer::new(Catalog::global(), 0);
        let mut buffer1 = TransactionBuffer::new(Catalog::global(), 1);
        let mut buffer2 = TransactionBuffer::new(Catalog::global(), 2);
        let mut buffer3 = TransactionBuffer::new(Catalog::global(), 3);
        let mut buffer4 = TransactionBuffer::new(Catalog::global(), 4);
        let mut buffer5 = TransactionBuffer::new(Catalog::global(), 5);
        let mut t0 = Transaction::new(&mut buffer0, false);
        t0.begin();
        let wid = 0;
        let did = 0;
        // let cid = 0;
        let tid = districts
            .search_tuple_id(&IndexType::Int64(district_key(wid, did)))
            .unwrap();
        let d0 = t0.read(districts, &tid).unwrap();
        t0.commit();
        let next_oid0 = u64::from_le_bytes(
            d0.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        let mut t1 = Transaction::new(&mut buffer1, false);
        t1.begin();
        t1.update(districts, &tid, 10, &(next_oid0 + 1).to_le_bytes())
            .unwrap();
        println!("11111");
        let mut t2 = Transaction::new(&mut buffer2, false);
        t2.begin();
        t2.update(districts, &tid, 10, &(next_oid0 + 1).to_le_bytes())
            .unwrap();
        t1.commit();
        assert_eq!(t2.commit(), true);
        let mut t3 = Transaction::new(&mut buffer3, false);
        t3.begin();
        let d1 = t3.read(districts, &tid).unwrap();
        t3.commit();
        let next_oid1 = u64::from_le_bytes(
            d1.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        let mut t4 = Transaction::new(&mut buffer4, false);
        t4.begin();
        t4.update(districts, &tid, 10, &(next_oid1 + 1).to_le_bytes())
            .unwrap();
        t4.commit();
        let mut t5 = Transaction::new(&mut buffer5, false);
        t5.begin();
        let d2 = t5.read(districts, &tid).unwrap();
        t5.commit();
        let next_oid2 = u64::from_le_bytes(
            d2.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        assert_eq!(next_oid0 + 1, next_oid1);
        assert_eq!(next_oid0 + 2, next_oid2);
        // test4 
        // t0: read next_oid0
        // t1: update
        // t2: update -> false
        // t1: abort
        // t3: read next_oid
        // t4: update -> commit
        // t5: read next_oid+1

        let mut buffer0 = TransactionBuffer::new(Catalog::global(), 0);
        let mut buffer1 = TransactionBuffer::new(Catalog::global(), 1);
        let mut buffer2 = TransactionBuffer::new(Catalog::global(), 2);
        let mut buffer3 = TransactionBuffer::new(Catalog::global(), 3);
        let mut buffer4 = TransactionBuffer::new(Catalog::global(), 4);
        let mut buffer5 = TransactionBuffer::new(Catalog::global(), 5);
        let mut t0 = Transaction::new(&mut buffer0, false);
        println!("22222");

        t0.begin();
        let wid = 0;
        let did = 0;
        // let cid = 0;
        let tid = districts
            .search_tuple_id(&IndexType::Int64(district_key(wid, did)))
            .unwrap();

        let d0 = t0.read(districts, &tid).unwrap();
        t0.commit();
        let next_oid0 = u64::from_le_bytes(
            d0.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        let mut t1 = Transaction::new(&mut buffer1, false);
        t1.begin();
        t1.update(districts, &tid, 10, &(next_oid0 + 1).to_le_bytes())
            .unwrap();
        let mut t2 = Transaction::new(&mut buffer2, false);
        t2.begin();
        t2.update(districts, &tid, 10, &(next_oid0 + 1).to_le_bytes())
            .unwrap();
        t2.abort();
        t1.abort();
        let mut t3 = Transaction::new(&mut buffer3, false);
        t3.begin();
        let d1 = t3.read(districts, &tid).unwrap();
        t3.commit();
        let next_oid1 = u64::from_le_bytes(
            d1.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        let mut t4 = Transaction::new(&mut buffer4, false);
        t4.begin();
        t4.update(districts, &tid, 10, &(next_oid1 + 1).to_le_bytes())
            .unwrap();
        t4.commit();
        let mut t5 = Transaction::new(&mut buffer5, false);
        t5.begin();
        let d2 = t5.read(districts, &tid).unwrap();
        t5.commit();
        let next_oid2 = u64::from_le_bytes(
            d2.get_column_by_id(&districts.schema, 10)
                .try_into()
                .unwrap(),
        );
        assert_eq!(next_oid0, next_oid1);
        assert_eq!(next_oid0 + 1, next_oid2);
        // TODO:test5 and test6

        // test7
        // T0 begin
        // T0 read i0
        // T0 read i1
        // T0 commit
        // T1 begin
        // T1 read t0 -> t00
        // T2 begin
        // T2 update t0 -> t01
        // T2 update t1 -> t11
        // T2 commit
        // T1 read t0  assert t00
        // T1 read t1  assert t10
        // T1 commit
        // T3 begin
        // T3 read t0  assert t01
        // T3 read t1  assert t11
        // T3 commit
        println!("11111");
        let mut buffer0 = TransactionBuffer::new(Catalog::global(), 0);
        let mut buffer1 = TransactionBuffer::new(Catalog::global(), 1);
        let mut buffer2 = TransactionBuffer::new(Catalog::global(), 2);
        let mut buffer3 = TransactionBuffer::new(Catalog::global(), 2);
        let items = &Catalog::global().get_table("ITEM");
        let mut t0 = Transaction::new(&mut buffer0, false);
        t0.begin();
        let iid0 = 0;
        let iid1 = 1;

        let tid0 = items
            .search_tuple_id(&IndexType::Int64(item_key(iid0)))
            .unwrap();
        let tid1 = items
            .search_tuple_id(&IndexType::Int64(item_key(iid1)))
            .unwrap();
        println!("44444");
        let i0 = t0.read(items, &tid0).unwrap();
        let i1 = t0.read(items, &tid1).unwrap();
        let price0 = f64::from_le_bytes(i0.get_column_by_id(&items.schema, 3).try_into().unwrap());
        let price1 = f64::from_le_bytes(i1.get_column_by_id(&items.schema, 3).try_into().unwrap());
        let new_price0 = price0 * 1.1;
        let new_price1 = price1 * 1.1;
        t0.commit();
        let mut t1 = Transaction::new(&mut buffer1, false);
        println!("44144");

        t1.begin();
        let i00 = t1.read(items, &tid0).unwrap();
        let price00 =
            f64::from_le_bytes(i00.get_column_by_id(&items.schema, 3).try_into().unwrap());
        assert_eq!(price0, price00);
        let mut t2 = Transaction::new(&mut buffer2, false);
        t2.begin();
        t2.update(items, &tid0, 3, &new_price0.to_le_bytes())
            .unwrap();
        t2.update(items, &tid1, 3, &new_price1.to_le_bytes())
            .unwrap();
        t2.commit();
        println!("44244");

        let i01 = t1.read(items, &tid0).unwrap();
        let i10 = t1.read(items, &tid1).unwrap();
        t1.commit();
        println!("44344");

        let price01 =
            f64::from_le_bytes(i01.get_column_by_id(&items.schema, 3).try_into().unwrap());
        let price10 =
            f64::from_le_bytes(i10.get_column_by_id(&items.schema, 3).try_into().unwrap());
        println!("55555");

        assert_eq!(price01, price00);
        assert_eq!(price10, price1);
        let mut t3 = Transaction::new(&mut buffer3, false);
        t3.begin();
        let i00 = t3.read(items, &tid0).unwrap();
        let i10 = t3.read(items, &tid1).unwrap();
        let price00 =
            f64::from_le_bytes(i00.get_column_by_id(&items.schema, 3).try_into().unwrap());
        let price10 =
            f64::from_le_bytes(i10.get_column_by_id(&items.schema, 3).try_into().unwrap());
        assert_eq!(price00, new_price0);
        assert_eq!(price10, new_price1);
        //TODO:test8 and test9
    }
}
