// use n2db::storage::nvm_file::NVMTableStorage;
// use n2db::storage::table::TupleId;
// use n2db::tpcc::u64_rand;
// use n2db::utils::{file, io};
// use std::ptr;
// use std::sync::atomic::AtomicU64;
// use std::sync::{mpsc, Arc, Barrier};
// use std::time::{Duration, SystemTime};
// use std::{thread, time};
// use n2db::storage::index::nbtree::NBTree;
// use n2db::storage::index::dash::Dash;
// use n2db::storage::index::dashstring::DashString;
// use chrono::prelude::*;

// use n2db::c::ffi::{init, plus};
use bztree::BzTree;

fn main() {
    let tree = BzTree::<u64, u64>::default();
    let guard = crossbeam_epoch::pin();

    assert_eq!(tree.upsert(3, 1, &guard), None);
    assert_eq!(tree.upsert(3, 5, &guard), Some(&1));
    tree.insert(6, 10, &guard);
    tree.insert(9, 12, &guard);

    // assert!(matches!(tree.delete(&key1, &guard), Some(&10)));

    // let key2 = "key_2".to_string();
    // tree.insert(key2.clone(), 2, &guard);
    // assert!(tree.compute(&key2, |(_, v)| Some(v + 1), &guard));
    // assert!(matches!(tree.get(&key2, &guard), Some(&3)));

    // assert!(tree.compute(&key2, |(_, v)| {
    // if *v == 3 {
    //     None
    // } else {
    //     Some(v + 1)
    // }
    // }, &guard));
    // assert!(matches!(tree.get(&key2, &guard), None));

    let r1 = tree.range(2..8, &guard).last();
    println!("{:?}", r1);

    println!("{:?}", tree.first(&guard));
    tree.pop_first(&guard);
    println!("{:?}", tree.first(&guard));
    tree.pop_first(&guard);
    println!("{:?}", tree.first(&guard));
    
    // ------------datetime------
    // let dt = Local::now();
    // println!("dt: {}", dt);
    // println!("dt: {}", dt.timestamp_millis());
    // unsafe {
    //     init();
    // }

    // ------------dash------

    // // let dash = DashString::new();
    // // let v = TupleId{page_start: AtomicU64::new(20)};
    // // let v2 = TupleId{page_start: AtomicU64::new(30)};

    // // unsafe{ println!("a + b = {}", plus(1, 2));}

    // // let key = String::from("aaa");
    // // dash.insert(key.clone(), key.len(), v);
    // // let b = dash.get(key.clone(), key.len()).unwrap();
    // // println!("{}", b.get_address());

    // // dash.update(key.clone(), key.len(), v2);
    // // let b = dash.get(key.clone(), key.len()).unwrap();
    // // println!("{}", b.get_address());

    // let v3 = TupleId {
    //     page_start: AtomicU64::new(40),
    // };

    // let dash2 = Dash::<u64>::new();
    // dash2.insert(1, v3);
    // let b = dash2.get(&1).unwrap();
    // println!("{}", b.get_address());

    // // let b = dash.get(key.clone(), key.len()).unwrap();
    // // println!("{}", b.get_address());

    // // let v4 = TupleId{page_start: AtomicU64::new(50)};
    // // let dash3 = Dash::<u64>::new();
    // // dash3.insert(1, v4);
    // // let b = dash3.get(&1).unwrap();
    // // println!("{}", b.get_address());

    // let b = dash2.get(&1).unwrap();
    // println!("{}", b.get_address());
    // let mut handles = Vec::with_capacity(10);

    // ----------NBTREE
    // let tree = NBTree::<u64>::new();
    // let index = Arc::new(tree);
    // let barrier = Arc::new(Barrier::new(1));

    // let t1 = index.clone();
    // let b1 = barrier.clone();
    // handles.push(thread::spawn(move || {
    //     t1.init(0);
    //     b1.wait();
    //     for i in 0..100 {
    //         t1.insert(i, TupleId{page_start: AtomicU64::new(i)});
    //     }
    //     b1.wait();
    //     println!("{}", t1.get(&11015).unwrap().get_address());

    // }));
    // let t2 = index.clone();
    // let b2 = barrier.clone();
    // handles.push(thread::spawn(move || {
    //     t2.init(2);
    //     b2.wait();
    //     for i in 100..200 {
    //         t2.insert(i, TupleId{page_start: AtomicU64::new(i)});

    //     }
    //     b2.wait();

    //     println!("{}", t2.get(&1015).unwrap().get_address());

    // }));
    // for handle in handles {
    //     handle.join().unwrap();
    // }
    // let t = index.clone();
    // t.init(2);
    // println!("{}", t.get(&10115).unwrap().get_address());

    // for i in 0..10000 {
    //     tree.insert(i, TupleId{page_start: AtomicU64::new(i)});
    // }
    // println!("{}", tree.get(&1015).unwrap().get_address());
}
