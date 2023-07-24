use crate::config::Address;
use std::arch::asm;
use std::ptr;

pub unsafe fn read<T>(address: Address) -> T {
    ptr::read(address as *const T)
}
pub unsafe fn read_slice<'a, T>(address: Address, len: usize) -> &'a [T] {
    std::slice::from_raw_parts(address as *const T, len)
}
pub unsafe fn clwb(ptr: *const u8) {
    asm!("clwb [{0}] ", in(reg) ptr);
}
pub unsafe fn write<T>(address: Address, data: T) {
    ptr::write(address as *mut T, data);
}

pub unsafe fn write_slice<T>(address: Address, data: &[T]) {
    let count = data.len();
    ptr::copy(data.as_ptr(), address as _, count);
}

#[cfg(test)]
mod tests {
    use crate::storage::nvm_file::NVMTableStorage;
    use crate::utils::{file, io};
    use crate::ycsb::u64_rand_new;
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::SystemTime;

    #[test]
    fn test_clwb() {
        //taskset -c 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59 cargo test test_clwb --release -- --nocapture
        NVMTableStorage::init_test_database();
        let mut storage = NVMTableStorage::global_mut();
        let page_id = storage.alloc_page().unwrap();
        let address = page_id.page_start;
        let (tx0, rx) = mpsc::channel();
        let total = 10_000_000;
        let per_thread_space = 0x100000 * 1024 * 10;
        let step = 256;
        let thread_count = 16;
        for tid in 0..8 {
            let tx = tx0.clone();
            thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut num = 0;
                for _ in 0..4 {
                    let mut iter = address + tid * total * 8 + 125236;
                    for i in 0..total {
                        iter += 256;
                        let v: u64 = unsafe { io::read(iter) };
                        num += v;
                    }
                }
            });
        }
        // writer
        for tid in 0..thread_count {
            let tx = tx0.clone();

            thread::spawn(move || {
                let mut num = 0;
                let mut rng = rand::thread_rng();
                let start_addr = address + per_thread_space * tid;
                let space = per_thread_space / step;
                let start = SystemTime::now();
                for i in 0..total {
                    let mut iter = start_addr + step * u64_rand_new(&mut rng, 0, space);

                    let mut j = 0;
                    while j < step {
                        unsafe {
                            io::write(iter + j, i);
                        }
                        j += 64;
                    }
                    j = 0;
                    while j < step {
                        unsafe { io::clwb((iter + j) as *const u8) };
                        j += 64;
                    }

                    // if num & 1 == 0 {
                    file::sfence();
                    num += 1;
                }

                let end = SystemTime::now();
                let sec = end.duration_since(start).unwrap().as_secs_f64();
                println!("finish 10^8 writes without clwb {}s", sec);
                tx.send((num, sec)).unwrap();
            });
        }

        let mut count = 0;
        let mut total0 = 0.0;
        let mut b0 = 0.0;

        for i in 0..thread_count {
            let (num0, t) = rx.recv().unwrap();
            count += 1;
            total0 += t;
            b0 += total as f64 / t;
        }
        for tid in 0..8 {
            let tx = tx0.clone();
            thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut num = 0;
                for _ in 0..4 {
                    let mut iter = address + tid * total * 8 + 125236;
                    for i in 0..total {
                        iter += 256;
                        let v: u64 = unsafe { io::read(iter) };
                        num += v;
                    }
                }
            });
        }
        for tid in 0..thread_count {
            let tx = tx0.clone();

            thread::spawn(move || {
                let mut num = 0;
                let mut rng = rand::thread_rng();
                let start_addr = address + per_thread_space * tid;
                let space = per_thread_space / step;
                let start = SystemTime::now();

                for i in 0..total {
                    let mut iter = start_addr + step * u64_rand_new(&mut rng, 0, space);
                    // println!("{}", iter);
                    let mut j = 0;
                    while j < step {
                        unsafe {
                            io::write(iter + j, i);
                        }
                        j += 64;
                    }
                    j = 0;
                    // while j < step{
                    //     unsafe { io::clwb((iter + j) as *const u8) };
                    //     j += 64;
                    // }

                    // if num & 1 == 0 {
                    file::sfence();
                    num += 1;
                }

                let end = SystemTime::now();
                let sec = end.duration_since(start).unwrap().as_secs_f64();
                println!("finish 10^8 writes without clwb {}s", sec);
                tx.send((num, sec)).unwrap();
            });
        }
        let mut count = 0;
        let mut total1 = 0.0;
        let mut b1 = 0.0;

        for i in 0..thread_count {
            let (num0, t) = rx.recv().unwrap();
            count += 1;
            total1 += t;
            b1 += total as f64 / t;
        }
        println!(
            "10^8 writes without clwb avg = {:.3}s, bandwith = {:.3}g",
            total0 / thread_count as f64,
            b0 * step as f64 / 1_000_000_000.0
        );
        println!(
            "10^8 writes    with clwb avg = {:.3}s, bandwith = {:.3}g",
            total1 / thread_count as f64,
            b1 * step as f64 / 1_000_000_000.0
        );
    }
    #[test]
    fn test_newclwb() {
        //taskset -c 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59 cargo test test_clwb --release -- --nocapture
        NVMTableStorage::init_test_database();
        let mut storage = NVMTableStorage::global_mut();
        let page_id = storage.alloc_page().unwrap();
        let address = page_id.page_start;
        let (tx0, rx) = mpsc::channel();
        let total = 50_000_000;
        let step = 256;
        let thread_count = 16;
        for tid in 0..thread_count {
            let tx = tx0.clone();

            thread::spawn(move || {
                let mut num = 0;
                let start = SystemTime::now();
                let mut iter = address + tid * total * step;
                for i in 0..total {
                    let mut j = 0;
                    while j < step {
                        unsafe {
                            io::write(iter + j, i);
                        }
                        j += step;
                    }
                    while j < step {
                        unsafe { io::clwb((iter + j) as *const u8) };
                        j += step;
                    }

                    // if num & 1 == 0 {
                    file::sfence();
                    num += 1;
                    iter += step;
                }

                let end = SystemTime::now();
                let sec = end.duration_since(start).unwrap().as_secs_f64();
                println!("finish 10^8 writes without clwb {}s", sec);
                tx.send((num, sec)).unwrap();
            });
        }

        let mut count = 0;
        let mut total0 = 0.0;
        let mut b0 = 0.0;

        for i in 0..thread_count {
            let (num0, t) = rx.recv().unwrap();
            count += 1;
            total0 += t;
            b0 += total as f64 / t;
        }

        println!(
            "10^8 writes without clwb avg = {:.3}s, bandwith = {:.3}g",
            total0 / thread_count as f64,
            b0 * step as f64 / 1_000_000_000.0
        );
        // println!("10^8 writes    with clwb avg = {:.3}s, bandwith = {:.3}g", total1/thread_count as f64, b1 * 64.0 /1_000_000_000.0);
    }
}
