pub mod executor;
pub mod mem;

pub use executor::yield_now;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_test() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (executor, spanwer) = executor::Executor::new();
        let handle = std::thread::spawn(move || executor.run());

        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        const NUM_TASKS: usize = 32;
        for id in 0..NUM_TASKS {
            spanwer.spawn(async move {
                COUNTER.fetch_add(1, Ordering::Relaxed);
                executor::yield_now().await;
                COUNTER.fetch_add(1, Ordering::Relaxed);
            });
            if id * 5 % 8 < 3 {
                std::thread::sleep(std::time::Duration::from_micros(5));
            }
        }
        drop(spanwer);
        handle.join().unwrap();
        assert_eq!(COUNTER.load(Ordering::Relaxed), NUM_TASKS * 2);
    }
}
