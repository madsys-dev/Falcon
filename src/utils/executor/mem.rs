//! Basic functions for asynchronous memory access.
use super::executor::yield_now;

/// Asynchronously prefetch the memory, for read.
pub async fn prefetch_read(x: u64) {
    use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
    unsafe {
        let ptr = x as *const i8;
        _mm_prefetch::<_MM_HINT_T0>(ptr);
    }
    yield_now().await
}
