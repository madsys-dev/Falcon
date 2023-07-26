cd dash
git checkout ycsb
cd ..
taskset -c 26-51,78-103 cargo test ycsb_test_sync --release -- --nocapture
# taskset -c 0-25,52-77 cargo test ycsb_test_sync --release -- --nocapture
