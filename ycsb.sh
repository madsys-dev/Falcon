rm /mnt/pmem1/pmem_hash.data
cd dash
git checkout ycsb
cd ..
taskset -c 26-51,78-103 cargo test ycsb_test_sync --release -- --nocapture
