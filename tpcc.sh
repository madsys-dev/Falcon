rm /mnt/pmem0/pmem_hash.data
cd dash
git checkout tpcc
cd ..
taskset -c 26-51,78-103 cargo test tpcc_test_sync --release -- --nocapture
