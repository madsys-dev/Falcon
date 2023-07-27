numa_set = "taskset -c 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,94,96"
db_file_path = "/mnt/pmem0/_test_persist"
pm_index = "/mnt/pmem0/pmem_hash.data"
TPCC_WAREHOUSE = 2048
YCSB_TOTAL = "256 * 1024 * 1024"

with open('tpcc.sh', "w") as script:
    script.write("rm %s\n"%pm_index)
    script.write("cd dash\n")
    script.write("git checkout tpcc\n")
    script.write("cd ..\n")
    script.write(numa_set + " cargo test tpcc_test_sync --release -- --nocapture\n")

with open('ycsb.sh', "w") as script:
    script.write("rm %s\n"%pm_index)
    script.write("cd dash\n")
    script.write("git checkout ycsb\n")
    script.write("cd ..\n")
    script.write(numa_set + " cargo test ycsb_test_sync --release -- --nocapture\n")

with open('restore.sh', "w") as script:
    script.write(numa_set + " cargo test ycsb_test_reload --release -- --nocapture\n")

with open('src/customer_config.rs', "w") as rust_code:
    rust_code.write('pub const TPCC_WAREHOUSE: u64 = %d;\n'%TPCC_WAREHOUSE)
    rust_code.write('pub const YCSB_TOTAL: u64 = %s;\n'%YCSB_TOTAL)
    rust_code.write('pub const NVM_FILE_PATH: &str = "%s";\n'%db_file_path)
    rust_code.write('pub const INDEX_FILE_PATH: &str = "%s";\n'%pm_index)
