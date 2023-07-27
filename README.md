# Falcon: Fast OLTP Engine for Persistent Cache and Non-Volatile Memory


## Building



The project contains two parts, the Rust-based Falcon and the external C++-based NVM Hash index, Dash.

After installing Rust, run `cargo build` to build Falcon.

### Dash

Init Dash:
```bash
git pull --recurse-submodules
source build.sh
```
or
```bash
git clone https://github.com/madsys-dev/dash.git
source build.sh
```
For more information, please refer to https://github.com/baotonglu/dash. 

If installing Dash is difficult, you can use Rust indexes directly to test Falcon.

## Settings

We use NVM in DAX mode enabled with the following commands.
```bash
mkfs-xfs -m reflink=0 -f /dev/pmem0
mount -t xfs /dev/pmem0 /mnt/pmem0 -o dax
```

### Step1: Setting Database File and Index File path

Set the path for database file and index file in `configure.py`:
``` bash
db_file_path = "database file path"
pm_index = "persist index file path"
```

### Step2: Pin Threads to Core

Falcon use `taskset` to pin threads to a single NUMA node. Use `numactl -H` to check your own hardware. **Make sure the CPU and NVM file at the same numa node.**

Set the numa information in `configure.py` (see examples below):
``` bash
numa_set = "taskset -c 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,94,96" # Database File and Index File saved in numa node 0 and numa node 0 includes even cores
```
or
```python
numa_set = "taskset -c 26-51,78-103" # Database File and Index File saved in numa node 1 and numa node 1 includes cores 26~51 and 78~103
```

### Step3: Run `configure.py`

### Workload Size Setting

The size of the test is configured in `src/customer_config.rs`. A smaller workload can be used if there is no enough NVM space (see examples below): 
```rust
// TPCC_WAREHOUSE is no less than THREAD_COUNT.
pub const TPCC_WAREHOUSE:u64 = 48;
pub const YCSB_TOTAL:u64 = 16*1024*1024;
```

A smaller index can be used if there is no enough NVM space(see examples below): 
```c++
// in dash/src/dash.cpp(branch tpcc)
static const size_t pool_size = 1024ul * 1024ul * 1024ul * 16ul;
size_t segment_number = 2048 * 32; // for tpcc
```
## Example program

Falcon uses conditional compilation to simulate different systems. In `Cargo.toml`, fill in the `sysname` variable with the relevant parameters:
```python
sysname = ["Falcon"] or sysname = ["Inp"] or ...
```

After setting, `ycsb.sh` and `tpcc.sh` can be used to run the corresponding test.

## Running benchmark

`run_test.py` has already configured all the tests. To run the test, simply use the command `python3 run_test.py`. If needed, you can modify `run_test.py` for customized settings, such as reducing the number of threads:
```python
"thread_count": [[48]] -> "thread_count": [[32]]
```

After the test, run `python3 collect.py` to collect the result, which will export a `result.csv` file.
To generate experiment figures, copy the `result.csv` to the appropriate location in `result.xlsx` to generate the charts.

There are a total of 182 test cases. With full workloads, each test case takes about 3 minutes to complete. Running the entire test suite will take around 9 to 10 hours. If desired, you can modify the Python code to run only a subset of these tests. Here is the number of test cases for each test:
```python
test_ycsb_nvm() # 10 test cases
test_ycsb_dram() # 6 test cases
test_tpcc_nvm() # 60 test cases
test_tpcc_dram() # 36 test cases
test_ycsb_scal() # 35 test cases
test_tpcc_scal() # 35 test cases
```

If you can’t install Dash, use `run_rust_only_test.py`, `collect_rust_only.py` and `result_rust_only.xlsx` instead.

## Recovery

Recovery evaluation is only for Falcon + Dash, please set feature `dash` and `Falcon` in `Cargo.toml`.

The recovery process is explained in `recovery.sh`. To create the database, run `ycsb.sh`. During the testing phase, you can kill Falcon after the completion of the insert phase. Please note that terminating Falcon before the insert phase ends may cause errors when attempting to read or write to a non-existent tuple. When running the recovery test, you can also kill Falcon during the testing phase.

## Contact

For any questions, please contact us at `jizc19@mails.tsinghua.edu.cn`.
