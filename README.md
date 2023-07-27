# Falcon: Fast OLTP Engine for Persistent Cache and Non-Volatile Memory


## Building



The project contains two parts, the Rust-based Falcon and the external C++-based NVM Hash index, Dash.

After installing Rust, run `cargo build` to build Falcon. Make sure you can download the dependency packages from the web.

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

If installing Dash is difficult, you can use Rust's indexes directly to use and initially test Falcon.

## Customer Setting

We use NVM by DAX mode. If you don't have one, refer the instructions below.
```bash
mkfs-xfs -m reflink=0 -f /dev/pmem0
mount -t xfs /dev/pmem0 /mnt/pmem0 -o dax
```

You can do the following customer setting quickly by updating `configure.py` and running it.

### Database File and Index File path

You need to set the path of NVM file, see in `src/custor_config.rs`:
``` bash
pub const NVM_FILE_PATH: &str = "your database file path";
pub const INDEX_FILE_PATH: &str = "persist index file path";
#---------------------------------------------------------------
```

### Pin Threads to Core

We use taskset to run Falcon in a single NUMA node. In our hardware, numa0 includes even cores. You need to update taskset with your hardware. Use `numactl -H` to check your own hardware. **Make sure the CPU and NVM file are the same numa node.**

Here are the changes, along with some examples:
``` bash
tpcc/ycsb/store.sh:

taskset -c 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,94,96 ... # Database File and Index File saved in numa node 0 and numa node 0 includes even cores
taskset -c 26-51,78-103 ...   # Database File and Index File saved in numa node 1 and numa node 1 includes cores 26~51 and 78~103

#---------------------------------------------------------------

configure.py:

numa_set = "taskset -c 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,94,96"
numa_set = "taskset -c 26-51,78-103"
```

## Quick start

The size of the test is configured in `src/customer_config.rs`. you can reduce the size of the workloads for quick experimentation.
```rust
// TPCC_WAREHOUSE is not less than THREAD_COUNT.
pub const TPCC_WAREHOUSE:u64 = 48;
pub const YCSB_TOTAL:u64 = 16*1024*1024;
```

If you do not have enough NVM space, you can also smaller the index. Here is an example: 
```c++
// for dash-tpcc 
static const size_t pool_size = 1024ul * 1024ul * 1024ul * 16ul;
size_t segment_number = 2048 * 32; // for tpcc
```
## Example program

Falcon uses conditional compilation to simulate different systems. We have stored several parameter configurations as examples.

We recommend that you set them up by modifying `Cargo.toml`. In `Cargo.toml`, fill in the `sysname` variable with the relevant parameters:

```python
sysname = ["Falcon"] or sysname = ["Inp"] or ...
```

The process of testing is described in `ycsb.sh` and `tpcc.sh`. Once you have configured your test system, you can use `ycsb.sh` or `tpcc.sh` to get started quickly.


## Running benchmark

In `run_test.py`, we have configured all the tests. All you need to do is run the test using `python3 run_test.py`. You can modify the `run_test.py` for some customer setting, for example, decrease the threads count: 
```python
"thread_count": [[48]] -> "thread_count": [[32]]
```

At the end of the test, you can collect the results of the experiment with `python3 collect.py`, which will export a `result.csv`.

We use excel to generate the experiment figures, you can copy the `result.csv` to the appropriate location in `result.xlsx` to generate the charts.

There are 182 test cases in total. With the full workloads, each test case takes about 3 minutes. The entire test takes 9 to 10 hours to run. You can run part of these tests by modifying the python codes. Here is the number of test cases for each test:
```python
test_ycsb_nvm() # 10 test cases
test_ycsb_dram() # 6 test cases
test_tpcc_nvm() # 60 test cases
test_tpcc_dram() # 36 test cases
test_ycsb_scal() # 35 test cases
test_tpcc_scal() # 35 test cases
```

note: Falcon(DRAM Index) does not perform well on YCSB-F(ZipFan) with small workloads due to concurrency conflicts, please use the full workloads if you have problems with this experiment. Other experiments perform similarly on the small workloads as the full workloads.

Without Dash, you can use `run_dram_test.py`, `collect_dram.py` and `result_dram.xlsx` instead.

## Recovery

Recovery evaluation is only for Falcon + Dash, please set feature `dash` and `Falcon` in `Cargo.toml`.

The process of recovery is described in `recovery.sh`. You need to run `ycsb.sh` to create the database. You can kill Falcon on testing phase, after all insert phase finished. Since the recovery experiment will be verified by random reads and writes, termination before the end of the insertion will result in an error reading or writing to a non-existent tuple.

On recovery test, you can also kill Falcon on testing phase.

## Contact

For any questions, please contact us at `jizc19@mails.tsinghua.edu.cn`.
