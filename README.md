# Falcon: Fast OLTP Engine for Persistent Cache and Non-Volatile Memory

## Building

The project contains two parts, the Rust-based Falcon and the external C++-based NVM Hash index, Dash.

After installing Rust, run `cargo build` to build Falcon. Make sure you can download the dependency packages from the web.

### Dash

init Dash:
```bash
git submodule update --remote
source build.sh
```

For more information, please refer to https://github.com/baotonglu/dash. 

If installing Dash is difficult, you can use Rust's indexes directly to use and initially test Falcon.

## Quick start

The size of the test is configured in `src/config.rs`. you can reduce the size of the workloads for quick experimentation.
```rust
// TPCC_WAREHOUSE is not less than THREAD_COUNT.
pub const TPCC_WAREHOUSE:u64 = 48;
pub const YCSB_TOTAL:u64 = 16*1024*1024;
```

## Example program

Falcon uses conditional compilation to simulate different systems. We have stored several parameter configurations as examples.

We recommend that you set them up by modifying `Cargo.toml`. In `Cargo.toml`, fill in the `sysname` variable with the relevant parameters:

```python
sysname = ["Falcon"] or sysname = ["Inp"] or ...
```

The process of testing is described in `ycsb.sh` and `tpcc.sh`. Once you have configured your test system, you can use `ycsb.sh` or `tpcc.sh` to get started quickly.

## Running benchmark

In `run_test.py`, we have configured all the tests. All you need to do is run the test using `python3 run_test.py`. 

At the end of the test, you can collect the results of the experiment with `python3 collect.py`, which will export a `result.csv`.

We use excel to generate the experiment figures, you can copy the `result.csv` to the appropriate location in `result.xlsx` to generate the charts.

note: Falcon(DRAM Index) does not perform well on YCSB-F(ZipFan) with small workloads due to concurrency conflicts, please use the full workloads if you have problems with this experiment. Other experiments perform similarly on the small workloads as the full workloads.

## Contact

For any questions, please contact us at `jizc19@mails.tsinghua.edu.cn`.
