
# cargo test test_isolation --features basic --features local_cc_cfg_occ --no-default-features --release -- --nocapture
import os
import copy
import re

from configure import numa_set
from run_test import *

def test_ycsb_dram_only():
    global k
    global index_type
    k = 0
    index_type = "DRAM"

    props = {
        "basic": [["basic_dram"]],
        "ycsb": [
            ["ycsb_a"], ["ycsb_f"], 
        ],
        "cc_cfg": [
            # ["local_cc_cfg_to"],
            # ["local_cc_cfg_2pl"],
            ["local_cc_cfg_occ"],
        ],  
        "mvcc": [
            [""],
            # ["mvcc"]
        ],
        "buffer": [
            sysname["Falcon"], sysname["ZenS"],
            sysname["ZenS(No Flush)"], sysname["Falcon(No Flush)"],
            sysname["Falcon(All Flush)"], sysname["Inp"],
            sysname["Outp"],
        ],
        "clock": [
            ["txn_clock"],
        ],
        "thread_count": [[48]]
    }
    test([], props, list(props.keys()), "ycsb_test_sync", "ycsb_dram")

def test_tpcc_dram_only():
    global k
    global index_type
    k = 0
    index_type = "DRAM"
    # set_index("tpcc")

    props = {
        "basic": [["basic_dram", "tpcc"]],
        "ycsb": [
            ["ycsb_a"], # only for compile
        ],
        "cc_cfg": [
            ["local_cc_cfg_to"],
            ["local_cc_cfg_2pl"],
            ["local_cc_cfg_occ"],
        ],  
        "mvcc": [
            [""],
            ["mvcc"]
        ],
        "buffer": [
            sysname["Falcon"], sysname["ZenS"],
            sysname["ZenS(No Flush)"], sysname["Falcon(No Flush)"],
            sysname["Falcon(All Flush)"], sysname["Inp"],
            sysname["Outp"],
        ],
        "clock": [
            ["txn_clock", "new_order_clock"],
            ["txn_clock", "payment_clock"],
        ],
        "thread_count": [[48]]
    }
    test([], props, list(props.keys()), "tpcc_test_sync", "tpcc_dram")


def test_ycsb_scal_dram_only():
    global k
    global index_type
    k = 0
    index_type = "DRAM"
    # set_index("ycsb")

    props = {
        "basic": [["basic_dram"]],
        "ycsb": [
            ["ycsb_a"], 
        ],
        "cc_cfg": [
            # ["local_cc_cfg_to"],
            # ["local_cc_cfg_2pl"],
            ["local_cc_cfg_occ"],
        ],  
        "mvcc": [
            [""],
            # ["mvcc"]
        ],
        "buffer": [
            sysname["Falcon"], sysname["Inp"],
            sysname["Inp(Small Log Window)"],            
            sysname["Inp(No Flush)"],
            sysname["Inp(Hot Tuple Cache)"],
        ],
        "thread_count": [[1],[2],[4],[8],[16],[32],[48]]
    }
    test([], props, list(props.keys()), "ycsb_test_sync", "ycsb_scal")

def test_tpcc_scal_dram_only():
    global k
    global index_type
    k = 0
    index_type = "DRAM"
    set_index("tpcc")

    props = {
        "basic": [["basic_dram", "tpcc"]],
        "ycsb": [
            ["ycsb_a"],  # default 
        ],
        "cc_cfg": [
            # ["local_cc_cfg_to"],
            # ["local_cc_cfg_2pl"],
            ["local_cc_cfg_occ"],
        ],  
        "mvcc": [
            [""],
            # ["mvcc"]
        ],
        "buffer": [
            sysname["Falcon"], sysname["Inp"],
            sysname["Inp(Small Log Window)"],            
            sysname["Inp(No Flush)"],
            sysname["Inp(Hot Tuple Cache)"],
        ],
        "thread_count": [[1],[2],[4],[8],[16],[32],[48]]
    }
    test([], props, list(props.keys()), "tpcc_test_sync", "tpcc_scal")

if __name__ == "__main__":
    test_ycsb_dram_only()

    test_tpcc_dram_only()

    test_ycsb_scal_dram_only()
    test_tpcc_scal_dram_only()
