
# cargo test test_isolation --features basic --features local_cc_cfg_occ --no-default-features --release -- --nocapture
import os
import copy
import re
from configure import numa_set


k = 0
index_type = "NVM"
sysname = {
    "Falcon": ["n2db_local", "ilog", "clwb_tuple", "hot_unflush"],
    "Falcon(All Flush)": ["n2db_local", "ilog", "clwb_tuple"],
    "Falcon(No Flush)": ["n2db_local", "ilog"],
    "Inp": ["n2db_local", "clwb_delta", "clwb_tuple"],
    "Outp": ["n2db_append", "clwb_tuple"],
    "ZenS": ["zen_local", "clwb_tuple"],
    "ZenS(No Flush)": ["zen_local"],
    "Inp(Small Log Window)": ["n2db_local", "clwb_tuple", "ilog"],            
    "Inp(No Flush)": ["n2db_local"],
    "Inp(Hot Tuple Cache)": ["n2db_local", "clwb_delta", "clwb_tuple", "hot_unflush"],
    "Unknown": []
}

def copy(dir, file):
    cmd = 'cp ' + file + ' ' + os.path.join(dir, file)
    print(cmd)
    os.system(cmd)

def set_thread_count(thread_count):
    path = './src/mvcc_config/mod.rs'
    # print(thread_count)
    with open(path, "w") as cfile:
        cfile.write("pub mod delta;\n")
        cfile.write("pub const THREAD_COUNT: usize = 48;\n")
        cfile.write("pub const TEST_THREAD_COUNT: usize = %d;\n" % (thread_count))
        cfile.write("pub const TRANSACTION_COUNT: usize = THREAD_COUNT;\n")

def get_sysname(features):
    global index_type
    global sysname

    result = "Unknown"

    for key in sysname:
        if set(sysname[key]) < set(features) and len(sysname[key]) > len(sysname[result]):
            result = key
    if result == "Falcon" and index_type == "DRAM":
        result = "Falcon(DRAM Index)"
    return result

def get_workload(features, result):
    if "tpcc" in features:
        if "new_order_clock" in features:
            return "TPC-C-NP new_order"
        if "payment_clock" in features:
            return "TPC-C-NP payment"
        return "TPC-C-NP"
    for ycsb in "abcdef":
        workload = "ycsb_"+ycsb
        if workload in features:
            theta = re.findall("theta = (\d+\.?\d*)", result)
            return workload + " zipf_theta = " + str(theta[0])
    return "Unknown"

def get_cc(features):
    cc = ""
    if "mvcc" in features:
        cc = "MV"
    if "local_cc_cfg_2pl" in features:
        cc += '2PL'
    elif "local_cc_cfg_to" in features:
        cc += 'TO'
    elif "local_cc_cfg_occ" in features:
        cc += 'OCC'
    else:
        cc = "Unknown"
    return cc

def get_txn(result):
    pattern = "txn (\d+) of (\d+) txns"
    txns = re.findall(pattern, result)
    return txns[0]

def get_latency(result):
    pattern = "avg_txn: (\d+\.?\d*), 10.: (\d+\.?\d*), 95.: (\d+\.?\d*), 99.: (\d+\.?\d*)"
    latency_group = re.findall(pattern, result)
    if latency_group:
        return latency_group[0]
    return []

# "sysname, workload, threads, cc, commit txns, total txns, avg latency, 10% latency, 95% latency. 99% latency"
def report(features, result, t_cnt):
    csv_line = []
    csv_line.append(get_sysname(features))
    csv_line.append(get_workload(features, result))
    csv_line.append(str(t_cnt))
    csv_line.append(get_cc(features))

    commits, total = get_txn(result)
    csv_line.append(str(commits))
    csv_line.append(str(total))

    latency_group = get_latency(result)
    for latency in latency_group:
        csv_line.append(str(latency))
    return ",".join(csv_line) + "\n"


def TIMEOUT_COMMAND(command, timeout):
    """call shell-command and either return its output or kill it
    if it doesn't normally exit within timeout seconds and return None"""
    import subprocess, datetime, os, time, signal
    start = datetime.datetime.now()
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    while process.poll() is None:
        time.sleep(0.2)
        now = datetime.datetime.now()
        if (now - start).seconds > timeout:
            os.kill(process.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            return None
    return process.stdout.read().decode("utf-8")

def run_test(features, workload, result_path):
    global k
    set_thread_count(16)

    k += 1
    # test tasks filter
    # if k <= 20 and result_path == "tpcc_nvm":
    #     return ""
    # if k <= 18 and result_path == "tpcc_scal":
    #     return ""
    # if k not in [112]:
    #     return ""
    # if "mvcc" in features and "zen_local" in features:
    #     return ""

    result_csv = ""

    # txt = "taskset -c 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63,65,67,69,71,73,75,77,79,81,83,85,87,89,91,93,95,97,99 cargo test "  + workload
    txt = numa_set + " cargo test "  + workload
    t_cnt = 16
    for f in features:
        if f == "":
            continue
        if type(f) == int:
            set_thread_count(f)
            t_cnt = f
            continue
        txt += " --features " + f
        # result_csv += f + " "
    txt += " --no-default-features --release -- --nocapture"
    print("test cmd: " + txt)
    text = ""
    r = os.popen("rm /mnt/pmem1/pmem_hash.data")
    # r = os.popen(txt)
    text = TIMEOUT_COMMAND(txt, 600)
    if text == None:
        with open("error.txt", "w") as err_file:
            err_file.write(result_path)
            err_file.write('\n')
            err_file.write(str(k))
            err_file.write('\n')
            err_file.write(txt)
            err_file.write('\n')
        r.close()
        return ""
    r.close()
    print(features)
    
    features.remove(t_cnt)
    features.append(str(t_cnt))
    # print(features)
    for line in text.splitlines():
        if "total" in line:
            result_csv += report(features, line, t_cnt)
    
    try:
        os.mkdir("result")
    except:
        pass
    try:
        os.mkdir("result/" + result_path)
    except:
        pass

    of = open("result/" + result_path + "/"+ str(k) + ".csv", "w")
    of.write(result_csv)
    of.close()

    of = open("result/" + result_path + "/" + str(k) + ".txt", "w")
    of.write(text)
    of.close()


    features.remove(str(t_cnt))
    features.append(t_cnt)

    return result_csv

def test(features, props, prop_list, workload, result_path, times = 1):
    
    if len(prop_list) == 0:
        for i in range(0, times):
            run_test(features, workload, result_path) 
        # print(features)
        return

    for f in props[prop_list[0]]:
        features += f
        test(features, props, prop_list[1:], workload, result_path, times)
        for item in f:
            features.remove(item)

# ycsb for ycsb-NVM
# tpcc for tpcc-NVM, support multi index without recovery
# dram for DRAM index
def set_index(index_type):
    pwd = os.getcwd()
    dash_owd = pwd + "/dash/src"
    os.chdir(dash_owd) 
    r = os.popen("git checkout " + index_type)
    os.chdir(pwd) 

def test_ycsb_nvm():
    global k
    global index_type
    k = 0
    index_type = "NVM"
    set_index("ycsb")

    props = {
        "basic": [["basic"]],
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
            sysname["Falcon"], sysname["Falcon(No Flush)"],
            sysname["Falcon(All Flush)"], sysname["Inp"],
            sysname["Outp"],
        ],
        "clock": [
            ["txn_clock"],
        ],
        "thread_count": [[48]]
    }
    test([], props, list(props.keys()), "ycsb_test_sync", "ycsb_nvm")


def test_ycsb_dram():
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
            sysname["ZenS(No Flush)"],
        ],
        "clock": [
            ["txn_clock"],
        ],
        "thread_count": [[48]]
    }
    test([], props, list(props.keys()), "ycsb_test_sync", "ycsb_dram")

def test_tpcc_nvm():
    global k
    global index_type

    k = 0
    index_type = "NVM"
    set_index("tpcc")

    props = {
        "basic": [["basic", "tpcc"]],
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
            sysname["Falcon"], sysname["Falcon(No Flush)"],
            sysname["Falcon(All Flush)"], sysname["Inp"],
            sysname["Outp"],
        ],
        "clock": [
            ["txn_clock", "new_order_clock"],
            ["txn_clock", "payment_clock"],
        ],
        "thread_count": [[48]]
    }
    test([], props, list(props.keys()), "tpcc_test_sync", "tpcc_nvm")


def test_tpcc_dram():
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
            sysname["ZenS(No Flush)"],
        ],
        "clock": [
            ["txn_clock", "new_order_clock"],
            ["txn_clock", "payment_clock"],
        ],
        "thread_count": [[48]]
    }
    test([], props, list(props.keys()), "tpcc_test_sync", "tpcc_dram")


def test_ycsb_scal():
    global k
    global index_type
    k = 0
    index_type = "NVM"
    set_index("ycsb")

    props = {
        "basic": [["basic"]],
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


def test_tpcc_scal():
    global k
    global index_type
    k = 0
    index_type = "NVM"
    set_index("tpcc")

    props = {
        "basic": [["basic", "tpcc"]],
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

    test_ycsb_nvm()
    test_ycsb_dram()

    test_tpcc_nvm()
    test_tpcc_dram()

    test_ycsb_scal()
    test_tpcc_scal()
