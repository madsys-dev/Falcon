import os

header = r"sysname,workload,threads,cc,commit txns,total txns,avg latency,10% latency,95% latency,99% latency"
sysnames = ["Falcon(DRAM Index)", "Falcon(All Flush)", "Falcon(No Flush)", "Inp", "Outp", "ZenS(No Flush)", "ZenS"]
total_csv = header + "\n"

def collect(tasks):
    global total_csv
    results = []
    try:
        for task in tasks:
            total_csv += (task + "\n")
            cur_path = "result/" + task + "/"
            test_cases = os.listdir(cur_path)
            for test_case in range(0, 200):
                file_name = str(test_case) + ".csv"
                if file_name in test_cases:
                    with open(cur_path + file_name, "r") as result_csv:
                        for result in result_csv.readlines():
                            total_csv += result
                            results.append(result.strip().split(','))
    except:
        pass
    return results

def rename(name):
    new_name = ""
    for ycsb in "abcdef":
        workload = "ycsb_"+ycsb
        if workload in name:
            new_name += ycsb
    if "zipf_theta = 0.99" in name:
        new_name += "Z"
    elif "zipf_theta = 0" in name:
        new_name += "U"
    if "new_order" in name:
        new_name = "N"
    if "payment" in name:
        new_name = "P"
    return new_name

collect_csv = ""

results = collect(["tpcc_dram"])
# collect tpcc-48
tpcc = {}
for result in results:
    sysname = result[0]
    commits = int(result[4])
    workload = result[3]
    tpcc.setdefault(sysname, {})
    tpcc[sysname].setdefault(workload, 0)
    tpcc[sysname][workload] += commits

collect_csv += "TPCC-48 \n"
collect_csv += "sysname,2PL,TO,OCC,MV2PL,MVTO,MVOCC\n"
for sysname in sysnames:
    collect_csv += sysname
    try:
        for w in ['2PL', 'TO', 'OCC', 'MV2PL', 'MVTO', 'MVOCC']:
            collect_csv += "," + str(tpcc[sysname][w]/2)
    except:
        pass
    collect_csv += "\n"
collect_csv += "\n"

tpcc = {}

for w in ["new_order", "payment"]:
    for result in results:
        sysname = result[0]
        commits = int(result[4])
        workload = result[3]
        if workload != "OCC":
            continue
        if w not in result[1]:
            continue
        tpcc.setdefault(sysname, [])
        tpcc[sysname].append(result[6])
        tpcc[sysname].append(result[8])

collect_csv += "TPCC-48 Latency\n"
collect_csv += "sysname,new_order-avg,new_order-95,payment-avg,payment-95\n"

for sysname in sysnames:
    collect_csv += sysname
    try:
        collect_csv += "," + ",".join(tpcc[sysname])
    except:
        pass
    collect_csv += "\n"
collect_csv += "\n"

results = collect(["ycsb_dram"])
# collect ycsb-48
ycsb = {}
for result in results:
    sysname = result[0]
    commits = int(result[4])
    workload = rename(result[1])
    ycsb.setdefault(sysname, {})
    ycsb[sysname][workload] = commits

collect_csv += "YCSB-48 \n"
collect_csv += "sysname,au,az,fu,fz\n"
for sysname in sysnames:
    collect_csv += sysname
    try:
        for w in ['aU', 'aZ', 'fU', 'fZ']:
            collect_csv += "," + str(ycsb[sysname][w])
    except:
        pass
    collect_csv += "\n"
collect_csv += "\n"

# collect tpcc-scal
sysnames = ["Inp", "Falcon(All Flush)", "Inp(No Flush)", "Inp(Hot Tuple Cache)", "Falcon(DRAM Index)"]
results = collect(["tpcc_scal"])
tpcc = {}
for result in results:
    sysname = result[0]
    commits = int(result[4])
    threads = result[2]
    tpcc.setdefault(sysname, {})
    tpcc[sysname][threads] = commits

collect_csv += "TPCC-scal \n"
collect_csv += "threads," + ",".join(sysnames) + "\n"
for thread in ["1", "2", "4", "8", "16", "32", "48"]:
    collect_csv += thread
    try:
        for sysname in sysnames:
            collect_csv += "," + str(tpcc[sysname][thread])
    except: 
        pass
    collect_csv += "\n"
collect_csv += "\n"

results = collect(["ycsb_scal"])
# collect ycsb-scal
for workload in ["0", "0.99"]:
    ycsb = {}
    for result in results:
        if not result[1].endswith(workload) or "ycsb_a" not in result[1]:
            continue
        sysname = result[0]
        commits = int(result[4])
        threads = result[2]
        ycsb.setdefault(sysname, {})
        ycsb[sysname][threads] = commits

    collect_csv += "YCSB Zipf=" + workload + "-scal \n"
    collect_csv += "threads," + ",".join(sysnames) + "\n"
    for thread in ["1", "2", "4", "8", "16", "32", "48"]:
        collect_csv += thread
        try:
            for sysname in sysnames:
                collect_csv += "," + str(ycsb[sysname][thread])
        except:
            pass
        collect_csv += "\n"
    collect_csv += "\n"

with open("data.csv", "w") as output_csv:
    output_csv.write(total_csv)

with open("result.csv", "w") as output_csv:
    output_csv.write(collect_csv)