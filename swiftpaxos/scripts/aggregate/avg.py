import csv
import os

import sys
sys.path.insert(0, "./scripts")
from utils import aggregate_alias

FILE_NAME="agg.csv"
ROOT_PATH="out"

def write_avg_csv(table, headers, file_path):
    f = open(file_path, "w")
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    for name in table:
        row = {headers[0]: name}
        row.update(table[name])
        writer.writerow(row)
    f.close()

def compute_clone_avg(client_folder):
    clones = [x for x in os.listdir(client_folder) if os.path.isfile(f"{client_folder}/{x}") and not os.path.splitext(x)[1]]
    avgs = {}
    for clone in clones:
        avgs[clone] = {"avg": 0, "count": 0}

    def iterative_avg(row, variables):
        avgs = variables[0]
        temp = avgs[row["clone"]]
        avg1, count1 = temp["avg"], temp["count"] 
        avg2 = float(row["latency"])

        avgs[row["clone"]] = {
            "avg": (avg1 * count1 + avg2) / (count1 + 1),
            "count": count1 + 1
        }
        return [avgs]
    avgs, = aggregate_alias(f"{client_folder}/transactions.csv", [avgs], iterative_avg)
    return avgs

def compute_alias_avg(protocol_folder):
    aliases = [x for x in os.listdir(protocol_folder) if os.path.isdir(f"{protocol_folder}/{x}")]
    avgs = {}
    
    def iterative_avg(row, variables):
        avg1, count1 = variables
        avg2, count2 = float(row["avg"]), int(row["count"])
        return [(avg1 * count1 + avg2 * count2) / (count1 + count2), count1 + count2]
    
    for alias in aliases:
        avg, count = aggregate_alias(f"{protocol_folder}/{alias}/avg.csv", [0,0], iterative_avg)
        avgs[alias] = {"avg": avg, "count": count}
    return avgs

def compute_protocol_avg(exp_folder):
    return compute_alias_avg(exp_folder)

experiment_numbers = [110,111,112,113,114,115,116,117,118,119,120]
for experiment_number in experiment_numbers:
    folder_path = f"{ROOT_PATH}/exp{experiment_number}"
    protocols = [x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")]
    clients = [[x for x in os.listdir(f"{folder_path}/{protocol}") if os.path.isdir(f"{folder_path}/{protocol}/{x}")] for protocol in protocols]

    for i, protocol in enumerate(protocols):
        client = clients[i]
        for c in client:
            table = compute_clone_avg(f"{folder_path}/{protocol}/{c}")
            write_avg_csv(table, ["clone", "avg", "count"], f"{folder_path}/{protocol}/{c}/avg.csv")

    for protocol in protocols:
        table = compute_alias_avg(f"{folder_path}/{protocol}")
        write_avg_csv(table, ["client", "avg", "count"], f"{folder_path}/{protocol}/avg.csv")

    table = compute_protocol_avg(folder_path)
    write_avg_csv(table, ["protocol", "avg", "count"], f"{folder_path}/avg.csv")