import os
import csv
from matplotlib import pyplot as plt
from pprint import pprint

import sys
sys.path.insert(0, "./scripts")
from utils import sorted_access, sorted_csv_read


#Please procduce files with transactions for each alias (sorted) before using this
#Produce a csv file for a protocol in an experiment.
#csv: protocol, latency, percentage

# ROOT_PATH = "/mnt/share/exp"
ROOT_PATH = "out"

def compute_cdf(file_paths):
    def agg(row, variables):
        return [row["latency"] if (variables[1] + 1)/row["num_of_rows"] * 100 <= percentage else variables[0], variables[1] + 1]
    
    cdf = {}
    for j in range(10):
        percentage = (j+1) * 10
        latency, count = sorted_csv_read(file_paths, "latency", [-1, 0], agg)
        cdf[percentage] = {"latency": latency, "count":count}
    return cdf

def compute_cdf_per_protocol(exp):
    folder_path = f"{ROOT_PATH}/exp{exp}"
    protocols = [x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")]

    field_names = ["protocol", "latency", "percentage"]
    protocol_f = open(f"{folder_path}/cdf.csv", "w")
    writer = csv.DictWriter(protocol_f, fieldnames=field_names)
    writer.writeheader()
    for protocol in protocols:
        folder_path = f"{ROOT_PATH}/exp{experiment_number}/{protocol}"

        alias_folder = [x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")]
        files = [f"{folder_path}/{alias}/transactions.csv" for alias in alias_folder]
        cdf = compute_cdf(files)

        for percentage in cdf:
            writer.writerow({"protocol": protocol, "latency": cdf[percentage]["latency"], "percentage": percentage})
    protocol_f.close()

def compute_cdf_per_client(protocol_path):
    aliases = [x for x in os.listdir(protocol_path) if os.path.isdir(f"{protocol_path}/{x}")]

    field_names = ["alias", "latency", "percentage"]
    protocol_f = open(f"{protocol_path}/cdf.csv", "w")
    writer = csv.DictWriter(protocol_f, fieldnames=field_names)
    writer.writeheader()
    for alias in aliases:
        cdf = compute_cdf([f"{protocol_path}/{alias}/transactions.csv"])
        for percentage in cdf:
            writer.writerow({"alias": alias, "latency": cdf[percentage]["latency"], "percentage": percentage})
    protocol_f.close()
    pass
    

experiment_number = 10
compute_cdf_per_client("out/exp10/swiftpaxos")
# compute_cdf_per_protocol(experiment_number)
        

