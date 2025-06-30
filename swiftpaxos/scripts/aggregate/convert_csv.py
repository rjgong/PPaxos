import os
import csv

# ROOT_PATH = "/mnt/share/exp"
ROOT_PATH = "out"

#We'll need (command, latency, clone)
def get_stat(exp, protocol, alias):
    folder_path = f"{ROOT_PATH}/exp{exp}/{protocol}/{alias}"
    file_names = [x for x in os.listdir(folder_path) if x.split("_")[0] == alias]

    table = []
    for file_name in file_names:
        f = open(f"{folder_path}/{file_name}", "r")

        line = f.readline()
        while line:
            arr = line.split()
            if len(arr) == 7 and arr[2].lower() == "sending":
                cmd = arr[4]
                line = f.readline()
                arr = line.split()
                while line and not(len(arr) == 7 and arr[2].lower() == "sending"):
                    if len(arr) >= 4 and arr[2] == "latency":
                        table.append({"command": cmd, "latency": arr[3], "clone": file_name, "protocol": protocol, "alias": alias})
                    line = f.readline()
                    arr = line.split()
            else:
                line = f.readline()
        f.close()
    return table

experiment_numbers = [1]
for experiment_number in experiment_numbers:
    folder_path = f"{ROOT_PATH}/exp{experiment_number}"
    protocols = [x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")]

    #Sort per alias in a protocol
    for protocol in protocols:
        folder_path = f"{ROOT_PATH}/exp{experiment_number}/{protocol}"
        aliases = [x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")]
        for alias in aliases:
            table = get_stat(experiment_number, protocol, alias)
            table = sorted(table, key = lambda row: float(row["latency"]))

            folder_path = f"{ROOT_PATH}/exp{experiment_number}/{protocol}/{alias}"
            field_names = ["protocol", "alias", "command", "latency", "clone"]
            f = open(f"{folder_path}/transactions.csv", "w")
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            for row in table:
                writer.writerow(row)
            f.close() 