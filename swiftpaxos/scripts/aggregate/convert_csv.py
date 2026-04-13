import os
import csv
from datetime import datetime
import sys

sys.path.insert(0, "./scripts")
from swiftpaxos.scripts.utils import is_float

# ROOT_PATH = "/mnt/share/exp"
ROOT_PATH = "out"
TIME_FORMAT = "%Y/%m/%d %H:%M:%S"
client_regions = {
    "client1": "Jakarta",
    "client2": "Jakarta",
    "client3": "Calgary",
    "client4": "Calgary",
    "client5": "Tel Aviv",
    "client6": "Tel Aviv",
    "client7": "Cape Town",
    "client8": "Cape Town",
    "client9": "Sao Paulo",
    "client10": "Sao Paulo",
}
field_names = [
    "protocol",
    "alias",
    "command",
    "latency",
    "clone",
    "time",
    "exp",
    "region",
]


def get_stat_no_debug(exp, protocol, alias):
    folder_path = f"{ROOT_PATH}/exp{exp}/{protocol}/{alias}"
    file_names = [x for x in os.listdir(folder_path) if x.split("_")[0] == alias]

    table = []
    for file_name in file_names:
        f = open(f"{folder_path}/{file_name}", "r")

        count = 0
        line = f.readline()
        while line:
            arr = line.split()
            if len(arr) == 4 and is_float(arr[-1]) and arr[-2] == "latency":
                table.append(
                    {
                        "command": count,
                        "latency": arr[-1],
                        "clone": file_name,
                        "protocol": protocol,
                        "alias": alias,
                        "time": datetime.strptime(
                            " ".join(arr[0:-2]), TIME_FORMAT
                        ).isoformat(),
                        "exp": exp,
                        "region": client_regions[alias],
                    }
                )
                count += 1
            line = f.readline()
        f.close()
    return table


experiment_numbers = ["30", "31", "32", "33", "34", "35"]

f_all = open(f"{'_'.join(experiment_numbers)}_transactions.csv", "w")
writer_all = csv.DictWriter(f_all, fieldnames=field_names)
writer_all.writeheader()

for experiment_number in experiment_numbers:
    print("Converting:", experiment_number)
    folder_path = f"{ROOT_PATH}/exp{experiment_number}"
    protocols = [
        x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")
    ]

    # Sort per alias in a protocol
    for protocol in protocols:
        folder_path = f"{ROOT_PATH}/exp{experiment_number}/{protocol}"
        aliases = [
            x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")
        ]
        for alias in aliases:
            table = get_stat_no_debug(experiment_number, protocol, alias)
            table = sorted(table, key=lambda row: float(row["latency"]))

            folder_path = f"{ROOT_PATH}/exp{experiment_number}/{protocol}/{alias}"
            f = open(f"{folder_path}/transactions.csv", "w")
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            for row in table:
                writer.writerow(row)
                writer_all.writerow(row)
            f.close()
f_all.close()
