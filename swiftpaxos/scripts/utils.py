import json
import csv
import os
from itertools import islice


def read_json(path, roles):
    objs = []
    f = open(path, "r")
    js = json.load(f)["nodes"]
    for j in js:
        role = j["roles"]
        if role in roles:
            objs.append(j)

    f.close()
    return objs


def read_conf(path, key):
    f = open(path, "r")
    val = json.load(f)[key]
    f.close()
    return val


def aggregate_alias(path, variables, func):
    f = open(path, "r")
    reader = csv.DictReader(f)
    num_rows = sum(1 for _ in reader)

    f.seek(0)
    for row in islice(reader, 1, None, None):
        row["num_of_rows"] = num_rows
        variables = func(row, variables)
    f.close()
    return variables


def aggregate_protocol(path, variables, f, file_name="transactions.csv"):
    aliases = [x for x in os.listdir(path) if os.path.isdir(f"{path}/{x}")]
    for alias in aliases:
        variables = aggregate_alias(f"{path}/{alias}/{file_name}", variables, f)
    return variables


def sorted_csv_read(
    file_paths, field_name, variables, f
):  # For csv files (no need to be sorted)
    alias_files = [
        open(path)
        for path in file_paths
        if os.path.isfile(path) and os.path.splitext(path)[-1] == ".csv"
    ]
    alias_readers = [csv.DictReader(x) for x in alias_files]
    n = len(alias_readers)

    if n == 0:
        return variables

    # Get total number of records
    total_records = 0
    for reader in alias_readers:
        total_records += sum(1 for _ in reader)

    iterators = []
    for i in range(1, n):
        alias_files[i - 1].seek(0)
        iterators.append(islice(alias_readers[i - 1], 1, None, None))
        total_records += sum(1 for _ in alias_readers[i])
    alias_files[n - 1].seek(0)
    iterators.append(islice(alias_readers[n - 1], 1, None, None))

    counts = [0 for _ in alias_readers]
    rows = [next(iter, None) for iter in iterators]
    latencies = [
        float(row[field_name]) if row is not None else float("inf") for row in rows
    ]
    idx = latencies.index(min(latencies))
    counts[idx] += 1
    while sum(counts) <= total_records:
        row = rows[idx]
        row["num_of_rows"] = total_records
        variables = f(row, variables)

        rows = [
            next(iter, None) if i == idx else rows[i]
            for i, iter in enumerate(iterators)
        ]
        latencies = [
            float(row[field_name]) if row is not None else float("inf") for row in rows
        ]
        idx = latencies.index(min(latencies))
        counts[idx] += 1

    for f in alias_files:
        f.close()
    return variables


def sorted_access(path, variables, f, file_name="transactions.csv"):
    alias_files = [
        f"{path}/{x}/{file_name}"
        for x in os.listdir(path)
        if os.path.isdir(f"{path}/{x}")
    ]
    return sorted_csv_read(alias_files, "latency", variables, f)


def sorted_access_per_alias(path, variables, f, file_name="transactions.csv"):
    alias_files = [f"{path}/{file_name}"]
    return sorted_csv_read(alias_files, "latency", variables, f)


def is_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


# Testing
if __name__ == "__main__":
    print(
        aggregate_alias(
            "out/exp1/swiftpaxos/client1/transactions.csv",
            [0],
            lambda row, variables: [variables[0] + 1],
        )
    )
