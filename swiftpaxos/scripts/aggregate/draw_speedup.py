from pprint import pprint
from matplotlib import pyplot as plt
import csv

import sys
sys.path.insert(0, "./scripts")
from utils import aggregate_alias

protocols = ["curp", "epaxos", "fastpaxos", "n2paxos", "paxos", "swiftpaxos"]
aliases = ["client1", "client2", "client3", "client4", "client5", "client6", "client7"]
# aliases = ["client6"]
wrt_idx = 4 #With respect to which protocol (the index)
csv_files = [
    {
        "name": "out/exp110",
        "conflict": 0
    },
    {
        "name": "out/exp111",
        "conflict": 10
    },
    {
        "name": "out/exp112",
        "conflict": 20
    },
    {
        "name": "out/exp113",
        "conflict": 30
    },
    {
        "name": "out/exp114",
        "conflict": 50
    },
    {
        "name": "out/exp115",
        "conflict": 40
    },
    {
        "name": "out/exp116",
        "conflict": 60
    },
    {
        "name": "out/exp117",
        "conflict": 70
    },
    {
        "name": "out/exp118",
        "conflict": 80
    },
    {
        "name": "out/exp119",
        "conflict": 100
    },
    {
        "name": "out/exp120",
        "conflict": 90
    },
]


def parse_csv(csv_files, protocols, aliases):
    def iterative_avg(row, variables):
        avg1, count1 = variables[0]["avg"], variables[0]["count"]
        avg2, count2 = float(row["avg"]), int(row["count"])
        return [
            {
                "avg": (avg1 * count1 + avg2 * count2) / (count1 + count2),
                "count": count1 + count2
            }
        ]
    def find_any_protocol_avg(protocol, avgs):
        for path in avgs:
            if protocol in avgs[path]:
                return avgs[path][protocol]
        return {"avg": 0, "count": 0}
    
    avgs = {}
    for obj in csv_files:
        path = obj["name"]
        conflict = obj["conflict"]

        avgs[conflict] = {}
        for protocol in protocols:
            avg = {"avg": 0, "count": 0}
            for alias in aliases:
                try:
                    avg, = aggregate_alias(f"{path}/{protocol}/{alias}/avg.csv", [avg], iterative_avg)
                except:
                    avg = find_any_protocol_avg(protocol, avgs)
            avgs[conflict][protocol] = avg
    return avgs


avgs = parse_csv(csv_files, protocols, aliases)
ref_protocol = protocols[wrt_idx]
ref_val = avgs[0][ref_protocol]["avg"]

speedup_matrix = [[] for _ in protocols]
conflicts = sorted(list(avgs.keys()))
for conflict in conflicts:
    latencies = avgs[conflict]
    for i, protocol in enumerate(protocols):
        speedup_matrix[i].append(ref_val/latencies[protocol]["avg"])

for i,latencies in enumerate(speedup_matrix):
    plt.plot(conflicts,latencies,label=protocols[i])
plt.legend()
plt.savefig("out.png")

