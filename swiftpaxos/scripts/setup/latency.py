import json
import subprocess

import sys

sys.path.insert(0, "./scripts")
from swiftpaxos.scripts.keep.utils import read_json

# Sets up latency between replicas


def arrangeLatencies(latencies):
    nodes = latencies.keys()
    N = len(nodes)

    for node in latencies:
        if (
            len(latencies[node]["latencies"].keys()) < N
        ):  # in case missing any stupid stuff
            for n in nodes:
                if n not in latencies[node]["latencies"]:
                    if node in latencies[n]["latencies"]:
                        latencies[node]["latencies"][n] = latencies[n]["latencies"][
                            node
                        ]
                    else:
                        latencies[node]["latencies"][n] = (
                            "0ms"  # 0ms if not specified by either side
                        )
    return latencies


nodes = read_json("scripts/conf.json", ["replica", "client"])

replicas_dict = {}
for node in nodes:
    replicas_dict[node["alias"]] = node
replicas_dict = arrangeLatencies(replicas_dict)
# print(json.dumps(replicas_dict, indent=4))

for replica in replicas_dict:
    print(f"\n\nSetting traffic for {replica}")
    r = replicas_dict[replica]
    latencies = r["latencies"]

    param_string = (
        "ens5"  # This is the network interface. Can make this a property of the json
    )
    for latency in latencies:
        ip = replicas_dict[latency]["node_address"]
        delay = latencies[latency]
        if delay[0] != "0":
            param_string += f" {ip} {delay}"
    subprocess.run(
        [
            "ssh",
            "-i",
            r["key_path"],
            f"{r['user']}@{r['node_address']}",
            f"sudo /mnt/share/src/set_traffic.sh {param_string}",
        ],
        check=True,
    )
