import subprocess
import sys
import math
from threading import Thread

sys.path.insert(0, "./scripts")
from swiftpaxos.scripts.utils import read_json

exps = [41]

OUT = "out"
NUMBER_OF_THREADS = 5

nodes = read_json("scripts/conf.json", ["replica", "master", "client"])


def downloadFolder(nodes, exps, path="~/exp"):
    for client in nodes:
        key_path = client["key_path"]
        user = client["user"]
        node_address = client["node_address"]

        address = f"{user}@{node_address}"
        for exp in exps:
            subprocess.run(
                f'rsync -az -e "ssh -o StrictHostKeyChecking=no -i {key_path}" {address}:{path if client["roles"] != "master" else "/mnt/share/exp"}/exp{exp} {OUT}',
                shell=True,
            )
        print(f"{client['alias']} done")


threads = []
s = math.ceil(len(nodes) / NUMBER_OF_THREADS)
i = 0
while i * s < len(nodes):
    t = Thread(target=downloadFolder, args=(nodes[i * s : (i + 1) * s], exps))
    t.start()
    threads.append(t)
    i += 1

for t in threads:
    t.join()
