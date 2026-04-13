import subprocess
import sys

sys.path.insert(0, "./scripts")
from swiftpaxos.scripts.utils import read_json

exps = [21]
protocols = ["epaxos"]

clients = read_json("scripts/conf.json", ["client"])
for client in clients:
    key_path = client["key_path"]
    user = client["user"]
    node_address = client["node_address"]
    alias = client["alias"]

    address = f"{user}@{node_address}"
    for exp in exps:
        for protocol in protocols:
            subprocess.run(
                [
                    "ssh",
                    "-i",
                    key_path,
                    address,
                    f"sudo rm -rf ~/exp/exp{exp}/{protocol}/{alias}",
                ]
            )
