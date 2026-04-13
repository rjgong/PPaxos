import subprocess

import sys

sys.path.insert(0, "./scripts")
from swiftpaxos.scripts.utils import read_json

# Set up nfs for everyone else

replicas = read_json("scripts/conf.json", ["client", "replica"])
mstr = read_json("scripts/conf.json", ["master"])[0]

node_addresses = []
users = []
key_paths = []
server_ip = mstr["server_ip"]
for replica in replicas:
    node_addresses.append(replica["node_address"])
    users.append(replica["user"])
    key_paths.append(replica["key_path"])

n = len(node_addresses)
commands = [
    "sudo umount -f -l /mnt/share/src",  # If any
    "sudo umount -f -l /mnt/share/exp",
    "sudo mkdir -p /mnt/share",  # Root directory
    "sudo mkdir -p /mnt/share/src",  # Code folder
    "sudo mkdir -p /mnt/share/exp",  # Experiment results folder
    "sudo apt-get -y install nfs-common",  # nfs client
    f"sudo mount {server_ip}:/mnt/share/src /mnt/share/src",
    f"sudo mount {server_ip}:/mnt/share/exp /mnt/share/exp",
]

for i in range(n):
    print("setting up: ", replicas[i]["alias"])
    address = f"{users[i]}@{node_addresses[i]}"
    key_path = key_paths[i]
    for cmd in commands:
        try:
            subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no", "-i", key_path, address, cmd],
                check=True,
            )
        except Exception as e:
            print(repr(e))
