import subprocess

import sys

sys.path.insert(0, "./scripts")
from swiftpaxos.scripts.utils import read_json, read_conf

from kill_all import kill_proc
from time import sleep

# Run master + replica

replicas = read_json("scripts/conf.json", ["replica"])
mstr = read_json("scripts/conf.json", ["master"])[0]

node_addresses = []
users = []
key_paths = []
aliases = []
for replica in replicas:
    node_addresses.append(replica["node_address"])
    users.append(replica["user"])
    key_paths.append(replica["key_path"])
    aliases.append(replica["alias"])

server_ip = mstr["server_ip"]
master_address = mstr["node_address"]
master_user = mstr["user"]
master_key_path = mstr["key_path"]
protocol = read_conf("scripts/conf.json", "protocol")
config_file = read_conf("scripts/conf.json", "config_file")
exp = read_conf("scripts/conf.json", "exp")
n = len(node_addresses)

# Kill master
kill_proc(master_key_path, f"{master_user}@{master_address}")
# Kill replicas
for i in range(n):
    kill_proc(key_paths[i], f"{users[i]}@{node_addresses[i]}")

# I don't know what will happen to half-open connections

# Run master
subprocess.run(
    [
        "ssh",
        "-i",
        master_key_path,
        f"{master_user}@{master_address}",
        f"sudo mkdir -p /mnt/share/exp/exp{exp}",
    ],
    check=True,
)
subprocess.run(
    [
        "ssh",
        "-i",
        master_key_path,
        f"{master_user}@{master_address}",
        f"sudo mkdir -p /mnt/share/exp/exp{exp}/{protocol} && sudo chmod 777 /mnt/share/exp/exp{exp}/{protocol}",
    ],
    check=True,
)
subprocess.run(
    [
        "ssh",
        "-i",
        master_key_path,
        f"{master_user}@{master_address}",
        f"sudo cp /mnt/share/src/swiftpaxos_copy/swiftpaxos/{config_file} /mnt/share/exp/exp{exp}/{protocol}.conf",
    ],
    check=True,
)

print("starting master...")
subprocess.Popen(
    [
        "ssh",
        "-i",
        master_key_path,
        f"{master_user}@{master_address}",
        f"cd /mnt/share/src/swiftpaxos_copy/swiftpaxos && go install -buildvcs=false && ~/go/bin/swiftpaxos -run master -config {config_file} -protocol {protocol} -log /mnt/share/exp/exp{exp}/{protocol}/master",
    ],
    stdout=subprocess.DEVNULL,
)

sleep(5)

# Run replica
for i in range(n):
    subprocess.run(
        [
            "ssh",
            "-i",
            key_paths[i],
            f"{users[i]}@{node_addresses[i]}",
            f"sudo mkdir -p ~/exp/exp{exp}/{protocol} && sudo chmod 777 ~/exp/exp{exp}/{protocol}",
        ],
        check=True,
    )
    print("starting " + aliases[i])
    subprocess.Popen(
        [
            "ssh",
            "-i",
            key_paths[i],
            f"{users[i]}@{node_addresses[i]}",
            f"cd /mnt/share/src/swiftpaxos_copy/swiftpaxos && go install -buildvcs=false && ~/go/bin/swiftpaxos -run server -config {config_file} -protocol {protocol} -alias {aliases[i]} -log ~/exp/exp{exp}/{protocol}/{aliases[i]}",
        ],
        stdout=subprocess.DEVNULL,
    )
