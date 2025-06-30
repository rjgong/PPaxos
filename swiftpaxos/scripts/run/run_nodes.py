import subprocess

import sys
sys.path.insert(0, "./scripts")
from utils import read_json, read_conf

from kill_all import kill_proc

#Run master + replica

replicas = read_json("scripts/conf.json", ["replica"])
mstr = read_json("scripts/conf.json", ["master"])[0]

node_addresses = []
users = []
key_paths = []
aliases=[]
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

#Kill master
kill_proc(master_key_path, f"{master_user}@{master_address}")
#Kill replicas
for i in range(n):
    kill_proc(key_paths[i], f"{users[i]}@{node_addresses[i]}")

#I don't know what will happen to half-open connections

#Run master
subprocess.run(["ssh", "-i", master_key_path, f"{master_user}@{master_address}", f"sudo mkdir -p /mnt/share/exp/exp{exp}"], check=True)
subprocess.run(["ssh", "-i", master_key_path, f"{master_user}@{master_address}", f"sudo mkdir -p /mnt/share/exp/exp{exp}/{protocol} && sudo chmod 777 /mnt/share/exp/exp{exp}/{protocol}"], check=True)
subprocess.run(["ssh", "-i", master_key_path, f"{master_user}@{master_address}", f"sudo cp /mnt/share/src/swiftpaxos_copy/{config_file} /mnt/share/exp/exp{exp}"], check=True)

print("starting master...")
subprocess.Popen(["ssh", "-i", master_key_path, f"{master_user}@{master_address}", f"cd /mnt/share/src/swiftpaxos_copy && sudo git pull && go install -buildvcs=false && ~/go/bin/swiftpaxos -run master -config {config_file} -protocol {protocol} -quorum quorum.conf"])

# breakpoint()

#Run replica
for i in range(n):
    print("starting " + aliases[i])
    subprocess.Popen(["ssh", "-i", key_paths[i], f"{users[i]}@{node_addresses[i]}", f"cd /mnt/share/src/swiftpaxos_copy && go install -buildvcs=false && ~/go/bin/swiftpaxos -run server -config {config_file} -protocol {protocol} -alias {aliases[i]} -quorum quorum.conf"])