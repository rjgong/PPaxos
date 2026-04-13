import subprocess

import sys

sys.path.insert(0, "./scripts")
from swiftpaxos.scripts.utils import read_json

# Set up nfs for host + clone repository
# This is supposed to only be done once

(mstr,) = read_json("scripts/conf.json", ["master"])
node_address = mstr["node_address"]
user = mstr["user"]
key_path = mstr["key_path"]

address = f"{user}@{node_address}"

commands = [
    "sudo mkdir -p /mnt/share",  # Root directory
    "sudo mkdir -p /mnt/share/src",  # Code folder
    "sudo mkdir -p /mnt/share/exp",  # Experiment results folder
    "sudo chown nobody:nogroup /mnt/share/src",
    "sudo chmod 777 /mnt/share/src",
    "sudo chown nobody:nogroup /mnt/share/exp",
    "sudo chmod 777 /mnt/share/exp",
    "sudo apt install nfs-kernel-server",  # nfs server
    "sudo rm -f /etc/exports",  # If any
    'sudo bash -c "echo \\"/mnt/share/exp *(rw)\\" >> /etc/exports"',  # Register folder under nfs
    'sudo bash -c "echo \\"/mnt/share/src *(rw)\\" >> /etc/exports"',
    "sudo systemctl restart nfs-kernel-server",
    "sudo rm -rf /mnt/share/src/swiftpaxos_copy",  # If have any
    "sudo git clone https://github.com/rjgong/PPaxos.git /mnt/share/src/swiftpaxos_copy",
]
for cmd in commands:
    try:
        subprocess.run(["ssh", "-i", key_path, address, cmd], check=True)
    except Exception as e:
        print(repr(e))
        break
