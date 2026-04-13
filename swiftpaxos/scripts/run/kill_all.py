import subprocess
import sys

sys.path.insert(0, "./scripts")
from swiftpaxos.scripts.keep.utils import read_json


def kill_proc(key_path, address):
    try:
        commands = (
            subprocess.run(
                ["ssh", "-i", key_path, address, "sudo ps -ef | grep swiftpaxos"],
                check=True,
                capture_output=True,
            )
            .stdout.decode("utf8")
            .split("\n")
        )
    except Exception as e:
        print(repr(e))
        sys.exit(1)
    for command in commands:
        cmds = command.split()
        if len(cmds) >= 2:
            pid = cmds[1]
            killed = subprocess.run(
                ["ssh", "-i", key_path, address, "kill " + pid], capture_output=True
            )
            if killed.returncode == 0:
                print("killed: " + command)


if __name__ == "__main__":
    nodes = read_json("scripts/conf.json", ["client", "replica", "master"])
    for node in nodes:
        kill_proc(node["key_path"], f"{node['user']}@{node['node_address']}")
