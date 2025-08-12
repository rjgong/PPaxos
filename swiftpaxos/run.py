import subprocess
import threading
import time
import yaml

def is_valid_ip(s):
    """一个简单的辅助函数，用来判断一个字符串是否是IP地址格式。"""
    parts = s.split('.')
    if len(parts) != 4:
        return False
    for item in parts:
        if not item.isdigit() or not 0 <= int(item) <= 255:
            return False
    return True

config_data = {
    "master": [],
    "replicas": [],
    "clients": []
}
current_section = None

with open('local.conf', 'r') as file:
    for line in file:
        
        line_content = line.split('//')[0]
        clean_line = line_content.strip()
        if not clean_line:
            continue
        if clean_line.startswith('--') and clean_line.endswith('--'):
            section_name = clean_line.strip('- ').lower()
            if 'replica' in section_name:
                current_section = 'replicas'
            elif 'client' in section_name:
                current_section = 'clients'
            elif 'master' in section_name:
                current_section = 'master'
            continue
        if current_section:
            parts = clean_line.split()
            if len(parts) == 2 and is_valid_ip(parts[1]):
                alias, ip = parts
                config_data[current_section].append((alias, ip))

master_nodes = config_data['master']
replica_nodes = config_data['replicas']
client_nodes = config_data['clients']
print(master_nodes)


REMOTE_SWIFΤPAXOS_EXECUTABLE = "./share/swiftpaxos/swiftpaxos" # 如果它就在用户的 home 目录或其他已知相对路径
REMOTE_CONFIG_FILE = "./share/swiftpaxos/local.conf"            # 同样，相对于 swiftpaxos 可执行文件的路径或绝对路径


def execute_ssh_and_background_no_log(host, swiftpaxos_base_command_args, node_alias, role_name):

    remote_shell_command = f"nohup {' '.join(swiftpaxos_base_command_args)} > /dev/null 2>&1 &"

    # 构建将由本地 Python 脚本运行的完整 SSH 命令参数列表
    full_ssh_command_args = [
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'BatchMode=yes',
        '-o', 'ConnectTimeout=10',
        host,
        remote_shell_command
    ]

    print(f"  [Thread for {node_alias} ({role_name}) on {host}] Attempting to launch: {remote_shell_command}")

    process = subprocess.run(full_ssh_command_args, capture_output=True, text=True, check=False, timeout=30)


def launch_node_group_async_no_log(nodes_to_launch, role_name_for_run_arg):
    """
    并行启动一组节点。每个节点的命令都在远程主机后台运行，输出被丢弃。
    此函数会等待本组所有 SSH 命令发送完毕后才返回。
    """
    threads = []
    # 使用 role_name_for_run_arg 来决定显示的是 "SERVER" (for replicas) 还是 "MASTER"/"CLIENT"
    display_role_name = "SERVER" if role_name_for_run_arg == "server" else role_name_for_run_arg.upper()
    print(f"\n--- Initiating launch for {display_role_name} nodes ---")

    for alias, host_ip in nodes_to_launch:
        # 构建 swiftpaxos 命令的参数列表
        # 确保路径是相对于用户在远程 SSH 登录后的当前目录，
        # 或者使用绝对路径。
        core_swiftpaxos_command_args = [
            REMOTE_SWIFΤPAXOS_EXECUTABLE,
            "-run", role_name_for_run_arg,
            "-config", REMOTE_CONFIG_FILE,
            "-alias", alias,
            "-log", f"./share/swiftpaxos/logs/{alias}.log"
        ]

        thread = threading.Thread(target=execute_ssh_and_background_no_log,
                                 args=(host_ip, core_swiftpaxos_command_args, alias, display_role_name))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print(f"--- All SSH launch commands for {display_role_name} group sent. ---")
    print(f"--- Since output is discarded, verify process status on remote hosts (e.g., using 'ps aux | grep swiftpaxos'). ---")


def launch_node_group_serial_no_log(nodes_to_launch, role_name_for_run_arg):
    """
    串行启动一组节点。每个节点的命令都在远程主机后台运行，输出被丢弃。
    此函数会等待上一个节点SSH命令发送完毕后，再发送下一个。
    """
    # 使用 role_name_for_run_arg 来决定显示的是 "SERVER" (for replicas) 还是 "MASTER"/"CLIENT"
    display_role_name = "SERVER" if role_name_for_run_arg == "server" else role_name_for_run_arg.upper()
    print(f"\n--- Initiating launch for {display_role_name} nodes (in serial) ---")

    # 按顺序遍历并启动每个节点
    for alias, host_ip in nodes_to_launch:
        # 构建 swiftpaxos 命令的参数列表
        # 确保路径是相对于用户在远程 SSH 登录后的当前目录，
        # 或者使用绝对路径。
        core_swiftpaxos_command_args = [
            REMOTE_SWIFΤPAXOS_EXECUTABLE,
            "-run", role_name_for_run_arg,
            "-config", REMOTE_CONFIG_FILE,
            "-alias", alias,
            "-log", f"./share/swiftpaxos/logs/{alias}.log"
        ]

        # *** 核心改动在这里 ***
        # 不再创建和启动线程，而是直接调用执行函数。
        # 程序会在这里等待 execute_ssh_and_background_no_log 完成，
        # 然后再开始下一次循环。
        print(f"--> Launching node {alias} on {host_ip}...")
        execute_ssh_and_background_no_log(host_ip, core_swiftpaxos_command_args, alias, display_role_name)
        print(f"--> SSH command for {alias} sent.")

    print(f"\n--- All SSH launch commands for {display_role_name} group sent sequentially. ---")
    print(f"--- Since output is discarded, verify process status on remote hosts (e.g., using 'ps aux | grep swiftpaxos'). ---")

# --- 主执行流程 ---

# 1. 启动 Master(s)
# `role_name_for_run_arg` 必须是 "master"
launch_node_group_async_no_log(master_nodes, "master")
print("Master launch sequence initiated. Pausing before starting servers (replicas)...")
MASTER_INIT_DELAY = 1
time.sleep(MASTER_INIT_DELAY)

# 2. 启动 Servers (Replicas)
# `role_name_for_run_arg` 必须是 "server"
launch_node_group_async_no_log(replica_nodes, "server") # 注意这里的 "server"
print("Server (Replica) launch sequence initiated. Pausing before starting clients...")
SERVER_INIT_DELAY = 1
time.sleep(SERVER_INIT_DELAY)

# 3. 启动 Client(s)
# `role_name_for_run_arg` 必须是 "client"
launch_node_group_async_no_log(client_nodes, "client")
print("Client launch sequence initiated.")

print("\nAll node launch sequences have been initiated.")