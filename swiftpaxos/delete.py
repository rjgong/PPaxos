import subprocess

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

all_active_nodes = master_nodes + replica_nodes + client_nodes
hosts = [node[1] for node in all_active_nodes]

#subprocess.call(['ssh', '-o', 'StrictHostKeyChecking=no', "57.182.103.74", "rm -rf ~/share/swiftpaxos/logs/*"])

for i in range(len(hosts)):
    # each region takes a subnet
    host = hosts[i % len(hosts)]
    print(host)
    #print(PORT1, PORT2)
    ssh_command = ["rm ~/share/swiftpaxos/logs/*"]

    '''
    ssh_command = [
        "for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do sudo apt-get remove $pkg; done",
        "sudo apt-get update",
        "sudo apt-get upgrade",
        "sudo apt-get install ca-certificates curl gnupg -y",
        "sudo install -m 0755 -d /etc/apt/keyrings",
        "curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg",
        "sudo chmod a+r /etc/apt/keyrings/docker.gpg",
        "echo \
        \"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
        $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable\" | \
        sudo tee /etc/apt/sources.list.d/docker.list > /dev/null",
        "sudo apt-get update",
        "sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y",
        "sudo apt install nfs-common -y",
        "sudo apt install nfs-server -y",
        "sudo systemctl start nfs-server",
        "sudo systemctl start rpcbind",
        "sudo systemctl enable nfs-server",
        "sudo systemctl enable rpcbind",
        "sudo docker swarm join --token SWMTKN-1-24uixvfxt6kbbrrt0q3sxpdnowl16mt8z4nhh3xetcq2hx5zuc-bwarqqxyucxt93u5e07r4rt59 57.182.103.74:2377",
        "mkdir share",
        "sudo exportfs -r",
        "sudo mount 57.182.103.74:/home/ubuntu/share share"
    ]
    '''

    for command in ssh_command:
        print(command)
        subprocess.call(['ssh', '-o', 'StrictHostKeyChecking=no', host, command])


