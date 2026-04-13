# Testing Scripts

There are 3 main parts: setting up, running, and aggregating. They're put in a folder accordingly.

> Notes:
>
> 1. The configurations of the files running in this folder is independent from the main repo.
> 2. These were not the scripts used for final set of testing

# Setting Up

They set up nfs for the targeted machines as the file name specified. The machine's address can be specified in `conf.json`. These are similar to start_mount.py

1. `setup_master_nfs.py`
2. `setup_other_nfs.py`

# Running

Run the repo. Please run nodes first before running clients (and wait until they're waiting for client connections)

1. `run_nodes.py`
2. `run_clients.py`

# Aggregation

Provided are the SQL scripts to do aggregation. Just convert the output (log) of the replicas and call `convert_csv.py`. Then dump the resulting csv file into some database systems like PostgreSQL and run the script

> Schema can be inferred from output of `convert_csv.py`

# Usage

1. Please make sure each machines specified in conf.json has go installed. Afterwards, make sure they have nfs set up by running the setup nfs files.
2. Adjust the actual config file for the repo and set the experiment number along with protocol in conf.json accordingly.
3. Run the nodes
4. Run the clients
5. Repeat these 3 steps until satisfied
6. Aggregate the experiments for a particular experiment number

# Others

There also some other hopefully convenient scripts

- `copy_ip_to_conf.sh` - copy stuff from conf.json (delete and rewrite)
- `set_traffic.sh` - set delay (used with `latency.py`)
