import csv
import json
from matplotlib import pyplot as plt

exps = ["30", "31", "32", "33", "34", "35"]

data = dict({})
f = open("data-1763128794902.csv", "r")
reader = csv.DictReader(f)
for row in reader:
    exp = row["exp"]
    protocol = row["protocol"]

    if exp not in data:
        data[exp] = dict({})
    if protocol not in data[exp]:
        data[exp][protocol] = dict({})

    data[exp][protocol]["throughput"] = float(row["throughput"]) / 1000
    data[exp][protocol]["avg"] = float(row["avg"])
f.close()

print(json.dumps(data, indent=4))

data_points = dict()
for exp in exps:
    for protocol in data[exp]:
        if protocol not in data_points:
            data_points[protocol] = {"throughputs": [], "avgs": []}
        data_points[protocol]["throughputs"].append(data[exp][protocol]["throughput"])
        data_points[protocol]["avgs"].append(data[exp][protocol]["avg"])

# data_points["swiftpaxos 0%"] = {
#     "throughputs": data_points["swiftpaxos"]["throughputs"][:2]
#     + data_points["swiftpaxos"]["throughputs"][7:],
#     "avgs": data_points["swiftpaxos"]["avgs"][:2]
#     + data_points["swiftpaxos"]["avgs"][7:],
# }
# data_points["swiftpaxos"] = {
#     "throughputs": data_points["swiftpaxos"]["throughputs"][:6],
#     "avgs": data_points["swiftpaxos"]["avgs"][:6],
# }
# data_points["eppaxos"] = {
#     "throughputs": data_points["eppaxos"]["throughputs"][:7],
#     "avgs": data_points["eppaxos"]["avgs"][:7],
# }
# data_points["epaxos"] = {
#     "throughputs": data_points["epaxos"]["throughputs"][:8],
#     "avgs": data_points["epaxos"]["avgs"][:8],
# }
print(json.dumps(data_points, indent=4))

fig = plt.figure(dpi=300)
ax = fig.add_subplot(1, 1, 1)
for protocol in data_points:
    if True:
        ax.plot(
            data_points[protocol]["throughputs"],
            data_points[protocol]["avgs"],
            "*-",
            label=protocol,
        )

# Shrink current axis by 20%
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
# Put a legend to the right of the current axis
ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))
ax.set_title("Throughput")
ax.set_xlabel("throughput (Kcmd/sec)")
ax.set_ylabel("avg latency (ms)")
# plt.show()
plt.savefig("throughput.png")
