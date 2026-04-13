from matplotlib import pyplot as plt
import numpy as np
import csv


ROOT_PATH = "out"
file_path = "median.csv"

regions = ["Jakarta", "Calgary", "Tel Aviv", "Cape Town", "Sao Paulo"]
protocol_names = {  # (protocol, exp number)
    ("eppaxos", "11"): "Eppaxos 0%",
    ("epaxos", "11"): "Epaxos 0%",
    ("swiftpaxos", "11"): "Swiftpaxos 0%",
    ("eppaxos", "10"): "Eppaxos 2%",
    ("epaxos", "10"): "Epaxos 2%",
    ("swiftpaxos", "10"): "Swiftpaxos 2%",
    ("eppaxos", "9"): "Eppaxos 100%",
    ("epaxos", "9"): "Epaxos 100%",
    ("swiftpaxos", "9"): "Swiftpaxos 100%",
    ("paxos", "9"): "Paxos",
}

f = open(file_path)
data = dict({})
reader = csv.DictReader(f)
for row in reader:
    region = row["region"]
    exp = row["exp"]
    protocol = row["protocol"]
    title = protocol_names[(protocol, exp)]

    if title not in data:
        data[title] = dict({})
    if region not in data[title]:
        data[title][region] = dict({})

    data[title][region]["median"] = float(row["median"])
    data[title][region]["99th_percentile"] = float(row["99th_percentile"])
f.close()

# print(json.dumps(data, indent=4))

x = np.arange(len(regions))
width = 0.09  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(layout="constrained")

for _, protocol_desc in protocol_names.items():
    medians = []
    percentiles = []
    for region in regions:
        if region in data[protocol_desc]:
            medians.append(data[protocol_desc][region]["median"])
            percentiles.append(data[protocol_desc][region]["99th_percentile"])

    offset = width * multiplier - 0.25
    rects = ax.bar(
        x + offset,
        medians,
        width,
        label=protocol_desc,
        align="center",
        yerr=[[0 for _ in percentiles], percentiles],
    )
    multiplier += 1


# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel("Latency (ms)")
ax.set_title("Median Latencies")
ax.set_xticks(x + width, regions)
ax.legend(loc="best", ncols=3, prop={"size": 6})

# plt.show()
plt.savefig("median.png", dpi=300)
