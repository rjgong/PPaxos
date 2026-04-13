import matplotlib.pyplot as plt
import numpy as np
import csv

ROOT_PATH = "out"

values = {}

file_path = "out/cdf.csv"
f = open(file_path, "r")
reader = csv.DictReader(f)

for row in reader:
  if row["protocol"] in values:
    values[row["protocol"]][row["percentage"]] = row["latency"]
  else:
    values[row["protocol"]] = {row["percentage"]: row["latency"]}
f.close()


fig, ax = plt.subplots(figsize=(8,8))
for protocol in values:
  keys = [int(x) for x in values[protocol]]
  vs = [float(values[protocol][str(x)]) for x in keys]
  ax.plot(vs, keys, '-', label=protocol)


# plt.plot(d, c, '.', color='blue')
plt.xlabel('Data Values')         
plt.ylabel('CDF')                  
plt.title('CDF via Sorting')  
plt.legend()     
plt.grid()                         
plt.savefig("cdf.png")