import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from itertools import accumulate
import csv

ROOT_PATH = "out"

experiment_number = 110
protocols = ["curp", "epaxos", "fastpaxos", "n2paxos", "paxos", "swiftpaxos"]
alias = "client1"

values = {}
for protocol in protocols:
  file_path = f"{ROOT_PATH}/exp{experiment_number}/{protocol}/{alias}/cdf.csv"
  df = pd.read_csv(file_path)

  values[protocol] = df["latency"].tolist()
df = pd.DataFrame(values)


# START A PLOT
fig,ax = plt.subplots()

for col in df.columns:

  # SKIP IF IT HAS ANY INFINITE VALUES
  if not all(np.isfinite(df[col].values)):
    continue

  # USE numpy's HISTOGRAM FUNCTION TO COMPUTE BINS
  xh, xb = np.histogram(df[col], bins=120,)

  # COMPUTE THE CUMULATIVE SUM WITH accumulate
  xh = list(accumulate(xh))
  # NORMALIZE THE RESULT
  xh = np.array(xh) / max(xh)

  # PLOT WITH LABEL
  ax.plot(xb[1:], xh, label=col)
ax.legend()
plt.title("CDFs of Columns")
plt.savefig("cdf.png")