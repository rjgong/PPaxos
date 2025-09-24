#!/usr/bin/env python3
"""
check_final_deps.py

遍历 replica1.log … replica5.log，
提取形如
    2025/08/19 07:04:39 pareply.Replica 4 pareply.Instance 364 final deps [-1 -1 -1 -1 363]
的行，比对相同 instance 的 deps 是否一致。
"""

import re
import sys
import random
from pathlib import Path
from collections import defaultdict

# 正则：提取 instance 号和 deps 数组
LINE_RE = re.compile(
    r"Replica\s+(\d+)\s+.*?Instance\s+(\d+)\s+.*?final\s+deps\s+\[([^\]]+)\]",
    re.IGNORECASE,
)

def parse_log(log_path):
    """解析并返回列表 (replica_id, instance_id, deps_string)"""
    results = []
    with log_path.open("r", errors="ignore") as f:
        for line in f:
            if "final deps" not in line:
                continue
            m = LINE_RE.search(line)
            if not m:
                continue
            replica_id = int(m.group(1))
            inst = int(m.group(2))
            deps_raw = m.group(3).strip()
            deps_str = " ".join(deps_raw.split())
            results.append((replica_id, inst, deps_str))
    return results


def main(log_dir):
    log_dir = Path(log_dir)
    replica_files = [
        log_dir / f"replica{i}.log" for i in range(1, 6)
    ]
    for fp in replica_files:
        if not fp.exists():
            sys.exit(f"Error: {fp} 不存在")

    # 所有数据：{(replica_id, instance_id) -> {log_replica_id: deps_str}}
    all_data = defaultdict(dict)

    for log_replica_id, fp in enumerate(replica_files, start=1):
        for replica_id, inst, deps in parse_log(fp):
            key = (replica_id, inst)
            # 如果同一日志中出现多次相同 key，只保留第一次出现的值（或最后一次，当前策略保留第一次）
            all_data[key].setdefault(log_replica_id, deps)

    inconsistent = []

    for (replica_id, inst), dep_map in sorted(all_data.items()):
        # 如果一个 (Replica, Instance) 在至少两个日志中出现并且 deps 不一致，则报告
        if len(dep_map) < 2:
            continue  # 只有一个来源，无法比较
        deps_set = set(dep_map.values())
        if len(deps_set) > 1:
            inconsistent.append(((replica_id, inst), dep_map))

    if not inconsistent:
        print(f"All {len(all_data)} (replica, instance) pairs are consistent across logs.")

    # 随机抽样输出
    if sample_n:
        print(f"\nRandomly sampling {sample_n} (replica, instance) pair(s):")
        sample_keys = random.sample(list(all_data.keys()), min(sample_n, len(all_data)))
        for replica_id, inst in sample_keys:
            dep_map = all_data[(replica_id, inst)]
            print(f"replica {replica_id} instance {inst}:")
            for log_replica_id, deps in sorted(dep_map.items()):
                print(f"  from replica{log_replica_id}.log: {deps}")
            print()

    if not inconsistent:
        return

    print("Found inconsistencies:")
    for (replica_id, inst), dep_map in inconsistent:
        print(f"replica {replica_id} instance {inst}:")
        for log_replica_id, deps in sorted(dep_map.items()):
            print(f"  from replica{log_replica_id}.log: {deps}")
        print()

if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        print("Usage: python3 check_final_deps.py /path/to/logs [sample_n]")
        sys.exit(1)
    sample_n = int(sys.argv[2]) if len(sys.argv) == 3 else 0
    main(sys.argv[1])