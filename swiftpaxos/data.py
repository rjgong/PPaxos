import re
import numpy as np
import os
from collections import defaultdict
from datetime import datetime, timedelta

def parse_latency_line(line_text):
    """
    解析一行日志，返回 (时间戳, 延迟值)；若无法解析则返回 (None, None)。
    支持两种格式：
      1) "YYYY/MM/DD HH:MM:SS <index> -> <latency>"
      2) "YYYY/MM/DD HH:MM:SS <latency>"
    """
    if not line_text:
        return None, None
    time_match = re.match(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line_text)
    if not time_match:
        return None, None
    try:
        ts = datetime.strptime(time_match.group(1), '%Y/%m/%d %H:%M:%S')
    except ValueError:
        return None, None

    # 只在时间戳之后解析。避免把诸如 "connecting ...:7070" 的端口号识别为延迟
    rest = line_text[time_match.end():].strip()

    # 优先匹配形如 "... -> <number>" 的格式
    arrow_match = re.search(r'->\s*([0-9]+(?:\.[0-9]+)?)\s*$', rest)
    if arrow_match:
        try:
            return ts, float(arrow_match.group(1))
        except ValueError:
            return None, None

    # 仅当时间戳后面只剩一个数字且是小数时，才当作 "时间 延迟"
    # 这样可避免把诸如 IP/端口等尾随数字误判为延迟
    simple_num_match = re.match(r'^([0-9]+\.[0-9]+)\s*$', rest)
    if simple_num_match:
        try:
            return ts, float(simple_num_match.group(1))
        except ValueError:
            return None, None

    return None, None

def extract_latencies_with_times(log_text):
    """
    从日志文本中提取所有 (时间戳, 延迟) 元组，按时间顺序返回。
    """
    results = []
    for raw_line in log_text.strip().split('\n'):
        line = raw_line.strip()
        ts, val = parse_latency_line(line)
        if ts is not None and val is not None:
            results.append((ts, val))
    results.sort(key=lambda tv: tv[0])
    return results

def extract_session_first_latency(log_text):
    """
    取“会话首次延迟”：
    - 先找文件中最后一次出现的开始标记时间（包含 "dialing master..." 或 "connecting to ")
    - 再返回该时间点之后（含当时）出现的第一条延迟的时间戳
    - 若没有开始标记，则退化为文件中的第一条延迟时间戳
    """
    lines = log_text.strip().split('\n')
    last_start_ts = None

    # 第一次遍历：定位最后一次开始标记的时间
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if ("dialing master" in line) or ("connecting to" in line):
            tm = re.match(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line)
            if tm:
                try:
                    ts = datetime.strptime(tm.group(1), '%Y/%m/%d %H:%M:%S')
                    last_start_ts = ts
                except ValueError:
                    continue

    # 第二次遍历：寻找 last_start_ts 之后的第一条延迟
    first_latency_overall = None
    for raw_line in lines:
        line = raw_line.strip()
        ts, val = parse_latency_line(line)
        if ts is None or val is None:
            continue
        if first_latency_overall is None:
            first_latency_overall = ts
        if last_start_ts is None or ts >= last_start_ts:
            return ts

    return first_latency_overall

def extract_start_time_and_latencies(log_text):
    """
    从日志中提取开始时间和延迟数据。
    开始时间定义为出现 "dialing master..." 的时间。
    只返回开始后 1-3 分钟内的延迟数据。
    """
    lines = log_text.strip().split('\n')
    start_time = None
    latencies = []
    
    for line in lines:
        line_content = line.strip()
        if not line_content:
            continue
            
        # 查找开始时间（"dialing master..." 出现的时间）
        if "dialing master..." in line_content and start_time is None:
            # 提取时间戳
            time_match = re.match(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line_content)
            if time_match:
                start_time = datetime.strptime(time_match.group(1), '%Y/%m/%d %H:%M:%S')
                continue
        
        # 如果还没找到开始时间，继续查找
        if start_time is None:
            continue
            
        # 尝试解析延迟数据行
        # 格式1: "2025/07/28 09:57:33 0 -> 229.157"
        # 格式2: "2025/07/28 09:57:30 52.309308"
        time_match = re.match(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line_content)
        if time_match:
            current_time = datetime.strptime(time_match.group(1), '%Y/%m/%d %H:%M:%S')
            time_diff = (current_time - start_time).total_seconds()
            
            # 只处理 1-3 分钟内的数据
            if 120 <= time_diff <= 180:  # 60秒到180秒
                # 尝试提取延迟值
                parts = line_content.split()
                if len(parts) >= 3:
                    try:
                        # 尝试格式1: "时间 索引 -> 延迟值"
                        if len(parts) >= 4 and parts[-2] == '->':
                            latency_value = float(parts[-1])
                            latencies.append(latency_value)
                        # 尝试格式2: "时间 延迟值"
                        elif len(parts) == 3:
                            latency_value = float(parts[-1])
                            latencies.append(latency_value)
                    except ValueError:
                        continue
                        
    return start_time, latencies

def calculate_and_write_stats(file_handle, latency_list, group_name="", custom_thresholds=None, start_time=None):
    """
    计算延迟列表的统计数据并将结果写入提供的文件句柄。
    """
    if not latency_list:
        if group_name:
            file_handle.write(f"\n--- \"{group_name}\" 组的统计数据 ---\n")
            file_handle.write("未能提取到任何有效的延迟数据（开始后1-3分钟内）。\n")
        return

    latencies_np = np.array(latency_list)
    count = len(latencies_np)
    average_latency = np.mean(latencies_np)
    min_latency = np.min(latencies_np)
    max_latency = np.max(latencies_np)
    
    p50_latency = np.percentile(latencies_np, 50)
    p90_latency = np.percentile(latencies_np, 90)
    p95_latency = np.percentile(latencies_np, 95)
    p99_latency = np.percentile(latencies_np, 99)

    file_handle.write(f"\n--- \"{group_name}\" 组的统计数据 ---\n")
    if start_time:
        file_handle.write(f"开始时间: {start_time.strftime('%Y/%m/%d %H:%M:%S')}\n")
        file_handle.write(f"统计时间范围: 对齐后0-3分钟\n")
    file_handle.write(f"提取的延迟数据点数量: {count}\n")
    file_handle.write(f"平均延迟 (Ave): {average_latency:.6f} ms\n")
    file_handle.write(f"最小延迟: {min_latency:.6f} ms\n")
    file_handle.write(f"最大延迟: {max_latency:.6f} ms\n")
    
    file_handle.write("延迟百分位分布:\n")
    file_handle.write(f"  - 50百分位 (P50): {p50_latency:.6f} ms\n")
    file_handle.write(f"  - 90百分位 (P90): {p90_latency:.6f} ms\n")
    file_handle.write(f"  - 95百分位 (P95): {p95_latency:.6f} ms\n")
    file_handle.write(f"  - 99百分位 (P99): {p99_latency:.6f} ms\n")

    if custom_thresholds and isinstance(custom_thresholds, list):
        file_handle.write("自定义阈值下的延迟比例:\n")
        for threshold in custom_thresholds:
            if not isinstance(threshold, (int, float)):
                file_handle.write(f"  - 无效阈值: {threshold} (必须是数字)\n")
                continue
            count_below_threshold = np.sum(latencies_np < threshold)
            percentage_below_threshold = (count_below_threshold / count) * 100 if count > 0 else 0
            file_handle.write(f"  - 延迟 < {threshold} ms 的比例: {percentage_below_threshold:.2f}%\n")

# --- 主脚本逻辑 ---
log_directory = "logs"
output_file_name = "latency_analysis_report_aligned_0to3min.txt"
latency_thresholds_to_check = [60, 100, 200, 500] 

all_latencies_combined = []
log_groups = defaultdict(list)

# 扫描目录并对文件进行分组
print(f"正在扫描日志目录: '{log_directory}'...")
print("对齐规则: 等所有 client 首次出现延迟后再等 30 秒，再统计 0-3 分钟窗口...")
try:
    if not os.path.isdir(log_directory):
        raise FileNotFoundError(f"错误: 日志目录 '{log_directory}' 不存在。")
    
    # 正则表达式用于匹配 'client' 后跟数字
    group_pattern = re.compile(r'^(client\d+)')

    for filename in os.listdir(log_directory):
        match = group_pattern.match(filename)
        if match:
            group_name = match.group(1)
            file_path = os.path.join(log_directory, filename)
            if os.path.isfile(file_path):
                log_groups[group_name].append(file_path)

    if not log_groups:
        print("警告: 在目录下未找到任何匹配 'client<数字>...' 模式的日志文件。")

except Exception as e:
    print(e)
    log_groups = {}

"""
两阶段处理：
1) 先为每个 client 读取日志，提取所有 (ts, latency)，并记录该 client 的首次延迟时间 first_latency_ts。
2) 计算全局对齐起点 global_start = max(first_latency_ts for all groups) + 30s。
   然后对每个 client 只统计 [global_start, global_start+3min] 窗口内的延迟。
"""

# 扫描所有 client 日志文件：
# - 每个文件提取其“首次延迟时间”（若存在）
# - 全局对齐起点 = 所有文件的首次延迟中的“最晚一个” + 30 秒
# - 同时，按组汇总所有 (ts, latency) 以便后续分组统计
group_to_all_points = {}
group_to_first_latency_ts = {}
all_file_first_latency_ts = []
first_latency_by_file = []  # (file_path, first_latency_ts)
for group_name, file_list in sorted(log_groups.items()):
    merged = ""
    for file_path in sorted(file_list):
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as e:
            print(f"错误: 读取文件 {file_path} 时失败: {e}")
            content = ""

        # 用“会话首次延迟”作为该文件的首次延迟
        first_ts = extract_session_first_latency(content)
        if first_ts is not None:
            all_file_first_latency_ts.append(first_ts)
            first_latency_by_file.append((file_path, first_ts))
        merged += content + "\n"

    points = extract_latencies_with_times(merged)
    group_to_all_points[group_name] = points
    group_to_first_latency_ts[group_name] = points[0][0] if points else None

# 计算全局对齐起点
valid_file_first_ts = [ts for ts in all_file_first_latency_ts if ts is not None]
if valid_file_first_ts:
    latest_first_ts = max(valid_file_first_ts)
    # 找出来源文件（若有多个相同时间，取其中任意一个）
    source_file = None
    for fp, ts in first_latency_by_file:
        if ts == latest_first_ts:
            source_file = fp
            break
    global_start = latest_first_ts + timedelta(seconds=30)
else:
    global_start = None

with open(output_file_name, 'w', encoding='utf-8') as report_file:
    report_file.write("Python 延迟分析报告 (按客户端分组，对齐后0-3分钟窗口)\n")
    report_file.write("=========================================\n")
    report_file.write(f"报告生成于: {os.path.abspath(output_file_name)}\n")
    report_file.write(f"分析目录: '{log_directory}'\n")
    report_file.write(f"自定义延迟阈值检查: {latency_thresholds_to_check}\n")
    report_file.write("\n文件选择策略: 扫描所有 client 日志文件，取所有文件的最晚首次延迟 + 30s 作为对齐起点\n")

    if global_start is None:
        report_file.write("\n未能确定全局对齐起点（没有任何客户端出现延迟）。\n")
        report_file.write("--- 脚本执行完毕 ---\n")
        print(f"脚本执行完成。结果已保存到: {os.path.abspath(output_file_name)}")
    else:
        report_file.write(f"全局对齐起点: {global_start.strftime('%Y/%m/%d %H:%M:%S')} (= 所有客户端首次延迟的最晚时间 + 30s)\n")
        if first_latency_by_file:
            report_file.write("贡献最晚首次延迟的文件: ")
            if source_file:
                report_file.write(f"{os.path.basename(source_file)}\n")
            else:
                report_file.write("(未定位到文件)\n")
            # 展示最晚的前若干个文件首次延迟（按时间倒序展示前5个）
            top = sorted(first_latency_by_file, key=lambda kv: kv[1], reverse=True)[:5]
            report_file.write("前5个文件首次延迟(倒序):\n")
            for fp, ts in top:
                report_file.write(f"  - {os.path.basename(fp)}: {ts.strftime('%Y/%m/%d %H:%M:%S')}\n")
        report_file.write(f"统计窗口: 对齐后0-3分钟\n")
        report_file.write(f"开始统计时间: {global_start.strftime('%Y/%m/%d %H:%M:%S')}\n")
        if source_file:
            print(f"开始统计时间: {global_start.strftime('%Y/%m/%d %H:%M:%S')} (来自 {os.path.basename(source_file)} 的首次延迟)")
        else:
            print(f"开始统计时间: {global_start.strftime('%Y/%m/%d %H:%M:%S')}")

        all_latencies_combined = []
        for group_name in sorted(group_to_all_points.keys()):
            points = group_to_all_points[group_name]
            # 过滤窗口 [global_start, global_start+3min]
            window_end = global_start + timedelta(minutes=3)
            filtered = [val for (ts, val) in points if ts >= global_start and ts <= window_end]

            report_file.write(f"\n######################################################\n")
            report_file.write(f"### 开始处理分组: {group_name}\n")
            first_ts = group_to_first_latency_ts.get(group_name)
            if first_ts is not None:
                report_file.write(f"该组首次延迟时间: {first_ts.strftime('%Y/%m/%d %H:%M:%S')}\n")
            else:
                report_file.write("该组未发现任何延迟数据。\n")

            if filtered:
                report_file.write(f"成功从 \"{group_name}\" 组提取了 {len(filtered)} 个数据点（对齐后0-3分钟）。\n")
                all_latencies_combined.extend(filtered)
                calculate_and_write_stats(report_file, filtered, group_name,
                                          custom_thresholds=latency_thresholds_to_check,
                                          start_time=global_start)
            else:
                calculate_and_write_stats(report_file, [], group_name,
                                          custom_thresholds=latency_thresholds_to_check,
                                          start_time=global_start)

        if all_latencies_combined:
            report_file.write("\n======================================================\n")
            report_file.write("       所有客户端合并后的总体统计数据（对齐后0-3分钟）\n")
            report_file.write("======================================================\n")
            calculate_and_write_stats(report_file, all_latencies_combined,
                                      "所有文件合并 (Overall)",
                                      custom_thresholds=latency_thresholds_to_check,
                                      start_time=global_start)
        else:
            report_file.write("\n未能从任何文件中提取到符合对齐窗口的延迟数据。\n")

        report_file.write("\n--- 脚本执行完毕 ---\n")
        print(f"脚本执行完成。结果已保存到: {os.path.abspath(output_file_name)}")