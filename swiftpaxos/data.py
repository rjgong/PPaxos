import re
import numpy as np
import os
from collections import defaultdict
from datetime import datetime, timedelta

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
        file_handle.write(f"统计时间范围: 开始后1-3分钟\n")
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
output_file_name = "latency_analysis_report_1to3min.txt"
latency_thresholds_to_check = [60, 100, 200, 500] 

all_latencies_combined = []
log_groups = defaultdict(list)

# 扫描目录并对文件进行分组
print(f"正在扫描日志目录: '{log_directory}'...")
print("只统计开始后1-3分钟内的数据...")
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

# 主处理流程
with open(output_file_name, 'w', encoding='utf-8') as report_file:
    report_file.write("Python 延迟分析报告 (按客户端分组，仅统计开始后1-3分钟数据)\n")
    report_file.write("=========================================\n")
    report_file.write(f"报告生成于: {os.path.abspath(output_file_name)}\n")
    report_file.write(f"分析目录: '{log_directory}'\n")
    report_file.write(f"统计时间范围: 开始后1-3分钟\n")
    report_file.write(f"自定义延迟阈值检查: {latency_thresholds_to_check}\n")

    # 按分组名称排序
    for group_name, file_list in sorted(log_groups.items()):
        report_file.write(f"\n######################################################\n")
        report_file.write(f"### 开始处理分组: {group_name}\n")
        report_file.write(f"### 包含文件: {', '.join([os.path.basename(f) for f in sorted(file_list)])}\n")
        report_file.write(f"######################################################\n")
        
        group_log_content = ""
        # 读取组内所有文件的内容并合并
        for file_path in sorted(file_list):
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    group_log_content += f.read() + "\n"
            except Exception as e:
                error_msg = f"错误: 读取文件 {file_path} 时失败: {e}\n"
                report_file.write(error_msg)
                print(error_msg)
        
        # 提取开始时间和符合时间范围的延迟数据
        start_time, group_latencies = extract_start_time_and_latencies(group_log_content)
        
        if start_time is None:
            report_file.write("警告: 未找到开始标记 'dialing master...'，跳过此组。\n")
            continue
        
        if group_latencies:
            report_file.write(f"成功从 \"{group_name}\" 组提取了 {len(group_latencies)} 个数据点（1-3分钟内）。\n")
            all_latencies_combined.extend(group_latencies)
            calculate_and_write_stats(report_file, group_latencies, group_name, 
                                    custom_thresholds=latency_thresholds_to_check, 
                                    start_time=start_time)
        else:
            calculate_and_write_stats(report_file, [], group_name, 
                                    custom_thresholds=latency_thresholds_to_check,
                                    start_time=start_time)

    # 计算并写入所有文件合并后的总体统计数据
    if all_latencies_combined:
        report_file.write("\n======================================================\n")
        report_file.write("       所有客户端合并后的总体统计数据（1-3分钟内）\n")
        report_file.write("======================================================\n")
        calculate_and_write_stats(report_file, all_latencies_combined, 
                                "所有文件合并 (Overall)", 
                                custom_thresholds=latency_thresholds_to_check)
    else:
        report_file.write("\n未能从任何文件中提取到符合时间范围的延迟数据。\n")

    report_file.write("\n--- 脚本执行完毕 ---\n")

print(f"脚本执行完成。结果已保存到: {os.path.abspath(output_file_name)}")