





import pandas as pd
import io
import itertools
import re
from typing import List, Tuple, Dict

# 新增依赖
import numpy as np
from minimum import find_optimal_priorities, calculate_node_value, calculate_total_value  # 引入calculate_total_value

def get_symmetric_delay(df: pd.DataFrame, node1: str, node2: str) -> float:
    """获取两个节点之间的对称延迟（双向平均）"""
    return (df.loc[node1, node2] + df.loc[node2, node1]) / 2

def select_leader(df: pd.DataFrame, nodes: List[str]) -> Tuple[str, Dict]:
    """
    选择leader：到其他所有节点的延迟总和最小的节点
    返回: (leader_node, info_dict)
    """
    min_total = float('inf')
    leader = None
    delay_sums = {}
    
    for node in nodes:
        total_delay = 0
        for other_node in nodes:
            if node != other_node:
                total_delay += get_symmetric_delay(df, node, other_node)
        
        delay_sums[node] = total_delay
        if total_delay < min_total:
            min_total = total_delay
            leader = node
    
    return leader, delay_sums

def check_pentagon_property_with_leader(df: pd.DataFrame, nodes: List[str]) -> Tuple[bool, Dict]:
    """
    检查5个节点是否满足带leader的五边形性质：
    1. 先选出leader（到其他节点延迟总和最小的节点）
    2. 对于非leader节点：到最远节点的往返延迟 < 经过包括leader的任意其他两个节点的环路延迟
    3. 对于leader节点：到最远节点的往返延迟 < 经过任意其他两个节点的环路延迟
    """
    n = len(nodes)
    if n != 5:
        return False, {}
    
    # 选择leader
    leader, delay_sums = select_leader(df, nodes)
    
    # 创建延迟矩阵
    delay_matrix = {}
    for i in range(n):
        for j in range(n):
            if i != j:
                delay_matrix[(nodes[i], nodes[j])] = get_symmetric_delay(df, nodes[i], nodes[j])

    # ---- 新增：计算两种算法的平均延迟及比值 ----
    # 方法一：使用 minimum.py 中的最优优先级算法
    dist_matrix = np.zeros((n, n))
    for x in range(n):
        for y in range(n):
            if x != y:
                dist_matrix[x][y] = delay_matrix[(nodes[x], nodes[y])]
    # 重新计算：要求 leader 优先级固定为 0，其余节点穷举
    leader_idx = nodes.index(leader)
    other_indices = [i for i in range(n) if i != leader_idx]
    min_total_value = float('inf')
    best_priorities = None
    from itertools import permutations as _perms
    for perm in _perms(range(1, n), n-1):
        priorities = [0]*n  # 初始化
        for idx, node_idx in enumerate(other_indices):
            priorities[node_idx] = perm[idx]
        total_val = calculate_total_value(dist_matrix, priorities)
        if total_val < min_total_value:
            min_total_value = total_val
            best_priorities = priorities

    # 逐节点值
    method1_values = [calculate_node_value(dist_matrix, idx, best_priorities) * 2 for idx in range(n)]
    method1_avg = sum(method1_values) / n

    # 方法二：经过包含 leader 的三节点环路延迟的最小值
    method2_total = 0.0
    method2_values = []
    for idx, cur in enumerate(nodes):
        is_ld = (cur == leader)
        if is_ld:
            # Leader 的 latency = 与其他节点延迟的第二小值（倒数第二名，非最小）
            delays = [delay_matrix[(cur, nodes[j])] for j in range(n) if j != idx]
            delays_sorted = sorted(delays)  # 升序
            val = (delays_sorted[1] if len(delays_sorted) >= 2 else delays_sorted[0]) * 2  # leader latency 翻倍
        else:
            # 非 leader 的 latency = 包含 leader 的最小 3 节点环路
            min_loop = float('inf')
            for other_idx in range(n):
                if other_idx == idx or nodes[other_idx] == leader:
                    continue
                other = nodes[other_idx]
                loop = (delay_matrix[(cur, leader)] +
                        delay_matrix[(leader, other)] +
                        delay_matrix[(other, cur)])
                if loop < min_loop:
                    min_loop = loop
            val = min_loop
        method2_total += val
        method2_values.append(val)
    method2_avg = method2_total / n

    ratio_delay = method1_avg / method2_avg if method2_avg else float('inf')
    # ---- 新增逻辑结束 ----

    violations = []
    checks = []
    
    for i in range(n):
        current_node = nodes[i]
        is_leader = (current_node == leader)
        
        # 找到距离当前节点最远的节点
        max_delay = 0
        farthest_node = None
        farthest_idx = None
        
        for j in range(n):
            if i != j:
                delay = delay_matrix[(current_node, nodes[j])]
                if delay > max_delay:
                    max_delay = delay
                    farthest_node = nodes[j]
                    farthest_idx = j
        
        # 计算到最远节点的往返延迟
        round_trip_delay = 2 * max_delay
        
        # 获取除了当前节点和最远节点之外的其他节点
        other_indices = [idx for idx in range(n) if idx != i and idx != farthest_idx]
        
        if is_leader:
            # Leader: 检查经过任意两个其他节点的环路
            for node1_idx, node2_idx in itertools.combinations(other_indices, 2):
                node1, node2 = nodes[node1_idx], nodes[node2_idx]
                
                loop_delay = (delay_matrix[(current_node, node1)] + 
                             delay_matrix[(node1, node2)] + 
                             delay_matrix[(node2, current_node)])
                
                check_info = {
                    'from': current_node,
                    'is_leader': True,
                    'farthest': farthest_node,
                    'round_trip': round_trip_delay,
                    'loop_path': f"{current_node}->{node1}->{node2}->{current_node}",
                    'loop_delay': loop_delay,
                    'satisfied': round_trip_delay < loop_delay
                }
                checks.append(check_info)
                
                
        else:
            # 非leader: 检查所有包含leader的环路
            # 获取除了当前节点之外的其他节点（包括最远节点）
            other_nodes = [n for idx, n in enumerate(nodes) if idx != i]
            
            # 检查所有包含leader的两节点组合
            for node1, node2 in itertools.combinations(other_nodes, 2):
                # 确保环路包含leader
                if node1 != leader and node2 != leader:
                    continue
                
                loop_delay = (delay_matrix[(current_node, node1)] + 
                             delay_matrix[(node1, node2)] + 
                             delay_matrix[(node2, current_node)])
                
                check_info = {
                    'from': current_node,
                    'is_leader': False,
                    'farthest': farthest_node,
                    'round_trip': round_trip_delay,
                    'loop_path': f"{current_node}->{node1}->{node2}->{current_node}",
                    'loop_delay': loop_delay,
                    'satisfied': round_trip_delay < loop_delay
                }
                checks.append(check_info)
                
                if round_trip_delay + 10 >= loop_delay:
                    violations.append(check_info)
    
    return ratio_delay < 0.845, {
        'leader': leader,
        'delay_sums': delay_sums,
        'checks': checks,
        'violations': violations,
        'method1_avg': method1_avg,
        'method2_avg': method2_avg,
        'ratio': ratio_delay,
        'method1_values': method1_values,
        'method2_values': method2_values
    }

def find_pentagon_nodes_with_leader(data_string: str, sample_size: int = 25):
    """
    找到满足带leader的五边形性质的5个节点
    """
    print("--- 开始分析 ---")
    print("目标：找到5个节点，形成带leader的五边形结构")
    print("规则：")
    print("  1. Leader为到其他节点延迟总和最小的节点")
    print("  2. 非Leader节点：到最远节点的往返延迟 < 经过包括leader的环路延迟")
    print("  3. Leader节点：到最远节点的往返延迟 < 经过任意其他两个节点的环路延迟\n")
    
    print("步骤 1/3: 正在读取和清理数据...")
    
    # ---- 解析延迟表，兼容两种格式 ----
    def _parse_latency_table(s: str) -> pd.DataFrame:
        """解析延迟表：
        1. 直接尝试以任意空白分隔读取（适用于旧格式和大部分新格式）。
        2. 若首行包含 "To \\ From"，则先替换成单词 "From" 再读取。
        """

        # helper: use pandas with arbitrary whitespace delimiter (single-line rows)
        def _read_whitespace(txt: str) -> pd.DataFrame:
            return pd.read_csv(
                io.StringIO(txt),
                delim_whitespace=True,
                engine="python",
                index_col=0,
                on_bad_lines="skip",
            )

        def _is_valid(df: pd.DataFrame) -> bool:
            """简单校验：列数≥2 且索引不像数值 (即不含 ms)"""
            if df is None or df.empty or len(df.columns) < 2:
                return False
            sample_idx = list(df.index)[:10]
            return not any(isinstance(x, str) and x.endswith("ms") for x in sample_idx)

        # 专用解析器：首行包含 "From"，之后按 token 计数
        def _parse_multiline(txt: str) -> pd.DataFrame:
            txt = txt.strip()
            header_line, *rest = [ln.strip() for ln in txt.splitlines() if ln.strip()]
            header_line = re.sub(r"To\s*\\\s*From", "From", header_line)
            header_tokens = re.split(r"\s+", header_line)
            from_idx = header_tokens.index("From") if "From" in header_tokens else 0
            columns = header_tokens[from_idx + 1 :]
            num_cols = len(columns)
            tokens = re.findall(r"\S+", "\n".join(rest))
            data = {}
            i = 0
            while i < len(tokens):
                row_label = tokens[i]
                i += 1
                if i + num_cols > len(tokens):
                    break
                data[row_label] = tokens[i : i + num_cols]
                i += num_cols
            dfp = pd.DataFrame.from_dict(data, orient="index", columns=columns)
            dfp = dfp.apply(lambda c: pd.to_numeric(c.astype(str).str.replace('ms', ''), errors='coerce'))
            return dfp

        # ① 直接读
        try:
            df_try = _read_whitespace(s)
            if _is_valid(df_try):
                return df_try
        except Exception:
            pass

        # ② 若首行含 "To \ From"，替换后再读
        first_line_end = s.find("\n")
        if first_line_end != -1 and "From" in s[:first_line_end]:
            cleaned_first = re.sub(r"To\s*\\\s*From", "From", s[:first_line_end])
            s_fixed = cleaned_first + s[first_line_end:]
            try:
                df_try = _read_whitespace(s_fixed)
                if _is_valid(df_try):
                    return df_try
            except Exception:
                pass

        # ③ 使用逐 token 解析器
        try:
            df_try = _parse_multiline(s)
            if _is_valid(df_try):
                return df_try
        except Exception:
            pass

        # 若仍失败，抛出异常
        raise ValueError("无法解析数据表格式")

    try:
        df = _parse_latency_table(data_string)
    except Exception as e:
        print(f"错误：无法解析数据。详细信息: {e}")
        return

    # 清理/标准化数据
    df.columns = df.columns.str.strip()
    df.index = df.index.str.strip()

    for col in df.columns:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace('ms', '', regex=False), errors='coerce')
    
    nodes = df.index.tolist()
    print(f"共有 {len(nodes)} 个节点")
    
    # 计算每个节点的特征
    node_features = {}
    for node in nodes:
        delays = []
        for other in nodes:
            if node != other:
                delay = get_symmetric_delay(df, node, other)
                if pd.notna(delay):
                    delays.append(delay)
        
        if delays:
            node_features[node] = {
                'mean': sum(delays) / len(delays),
                'max': max(delays),
                'min': min(delays),
                'range': max(delays) - min(delays),
                'variance': pd.Series(delays).var()
            }
    
    # 选择具有大延迟范围的节点
    sorted_by_range = sorted(nodes, key=lambda x: node_features[x]['range'], reverse=True)
    candidate_nodes = sorted_by_range[:min(sample_size, len(nodes))]
    
    print(f"选择延迟范围最大的 {len(candidate_nodes)} 个节点作为候选")
    print(f"候选节点的延迟范围: {node_features[candidate_nodes[0]]['range']:.2f}ms - {node_features[candidate_nodes[-1]]['range']:.2f}ms")
    
    print("\n步骤 3/3: 搜索满足条件的5节点组合...")
    
    found_combinations = []
    total_combinations = len(list(itertools.combinations(candidate_nodes, 5)))
    checked = 0
    
    print(f"总共需要检查 {total_combinations} 个组合")
    
    # 尝试所有5节点组合
    for combo in itertools.combinations(candidate_nodes, 5):
        checked += 1
        
        if checked % 100 == 0:
            print(f"进度: {checked}/{total_combinations} ({checked/total_combinations*100:.1f}%)")
        
        is_valid, info = check_pentagon_property_with_leader(df, list(combo))
        
        if is_valid:
            found_combinations.append((combo, info))
            print(f"\n✓ 找到满足条件的组合 #{len(found_combinations)}: {list(combo)}")
            print(f"   Leader: {info['leader']}")
            print(f"   方法1平均延迟: {info['method1_avg']:.2f}ms")
            print(f"   方法2平均延迟: {info['method2_avg']:.2f}ms")
            print(f"   比值(Method1/Method2): {info['ratio']:.3f}")

            # 新增：输出每个节点两种算法的值对比
            print("   节点级别值(方法1 vs 方法2):")
            for node, v1, v2 in zip(combo, info['method1_values'], info['method2_values']):
                print(f"     {node}: {v1:.2f}ms  |  {v2:.2f}ms")
            
            # 打印组合的延迟矩阵
            print("\n延迟矩阵 (ms):")
            print(f"{'':15s}", end='')
            for node in combo:
                if node == info['leader']:
                    print(f"{node+'(L)':15s}", end='')
                else:
                    print(f"{node:15s}", end='')
            print()
            
            for i, n1 in enumerate(combo):
                if n1 == info['leader']:
                    print(f"{n1+'(L)':15s}", end='')
                else:
                    print(f"{n1:15s}", end='')
                for j, n2 in enumerate(combo):
                    if i == j:
                        print(f"{'---':>15s}", end='')
                    else:
                        delay = get_symmetric_delay(df, n1, n2)
                        print(f"{delay:15.2f}", end='')
                print()
            
            # 显示leader信息
            print(f"\nLeader选举结果:")
            sorted_nodes = sorted(info['delay_sums'].items(), key=lambda x: x[1])
            for node, total_delay in sorted_nodes[:3]:  # 显示前3个
                print(f"  {node}: 总延迟 = {total_delay:.2f}ms" + 
                      (" ← Leader" if node == info['leader'] else ""))
            
            # （已移除旧逐节点环路验证）
            print("\n组合整体验证：")
            if info['ratio'] < 0.845:
                print(f"  ✓ 方法1/方法2 = {info['ratio']:.3f} < 0.8，符合要求")
            else:
                print(f"  ✗ 方法1/方法2 = {info['ratio']:.3f} ≥ 0.8，不符合要求")
            
            if len(found_combinations) >= 30:
                print(f"\n已找到 {len(found_combinations)} 个满足条件的组合，停止搜索")
                break
    
    print("\n--- 分析完成 ---")
    
    if found_combinations:
        print(f"\n总共找到 {len(found_combinations)} 个满足条件的带leader五边形结构")
    else:
        print("\n未找到满足条件的带leader五边形结构。")
        print("\n分析一个样本组合看看问题在哪:")
        
        # 显示一个违反条件的例子
        sample_combo = list(itertools.combinations(candidate_nodes[:5], 5))[0]
        is_valid, info = check_pentagon_property_with_leader(df, list(sample_combo))
        
        print(f"\n组合: {list(sample_combo)}")
        print(f"Leader: {info['leader']}")
        
        if info['violations']:
            print(f"\n违反条件的情况（显示前5个）:")
            for v in info['violations'][:5]:
                print(f"  ✗ {v['from']}{' (Leader)' if v['is_leader'] else ''}:")
                print(f"     到最远节点 {v['farthest']} 往返: {v['round_trip']:.2f}ms")
                print(f"     环路 {v['loop_path']}: {v['loop_delay']:.2f}ms")
                print(f"     违反: {v['round_trip']:.2f} >= {v['loop_delay']:.2f}")

# 您提供的延迟数据
latency_data = '''
To \ From	af-south-1	ap-east-1	ap-east-2	ap-northeast-1	ap-northeast-2	ap-northeast-3	ap-south-1	ap-south-2	ap-southeast-1	ap-southeast-2	ap-southeast-3	ap-southeast-4	ap-southeast-5	ap-southeast-7	ca-central-1	ca-west-1	eu-central-1	eu-central-2	eu-north-1	eu-south-1	eu-south-2	eu-west-1	eu-west-2	eu-west-3	il-central-1	me-central-1	me-south-1	mx-central-1	sa-east-1	us-east-1	us-east-2	us-west-1	us-west-2
af-south-1	
4.41ms
346.41ms
377.35ms
383.82ms
383.26ms
386.52ms
302.55ms
331.09ms
315.00ms
411.40ms
330.25ms
419.95ms
321.23ms
340.42ms
223.66ms
290.06ms
157.55ms
153.44ms
177.67ms
189.48ms
136.80ms
159.34ms
151.31ms
147.56ms
221.03ms
279.95ms
289.81ms
289.16ms
340.27ms
230.27ms
239.70ms
290.71ms
275.42ms
ap-east-1	
347.09ms
3.18ms
30.82ms
51.57ms
39.85ms
50.00ms
97.17ms
81.47ms
38.48ms
128.40ms
53.04ms
140.55ms
43.74ms
65.15ms
204.64ms
161.57ms
193.54ms
188.40ms
212.81ms
184.24ms
186.56ms
261.86ms
200.02ms
197.74ms
235.51ms
122.49ms
128.63ms
216.74ms
318.61ms
209.17ms
195.04ms
154.99ms
145.05ms
ap-east-2	
378.43ms
35.30ms
4.85ms
39.73ms
66.90ms
37.11ms
130.47ms
118.26ms
70.66ms
154.54ms
84.97ms
165.64ms
76.00ms
96.86ms
181.39ms
143.66ms
228.43ms
221.44ms
246.57ms
221.18ms
219.26ms
235.93ms
247.95ms
255.54ms
272.49ms
158.37ms
166.26ms
198.60ms
293.40ms
181.48ms
165.94ms
143.95ms
133.86ms
ap-northeast-1	
383.51ms
53.65ms
38.04ms
6.64ms
39.11ms
12.87ms
134.17ms
120.06ms
73.69ms
106.58ms
87.34ms
137.75ms
77.94ms
100.30ms
159.29ms
112.02ms
229.88ms
225.59ms
249.87ms
221.58ms
221.42ms
205.69ms
217.44ms
223.99ms
273.13ms
172.65ms
177.95ms
169.33ms
264.89ms
153.30ms
140.38ms
111.47ms
101.73ms
ap-northeast-2	
383.48ms
41.29ms
65.12ms
38.34ms
5.64ms
25.30ms
136.71ms
120.28ms
82.10ms
141.06ms
93.82ms
152.13ms
87.49ms
104.87ms
178.51ms
140.02ms
251.95ms
245.77ms
270.23ms
242.08ms
224.64ms
236.96ms
247.54ms
254.17ms
293.00ms
158.57ms
166.01ms
193.70ms
293.18ms
181.96ms
168.60ms
132.57ms
123.75ms
ap-northeast-3	
388.39ms
51.00ms
33.79ms
13.31ms
26.75ms
5.26ms
140.56ms
125.95ms
80.30ms
121.79ms
93.00ms
133.27ms
84.18ms
103.95ms
159.30ms
118.63ms
236.97ms
229.97ms
256.24ms
227.28ms
228.63ms
217.00ms
227.15ms
233.88ms
278.83ms
167.92ms
173.63ms
173.20ms
271.87ms
161.51ms
148.67ms
112.84ms
102.59ms
ap-south-1	
300.99ms
97.57ms
126.78ms
134.12ms
134.92ms
140.52ms
4.65ms
29.18ms
65.28ms
156.05ms
80.17ms
167.01ms
70.16ms
91.58ms
196.23ms
239.38ms
130.93ms
133.66ms
148.39ms
127.27ms
150.92ms
136.98ms
121.86ms
114.39ms
165.53ms
32.37ms
38.62ms
250.72ms
305.04ms
207.71ms
212.54ms
234.43ms
223.80ms
ap-south-2	
327.93ms
81.71ms
110.84ms
117.03ms
117.39ms
122.27ms
24.23ms
8.39ms
49.10ms
139.58ms
63.42ms
149.68ms
53.98ms
74.39ms
209.61ms
223.09ms
147.18ms
150.26ms
166.29ms
143.62ms
167.13ms
144.92ms
135.33ms
128.15ms
182.56ms
47.26ms
55.81ms
281.40ms
319.60ms
208.61ms
219.18ms
218.43ms
207.59ms
ap-southeast-1	
313.19ms
39.03ms
68.85ms
73.81ms
81.10ms
79.78ms
66.03ms
52.03ms
5.13ms
95.81ms
20.13ms
106.84ms
10.03ms
30.14ms
217.20ms
180.31ms
162.86ms
156.92ms
182.41ms
153.57ms
154.34ms
177.23ms
168.90ms
166.66ms
205.60ms
92.88ms
100.34ms
242.30ms
330.00ms
219.55ms
204.07ms
176.90ms
165.64ms
ap-southeast-2	
411.57ms
129.64ms
151.27ms
113.24ms
142.25ms
122.30ms
156.11ms
144.35ms
96.83ms
5.17ms
110.26ms
16.04ms
100.90ms
123.42ms
200.18ms
159.79ms
255.34ms
247.20ms
273.81ms
244.17ms
245.44ms
257.73ms
268.85ms
283.30ms
297.47ms
184.93ms
190.69ms
215.14ms
314.75ms
202.09ms
189.07ms
141.85ms
143.43ms
ap-southeast-3	
333.75ms
55.47ms
84.00ms
89.05ms
94.12ms
94.73ms
84.57ms
68.29ms
20.31ms
113.81ms
3.82ms
125.73ms
26.90ms
47.47ms
232.68ms
196.00ms
178.54ms
173.23ms
200.73ms
171.25ms
171.52ms
190.18ms
182.66ms
183.28ms
221.23ms
109.44ms
116.44ms
258.67ms
347.99ms
236.09ms
220.76ms
192.78ms
178.24ms
ap-southeast-4	
420.57ms
140.78ms
163.05ms
138.58ms
154.12ms
133.71ms
167.37ms
153.41ms
107.64ms
16.79ms
122.36ms
4.62ms
113.10ms
132.44ms
211.35ms
169.94ms
263.63ms
258.43ms
283.64ms
255.52ms
257.65ms
268.71ms
278.59ms
293.60ms
307.46ms
194.67ms
203.11ms
225.71ms
325.29ms
213.21ms
201.09ms
152.74ms
154.02ms
ap-southeast-5	
319.14ms
44.47ms
73.91ms
78.90ms
85.55ms
85.54ms
72.10ms
59.37ms
12.05ms
102.53ms
26.45ms
112.81ms
2.41ms
25.93ms
222.97ms
186.08ms
168.93ms
162.66ms
187.20ms
159.57ms
160.08ms
180.51ms
172.10ms
171.70ms
211.28ms
99.08ms
107.23ms
249.57ms
335.55ms
224.99ms
210.21ms
181.56ms
170.23ms
ap-southeast-7	
342.20ms
65.15ms
94.01ms
100.38ms
104.83ms
104.46ms
92.87ms
78.26ms
31.36ms
122.36ms
46.91ms
134.65ms
27.34ms
4.92ms
243.99ms
206.53ms
190.69ms
183.51ms
209.03ms
181.17ms
181.00ms
199.56ms
190.62ms
194.03ms
233.67ms
118.71ms
125.81ms
269.09ms
356.79ms
245.60ms
231.76ms
203.12ms
191.33ms
ca-central-1	
223.62ms
205.75ms
179.98ms
159.31ms
178.98ms
158.64ms
195.35ms
214.72ms
216.73ms
200.58ms
231.61ms
211.06ms
222.62ms
242.90ms
5.89ms
50.52ms
95.85ms
96.46ms
107.04ms
104.08ms
112.67ms
71.62ms
82.11ms
88.63ms
138.45ms
199.63ms
178.71ms
77.45ms
127.79ms
20.60ms
31.25ms
82.74ms
63.34ms
ca-west-1	
295.53ms
166.20ms
142.23ms
114.26ms
139.98ms
120.06ms
249.26ms
231.13ms
183.53ms
162.07ms
199.35ms
174.56ms
187.35ms
207.53ms
52.70ms
3.70ms
164.37ms
163.89ms
175.86ms
172.08ms
175.52ms
140.45ms
151.13ms
156.66ms
207.94ms
274.71ms
268.35ms
79.12ms
196.51ms
60.48ms
43.86ms
43.06ms
22.83ms
eu-central-1	
156.85ms
194.78ms
224.84ms
229.74ms
248.81ms
237.12ms
129.88ms
152.98ms
162.58ms
253.19ms
177.36ms
262.96ms
167.61ms
188.04ms
95.99ms
160.01ms
5.33ms
12.01ms
25.56ms
16.07ms
38.43ms
29.63ms
18.33ms
13.10ms
65.74ms
116.59ms
88.03ms
152.93ms
205.50ms
96.08ms
105.71ms
156.81ms
144.04ms
eu-central-2	
154.22ms
189.15ms
219.75ms
224.30ms
245.09ms
231.24ms
134.19ms
155.56ms
157.48ms
248.07ms
171.79ms
257.50ms
161.81ms
183.71ms
96.11ms
162.84ms
12.34ms
4.87ms
30.37ms
10.65ms
33.55ms
30.16ms
21.59ms
14.69ms
46.00ms
111.53ms
93.58ms
152.25ms
205.41ms
94.88ms
105.15ms
155.29ms
146.34ms
eu-north-1	
174.55ms
213.00ms
242.24ms
247.25ms
267.21ms
253.98ms
148.05ms
166.78ms
182.69ms
272.33ms
196.24ms
283.10ms
187.37ms
207.95ms
105.12ms
171.56ms
23.91ms
27.77ms
4.26ms
31.37ms
53.50ms
40.05ms
30.87ms
32.17ms
83.92ms
133.96ms
106.33ms
170.69ms
222.68ms
114.88ms
124.28ms
173.39ms
154.47ms
eu-south-1	
189.97ms
185.38ms
216.19ms
220.28ms
240.88ms
226.14ms
126.96ms
145.61ms
153.74ms
243.81ms
168.23ms
254.44ms
158.97ms
178.76ms
104.96ms
171.29ms
15.95ms
9.18ms
34.20ms
5.40ms
28.35ms
38.96ms
29.87ms
23.26ms
50.18ms
107.77ms
113.37ms
162.96ms
214.11ms
102.76ms
113.86ms
164.82ms
153.96ms
eu-south-2	
139.97ms
191.11ms
219.65ms
221.76ms
225.78ms
230.13ms
152.37ms
172.24ms
156.88ms
248.15ms
171.58ms
259.83ms
163.07ms
181.78ms
115.48ms
174.40ms
39.28ms
35.27ms
57.89ms
31.88ms
5.84ms
35.94ms
39.91ms
20.05ms
64.14ms
126.43ms
134.19ms
169.51ms
222.76ms
104.94ms
115.61ms
173.38ms
155.93ms
eu-west-1	
160.23ms
264.33ms
233.47ms
206.29ms
237.34ms
216.51ms
137.02ms
147.84ms
176.92ms
257.40ms
190.03ms
267.78ms
179.17ms
198.56ms
72.02ms
136.71ms
29.95ms
31.34ms
43.62ms
38.67ms
35.16ms
4.73ms
15.63ms
22.29ms
73.33ms
133.92ms
113.22ms
130.69ms
181.40ms
72.16ms
83.06ms
133.01ms
121.98ms
eu-west-2	
150.19ms
202.39ms
243.58ms
218.30ms
245.84ms
227.71ms
120.30ms
140.71ms
166.70ms
267.38ms
182.59ms
278.34ms
170.10ms
189.01ms
82.02ms
147.83ms
19.63ms
22.48ms
33.93ms
30.40ms
37.01ms
14.92ms
5.41ms
12.81ms
62.88ms
124.62ms
103.36ms
136.87ms
189.32ms
79.00ms
90.37ms
149.77ms
130.23ms
eu-west-3	
147.86ms
198.37ms
250.74ms
223.79ms
254.33ms
233.98ms
114.75ms
131.98ms
167.25ms
283.21ms
180.23ms
293.79ms
170.83ms
191.09ms
88.54ms
154.52ms
13.29ms
14.06ms
33.67ms
23.50ms
18.60ms
21.37ms
12.03ms
6.18ms
56.11ms
116.47ms
97.42ms
144.17ms
197.27ms
86.75ms
97.67ms
148.02ms
137.72ms
il-central-1	
229.18ms
244.27ms
275.40ms
280.22ms
297.33ms
286.08ms
173.80ms
190.15ms
211.50ms
304.00ms
227.91ms
313.83ms
218.73ms
236.88ms
145.58ms
212.04ms
69.65ms
49.95ms
91.55ms
54.41ms
64.40ms
77.96ms
71.07ms
55.92ms
4.84ms
150.05ms
160.69ms
203.73ms
255.19ms
137.82ms
147.02ms
206.16ms
188.63ms
me-central-1	
280.11ms
123.62ms
154.16ms
171.21ms
161.62ms
166.75ms
32.63ms
52.86ms
93.00ms
184.29ms
108.18ms
195.86ms
98.83ms
118.15ms
200.28ms
266.22ms
116.33ms
111.19ms
137.13ms
107.24ms
121.95ms
132.65ms
123.98ms
116.95ms
147.67ms
4.51ms
16.21ms
255.81ms
307.81ms
196.72ms
207.60ms
258.75ms
257.53ms
me-south-1	
288.38ms
129.19ms
161.49ms
175.41ms
163.31ms
171.82ms
37.70ms
55.30ms
99.30ms
189.83ms
113.70ms
200.92ms
104.20ms
125.48ms
176.25ms
260.52ms
87.77ms
92.26ms
106.10ms
111.51ms
130.03ms
110.74ms
102.44ms
96.04ms
152.67ms
14.37ms
2.75ms
235.04ms
286.64ms
177.70ms
187.50ms
236.98ms
264.72ms
mx-central-1	
290.79ms
219.43ms
199.46ms
170.97ms
193.26ms
172.96ms
256.20ms
288.11ms
241.80ms
214.80ms
259.29ms
223.86ms
248.91ms
270.83ms
76.89ms
77.09ms
154.49ms
153.61ms
173.65ms
165.17ms
169.80ms
130.16ms
139.31ms
146.71ms
198.12ms
256.75ms
237.90ms
4.90ms
176.53ms
60.36ms
53.70ms
76.45ms
92.47ms
sa-east-1	
339.85ms
321.47ms
290.82ms
263.80ms
293.80ms
272.50ms
304.83ms
322.00ms
329.26ms
313.55ms
344.05ms
323.89ms
334.02ms
355.27ms
129.26ms
193.78ms
205.45ms
203.82ms
225.44ms
214.30ms
220.12ms
181.08ms
190.11ms
196.93ms
248.50ms
308.24ms
287.39ms
173.51ms
4.67ms
115.78ms
125.51ms
176.68ms
177.66ms
us-east-1	
230.88ms
210.14ms
180.34ms
153.92ms
182.51ms
162.62ms
210.84ms
213.27ms
220.38ms
206.62ms
237.00ms
214.96ms
227.98ms
249.07ms
19.58ms
57.05ms
98.05ms
96.76ms
116.48ms
105.07ms
105.90ms
73.99ms
80.93ms
88.01ms
138.44ms
198.66ms
179.53ms
63.33ms
117.09ms
9.20ms
20.19ms
72.53ms
68.43ms
us-east-2	
244.15ms
200.61ms
170.17ms
142.95ms
174.00ms
153.60ms
216.42ms
224.37ms
210.30ms
195.15ms
224.75ms
205.44ms
214.81ms
235.62ms
34.90ms
46.79ms
111.41ms
109.26ms
129.83ms
118.76ms
119.09ms
87.16ms
95.28ms
102.68ms
152.44ms
212.32ms
192.46ms
59.05ms
130.74ms
20.00ms
8.43ms
57.39ms
54.81ms
us-west-1	
289.86ms
156.18ms
141.40ms
111.31ms
133.68ms
112.17ms
234.39ms
222.32ms
176.55ms
142.01ms
190.41ms
150.90ms
180.42ms
201.64ms
82.39ms
42.06ms
155.25ms
155.23ms
173.87ms
164.49ms
170.45ms
131.96ms
150.07ms
148.01ms
196.64ms
257.01ms
237.77ms
75.45ms
176.65ms
70.63ms
51.50ms
4.99ms
21.51ms
us-west-2	
273.81ms
146.87ms
131.89ms
101.54ms
123.97ms
102.43ms
224.60ms
213.57ms
167.61ms
142.84ms
180.03ms
152.64ms
171.17ms
191.64ms
63.66ms
25.16ms
145.71ms
146.51ms
157.05ms
154.76ms
153.22ms
121.22ms
132.20ms
136.17ms
187.58ms
259.57ms
267.51ms
92.43ms
177.97ms
65.93ms
49.05ms
21.89ms
6.01ms

'''
# --- 运行脚本 ---
# 将上面的字符串数据传入函数并执行
find_pentagon_nodes_with_leader(latency_data.strip(), sample_size=30)

# ---------------- Debug helper: check missing single-direction entries -----------------
if __name__ == "__main__":
    import itertools, io, re, textwrap

    def _parse_multiline(txt: str) -> pd.DataFrame:
        txt = txt.strip()
        header_line, *rest = [ln.strip() for ln in txt.splitlines() if ln.strip()]
        header_line = re.sub(r"To\s*\\\s*From", "From", header_line)
        header_tokens = re.split(r"\s+", header_line)
        from_idx = header_tokens.index("From") if "From" in header_tokens else 0
        columns = header_tokens[from_idx + 1 :]
        num_cols = len(columns)
        tokens = re.findall(r"\S+", "\n".join(rest))
        data = {}
        i = 0
        while i < len(tokens):
            row_label = tokens[i]
            i += 1
            if i + num_cols > len(tokens):
                break
            data[row_label] = tokens[i : i + num_cols]
            i += num_cols
        dfp = pd.DataFrame.from_dict(data, orient="index", columns=columns)
        dfp = dfp.apply(lambda c: pd.to_numeric(c.astype(str).str.replace('ms', ''), errors='coerce'))
        return dfp

    df_dbg = _parse_multiline(latency_data)

    # ---- Custom check for the specified 5-node combination ----
    specific_nodes = ['ap-southeast-2', 'me-south-1', 'ap-northeast-2', 'eu-south-1', 'mx-central-1']
    if all(node in df_dbg.index for node in specific_nodes):
        valid, info = check_pentagon_property_with_leader(df_dbg, specific_nodes)
        print("\n=== Specific 5-node combination ===")
        print(f"Nodes: {specific_nodes}")
        print(f"Leader: {info['leader']}")
        print(f"Method1 avg latency: {info['method1_avg']:.2f}ms")
        print(f"Method2 avg latency: {info['method2_avg']:.2f}ms")
        print(f"Ratio (Method1/Method2): {info['ratio']:.3f}")

        # 输出延迟矩阵
        print("\nLatency matrix (ms):")
        print(f"{'':15s}", end='')
        for node in specific_nodes:
            label = node + ('(L)' if node == info['leader'] else '')
            print(f"{label:15s}", end='')
        print()

        for i, n1 in enumerate(specific_nodes):
            label = n1 + ('(L)' if n1 == info['leader'] else '')
            print(f"{label:15s}", end='')
            for j, n2 in enumerate(specific_nodes):
                if i == j:
                    print(f"{'---':>15s}", end='')
                else:
                    delay = get_symmetric_delay(df_dbg, n1, n2)
                    print(f"{delay:15.2f}", end='')
            print()
    else:
        print("\n[Warning] Some of the specified nodes are missing from the latency dataset.")
    # ---- End custom check ----

    missing = []
    for a, b in itertools.permutations(df_dbg.index, 2):
        try:
            if pd.isna(df_dbg.at[a, b]):
                missing.append(f"{a} → {b}")
        except KeyError:
            missing.append(f"{a} → {b}")

    print(f"\n[Debug] 缺失单向条目共 {len(missing)} 条")
    for item in missing[:20]:
        print("  ", item)