





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
    # 不再固定 leader 的优先级：遍历所有优先级排列，选择最优解
    optimal_arrangements, min_total_value = find_optimal_priorities(dist_matrix)
    best_priorities = optimal_arrangements[0]

    # 逐节点值
    method1_values = [calculate_node_value(dist_matrix, idx, best_priorities) * 2 for idx in range(n)]
    method1_avg = sum(method1_values) / n

    # 方法二使用的方法一优先级最高的节点作为 leader
    m2_leader_idx = min(range(n), key=lambda i: best_priorities[i])
    leader_m2 = nodes[m2_leader_idx]

    # 方法二：经过包含 leader 的三节点环路延迟的最小值
    method2_total = 0.0
    method2_values = []
    for idx, cur in enumerate(nodes):
        is_ld = (cur == leader_m2)
        if is_ld:
            # Leader 的 latency = 与其他节点延迟的第二小值（倒数第二名，非最小）
            delays = [delay_matrix[(cur, nodes[j])] for j in range(n) if j != idx]
            delays_sorted = sorted(delays)  # 升序
            val = (delays_sorted[1] if len(delays_sorted) >= 2 else delays_sorted[0]) * 2  # leader latency 翻倍
        else:
            # 非 leader 的 latency = 包含 leader 的最小 3 节点环路
            min_loop = float('inf')
            for other_idx in range(n):
                if other_idx == idx or nodes[other_idx] == leader_m2:
                    continue
                other = nodes[other_idx]
                loop = (delay_matrix[(cur, leader_m2)] +
                        delay_matrix[(leader_m2, other)] +
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
    
    priority_order = [nodes[idx] for idx in sorted(range(n), key=lambda i: best_priorities[i])]

    return ratio_delay < 0.83, {
        'leader': leader,
        'delay_sums': delay_sums,
        'checks': checks,
        'violations': violations,
        'method1_avg': method1_avg,
        'method2_avg': method2_avg,
        'ratio': ratio_delay,
        'method1_values': method1_values,
        'method2_values': method2_values,
        'priorities': best_priorities,
        'priority_order': priority_order
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
            print(f"   优先级顺序(高->低): {info['priority_order']}")
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
            if info['ratio'] < 0.83:
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

To \ From	af-south-1	ap-east-1	ap-east-2	ap-northeast-1	ap-northeast-2	ap-northeast-3	ap-south-1	ap-south-2	ap-southeast-1	ap-southeast-2	ap-southeast-3	ap-southeast-4	ap-southeast-5	ap-southeast-6	ap-southeast-7	ca-central-1	ca-west-1	eu-central-1	eu-central-2	eu-north-1	eu-south-1	eu-south-2	eu-west-1	eu-west-2	eu-west-3	il-central-1	me-central-1	me-south-1	mx-central-1	sa-east-1	us-east-1	us-east-2	us-west-1	us-west-2
af-south-1	
4.63ms
309.88ms
335.73ms
346.75ms
365.04ms
357.52ms
214.67ms
225.60ms
273.44ms
384.64ms
291.11ms
398.93ms
281.93ms
412.76ms
300.68ms
225.14ms
292.43ms
155.59ms
152.25ms
174.66ms
198.77ms
136.00ms
160.58ms
151.00ms
145.09ms
231.49ms
148.36ms
152.61ms
283.39ms
336.55ms
226.68ms
237.26ms
286.87ms
275.71ms
ap-east-1	
307.94ms
2.90ms
31.98ms
51.30ms
39.09ms
50.38ms
97.91ms
83.26ms
40.86ms
129.49ms
54.97ms
141.80ms
44.74ms
159.90ms
66.74ms
204.69ms
162.11ms
193.93ms
187.80ms
213.38ms
184.34ms
213.04ms
261.78ms
200.95ms
197.55ms
219.25ms
126.63ms
130.50ms
203.28ms
318.58ms
208.47ms
196.32ms
156.49ms
145.99ms
ap-east-2	
337.16ms
33.93ms
5.14ms
37.70ms
66.56ms
35.12ms
130.87ms
115.33ms
70.29ms
153.45ms
83.76ms
165.66ms
75.44ms
177.98ms
96.81ms
186.09ms
142.55ms
226.25ms
221.42ms
245.05ms
218.86ms
219.36ms
235.70ms
243.73ms
249.76ms
254.01ms
155.32ms
162.09ms
192.45ms
291.67ms
178.87ms
165.47ms
143.82ms
131.89ms
ap-northeast-1	
347.88ms
54.35ms
37.46ms
6.44ms
37.70ms
13.16ms
132.67ms
119.88ms
74.25ms
113.38ms
86.20ms
138.57ms
79.19ms
146.49ms
100.26ms
159.60ms
111.78ms
232.36ms
224.11ms
249.54ms
221.70ms
248.04ms
203.49ms
214.20ms
221.03ms
255.91ms
162.43ms
163.81ms
160.07ms
261.43ms
149.82ms
136.01ms
111.79ms
102.40ms
ap-northeast-2	
363.30ms
41.01ms
64.55ms
38.12ms
5.40ms
27.84ms
133.92ms
121.06ms
75.23ms
146.22ms
89.00ms
155.90ms
79.88ms
166.98ms
100.53ms
179.95ms
139.37ms
232.19ms
225.47ms
251.91ms
222.54ms
225.02ms
238.75ms
248.18ms
255.74ms
258.02ms
162.85ms
165.69ms
189.29ms
293.63ms
184.52ms
169.46ms
132.67ms
123.47ms
ap-northeast-3	
357.10ms
51.01ms
35.32ms
12.65ms
27.71ms
5.42ms
136.92ms
125.45ms
79.62ms
123.75ms
92.33ms
134.47ms
84.28ms
147.24ms
103.91ms
159.29ms
118.69ms
234.86ms
228.81ms
254.46ms
227.07ms
226.97ms
215.88ms
227.44ms
234.36ms
259.94ms
165.69ms
170.81ms
165.58ms
273.65ms
162.80ms
148.52ms
111.86ms
102.06ms
ap-south-1	
206.42ms
99.15ms
127.71ms
133.14ms
132.38ms
138.13ms
5.17ms
26.33ms
65.48ms
156.01ms
79.78ms
166.80ms
71.02ms
186.20ms
91.74ms
192.42ms
238.22ms
125.27ms
119.14ms
143.85ms
114.73ms
140.93ms
126.39ms
116.90ms
108.59ms
140.05ms
31.83ms
37.85ms
248.97ms
299.49ms
190.46ms
200.79ms
232.29ms
222.32ms
ap-south-2	
215.93ms
81.96ms
111.32ms
115.16ms
118.59ms
121.80ms
21.94ms
8.34ms
49.28ms
140.25ms
62.84ms
150.34ms
53.42ms
171.48ms
74.06ms
206.20ms
221.25ms
138.65ms
131.35ms
160.06ms
128.32ms
127.84ms
139.23ms
130.94ms
122.93ms
151.88ms
46.06ms
52.69ms
259.89ms
313.08ms
204.03ms
212.75ms
214.79ms
205.15ms
ap-southeast-1	
277.04ms
40.04ms
69.40ms
73.10ms
75.83ms
79.60ms
64.52ms
52.59ms
5.08ms
96.91ms
20.14ms
107.05ms
12.09ms
124.27ms
30.15ms
220.97ms
176.18ms
163.38ms
157.38ms
182.59ms
154.87ms
180.14ms
175.44ms
167.00ms
164.00ms
188.68ms
93.65ms
96.86ms
223.83ms
327.39ms
219.66ms
202.08ms
173.75ms
164.59ms
ap-southeast-2	
386.81ms
129.49ms
153.08ms
109.27ms
148.07ms
123.74ms
156.36ms
141.43ms
96.91ms
4.32ms
111.18ms
15.88ms
100.61ms
34.14ms
122.82ms
200.92ms
159.90ms
253.91ms
245.99ms
273.01ms
243.78ms
270.22ms
258.07ms
266.83ms
281.23ms
277.83ms
184.22ms
186.65ms
206.03ms
314.15ms
201.26ms
189.27ms
142.24ms
143.29ms
ap-southeast-3	
301.40ms
55.39ms
83.69ms
88.07ms
90.09ms
94.63ms
83.22ms
67.40ms
21.08ms
111.93ms
4.25ms
120.49ms
26.58ms
138.83ms
46.33ms
238.10ms
191.11ms
180.41ms
171.78ms
200.37ms
169.92ms
169.78ms
194.80ms
184.13ms
180.16ms
203.94ms
110.49ms
111.26ms
240.75ms
341.22ms
233.60ms
216.31ms
188.60ms
178.41ms
ap-southeast-4	
397.45ms
143.05ms
164.73ms
140.00ms
155.87ms
133.75ms
167.84ms
154.13ms
107.15ms
16.76ms
121.90ms
4.09ms
112.64ms
43.51ms
133.13ms
211.40ms
170.21ms
264.24ms
257.99ms
281.64ms
254.95ms
281.65ms
268.77ms
279.46ms
292.90ms
289.32ms
194.94ms
197.29ms
215.66ms
324.00ms
213.80ms
201.00ms
153.19ms
154.71ms
ap-southeast-5	
283.39ms
46.92ms
74.11ms
78.15ms
80.39ms
84.66ms
70.46ms
58.88ms
12.02ms
101.57ms
25.48ms
113.02ms
4.93ms
129.10ms
25.07ms
228.11ms
182.04ms
169.42ms
162.15ms
187.72ms
161.20ms
160.18ms
179.47ms
173.01ms
172.95ms
194.42ms
100.53ms
102.69ms
230.22ms
332.99ms
225.40ms
209.60ms
180.22ms
169.72ms
ap-southeast-6	
417.91ms
160.98ms
178.93ms
152.53ms
166.95ms
147.93ms
188.10ms
180.07ms
128.19ms
34.84ms
140.18ms
45.73ms
129.51ms
2.93ms
150.34ms
236.31ms
195.51ms
287.66ms
282.63ms
309.62ms
280.52ms
302.26ms
291.80ms
302.37ms
309.99ms
314.47ms
212.91ms
215.71ms
241.35ms
349.90ms
230.78ms
217.05ms
167.69ms
177.71ms
ap-southeast-7	
300.16ms
67.29ms
96.75ms
101.94ms
102.18ms
105.87ms
93.40ms
80.57ms
32.14ms
123.44ms
46.18ms
134.45ms
25.84ms
152.49ms
3.80ms
250.50ms
204.37ms
193.49ms
185.03ms
209.64ms
183.01ms
182.12ms
201.89ms
194.25ms
193.60ms
215.71ms
122.15ms
123.75ms
251.86ms
355.06ms
245.90ms
229.99ms
202.74ms
191.13ms
ca-central-1	
225.21ms
206.27ms
186.50ms
158.82ms
180.09ms
159.36ms
191.22ms
209.36ms
222.60ms
200.70ms
236.89ms
211.48ms
227.66ms
235.56ms
251.31ms
5.12ms
50.42ms
95.92ms
96.97ms
108.41ms
105.74ms
110.11ms
71.85ms
81.51ms
88.93ms
139.87ms
198.38ms
166.98ms
76.27ms
128.00ms
19.72ms
29.83ms
81.23ms
63.57ms
ca-west-1	
292.63ms
164.49ms
141.85ms
112.23ms
140.09ms
118.26ms
241.52ms
226.63ms
177.32ms
160.75ms
192.34ms
170.86ms
183.54ms
195.28ms
205.48ms
51.51ms
4.02ms
164.49ms
163.05ms
176.64ms
172.81ms
171.31ms
137.71ms
148.61ms
155.27ms
205.94ms
265.40ms
241.39ms
78.16ms
194.47ms
58.57ms
43.86ms
42.59ms
22.61ms
eu-central-1	
155.12ms
194.74ms
224.95ms
230.65ms
232.47ms
235.68ms
124.31ms
141.14ms
163.94ms
253.63ms
177.79ms
262.70ms
168.80ms
287.26ms
190.78ms
95.85ms
161.42ms
5.11ms
11.60ms
25.69ms
15.78ms
36.78ms
29.71ms
19.13ms
12.99ms
50.11ms
101.09ms
88.06ms
153.27ms
206.23ms
94.40ms
105.07ms
156.27ms
145.85ms
eu-central-2	
152.86ms
189.44ms
219.45ms
224.18ms
225.88ms
229.89ms
119.87ms
138.24ms
157.68ms
247.77ms
170.06ms
259.52ms
162.48ms
281.01ms
183.54ms
97.39ms
162.85ms
12.61ms
4.74ms
30.79ms
9.95ms
32.48ms
30.86ms
21.53ms
14.55ms
44.26ms
116.21ms
92.99ms
152.61ms
204.14ms
94.36ms
104.96ms
156.59ms
146.03ms
eu-north-1	
172.84ms
212.70ms
241.92ms
247.35ms
249.29ms
252.89ms
142.63ms
161.05ms
182.32ms
272.26ms
196.96ms
283.79ms
187.34ms
305.13ms
209.75ms
105.61ms
171.90ms
24.47ms
28.20ms
3.63ms
32.84ms
51.17ms
39.70ms
31.32ms
32.14ms
67.25ms
118.76ms
105.58ms
170.04ms
222.48ms
114.05ms
124.50ms
173.00ms
155.17ms
eu-south-1	
196.44ms
185.95ms
216.11ms
220.61ms
222.48ms
226.60ms
116.28ms
134.34ms
154.78ms
243.22ms
167.93ms
254.66ms
159.28ms
277.67ms
180.24ms
105.54ms
170.24ms
15.56ms
9.99ms
34.28ms
5.02ms
28.55ms
39.12ms
30.04ms
24.33ms
40.81ms
112.79ms
80.85ms
162.34ms
213.88ms
104.32ms
112.97ms
164.97ms
155.75ms
eu-south-2	
137.66ms
218.99ms
222.82ms
248.39ms
226.15ms
230.64ms
142.92ms
134.39ms
181.22ms
271.76ms
170.70ms
286.66ms
160.69ms
302.03ms
182.62ms
113.62ms
174.09ms
37.62ms
34.32ms
56.47ms
30.21ms
5.56ms
35.71ms
35.00ms
19.81ms
63.53ms
124.02ms
91.36ms
166.72ms
221.44ms
102.56ms
113.85ms
173.62ms
154.99ms
eu-west-1	
160.61ms
264.66ms
233.08ms
203.64ms
239.36ms
216.87ms
125.62ms
142.41ms
175.98ms
259.55ms
190.69ms
268.74ms
179.60ms
291.31ms
200.99ms
72.70ms
137.35ms
30.39ms
30.66ms
42.41ms
39.11ms
35.70ms
6.83ms
14.86ms
23.43ms
73.40ms
133.08ms
100.62ms
130.60ms
181.38ms
72.73ms
82.25ms
133.17ms
121.73ms
eu-west-2	
151.10ms
203.74ms
242.62ms
214.21ms
249.95ms
227.03ms
116.21ms
136.89ms
168.57ms
267.53ms
180.84ms
278.94ms
170.55ms
301.24ms
191.43ms
82.62ms
148.30ms
19.10ms
22.17ms
33.89ms
31.37ms
33.26ms
15.74ms
5.36ms
12.66ms
63.67ms
123.29ms
91.78ms
137.11ms
189.76ms
80.25ms
89.67ms
151.21ms
132.18ms
eu-west-3	
144.73ms
200.14ms
249.00ms
220.39ms
256.08ms
234.29ms
108.36ms
126.13ms
166.86ms
282.57ms
179.68ms
292.22ms
171.94ms
309.23ms
192.48ms
87.35ms
155.08ms
13.02ms
14.61ms
33.96ms
24.08ms
18.77ms
23.08ms
13.30ms
5.69ms
56.11ms
115.67ms
88.92ms
144.95ms
197.07ms
87.61ms
97.46ms
146.58ms
138.78ms
il-central-1	
234.32ms
222.79ms
251.25ms
259.31ms
262.03ms
263.34ms
142.68ms
155.74ms
190.99ms
281.11ms
205.27ms
293.86ms
197.53ms
316.21ms
215.77ms
140.18ms
208.26ms
50.44ms
46.71ms
72.36ms
40.22ms
61.54ms
75.83ms
67.71ms
56.37ms
4.60ms
145.84ms
118.69ms
198.56ms
250.34ms
140.78ms
146.93ms
203.31ms
188.27ms
me-central-1	
150.78ms
128.69ms
153.07ms
161.98ms
161.77ms
166.57ms
33.26ms
49.51ms
94.33ms
185.16ms
108.50ms
194.98ms
99.10ms
210.43ms
120.26ms
198.83ms
264.37ms
99.93ms
116.52ms
120.71ms
112.85ms
121.40ms
131.38ms
124.19ms
116.22ms
144.78ms
6.10ms
18.13ms
253.89ms
306.45ms
196.34ms
206.32ms
256.49ms
247.10ms
me-south-1	
154.30ms
131.15ms
157.70ms
161.72ms
163.49ms
167.13ms
37.58ms
55.72ms
96.34ms
186.12ms
109.94ms
196.43ms
101.10ms
212.16ms
123.51ms
165.39ms
240.35ms
87.03ms
91.89ms
106.15ms
78.55ms
87.36ms
99.11ms
90.47ms
87.95ms
112.47ms
15.58ms
3.60ms
221.32ms
273.59ms
164.29ms
174.61ms
223.92ms
242.25ms
mx-central-1	
286.42ms
207.27ms
193.48ms
160.12ms
191.55ms
168.06ms
254.83ms
267.40ms
226.49ms
205.38ms
238.53ms
216.74ms
229.02ms
240.13ms
251.82ms
77.87ms
77.35ms
154.77ms
154.54ms
172.77ms
163.33ms
163.82ms
131.26ms
139.97ms
145.65ms
198.30ms
256.26ms
221.96ms
4.85ms
174.72ms
63.30ms
54.74ms
68.23ms
86.14ms
sa-east-1	
337.13ms
319.93ms
287.88ms
260.15ms
295.43ms
272.74ms
299.31ms
316.91ms
327.98ms
314.13ms
340.15ms
324.16ms
332.66ms
345.33ms
356.08ms
128.19ms
194.04ms
205.37ms
204.65ms
224.49ms
214.39ms
218.14ms
181.94ms
189.47ms
197.14ms
247.31ms
307.34ms
274.90ms
174.44ms
6.25ms
116.42ms
126.52ms
176.76ms
177.46ms
us-east-1	
227.34ms
210.07ms
179.53ms
151.02ms
184.28ms
162.75ms
189.89ms
206.46ms
218.78ms
204.71ms
234.50ms
213.89ms
226.37ms
232.26ms
248.54ms
21.35ms
56.19ms
98.01ms
96.27ms
116.21ms
106.57ms
101.27ms
72.87ms
81.90ms
88.67ms
138.69ms
197.37ms
165.97ms
61.63ms
116.94ms
9.17ms
20.05ms
71.09ms
67.91ms
us-east-2	
240.83ms
200.57ms
168.80ms
140.97ms
175.35ms
154.01ms
204.59ms
218.87ms
206.15ms
195.31ms
220.35ms
205.51ms
212.35ms
220.34ms
233.78ms
34.13ms
46.52ms
110.08ms
108.41ms
129.45ms
118.79ms
118.53ms
87.28ms
95.34ms
101.19ms
151.89ms
210.91ms
179.84ms
59.77ms
131.10ms
19.43ms
8.61ms
57.62ms
55.06ms
us-west-1	
289.28ms
156.88ms
141.57ms
112.07ms
133.60ms
112.91ms
231.96ms
219.23ms
174.82ms
142.63ms
188.05ms
153.51ms
178.70ms
167.81ms
200.16ms
83.40ms
42.61ms
157.35ms
156.15ms
174.97ms
165.86ms
169.72ms
132.65ms
151.05ms
147.93ms
198.08ms
256.96ms
224.85ms
69.28ms
176.43ms
69.73ms
53.17ms
5.14ms
23.43ms
us-west-2	
274.01ms
148.22ms
132.03ms
100.90ms
123.80ms
103.03ms
222.42ms
208.59ms
165.18ms
142.07ms
178.78ms
152.88ms
169.68ms
177.70ms
192.14ms
63.51ms
24.34ms
144.75ms
146.11ms
157.41ms
153.76ms
154.48ms
121.12ms
131.30ms
137.33ms
188.76ms
246.95ms
241.83ms
85.24ms
177.11ms
66.45ms
49.75ms
23.43ms
5.76ms


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
    specific_nodes = ['ap-southeast-2', 'ap-northeast-2', 'ap-south-2', 'us-west-2', 'eu-central-2']
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