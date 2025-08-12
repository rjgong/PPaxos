import pandas as pd
import io
import itertools
from typing import List, Tuple, Dict

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
    
    return len(violations) == 0, {
        'leader': leader,
        'delay_sums': delay_sums,
        'checks': checks,
        'violations': violations
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
    
    # 解析数据
    data = io.StringIO(data_string)
    try:
        df = pd.read_csv(data, sep='\\s*\\t\\s*', engine='python', index_col=0)
    except Exception as e:
        print(f"错误：无法解析数据。详细信息: {e}")
        return

    # 清理数据
    df.columns = df.columns.str.strip()
    df.index = df.index.str.strip()
    
    for col in df.columns:
        df[col] = pd.to_numeric(df[col].str.replace('ms', '', regex=False), errors='coerce')
    
    nodes = df.index.tolist()
    print(f"共有 {len(nodes)} 个节点")
    
    print("\n步骤 2/3: 分析节点特征...")
    
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
            
            # 分析每个节点的情况
            print("\n每个节点的验证:")
            nodes_list = list(combo)
            for node in nodes_list:
                is_leader = (node == info['leader'])
                
                # 找最远节点
                max_delay = 0
                farthest = None
                for other in nodes_list:
                    if node != other:
                        delay = get_symmetric_delay(df, node, other)
                        if delay > max_delay:
                            max_delay = delay
                            farthest = other
                
                print(f"\n{node}{' (Leader)' if is_leader else ''}:")
                print(f"  最远节点: {farthest}, 单程延迟: {max_delay:.2f}ms, 往返: {2*max_delay:.2f}ms")
                
                # 显示一个环路例子
                if is_leader:
                    # Leader: 任意两个节点
                    others = [n for n in nodes_list if n != node and n != farthest]
                    if len(others) >= 2:
                        n1, n2 = others[0], others[1]
                        loop_delay = (get_symmetric_delay(df, node, n1) + 
                                    get_symmetric_delay(df, n1, n2) + 
                                    get_symmetric_delay(df, n2, node))
                        print(f"  环路示例: {node}->{n1}->{n2}->{node} = {loop_delay:.2f}ms")
                        print(f"  验证: {2*max_delay:.2f} < {loop_delay:.2f} ✓" if 2*max_delay < loop_delay else f"  验证: {2*max_delay:.2f} >= {loop_delay:.2f} ✗")
                else:
                    # 非Leader: 必须包含leader的环路
                    if farthest == info['leader']:
                        # 如果leader是最远点，选择leader和另一个节点
                        others = [n for n in nodes_list if n != node and n != info['leader']]
                        if others:
                            other = others[0]
                            loop_delay = (get_symmetric_delay(df, node, info['leader']) + 
                                        get_symmetric_delay(df, info['leader'], other) + 
                                        get_symmetric_delay(df, other, node))
                            print(f"  环路示例(含Leader): {node}->{info['leader']}->{other}->{node} = {loop_delay:.2f}ms")
                            print(f"  验证: {2*max_delay:.2f} < {loop_delay:.2f} ✓" if 2*max_delay < loop_delay else f"  验证: {2*max_delay:.2f} >= {loop_delay:.2f} ✗")
                    else:
                        # leader不是最远点，选择包含leader的环路
                        others = [n for n in nodes_list if n != node and n != farthest]
                        if info['leader'] in others:
                            others.remove(info['leader'])
                            if others:
                                other = others[0]
                                loop_delay = (get_symmetric_delay(df, node, info['leader']) + 
                                            get_symmetric_delay(df, info['leader'], other) + 
                                            get_symmetric_delay(df, other, node))
                                print(f"  环路示例(含Leader): {node}->{info['leader']}->{other}->{node} = {loop_delay:.2f}ms")
                                print(f"  验证: {2*max_delay:.2f} < {loop_delay:.2f} ✓" if 2*max_delay < loop_delay else f"  验证: {2*max_delay:.2f} >= {loop_delay:.2f} ✗")
            
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
From	af-south-1	ap-east-1	ap-east-2	ap-northeast-1	ap-northeast-2	ap-northeast-3	ap-south-1	ap-south-2	ap-southeast-1	ap-southeast-2	ap-southeast-3	ap-southeast-4	ap-southeast-5	ap-southeast-7	ca-central-1	ca-west-1	eu-central-1	eu-central-2	eu-north-1	eu-south-1	eu-south-2	eu-west-1	eu-west-2	eu-west-3	il-central-1	me-central-1	me-south-1	mx-central-1	sa-east-1	us-east-1	us-east-2	us-west-1	us-west-2
af-south-1	4.73ms	241.15ms	272.31ms	286.98ms	278.04ms	279.21ms	147.19ms	170.29ms	207.73ms	297.61ms	221.53ms	307.42ms	213.53ms	233.69ms	226.02ms	278.90ms	155.60ms	152.36ms	176.77ms	193.47ms	137.50ms	159.11ms	150.00ms	148.18ms	228.52ms	129.66ms	138.33ms	280.90ms	340.59ms	230.91ms	240.47ms	289.77ms	274.48ms
ap-east-1	240.76ms	3.82ms	31.89ms	51.65ms	40.32ms	49.95ms	98.99ms	81.66ms	40.52ms	129.81ms	54.78ms	141.51ms	46.64ms	68.04ms	203.91ms	188.27ms	194.41ms	188.11ms	215.08ms	185.59ms	187.42ms	262.20ms	200.87ms	198.01ms	241.68ms	123.62ms	129.05ms	216.60ms	318.75ms	210.33ms	194.76ms	155.89ms	145.78ms
ap-east-2	273.23ms	33.38ms	5.23ms	37.67ms	68.07ms	36.14ms	129.90ms	116.08ms	66.72ms	153.99ms	86.26ms	167.05ms	76.08ms	98.27ms	179.60ms	160.20ms	229.01ms	219.07ms	244.28ms	217.02ms	216.35ms	235.95ms	249.76ms	248.07ms	276.03ms	153.23ms	166.96ms	197.10ms	288.67ms	180.49ms	165.42ms	144.39ms	132.73ms
ap-northeast-1	289.99ms	53.50ms	36.01ms	4.78ms	38.70ms	11.61ms	133.99ms	122.43ms	73.68ms	109.72ms	87.41ms	139.95ms	78.44ms	100.18ms	159.60ms	133.76ms	231.05ms	222.03ms	247.10ms	219.03ms	221.92ms	209.28ms	217.99ms	223.35ms	275.86ms	168.92ms	178.26ms	169.84ms	263.00ms	151.14ms	138.57ms	110.75ms	102.31ms
ap-northeast-2	281.50ms	42.01ms	67.44ms	39.95ms	7.69ms	27.55ms	138.15ms	123.48ms	75.47ms	149.32ms	91.29ms	156.89ms	83.82ms	103.05ms	181.93ms	165.48ms	237.13ms	235.20ms	262.27ms	237.53ms	223.83ms	242.13ms	248.38ms	256.45ms	291.82ms	162.10ms	169.26ms	196.70ms	297.62ms	185.79ms	170.95ms	134.48ms	123.72ms
ap-northeast-3	280.41ms	52.76ms	35.27ms	13.40ms	31.20ms	4.15ms	142.82ms	126.87ms	81.88ms	124.32ms	97.14ms	133.97ms	86.33ms	105.91ms	159.71ms	143.14ms	239.14ms	228.64ms	254.46ms	227.58ms	228.50ms	214.95ms	227.51ms	231.58ms	281.90ms	165.07ms	172.26ms	172.57ms	273.52ms	161.71ms	149.22ms	114.21ms	103.45ms
ap-south-1	148.12ms	100.19ms	129.64ms	135.92ms	137.38ms	144.13ms	5.79ms	32.95ms	67.55ms	156.60ms	83.91ms	168.30ms	71.41ms	93.38ms	196.64ms	237.38ms	131.41ms	117.48ms	149.76ms	114.05ms	148.74ms	133.36ms	120.19ms	113.16ms	151.08ms	33.09ms	43.02ms	250.97ms	302.81ms	202.90ms	208.93ms	237.50ms	225.25ms
ap-south-2	169.20ms	82.27ms	111.06ms	118.47ms	119.88ms	123.33ms	24.51ms	8.73ms	44.94ms	139.47ms	61.52ms	150.19ms	53.63ms	74.16ms	210.29ms	239.86ms	148.57ms	135.80ms	170.05ms	133.64ms	166.86ms	142.97ms	139.54ms	127.64ms	168.10ms	48.74ms	56.14ms	273.47ms	318.52ms	208.95ms	221.91ms	221.52ms	207.42ms
ap-southeast-1	208.63ms	41.94ms	66.30ms	71.97ms	75.84ms	80.17ms	66.98ms	51.65ms	7.20ms	97.41ms	19.57ms	106.26ms	10.80ms	31.69ms	216.83ms	201.52ms	165.37ms	156.45ms	182.02ms	153.27ms	153.94ms	181.27ms	166.70ms	165.79ms	207.84ms	95.08ms	101.43ms	246.39ms	329.26ms	220.86ms	204.79ms	173.94ms	164.45ms
ap-southeast-2	297.92ms	130.06ms	151.12ms	109.59ms	145.52ms	125.21ms	153.40ms	143.13ms	96.99ms	3.90ms	110.37ms	16.13ms	100.71ms	121.81ms	200.62ms	184.36ms	253.86ms	245.95ms	271.78ms	244.32ms	244.72ms	257.14ms	267.66ms	280.51ms	300.46ms	183.50ms	192.38ms	214.44ms	314.53ms	203.02ms	188.50ms	140.35ms	142.34ms
ap-southeast-3	224.03ms	53.85ms	84.03ms	91.63ms	91.42ms	97.17ms	82.02ms	64.87ms	17.96ms	111.96ms	5.01ms	121.28ms	27.50ms	48.69ms	232.54ms	217.69ms	176.19ms	172.11ms	198.06ms	167.80ms	176.37ms	185.77ms	192.64ms	182.30ms	226.61ms	112.61ms	117.59ms	260.65ms	345.40ms	232.70ms	219.71ms	188.13ms	176.32ms
ap-southeast-4	307.37ms	140.97ms	164.78ms	138.98ms	156.83ms	132.94ms	166.05ms	154.03ms	107.01ms	16.42ms	122.42ms	3.24ms	112.42ms	135.57ms	211.59ms	193.29ms	262.53ms	257.88ms	283.69ms	255.88ms	252.24ms	269.34ms	282.07ms	292.90ms	311.12ms	191.01ms	201.92ms	223.77ms	324.87ms	214.59ms	199.26ms	152.93ms	151.56ms
ap-southeast-5	213.87ms	44.96ms	73.28ms	78.80ms	84.37ms	85.80ms	73.25ms	58.34ms	12.03ms	102.46ms	26.66ms	114.90ms	4.01ms	26.52ms	223.07ms	207.69ms	168.41ms	160.60ms	189.99ms	161.50ms	161.26ms	181.04ms	172.83ms	175.25ms	216.50ms	95.33ms	107.53ms	250.09ms	342.01ms	225.55ms	211.34ms	178.22ms	167.27ms
ap-southeast-7	236.44ms	65.39ms	95.10ms	101.30ms	104.85ms	106.47ms	93.28ms	77.14ms	31.11ms	128.89ms	45.22ms	132.65ms	31.72ms	4.02ms	244.21ms	227.36ms	190.84ms	184.50ms	208.09ms	180.62ms	181.94ms	201.51ms	192.10ms	194.67ms	239.09ms	117.31ms	123.35ms	271.99ms	358.34ms	245.57ms	230.71ms	198.65ms	189.95ms
ca-central-1	223.86ms	206.80ms	178.71ms	158.72ms	182.55ms	157.97ms	195.41ms	215.42ms	216.88ms	201.95ms	230.32ms	210.51ms	224.75ms	244.97ms	5.29ms	50.26ms	95.02ms	95.68ms	105.87ms	105.66ms	109.52ms	72.03ms	83.86ms	87.59ms	139.17ms	198.84ms	176.19ms	73.02ms	127.03ms	19.37ms	28.98ms	83.96ms	61.60ms
ca-west-1	286.31ms	193.36ms	158.10ms	132.98ms	171.70ms	146.15ms	240.22ms	250.29ms	209.03ms	184.46ms	219.63ms	196.30ms	210.82ms	233.31ms	53.22ms	1.56ms	149.37ms	151.77ms	160.53ms	155.71ms	163.04ms	127.60ms	134.89ms	140.69ms	193.65ms	255.45ms	232.66ms	81.51ms	184.14ms	62.21ms	42.38ms	71.36ms	24.97ms
eu-central-1	156.63ms	195.38ms	226.02ms	228.76ms	243.85ms	235.52ms	130.70ms	153.23ms	162.68ms	252.63ms	174.94ms	265.53ms	166.81ms	187.89ms	96.56ms	145.33ms	8.02ms	11.55ms	25.91ms	16.71ms	37.77ms	27.68ms	18.69ms	13.90ms	69.11ms	116.24ms	89.27ms	147.86ms	205.25ms	94.22ms	105.92ms	157.36ms	144.08ms
eu-central-2	153.69ms	189.11ms	220.05ms	222.20ms	231.61ms	231.45ms	119.15ms	131.62ms	156.86ms	245.79ms	170.60ms	257.15ms	162.70ms	181.53ms	97.75ms	147.72ms	13.24ms	4.07ms	28.90ms	11.17ms	33.37ms	29.67ms	20.74ms	12.57ms	42.15ms	110.64ms	95.34ms	147.95ms	204.79ms	94.37ms	105.52ms	155.22ms	146.43ms
eu-north-1	175.14ms	213.67ms	241.99ms	245.86ms	256.00ms	254.27ms	148.91ms	172.46ms	185.86ms	271.60ms	194.57ms	280.61ms	186.86ms	207.97ms	104.95ms	156.86ms	23.54ms	27.84ms	2.30ms	31.69ms	53.78ms	39.81ms	31.41ms	32.18ms	87.33ms	134.29ms	106.11ms	167.22ms	222.75ms	114.26ms	124.70ms	173.75ms	155.01ms
eu-south-1	189.27ms	187.52ms	218.55ms	217.78ms	233.53ms	228.48ms	115.44ms	132.78ms	153.10ms	244.06ms	167.43ms	252.03ms	157.73ms	179.99ms	104.92ms	156.27ms	13.62ms	10.04ms	33.48ms	2.70ms	31.59ms	37.64ms	30.26ms	24.29ms	50.11ms	108.80ms	118.45ms	158.26ms	215.02ms	104.16ms	113.23ms	165.56ms	154.56ms
eu-south-2	139.54ms	193.43ms	217.80ms	223.27ms	228.08ms	231.04ms	152.61ms	173.07ms	158.74ms	249.24ms	172.44ms	257.40ms	164.09ms	184.11ms	111.73ms	165.60ms	38.38ms	39.61ms	58.05ms	27.93ms	4.86ms	33.76ms	38.75ms	20.37ms	65.28ms	125.40ms	129.53ms	167.42ms	221.66ms	109.73ms	121.91ms	172.46ms	160.92ms
eu-west-1	159.65ms	265.02ms	230.81ms	205.87ms	240.67ms	215.95ms	129.76ms	149.00ms	174.64ms	258.50ms	187.21ms	268.79ms	177.90ms	199.87ms	71.94ms	122.66ms	28.32ms	28.75ms	43.59ms	37.47ms	35.90ms	4.38ms	16.50ms	22.42ms	71.08ms	131.78ms	111.18ms	127.20ms	179.48ms	71.87ms	82.34ms	132.00ms	119.15ms
eu-west-2	148.42ms	198.87ms	243.22ms	217.43ms	252.60ms	227.85ms	120.78ms	137.71ms	163.83ms	265.81ms	183.35ms	278.27ms	169.74ms	189.83ms	81.09ms	130.79ms	16.27ms	20.03ms	33.44ms	28.40ms	37.90ms	12.19ms	4.77ms	12.08ms	64.87ms	124.65ms	103.20ms	131.38ms	190.20ms	80.05ms	89.40ms	152.35ms	129.78ms
eu-west-3	149.90ms	198.99ms	249.15ms	222.87ms	257.37ms	233.97ms	112.69ms	130.04ms	163.70ms	280.70ms	180.48ms	290.10ms	171.94ms	190.68ms	87.90ms	141.75ms	13.09ms	13.46ms	34.88ms	23.15ms	16.84ms	21.57ms	11.66ms	6.03ms	55.94ms	113.68ms	98.08ms	139.50ms	195.04ms	85.48ms	96.69ms	147.90ms	137.23ms
il-central-1	232.48ms	255.69ms	282.90ms	285.46ms	298.00ms	298.28ms	159.88ms	178.83ms	215.84ms	308.03ms	234.00ms	316.65ms	216.49ms	242.47ms	147.07ms	198.55ms	77.90ms	45.37ms	99.41ms	61.74ms	69.26ms	78.32ms	71.39ms	56.90ms	2.50ms	152.42ms	160.44ms	198.62ms	255.03ms	140.26ms	147.73ms	203.80ms	188.83ms
me-central-1	128.58ms	124.28ms	156.39ms	173.29ms	161.02ms	167.08ms	34.36ms	52.10ms	95.11ms	185.12ms	106.48ms	191.40ms	97.06ms	118.07ms	198.12ms	252.45ms	117.70ms	111.41ms	139.90ms	108.11ms	120.59ms	131.33ms	126.10ms	116.99ms	143.99ms	3.91ms	16.05ms	252.27ms	307.57ms	195.65ms	207.79ms	256.87ms	256.85ms
me-south-1	134.92ms	129.54ms	161.34ms	174.93ms	166.38ms	171.97ms	38.65ms	55.57ms	98.04ms	190.68ms	114.21ms	199.67ms	103.40ms	127.76ms	175.98ms	227.78ms	87.21ms	91.61ms	106.80ms	113.56ms	128.15ms	108.75ms	100.72ms	96.86ms	150.97ms	15.33ms	2.63ms	230.69ms	286.96ms	177.33ms	188.36ms	237.17ms	264.81ms
mx-central-1	284.31ms	216.16ms	200.01ms	170.32ms	202.82ms	172.36ms	249.64ms	280.44ms	240.54ms	214.70ms	259.09ms	227.22ms	250.57ms	270.27ms	76.08ms	80.27ms	150.22ms	152.32ms	171.97ms	161.93ms	169.68ms	130.69ms	132.88ms	143.00ms	196.25ms	254.58ms	235.93ms	4.96ms	175.47ms	64.66ms	54.75ms	78.32ms	91.38ms
sa-east-1	338.58ms	319.76ms	288.47ms	263.37ms	296.46ms	272.50ms	302.83ms	322.54ms	328.31ms	313.85ms	341.69ms	323.12ms	335.75ms	354.60ms	127.80ms	178.87ms	205.25ms	204.37ms	224.33ms	214.12ms	221.92ms	179.47ms	189.34ms	196.98ms	248.68ms	306.77ms	288.34ms	171.36ms	4.15ms	114.52ms	125.71ms	177.60ms	177.44ms
us-east-1	230.61ms	212.44ms	179.80ms	155.12ms	184.02ms	164.16ms	198.33ms	211.63ms	220.26ms	204.19ms	239.14ms	214.13ms	228.11ms	250.55ms	19.39ms	57.10ms	95.54ms	96.36ms	115.68ms	104.70ms	110.10ms	74.18ms	81.05ms	88.15ms	138.17ms	199.35ms	180.66ms	59.27ms	115.98ms	9.54ms	16.74ms	70.81ms	69.81ms
us-east-2	242.97ms	202.53ms	168.05ms	144.22ms	177.32ms	153.59ms	212.11ms	221.68ms	210.47ms	195.46ms	224.67ms	205.47ms	215.94ms	236.05ms	33.02ms	47.19ms	108.82ms	109.68ms	128.00ms	118.31ms	124.88ms	85.64ms	94.51ms	101.36ms	153.67ms	211.45ms	193.71ms	58.15ms	133.87ms	20.20ms	8.68ms	57.72ms	53.12ms
us-west-1	288.72ms	158.52ms	143.91ms	111.83ms	137.59ms	111.41ms	236.63ms	222.83ms	171.12ms	142.51ms	186.71ms	150.46ms	176.07ms	197.94ms	80.46ms	66.04ms	154.57ms	154.72ms	176.24ms	161.98ms	170.71ms	132.95ms	150.58ms	146.06ms	194.62ms	256.56ms	237.99ms	76.06ms	173.47ms	69.19ms	53.68ms	3.31ms	22.11ms
us-west-2	272.82ms	148.07ms	131.28ms	102.15ms	127.65ms	102.92ms	225.42ms	212.37ms	165.24ms	141.97ms	176.67ms	152.30ms	169.65ms	189.30ms	63.77ms	24.50ms	144.27ms	145.55ms	155.77ms	154.64ms	160.18ms	119.46ms	131.11ms	135.57ms	190.41ms	254.80ms	268.05ms	91.39ms	179.03ms	65.13ms	52.42ms	21.02ms	4.69ms
'''

# --- 运行脚本 ---
# 将上面的字符串数据传入函数并执行
find_pentagon_nodes_with_leader(latency_data.strip(), sample_size=30)










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
    
    return ratio_delay < 0.84, {
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
            print(f"   比值(Method1/Method2): {info['ratio']:.2f}")

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
            if info['ratio'] < 0.84:
                print(f"  ✓ 方法1/方法2 = {info['ratio']:.2f} < 0.8，符合要求")
            else:
                print(f"  ✗ 方法1/方法2 = {info['ratio']:.2f} ≥ 0.8，不符合要求")
            
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
af-south-1	1.69ms	351.57ms	379.68ms	386.42ms	385.32ms	386.65ms	306.99ms	333.48ms	314.95ms	411.39ms	332.07ms	419.56ms	325.26ms	344.36ms	226.00ms	292.44ms	156.94ms	156.39ms	177.87ms	191.51ms	138.39ms	158.81ms	150.46ms	146.14ms	220.95ms	279.51ms	290.52ms	288.48ms	339.35ms	228.60ms	239.88ms	289.08ms	275.55ms
ap-east-1	351.15ms	3.13ms	30.74ms	51.57ms	41.38ms	51.58ms	98.00ms	81.10ms	38.54ms	128.61ms	54.38ms	137.95ms	44.50ms	62.87ms	204.56ms	161.93ms	193.16ms	188.37ms	211.79ms	183.25ms	187.29ms	261.85ms	198.72ms	197.55ms	234.84ms	122.73ms	129.92ms	216.18ms	319.54ms	207.79ms	194.49ms	155.00ms	145.06ms
ap-east-2	377.88ms	35.38ms	6.02ms	40.84ms	66.44ms	36.63ms	129.60ms	116.10ms	72.18ms	157.74ms	80.48ms	165.82ms	77.62ms	101.22ms	183.99ms	146.22ms	226.17ms	220.98ms	248.53ms	224.88ms	221.01ms	236.27ms	253.79ms	255.05ms	270.49ms	158.94ms	166.11ms	197.73ms	289.95ms	180.67ms	170.47ms	144.85ms	131.77ms
ap-northeast-1	384.35ms	51.96ms	37.56ms	6.23ms	40.06ms	13.30ms	131.60ms	121.19ms	74.08ms	106.45ms	87.20ms	139.61ms	78.51ms	101.18ms	157.06ms	111.34ms	230.34ms	225.59ms	249.95ms	221.73ms	220.54ms	206.03ms	217.11ms	226.68ms	271.41ms	169.37ms	178.53ms	167.64ms	263.28ms	153.26ms	141.36ms	112.78ms	102.02ms
ap-northeast-2	386.97ms	41.10ms	64.38ms	39.74ms	4.06ms	25.85ms	137.69ms	121.82ms	83.57ms	140.34ms	95.52ms	152.31ms	87.39ms	105.90ms	178.16ms	140.12ms	251.52ms	246.45ms	265.77ms	238.98ms	223.63ms	236.74ms	246.91ms	253.00ms	296.12ms	159.45ms	165.55ms	194.66ms	291.98ms	179.99ms	167.97ms	133.76ms	122.91ms
ap-northeast-3	389.69ms	51.15ms	35.78ms	12.85ms	28.58ms	6.03ms	141.03ms	129.75ms	80.01ms	121.89ms	96.50ms	134.94ms	85.34ms	104.28ms	161.62ms	117.46ms	238.20ms	229.33ms	257.55ms	227.26ms	228.84ms	217.46ms	229.88ms	235.15ms	278.17ms	164.93ms	171.73ms	173.34ms	271.71ms	160.93ms	147.63ms	113.25ms	103.22ms
ap-south-1	307.92ms	97.86ms	125.34ms	133.11ms	137.65ms	140.95ms	4.26ms	28.63ms	64.77ms	154.69ms	80.76ms	170.72ms	70.28ms	93.55ms	195.97ms	239.44ms	130.23ms	134.48ms	149.31ms	129.21ms	148.73ms	133.52ms	120.66ms	114.05ms	168.72ms	31.04ms	39.23ms	251.53ms	304.49ms	206.96ms	211.27ms	235.47ms	226.02ms
ap-south-2	331.34ms	79.72ms	110.23ms	116.92ms	117.94ms	122.25ms	26.03ms	10.40ms	46.59ms	137.65ms	62.07ms	147.81ms	54.32ms	75.99ms	207.46ms	226.81ms	147.40ms	150.84ms	166.72ms	145.66ms	168.71ms	145.42ms	135.21ms	125.93ms	185.10ms	45.99ms	56.36ms	288.44ms	318.89ms	207.32ms	218.07ms	216.72ms	206.74ms
ap-southeast-1	317.38ms	40.34ms	69.78ms	73.33ms	84.21ms	78.99ms	65.80ms	50.26ms	5.40ms	94.69ms	21.57ms	106.24ms	10.04ms	31.34ms	216.84ms	179.22ms	161.80ms	158.32ms	184.17ms	153.57ms	155.74ms	179.32ms	173.56ms	169.06ms	204.39ms	93.97ms	101.45ms	241.32ms	328.62ms	216.61ms	204.06ms	178.31ms	168.49ms
ap-southeast-2	411.53ms	129.00ms	154.02ms	116.57ms	141.00ms	122.58ms	156.87ms	139.28ms	97.81ms	5.68ms	110.10ms	17.05ms	102.03ms	124.43ms	200.70ms	158.94ms	254.30ms	247.88ms	273.29ms	244.18ms	243.00ms	257.99ms	266.94ms	284.61ms	298.48ms	185.98ms	190.90ms	216.05ms	316.11ms	201.73ms	187.97ms	141.85ms	141.78ms
ap-southeast-3	339.22ms	55.08ms	87.60ms	88.39ms	95.14ms	94.71ms	88.62ms	71.09ms	20.27ms	119.77ms	1.91ms	127.02ms	28.02ms	48.65ms	237.02ms	197.50ms	183.80ms	173.31ms	199.58ms	172.78ms	169.11ms	198.15ms	180.67ms	183.03ms	221.76ms	112.03ms	115.05ms	263.59ms	346.16ms	236.84ms	220.35ms	192.78ms	177.53ms
ap-southeast-4	419.87ms	140.79ms	164.19ms	138.82ms	157.64ms	133.71ms	167.97ms	153.00ms	109.48ms	13.08ms	122.04ms	5.97ms	114.87ms	132.98ms	211.18ms	170.46ms	264.47ms	258.72ms	284.38ms	255.23ms	257.97ms	270.14ms	276.38ms	291.76ms	306.71ms	197.85ms	202.51ms	226.09ms	324.55ms	215.01ms	197.98ms	154.25ms	154.38ms
ap-southeast-5	322.96ms	41.17ms	73.32ms	78.87ms	85.97ms	84.21ms	72.02ms	60.42ms	12.77ms	103.44ms	25.90ms	110.79ms	2.21ms	24.97ms	223.50ms	187.90ms	168.42ms	162.07ms	186.39ms	159.97ms	157.54ms	180.44ms	174.85ms	171.47ms	209.37ms	97.20ms	105.61ms	248.01ms	336.63ms	225.31ms	208.96ms	179.30ms	169.15ms
ap-southeast-7	344.45ms	65.00ms	97.05ms	100.38ms	102.23ms	104.03ms	93.18ms	80.08ms	29.36ms	124.62ms	48.25ms	135.73ms	27.65ms	3.87ms	241.01ms	206.48ms	192.76ms	182.92ms	208.56ms	181.59ms	182.22ms	199.74ms	188.81ms	197.18ms	232.79ms	113.82ms	125.75ms	269.09ms	353.43ms	244.10ms	233.49ms	202.80ms	191.11ms
ca-central-1	223.68ms	205.19ms	180.40ms	159.06ms	180.33ms	161.11ms	194.99ms	217.36ms	216.69ms	200.81ms	230.93ms	210.81ms	221.73ms	243.84ms	5.66ms	51.29ms	96.58ms	96.51ms	107.31ms	104.03ms	113.76ms	71.20ms	81.98ms	88.76ms	138.65ms	199.95ms	179.88ms	76.29ms	130.84ms	20.03ms	31.21ms	84.48ms	62.92ms
ca-west-1	295.43ms	165.30ms	146.91ms	116.87ms	143.87ms	120.42ms	249.69ms	233.98ms	179.82ms	163.08ms	201.83ms	175.11ms	184.97ms	210.53ms	52.95ms	3.47ms	165.91ms	166.33ms	175.08ms	169.17ms	174.84ms	137.91ms	150.08ms	157.74ms	208.85ms	276.21ms	264.80ms	80.53ms	196.95ms	80.30ms	67.14ms	43.02ms	25.43ms
eu-central-1	155.11ms	193.67ms	224.71ms	229.06ms	245.90ms	236.18ms	129.71ms	155.05ms	161.95ms	253.11ms	177.51ms	263.57ms	165.86ms	191.49ms	97.40ms	160.51ms	7.99ms	11.56ms	25.05ms	15.40ms	38.45ms	30.73ms	17.66ms	12.75ms	65.47ms	118.16ms	86.88ms	152.26ms	205.93ms	98.33ms	105.08ms	155.10ms	143.31ms
eu-central-2	155.48ms	190.20ms	218.91ms	225.81ms	243.07ms	233.48ms	135.41ms	155.96ms	155.87ms	246.63ms	173.53ms	257.21ms	161.37ms	182.03ms	95.84ms	161.80ms	10.04ms	6.26ms	29.91ms	10.34ms	30.95ms	30.35ms	20.44ms	14.40ms	47.73ms	112.64ms	91.47ms	154.15ms	204.83ms	96.53ms	104.71ms	154.21ms	145.13ms
eu-north-1	174.08ms	212.89ms	241.37ms	246.35ms	268.38ms	254.37ms	148.63ms	167.37ms	182.14ms	271.24ms	197.07ms	284.95ms	187.54ms	208.40ms	104.98ms	171.16ms	23.77ms	27.69ms	3.98ms	31.45ms	54.05ms	39.61ms	31.26ms	31.33ms	82.90ms	134.18ms	105.74ms	170.82ms	222.68ms	117.51ms	123.63ms	173.93ms	156.30ms
eu-south-1	191.45ms	184.48ms	215.28ms	219.30ms	238.16ms	225.62ms	127.67ms	147.53ms	152.64ms	242.94ms	166.56ms	254.74ms	156.66ms	178.58ms	106.87ms	171.13ms	15.13ms	8.89ms	34.91ms	5.53ms	28.41ms	38.68ms	30.01ms	23.53ms	49.12ms	108.11ms	111.82ms	164.80ms	215.02ms	103.11ms	112.44ms	165.99ms	153.65ms
eu-south-2	142.09ms	191.60ms	218.48ms	222.21ms	228.65ms	229.08ms	150.85ms	173.43ms	157.85ms	250.64ms	172.29ms	260.11ms	164.28ms	185.19ms	115.23ms	174.92ms	38.30ms	36.29ms	57.77ms	29.59ms	5.46ms	36.73ms	39.75ms	20.02ms	62.77ms	124.26ms	133.71ms	171.06ms	222.10ms	104.53ms	117.27ms	173.20ms	157.93ms
eu-west-1	162.21ms	264.95ms	234.04ms	206.57ms	239.23ms	214.68ms	134.60ms	144.69ms	180.55ms	256.87ms	190.41ms	267.31ms	181.36ms	198.71ms	72.26ms	135.76ms	27.38ms	32.61ms	44.14ms	39.35ms	34.50ms	5.11ms	15.81ms	20.80ms	75.21ms	134.40ms	112.29ms	132.50ms	181.10ms	73.09ms	82.48ms	130.66ms	120.64ms
eu-west-2	150.50ms	199.14ms	242.21ms	218.53ms	246.55ms	228.31ms	117.73ms	141.42ms	168.46ms	266.83ms	182.86ms	277.48ms	170.16ms	191.53ms	80.02ms	147.23ms	18.35ms	20.00ms	31.61ms	29.54ms	38.05ms	14.51ms	3.70ms	12.56ms	62.47ms	122.78ms	102.72ms	136.84ms	188.96ms	79.89ms	88.63ms	149.30ms	130.14ms
eu-west-3	146.88ms	197.07ms	250.00ms	224.25ms	250.89ms	232.94ms	111.68ms	132.11ms	166.13ms	281.73ms	178.09ms	292.07ms	167.93ms	190.29ms	88.35ms	155.18ms	13.31ms	14.90ms	32.40ms	24.85ms	19.26ms	22.68ms	13.54ms	7.30ms	53.93ms	117.10ms	95.96ms	145.26ms	197.59ms	83.67ms	96.04ms	146.91ms	137.30ms
il-central-1	228.77ms	242.43ms	275.44ms	281.60ms	295.77ms	281.00ms	177.64ms	195.46ms	210.76ms	296.59ms	226.05ms	307.92ms	218.30ms	237.61ms	143.65ms	209.93ms	70.27ms	54.69ms	89.80ms	55.88ms	62.81ms	75.02ms	66.96ms	57.41ms	6.36ms	153.05ms	163.30ms	205.19ms	254.78ms	135.57ms	149.36ms	205.24ms	187.76ms
me-central-1	280.53ms	122.37ms	154.04ms	171.26ms	163.59ms	164.85ms	32.40ms	48.89ms	91.81ms	184.75ms	107.71ms	194.70ms	100.92ms	117.21ms	200.97ms	267.29ms	118.10ms	112.05ms	135.11ms	106.42ms	121.95ms	131.18ms	124.17ms	117.23ms	149.06ms	3.83ms	17.06ms	258.43ms	309.04ms	196.66ms	208.52ms	256.45ms	260.20ms
me-south-1	292.25ms	129.41ms	161.41ms	171.87ms	163.64ms	168.10ms	38.43ms	54.52ms	98.35ms	189.69ms	113.39ms	199.22ms	106.79ms	126.91ms	175.33ms	259.96ms	87.22ms	92.45ms	106.08ms	111.79ms	130.95ms	109.84ms	102.26ms	97.22ms	154.45ms	14.57ms	2.04ms	235.20ms	285.90ms	176.56ms	186.29ms	237.43ms	262.55ms
mx-central-1	289.72ms	221.69ms	202.76ms	172.31ms	192.44ms	170.97ms	256.20ms	295.56ms	242.60ms	210.54ms	260.09ms	228.20ms	248.58ms	274.80ms	79.12ms	79.74ms	155.85ms	154.36ms	173.79ms	165.93ms	171.95ms	134.14ms	137.95ms	147.09ms	198.86ms	257.47ms	238.24ms	4.90ms	176.56ms	60.47ms	52.78ms	74.21ms	92.55ms	sa-east-1	342.92ms	321.35ms	287.96ms	264.34ms	293.43ms	274.28ms	303.63ms	322.00ms	329.59ms	313.58ms	347.29ms	323.18ms	335.69ms	353.65ms	130.71ms	193.41ms	206.83ms	203.93ms	228.46ms	214.54ms	218.66ms	180.56ms	189.28ms	197.61ms	244.93ms	308.09ms	288.65ms	176.76ms	4.98ms	115.37ms	126.68ms	178.29ms	179.79ms	us-east-1	231.14ms	210.61ms	179.65ms	152.32ms	181.91ms	163.19ms	208.89ms	213.33ms	219.16ms	203.55ms	238.05ms	213.41ms	227.89ms	250.17ms	20.40ms	80.37ms	94.94ms	96.47ms	115.59ms	105.23ms	106.52ms	71.93ms	82.39ms	86.17ms	135.05ms	198.62ms	179.55ms	62.67ms	116.77ms	9.32ms	19.74ms	72.61ms	68.88ms	us-east-2	242.45ms	200.04ms	172.66ms	144.66ms	175.98ms	153.04ms	210.25ms	222.32ms	209.91ms	195.11ms	222.56ms	205.60ms	215.41ms	235.65ms	34.75ms	71.02ms	109.21ms	109.87ms	132.40ms	120.50ms	118.65ms	87.31ms	95.54ms	102.56ms	152.42ms	210.89ms	191.44ms	59.12ms	130.79ms	20.89ms	8.64ms	55.61ms	55.11ms	us-west-1	290.03ms	158.94ms	143.16ms	110.72ms	134.73ms	111.99ms	232.15ms	219.02ms	178.09ms	142.58ms	189.94ms	152.42ms	182.65ms	204.75ms	83.46ms	42.73ms	156.04ms	155.23ms	173.94ms	164.06ms	170.63ms	131.39ms	152.26ms	146.30ms	194.03ms	255.30ms	237.28ms	74.50ms	175.29ms	68.81ms	53.85ms	6.35ms	23.09ms	us-west-2	271.77ms	146.70ms	132.09ms	101.07ms	123.17ms	102.08ms	225.11ms	214.60ms	166.90ms	143.96ms	178.30ms	151.78ms	171.16ms	194.40ms	63.21ms	25.81ms	146.75ms	146.44ms	156.79ms	154.40ms	152.58ms	118.73ms	131.82ms	137.86ms	187.60ms	254.46ms	268.57ms	90.85ms	176.57ms	66.15ms	50.19ms	22.36ms	5.50ms

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
        print(f"Ratio (Method1/Method2): {info['ratio']:.2f}")

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