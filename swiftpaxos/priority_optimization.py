"""
优先级分配优化程序

给定一个5*5的距离矩阵，为每个节点分配唯一优先级，使得每个节点计算出的特定值的总和最小。

计算规则：
1. 对于每个节点，首先选取所有优先级高于自己的节点的距离
2. 如果优先级高的节点数量不足2个，从优先级低的节点中选取距离最短的节点补足2个
3. 取选取出来的距离的最大值
4. 目标是最小化所有节点这个最大值的总和
"""

import numpy as np
from itertools import permutations

def calculate_node_value(dist_matrix, node_idx, priorities):
    """
    计算单个节点的值
    
    参数:
    - dist_matrix: 5×5距离矩阵
    - node_idx: 当前节点索引
    - priorities: 优先级数组，priorities[i]表示节点i的优先级（0是最高优先级）
    
    返回:
    - 该节点的计算值（距离的最大值）
    """
    n = len(dist_matrix)
    current_priority = priorities[node_idx]
    
    # 找出优先级高于当前节点的节点（优先级值更小）
    higher_priority_nodes = []
    lower_priority_nodes = []
    
    for i in range(n):
        if i != node_idx:
            if priorities[i] < current_priority:  # 优先级值更小表示优先级更高
                higher_priority_nodes.append((i, dist_matrix[node_idx][i]))
            else:
                lower_priority_nodes.append((i, dist_matrix[node_idx][i]))
    
    # 需要选取的节点数量
    required_count = 2
    
    # 选中的距离列表
    selected_distances = []
    
    # 首先添加所有优先级更高的节点
    for node_idx_high, dist in higher_priority_nodes:
        selected_distances.append(dist)
    
    # 如果优先级高的节点数量不足2个，从优先级低的节点中补充
    if len(selected_distances) < required_count:
        # 按距离排序优先级低的节点
        lower_priority_nodes.sort(key=lambda x: x[1])
        
        # 补充需要的节点数量
        needed = required_count - len(selected_distances)
        for i in range(min(needed, len(lower_priority_nodes))):
            selected_distances.append(lower_priority_nodes[i][1])
    
    # 返回距离的最大值
    if selected_distances:
        return max(selected_distances)
    else:
        return 0

def calculate_total_value(dist_matrix, priorities):
    """
    计算给定优先级安排下所有节点的总价值
    
    参数:
    - dist_matrix: 5×5距离矩阵
    - priorities: 优先级数组
    
    返回:
    - 所有节点值的总和
    """
    n = len(dist_matrix)
    total_value = 0
    
    for i in range(n):
        node_value = calculate_node_value(dist_matrix, i, priorities)
        total_value += node_value
    
    return total_value

def find_optimal_priorities(dist_matrix):
    """
    找到最优的优先级分配
    
    参数:
    - dist_matrix: 5×5距离矩阵
    
    返回:
    - optimal_arrangements: 所有最优优先级安排
    - min_total_value: 最小总价值
    """
    n = len(dist_matrix)
    
    # 生成所有可能的优先级安排（0到n-1的排列）
    all_permutations = permutations(range(n))
    
    min_total_value = float('inf')
    optimal_arrangements = []
    
    # 遍历所有排列
    for perm in all_permutations:
        priorities = list(perm)
        total_value = calculate_total_value(dist_matrix, priorities)
        
        if total_value < min_total_value:
            min_total_value = total_value
            optimal_arrangements = [priorities]
        elif total_value == min_total_value:
            optimal_arrangements.append(priorities)
    
    return optimal_arrangements, min_total_value

def print_detailed_analysis(dist_matrix, priorities):
    """
    打印详细的优先级分析
    
    参数:
    - dist_matrix: 距离矩阵
    - priorities: 优先级数组
    """
    n = len(dist_matrix)
    print(f"\n优先级安排: {priorities}")
    print("(0是最高优先级，数值越大优先级越低)")
    
    # 显示优先级顺序
    node_priority_pairs = [(j, priorities[j]) for j in range(n)]
    node_priority_pairs.sort(key=lambda x: x[1])
    priority_order = [pair[0] for pair in node_priority_pairs]
    print(f"优先级顺序（高到低）: {' > '.join(f'节点{p}' for p in priority_order)}")
    
    # 显示每个节点的详细计算
    print("\n各节点详细计算:")
    total_value = 0
    for j in range(n):
        node_value = calculate_node_value(dist_matrix, j, priorities)
        total_value += node_value
        print(f"  节点 {j} (优先级={priorities[j]}): {node_value:.2f}")
    
    print(f"\n总价值: {total_value:.2f}")

def main():
    # 示例：5个节点的距离矩阵
    dist_matrix = np.array([
        [0, 191.53, 147.42, 244.19, 214.57],
        [191.53, 0, 167.82, 116.00, 233.31],
        [147.42, 167.82, 0, 235.53, 199.76],
        [244.19, 116.00, 235.53, 0, 160.09],
        [214.57, 233.31, 199.76, 160.09, 0]
    ])
    
    print("距离矩阵:")
    print(dist_matrix)
    print()
    
    # 找到最优的优先级安排
    optimal_arrangements, min_total_value = find_optimal_priorities(dist_matrix)
    
    print(f"最小总价值: {min_total_value:.2f}")
    print(f"最优安排数量: {len(optimal_arrangements)}")
    
    # 显示所有最优安排
    for i, priorities in enumerate(optimal_arrangements):
        print_detailed_analysis(dist_matrix, priorities)
        if i < len(optimal_arrangements) - 1:
            print("\n" + "="*50)

if __name__ == "__main__":
    main() 