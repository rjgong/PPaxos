"""
    Ruijie: I think to prioritize nodes, you should start replicas in order of priority and ensure that the order is not messed up.

    Finds the optimal priority assignment for a set of nodes to minimize a calculated average value.

    Problem Description:
    Given a square n*n matrix representing the distances between n nodes, the goal is to find an optimal
    assignment of unique priorities to each node. The optimality is defined by minimizing the average
    of a specific value calculated for each node. The process involves iterating through every possible
    priority permutation.

    For each node (let's call it the 'current_node'), a value is calculated based on its distances to
    other nodes, conditioned by their relative priorities. The calculation rule is as follows:

    1.  Partition all other nodes into two groups: those with a higher priority and those with a
        lower priority than the 'current_node'.

    2.  A threshold `k` is defined as half the total number of nodes (n // 2). The set of nodes used for
        the final calculation must contain `k` nodes.

    3.  First, consider all nodes with a higher priority.
        - If the number of these higher-priority nodes is less than `k`, we must select additional nodes
          from the lower-priority group to meet the count of `k`. These supplemental nodes are chosen
          sequentially, starting with the one closest to the 'current_node', until the total count of
          selected nodes reaches `k`.
        - If the number of higher-priority nodes is already `k` or more, we use only this set of
          higher-priority nodes for the calculation.

    4.  The value for the 'current_node' is the maximum distance from it to any of the nodes in the
        set determined in the previous step. If the set is empty (which can happen for the highest
        priority node when k=0), its value is 0.

    5.  After calculating this value for every node under a specific priority assignment, the average
        of these values is computed.

    6.  The function iterates through all n! possible priority assignments and returns the assignment
        that results in the minimum possible average value.
"""
import numpy as np
from itertools import permutations
import math

def calculate_node_value(dist_matrix, node_idx, priorities, n):
    """
    Calculate the value for a single node
    
    Parameters:
    - dist_matrix: n×n distance matrix
    - node_idx: current node index
    - priorities: priority array, priorities[i] represents the priority of node i (0 is highest)
    - n: total number of nodes
    """
    # Priority of the current node
    current_priority = priorities[node_idx]
    
    # Find nodes with higher priority than the current node (lower priority value)
    higher_priority_nodes = []
    lower_priority_nodes = []
    
    for i in range(n):
        if i != node_idx:
            if priorities[i] < current_priority:  # Higher priority nodes have lower priority values
                higher_priority_nodes.append((i, dist_matrix[node_idx][i]))
            else:
                lower_priority_nodes.append((i, dist_matrix[node_idx][i]))
    
    # Number of nodes to select (at least half)
    required_count = math.ceil(n / 2)
    
    # Selected nodes and their distances to the current node
    selected_distances = []
    
    # First add all nodes with higher priority
    for node_idx_high, dist in higher_priority_nodes:
        selected_distances.append(dist)
    
    # If the number of higher priority nodes is insufficient
    if len(selected_distances) < required_count:
        # Sort lower priority nodes by distance in ascending order
        lower_priority_nodes.sort(key=lambda x: x[1])
        
        # Add the required number of nodes
        needed = required_count - len(selected_distances)
        for i in range(min(needed, len(lower_priority_nodes))):
            selected_distances.append(lower_priority_nodes[i][1])
    
    # Return the maximum distance among selected nodes
    if selected_distances:
        return max(selected_distances)
    else:
        return 0

def calculate_average_value(dist_matrix, priorities):
    """
    Calculate the average value of all nodes for a given priority arrangement
    """
    n = len(dist_matrix)
    total_value = 0
    
    for i in range(n):
        node_value = calculate_node_value(dist_matrix, i, priorities, n)
        total_value += node_value
    
    return total_value / n

def find_all_optimal_priorities(dist_matrix):
    """
    Find all priority arrangements that achieve the minimum average
    
    Parameters:
    - dist_matrix: n×n distance matrix
    
    Returns:
    - optimal_arrangements: list of all optimal priority arrangements
    - min_average: minimum average value
    """
    n = len(dist_matrix)
    
    # Generate all possible priority arrangements (permutations of 0 to n-1)
    all_permutations = permutations(range(n))
    
    min_average = float('inf')
    optimal_arrangements = []
    
    # Iterate through all permutations
    for perm in all_permutations:
        priorities = list(perm)
        avg_value = calculate_average_value(dist_matrix, priorities)
        
        if avg_value < min_average:
            min_average = avg_value
            optimal_arrangements = [priorities]
        elif avg_value == min_average:
            optimal_arrangements.append(priorities)
    
    return optimal_arrangements, min_average

# Example usage
def main():
    # Example: distance matrix for 5 nodes
    dist_matrix = np.array([
        [0, 191.53,     147.42, 244.19, 214.57],
        [191.53, 0, 167.82, 116.00, 233.31],
        [147.42, 167.82, 0, 235.53, 199.76],
        [244.19, 116.00, 235.53, 0, 160.09], # 134
        [214.57, 233.31, 199.76, 160.09, 0]
    ])
    
    print("Distance matrix:")
    print(dist_matrix)
    print()
    
    # Find all optimal priority arrangements
    optimal_arrangements, min_average = find_all_optimal_priorities(dist_matrix)
    
    print(f"Minimum average value: {min_average:.4f}")
    print(f"Number of optimal arrangements: {len(optimal_arrangements)}")
    print("\nAll optimal priority arrangements:")
    print("(0 is highest priority, larger values mean lower priority)")
    
    for i, priorities in enumerate(optimal_arrangements):
        print(f"\nArrangement {i+1}: {priorities}")
        # Show priority order
        node_priority_pairs = [(j, priorities[j]) for j in range(len(priorities))]
        node_priority_pairs.sort(key=lambda x: x[1])
        priority_order = [pair[0] for pair in node_priority_pairs]
        print(f"Priority order (high to low): {' > '.join(f'Node{p}' for p in priority_order)}")
        
        # Show each node's value
        print("Node values:")
        n = len(dist_matrix)
        for j in range(n):
            node_value = calculate_node_value(dist_matrix, j, priorities, n)
            print(f"  Node {j} (priority={priorities[j]}): {node_value}")

if __name__ == "__main__":
    main() 